package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"charm.land/bubbles/v2/list"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

func TestSnapshotIncludesDownstreamPipeline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v4/projects/1/pipelines/10":
			if r.URL.Query().Get("unused") != "" {
				t.Fatal("unexpected query parameter")
			}
			writeJSON(t, w, pipeline{ID: 10, ProjectID: 1, Status: "running", Ref: "main"})
		case "/api/v4/projects/1/pipelines/10/jobs":
			writeJSON(t, w, []job{{ID: 101, Name: "build", Stage: "build", Status: "running"}})
		case "/api/v4/projects/1/pipelines/10/bridges":
			writeJSON(t, w, []bridge{{DownstreamPipe: &struct {
				ID        int64  `json:"id"`
				ProjectID int64  `json:"project_id"`
				Status    string `json:"status"`
				Ref       string `json:"ref"`
				SHA       string `json:"sha"`
				WebURL    string `json:"web_url"`
				Name      string `json:"name"`
			}{ID: 20, ProjectID: 2, Status: "success", Ref: "main", Name: "child"}}})
		case "/api/v4/projects/2/pipelines/20":
			writeJSON(t, w, pipeline{ID: 20, ProjectID: 2, Status: "success", Ref: "main", Name: "child"})
		case "/api/v4/projects/2/pipelines/20/jobs":
			writeJSON(t, w, []job{{ID: 201, Name: "deploy", Stage: "deploy", Status: "success"}})
		case "/api/v4/projects/2/pipelines/20/bridges":
			writeJSON(t, w, []bridge{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	root := pipeline{ID: 10, ProjectID: 1, Status: "running", Ref: "main"}
	state, err := c.snapshot(context.Background(), root, true, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Children) != 1 || state.Children[0].Pipeline.ID != 20 {
		t.Fatalf("expected one downstream pipeline, got %#v", state.Children)
	}
	if !hasPollable(state) {
		t.Fatal("expected root pipeline to be pollable")
	}
}

func TestShouldOfferJobLogs(t *testing.T) {
	tests := map[string]struct {
		opts options
		want bool
	}{
		"text output": {opts: options{output: "text"}, want: true},
		"wait":        {opts: options{output: "text", wait: true}, want: false},
		"compact":     {opts: options{output: "text", compact: true}, want: false},
		"json":        {opts: options{output: "json"}, want: false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := shouldOfferJobLogs(test.opts); got != test.want {
				t.Fatalf("shouldOfferJobLogs() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSnapshotRefreshesRootPipeline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v4/projects/1/pipelines/10":
			writeJSON(t, w, pipeline{ID: 10, ProjectID: 1, Status: "success", Ref: "main"})
		case "/api/v4/projects/1/pipelines/10/jobs", "/api/v4/projects/1/pipelines/10/bridges":
			writeJSON(t, w, []any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	state, err := c.snapshot(context.Background(), pipeline{ID: 10, ProjectID: 1, Status: "running"}, true, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if state.Pipeline.Status != "success" {
		t.Fatalf("expected refreshed root status success, got %q", state.Pipeline.Status)
	}
}

func TestJobTrace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v4/projects/1/jobs/101/trace" {
			t.Fatalf("unexpected trace path: %s", r.URL.Path)
		}
		if got := r.Header.Get("PRIVATE-TOKEN"); got != "test-token" {
			t.Fatalf("PRIVATE-TOKEN = %q, want %q", got, "test-token")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("job output\n"))
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	got, err := c.jobTrace(context.Background(), 1, 101)
	if err != nil {
		t.Fatal(err)
	}
	if got != "job output\n" {
		t.Fatalf("jobTrace() = %q, want %q", got, "job output\\n")
	}
}

func TestSaveJobTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job-trace.log")
	if err := saveJobTrace(path, "job output\n"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "job output\n" {
		t.Fatalf("saved trace = %q, want %q", got, "job output\\n")
	}
}

func TestSaveJobTraceRejectsEmptyPath(t *testing.T) {
	if err := saveJobTrace("", "job output\n"); err == nil {
		t.Fatal("saveJobTrace() succeeded with an empty path")
	}
}

func TestRetryJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v4/projects/2/jobs/201/retry" {
			t.Fatalf("unexpected retry path: %s", r.URL.Path)
		}
		if got := r.Header.Get("PRIVATE-TOKEN"); got != "test-token" {
			t.Fatalf("PRIVATE-TOKEN = %q, want %q", got, "test-token")
		}
		writeJSON(t, w, job{ID: 202, Name: "deploy", Status: "pending", Ref: "main", WebURL: "https://gitlab.example.com/jobs/202"})
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	got, err := c.retryJob(context.Background(), 2, 201)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 202 || got.Status != "pending" || got.Ref != "main" {
		t.Fatalf("retryJob() = %#v, want replacement job", got)
	}
}

func TestRetryJobReturnsAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	_, err := c.retryJob(context.Background(), 1, 101)
	if err == nil || !strings.Contains(err.Error(), "403 Forbidden") {
		t.Fatalf("retryJob() error = %v, want 403 Forbidden", err)
	}
}

func TestHasFailedIgnoresAllowedFailure(t *testing.T) {
	state := &monitoredPipeline{
		Pipeline: pipeline{Status: "success"},
		Jobs:     []job{{Status: "failed", AllowFailure: true}},
		Children: []*monitoredPipeline{{Pipeline: pipeline{Status: "failed"}}},
	}
	if !hasFailed(state) {
		t.Fatal("expected failed child pipeline to fail the snapshot")
	}
}

func TestProjectFromRemote(t *testing.T) {
	tests := map[string]string{
		"git@gitlab.com:group/project.git":               "group/project",
		"https://gitlab.com/group/project.git":           "group/project",
		"ssh://git@gitlab.example.com/group/project.git": "group/project",
	}
	for remote, want := range tests {
		if got := projectFromRemote(remote); got != want {
			t.Errorf("projectFromRemote(%q) = %q, want %q", remote, got, want)
		}
	}
}

func TestColorStatus(t *testing.T) {
	status := colorStatus("success")
	if !strings.Contains(status, "success") {
		t.Fatalf("colorStatus() = %q, want status text", status)
	}
}

func TestMenuItemProvidesDefaultDelegateText(t *testing.T) {
	item := menuItem{title: "View logs"}
	if item.Title() != "View logs" {
		t.Fatalf("Title() = %q, want %q", item.Title(), "View logs")
	}
	if item.Description() != "" {
		t.Fatalf("Description() = %q, want empty description", item.Description())
	}
	if item.FilterValue() != item.Title() {
		t.Fatalf("FilterValue() = %q, want %q", item.FilterValue(), item.Title())
	}
}

func TestTraceableJobsIncludesDownstreamPipelines(t *testing.T) {
	jobs := traceableJobs(&monitoredPipeline{
		Pipeline: pipeline{ProjectID: 1},
		Jobs:     []job{{ID: 101, Name: "root-job"}},
		Children: []*monitoredPipeline{{
			Pipeline: pipeline{ProjectID: 2},
			Jobs:     []job{{ID: 201, Name: "child-job"}},
		}},
	}, "")
	if len(jobs) != 2 {
		t.Fatalf("traceableJobs() returned %d jobs, want 2", len(jobs))
	}
	if jobs[1].projectID != 2 || jobs[1].job.ID != 201 {
		t.Fatalf("downstream job = %#v, want project 2 job 201", jobs[1])
	}
	if jobs[1].path != "[downstream] " {
		t.Fatalf("downstream job path = %q, want %q", jobs[1].path, "[downstream] ")
	}
}

func TestTraceableJobsPreservesDownstreamProjectForRetry(t *testing.T) {
	jobs := traceableJobs(&monitoredPipeline{
		Pipeline: pipeline{ProjectID: 1},
		Children: []*monitoredPipeline{{
			Pipeline: pipeline{ProjectID: 42},
			Jobs:     []job{{ID: 4201, Name: "deploy", Status: "failed"}},
		}},
	}, "")
	if len(jobs) != 1 {
		t.Fatalf("traceableJobs() returned %d jobs, want 1", len(jobs))
	}
	if jobs[0].projectID != 42 || jobs[0].job.ID != 4201 {
		t.Fatalf("downstream retry target = %#v, want project 42 job 4201", jobs[0])
	}
}

func TestMenuDelegateRendersOneLinePerItem(t *testing.T) {
	var output bytes.Buffer
	model := list.New([]list.Item{menuItem{title: "View logs"}}, menuDelegate{}, 80, 3)
	menuDelegate{}.Render(&output, model, 0, menuItem{title: "View logs"})
	if output.String() != "> View logs" {
		t.Fatalf("menu delegate output = %q, want %q", output.String(), "> View logs")
	}
}

func TestMenuResizesToTerminal(t *testing.T) {
	items := make([]list.Item, 20)
	for i := range items {
		items[i] = menuItem{title: fmt.Sprintf("item-%d", i)}
	}
	model := list.New(items, menuDelegate{}, 120, 4)
	model.SetShowFilter(false)
	model.SetShowStatusBar(false)
	model.SetShowPagination(false)
	model.SetShowHelp(false)
	model.SetShowTitle(true)
	menu := menuModel{list: model}

	updated, _ := menu.Update(tea.WindowSizeMsg{Width: 40, Height: 6})
	resized := updated.(menuModel)
	if resized.list.Width() != 40 {
		t.Fatalf("menu width = %d, want 40", resized.list.Width())
	}
	if resized.list.Height() != 6 {
		t.Fatalf("menu height = %d, want 6", resized.list.Height())
	}
	if got := lipgloss.Height(resized.list.View()); got > 6 {
		t.Fatalf("menu rendered %d lines in a 6-line terminal viewport", got)
	}
}

func TestFormatElapsed(t *testing.T) {
	tests := map[time.Duration]string{
		5 * time.Second:    "05s",
		125 * time.Second:  "02m 05s",
		3725 * time.Second: "01h 02m 05s",
	}
	for duration, want := range tests {
		if got := formatElapsed(duration); got != want {
			t.Errorf("formatElapsed(%s) = %q, want %q", duration, got, want)
		}
	}
}

func TestFormatDurationUsesFinishedAt(t *testing.T) {
	got := formatDuration(job{
		StartedAt:  "2026-09-11T10:00:00Z",
		FinishedAt: "2026-09-11T10:02:05Z",
	})
	if got != "02m 05s" {
		t.Fatalf("formatDuration() = %q, want %q", got, "02m 05s")
	}
}

func TestFormatPipelineDurationUsesFinishedAt(t *testing.T) {
	got := formatPipelineDuration(pipeline{
		StartedAt:  "2026-09-11T10:00:00Z",
		FinishedAt: "2026-09-11T10:02:05Z",
	})
	if got != "02m 05s" {
		t.Fatalf("formatPipelineDuration() = %q, want %q", got, "02m 05s")
	}
}

func TestFormatJobLineKeepsPendingColumnsAligned(t *testing.T) {
	finishedAt := "2026-09-11T10:01:00Z"
	created := formatJobLine("", job{Status: "created", Stage: "plan", Name: "first", StartedAt: "2026-09-11T10:00:00Z", FinishedAt: finishedAt})
	pending := formatJobLine("", job{Status: "waiting_for_resource", Stage: "plan", Name: "second", StartedAt: "2026-09-11T10:00:00Z", FinishedAt: finishedAt})
	if strings.Contains(created, "\t") || strings.Contains(pending, "\t") {
		t.Fatal("job lines should use fixed-width columns instead of tabs")
	}
	for _, column := range []string{"01m 00s", "plan"} {
		if strings.Index(created, column) != strings.Index(pending, column) {
			t.Fatalf("column %q is not aligned:\ncreated: %q\npending: %q", column, created, pending)
		}
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
