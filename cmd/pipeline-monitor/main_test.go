package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestNextPageUsesLinkHeader(t *testing.T) {
	path := "/api/v4/projects/1/pipelines/10/jobs?per_page=100"
	got, ok := nextPage(path, http.Header{"Link": []string{"<https://gitlab.example.com/api/v4/projects/1/pipelines/10/jobs?page=2&per_page=100>; rel=\"next\""}})
	if !ok || got != "https://gitlab.example.com/api/v4/projects/1/pipelines/10/jobs?page=2&per_page=100" {
		t.Fatalf("nextPage() = %q, %v", got, ok)
	}
}

func TestNextPageUsesGitLabHeader(t *testing.T) {
	path := "/api/v4/projects/1/pipelines/10/jobs?per_page=100"
	got, ok := nextPage(path, http.Header{"X-Next-Page": []string{"2"}})
	if !ok || got != "/api/v4/projects/1/pipelines/10/jobs?page=2&per_page=100" {
		t.Fatalf("nextPage() = %q, %v", got, ok)
	}
}

func TestSnapshotIncludesAllPaginatedJobsAndDownstreamPipelines(t *testing.T) {
	const childCount = 101
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v4/projects/1/pipelines/10":
			writeJSON(t, w, pipeline{ID: 10, ProjectID: 1, Status: "success", Ref: "main"})
		case r.URL.Path == "/api/v4/projects/1/pipelines/10/jobs":
			page := r.URL.Query().Get("page")
			if page == "" || page == "1" {
				jobs := make([]job, 100)
				for i := range jobs {
					jobs[i] = job{ID: int64(i + 1), Status: "success"}
				}
				w.Header().Set("X-Next-Page", "2")
				writeJSON(t, w, jobs)
				return
			}
			writeJSON(t, w, []job{{ID: 101, Status: "failed"}})
		case r.URL.Path == "/api/v4/projects/1/pipelines/10/bridges":
			page := r.URL.Query().Get("page")
			start, end := 1, 100
			if page == "2" {
				start, end = 101, childCount
			} else {
				w.Header().Set("X-Next-Page", "2")
			}
			bridges := make([]bridge, 0, end-start+1)
			for i := start; i <= end; i++ {
				bridges = append(bridges, bridge{DownstreamPipe: &struct {
					ID        int64  `json:"id"`
					ProjectID int64  `json:"project_id"`
					Status    string `json:"status"`
					Ref       string `json:"ref"`
					SHA       string `json:"sha"`
					WebURL    string `json:"web_url"`
					Name      string `json:"name"`
				}{ID: int64(i), ProjectID: 2, Status: "success", Ref: "main"}})
			}
			writeJSON(t, w, bridges)
		case strings.HasPrefix(r.URL.Path, "/api/v4/projects/2/pipelines/") && !strings.HasSuffix(r.URL.Path, "/jobs") && !strings.HasSuffix(r.URL.Path, "/bridges"):
			id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			pipelineID := 0
			if _, err := fmt.Sscan(id, &pipelineID); err != nil || pipelineID < 1 || pipelineID > childCount {
				http.NotFound(w, r)
				return
			}
			status := "success"
			if pipelineID == childCount {
				status = "failed"
			}
			writeJSON(t, w, pipeline{ID: int64(pipelineID), ProjectID: 2, Status: status, Ref: "main"})
		case strings.HasPrefix(r.URL.Path, "/api/v4/projects/2/pipelines/") && strings.HasSuffix(r.URL.Path, "/jobs"):
			writeJSON(t, w, []job{})
		case strings.HasPrefix(r.URL.Path, "/api/v4/projects/2/pipelines/") && strings.HasSuffix(r.URL.Path, "/bridges"):
			writeJSON(t, w, []bridge{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	state, err := c.snapshot(context.Background(), pipeline{ID: 10, ProjectID: 1}, true, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Jobs) != 101 {
		t.Fatalf("root jobs = %d, want 101", len(state.Jobs))
	}
	if len(state.Children) != childCount {
		t.Fatalf("downstream pipelines = %d, want %d", len(state.Children), childCount)
	}
	if !hasFailed(state) {
		t.Fatal("expected late paginated failure to be detected")
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

func TestValidateOptions(t *testing.T) {
	tests := []struct {
		name    string
		opts    options
		wantErr string
	}{
		{name: "live zero", opts: options{live: true}, wantErr: "--interval must be greater than zero"},
		{name: "live negative", opts: options{live: true, interval: -time.Second}, wantErr: "--interval must be greater than zero"},
		{name: "wait zero", opts: options{wait: true}, wantErr: "--interval must be greater than zero"},
		{name: "non live zero", opts: options{}, wantErr: ""},
		{name: "positive", opts: options{live: true, interval: time.Second}, wantErr: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateOptions(test.opts)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("validateOptions() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("validateOptions() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestUsageDateRange(t *testing.T) {
	now := time.Date(2026, time.October, 27, 16, 30, 0, 0, time.UTC)
	tests := []struct {
		name      string
		since     string
		until     string
		wantSince time.Time
		wantUntil time.Time
		wantErr   bool
	}{
		{
			name:      "defaults to current month to date",
			wantSince: time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC),
			wantUntil: now,
		},
		{
			name:      "date endpoints include full days",
			since:     "2026-10-02",
			until:     "2026-10-04",
			wantSince: time.Date(2026, time.October, 2, 0, 0, 0, 0, time.UTC),
			wantUntil: time.Date(2026, time.October, 4, 23, 59, 59, 999999999, time.UTC),
		},
		{
			name:      "timestamp endpoints retain exact times",
			since:     "2026-10-02T12:00:00Z",
			until:     "2026-10-04T12:00:00Z",
			wantSince: time.Date(2026, time.October, 2, 12, 0, 0, 0, time.UTC),
			wantUntil: time.Date(2026, time.October, 4, 12, 0, 0, 0, time.UTC),
		},
		{
			name:    "reversed range rejected",
			since:   "2026-10-05",
			until:   "2026-10-04",
			wantErr: true,
		},
		{
			name:    "invalid date rejected",
			since:   "not-a-date",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Act
			gotSince, gotUntil, err := usageDateRange(test.since, test.until, now)

			// Assert
			if test.wantErr {
				if err == nil {
					t.Fatal("usageDateRange() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("usageDateRange() error = %v", err)
			}
			if !gotSince.Equal(test.wantSince) || !gotUntil.Equal(test.wantUntil) {
				t.Fatalf("usageDateRange() = (%s, %s), want (%s, %s)", gotSince, gotUntil, test.wantSince, test.wantUntil)
			}
		})
	}
}

func TestRunUsageAggregatesAcrossGroupAndAppliesFiltersAndLimit(t *testing.T) {
	var jobRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v4/groups/example-group/projects":
			if r.URL.Query().Get("include_subgroups") != "true" || r.URL.Query().Get("with_shared") != "false" {
				t.Fatalf("group project query = %s, expected subgroup projects without shared projects", r.URL.RawQuery)
			}
			if r.URL.Query().Get("simple") != "" {
				t.Fatalf("group project query = %s, want full project details for archived flag", r.URL.RawQuery)
			}
			if r.URL.Query().Get("page") == "2" {
				writeJSON(t, w, []groupProject{
					{ID: 2, PathWithNamespace: "example-group/two"},
					{ID: 3, PathWithNamespace: "example-group/archived", Archived: true},
				})
				return
			}
			w.Header().Set("X-Next-Page", "2")
			writeJSON(t, w, []groupProject{{ID: 1, PathWithNamespace: "example-group/one"}})
		case "/api/v4/groups/example-group":
			writeJSON(t, w, groupQuotaSettings{ID: 99, SharedRunnersMinutesLimit: int64Pointer(500), ExtraSharedRunnersMinutesLimit: int64Pointer(100)})
		case "/api/graphql":
			if r.Method != http.MethodPost {
				t.Fatalf("GraphQL method = %s, want POST", r.Method)
			}
			var request struct {
				Query     string            `json:"query"`
				Variables map[string]string `json:"variables"`
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read GraphQL request: %v", err)
			}
			if err := json.Unmarshal(body, &request); err != nil {
				t.Fatalf("decode GraphQL request: %v", err)
			}
			if request.Variables["namespaceId"] != "gid://gitlab/Group/99" || request.Variables["date"] != "2026-10-01" {
				t.Fatalf("GraphQL variables = %#v, want selected group and month", request.Variables)
			}
			if strings.Contains(request.Query, "ciMinutesProjectMonthlyUsage") {
				writeJSON(t, w, map[string]any{"data": map[string]any{"ciMinutesProjectMonthlyUsage": map[string]any{
					"nodes": []any{
						map[string]any{"minutes": 10, "project": map[string]any{"id": "gid://gitlab/Project/1", "name": "one"}},
						map[string]any{"minutes": 20, "project": map[string]any{"id": "gid://gitlab/Project/2", "name": "two"}},
						map[string]any{"minutes": 30, "project": map[string]any{"id": "gid://gitlab/Project/3", "name": "archived"}},
					},
					"pageInfo": map[string]any{"hasNextPage": false, "endCursor": "cursor-1"},
				}}})
				return
			}
			writeJSON(t, w, map[string]any{"data": map[string]any{"ciMinutesUsage": map[string]any{"nodes": []any{map[string]any{"minutes": 400, "month": "October", "monthIso8601": "2026-10-01"}}}}})
		case "/api/v4/projects/1/jobs", "/api/v4/projects/2/jobs":
			jobRequests.Add(1)
			if r.URL.Query().Get("order_by") != "id" || r.URL.Query().Get("sort") != "desc" {
				t.Fatalf("job query = %s, want newest jobs first", r.URL.RawQuery)
			}
			if r.URL.Query().Get("pagination") != "keyset" || r.URL.Query().Get("page") != "" {
				t.Fatalf("job query = %s, want keyset pagination", r.URL.RawQuery)
			}
			if r.URL.Query().Get("id_before") != "" {
				if r.URL.Path == "/api/v4/projects/1/jobs" {
					writeJSON(t, w, []usageJob{{ID: 9, Name: "build", Duration: floatPointer(600), CreatedAt: "2026-09-30T23:59:59Z"}})
					return
				}
				writeJSON(t, w, []usageJob{})
				return
			}
			if r.URL.Path == "/api/v4/projects/1/jobs" {
				next := *r.URL
				query := next.Query()
				query.Set("id_before", "10")
				next.RawQuery = query.Encode()
				w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", next.String()))
				writeJSON(t, w, []usageJob{
					{ID: 12, Name: "test", Duration: floatPointer(120), CreatedAt: "2026-10-04T10:00:00Z", Tags: []string{"saas-linux-medium-amd64"}, Runner: &usageRunner{RunnerType: "instance_type"}},
					{ID: 11, Name: "build", Duration: floatPointer(30), CreatedAt: "2026-10-03T10:00:00Z", Tags: []string{"saas-linux-medium-amd64"}, Runner: &usageRunner{RunnerType: "instance_type"}},
					{ID: 10, Name: "build", Duration: floatPointer(60), CreatedAt: "2026-10-02T10:00:00Z", Tags: []string{"saas-linux-small-amd64"}, Runner: &usageRunner{RunnerType: "instance_type"}},
				})
				return
			}
			next := *r.URL
			query := next.Query()
			query.Set("id_before", "20")
			next.RawQuery = query.Encode()
			w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", next.String()))
			writeJSON(t, w, []usageJob{{ID: 20, Name: "build", Duration: floatPointer(180), CreatedAt: "2026-10-05T10:00:00Z", Tags: []string{"saas-linux-large-amd64"}, Runner: &usageRunner{RunnerType: "instance_type"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	cacheDir := t.TempDir()
	databasePath := filepath.Join(t.TempDir(), "usage.sqlite")

	// Act
	var output bytes.Buffer
	var summary bytes.Buffer
	err := runUsage(context.Background(), c, usageOptions{
		group:              "example-group",
		since:              "2026-10-01",
		until:              "2026-10-31",
		limit:              1,
		sortBy:             "minutes",
		order:              "desc",
		output:             "json",
		dbPath:             databasePath,
		legacyCacheDir:     cacheDir,
		cacheTTL:           5 * time.Minute,
		historicalCacheTTL: 30 * 24 * time.Hour,
	}, &output, &summary)

	// Assert
	if err != nil {
		t.Fatalf("runUsage() error = %v", err)
	}
	var report usageReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatalf("decode usage report: %v", err)
	}
	if report.TotalRows != 3 || report.ReturnedRows != 1 || len(report.Rows) != 1 {
		t.Fatalf("usage row counts = total %d, returned %d, len %d; want 3, 1, 1", report.TotalRows, report.ReturnedRows, len(report.Rows))
	}
	if report.Rows[0].Project != "example-group/two" || report.Rows[0].Job != "build" || report.Rows[0].Runs != 1 || report.Rows[0].DurationMinutes != 3 {
		t.Fatalf("top usage row = %#v, want example-group/two build 1 run 3 minutes", report.Rows[0])
	}
	if report.Rows[0].MeanDurationMinutes != 3 || report.Rows[0].MedianDurationMinutes != 3 || report.Rows[0].P95DurationMinutes != 3 || report.Rows[0].MaxDurationMinutes != 3 {
		t.Fatalf("single-run duration stats = %#v, want all 3 minutes", report.Rows[0])
	}
	if report.JobDurationMinutes != 6.5 {
		t.Fatalf("job duration total = %.1f, want 6.5", report.JobDurationMinutes)
	}
	if !report.ProjectComputeAvailable || report.ProjectComputeMinutes == nil || *report.ProjectComputeMinutes != 30 {
		t.Fatalf("project compute usage = available %v, total %v; want 30 GitLab-reported minutes for selected projects", report.ProjectComputeAvailable, report.ProjectComputeMinutes)
	}
	if report.Rows[0].EstimatedComputeMinutes == nil || *report.Rows[0].EstimatedComputeMinutes != 9 {
		t.Fatalf("top row estimated compute = %v, want 9 minutes from the large hosted runner factor", report.Rows[0].EstimatedComputeMinutes)
	}
	if report.CIMinutesQuota == nil || !report.CIMinutesQuota.Available || report.CIMinutesQuota.Month != "2026-10-01" {
		t.Fatalf("CI minute quota = %#v, want available October usage", report.CIMinutesQuota)
	}
	if report.CIMinutesQuota.TotalMinutes == nil || *report.CIMinutesQuota.TotalMinutes != 600 || report.CIMinutesQuota.UsedMinutes == nil || *report.CIMinutesQuota.UsedMinutes != 400 || report.CIMinutesQuota.RemainingMinutes == nil || *report.CIMinutesQuota.RemainingMinutes != 200 {
		t.Fatalf("CI minute totals = %#v, want 400 used, 600 total, 200 remaining", report.CIMinutesQuota)
	}

	// Act
	output.Reset()
	err = runUsage(context.Background(), c, usageOptions{
		group:              "example-group",
		projects:           []string{"example-group/one"},
		jobs:               []string{"build"},
		since:              "2026-10-01",
		until:              "2026-10-31",
		limit:              0,
		sortBy:             "minutes",
		order:              "desc",
		output:             "json",
		dbPath:             databasePath,
		legacyCacheDir:     cacheDir,
		cacheTTL:           5 * time.Minute,
		historicalCacheTTL: 30 * 24 * time.Hour,
	}, &output, &summary)

	// Assert
	if err != nil {
		t.Fatalf("runUsage() with filters error = %v", err)
	}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatalf("decode filtered usage report: %v", err)
	}
	if report.TotalRows != 1 || report.Rows[0].Project != "example-group/one" || report.Rows[0].Job != "build" || report.Rows[0].Runs != 2 || report.Rows[0].DurationMinutes != 1.5 {
		t.Fatalf("filtered usage report = %#v, want one example-group/one build row with 2 runs and 1.5 minutes", report)
	}
	if report.Rows[0].MeanDurationMinutes != 0.75 || report.Rows[0].MedianDurationMinutes != 0.75 || report.Rows[0].P95DurationMinutes != 1 || report.Rows[0].MaxDurationMinutes != 1 {
		t.Fatalf("filtered run-duration stats = %#v, want mean/median 0.75 and p95/max 1 minute", report.Rows[0])
	}
	if report.ProjectComputeMinutes == nil || *report.ProjectComputeMinutes != 10 {
		t.Fatalf("filtered project compute minutes = %v, want 10", report.ProjectComputeMinutes)
	}
	if report.EstimatedComputeMinutes != 2 {
		t.Fatalf("filtered estimated compute minutes = %.2f, want 2.00", report.EstimatedComputeMinutes)
	}
	if report.ProjectsReadFromDatabase == 0 {
		t.Fatal("filtered usage report did not read stored project data")
	}
	if got := jobRequests.Load(); got != 4 {
		t.Fatalf("job API requests = %d, want 4 (first run only)", got)
	}
}

func TestDurationStatisticsMinutesUsesNearestRankP95(t *testing.T) {
	durations := make([]float64, 20)
	for i := range durations {
		durations[i] = float64(i+1) * 60
	}

	// Act
	mean, median, p95, maximum := durationStatisticsMinutes(durations)

	// Assert
	if mean != 10.5 || median != 10.5 || p95 != 19 || maximum != 20 {
		t.Fatalf("duration statistics = mean %.2f, median %.2f, p95 %.2f, max %.2f; want 10.5, 10.5, 19, 20", mean, median, p95, maximum)
	}
}

func TestSelectUsageProjectsExcludesArchivedByDefault(t *testing.T) {
	projects := []groupProject{
		{ID: 1, PathWithNamespace: "example-group/active"},
		{ID: 2, PathWithNamespace: "example-group/archived", Archived: true},
	}

	// Act
	selected, err := selectUsageProjects(projects, nil, false)

	// Assert
	if err != nil {
		t.Fatalf("selectUsageProjects() error = %v", err)
	}
	if len(selected) != 1 || selected[0].PathWithNamespace != "example-group/active" {
		t.Fatalf("default projects = %#v, want only the active project", selected)
	}

	// Act
	selected, err = selectUsageProjects(projects, nil, true)

	// Assert
	if err != nil || len(selected) != 2 {
		t.Fatalf("selectUsageProjects(includeArchived) = %#v, %v; want both projects", selected, err)
	}
	if _, err := selectUsageProjects(projects, []string{"example-group/archived"}, false); err == nil || !strings.Contains(err.Error(), "--include-archived") {
		t.Fatalf("archived project filter error = %v, want include-archived guidance", err)
	}
}

func TestUsagePageCacheTTLUsesLongerLifetimeForHistoricalPages(t *testing.T) {
	now := time.Date(2026, time.October, 27, 12, 0, 0, 0, time.UTC)
	opts := usageOptions{cacheTTL: 5 * time.Minute, historicalCacheTTL: 30 * 24 * time.Hour}
	tests := []struct {
		name string
		jobs []usageJob
		want time.Duration
	}{
		{name: "recent", jobs: []usageJob{{CreatedAt: "2026-10-27T11:00:00Z"}}, want: opts.cacheTTL},
		{name: "historical", jobs: []usageJob{{CreatedAt: "2026-10-25T11:00:00Z"}}, want: opts.historicalCacheTTL},
		{name: "empty page", want: opts.historicalCacheTTL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Act
			got := usagePageCacheTTL(&usagePageCache{Jobs: test.jobs}, opts, now)

			// Assert
			if got != test.want {
				t.Fatalf("usagePageCacheTTL() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestRequestPageRetriesRateLimitUsingRetryAfter(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			writeJSON(t, w, map[string]string{"message": "rate limited"})
			return
		}
		writeJSON(t, w, []job{{ID: 1, Name: "build"}})
	}))
	defer server.Close()
	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}

	// Act
	var jobs []job
	err := c.get(context.Background(), "/api/v4/projects/1/jobs", &jobs)

	// Assert
	if err != nil {
		t.Fatalf("get() error = %v", err)
	}
	if requests.Load() != 2 || len(jobs) != 1 || jobs[0].Name != "build" {
		t.Fatalf("retries = %d, jobs = %#v; want 2 requests and one build job", requests.Load(), jobs)
	}
}

func TestUsageJobPageFallsBackToStaleCacheAfterRateLimit(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		writeJSON(t, w, map[string]string{"message": "rate limited"})
	}))
	defer server.Close()
	cacheDir := t.TempDir()
	requestPath := "/api/v4/projects/9/jobs?pagination=keyset&per_page=100&order_by=id&sort=desc"
	cachePath, err := usagePageCachePath(cacheDir, server.URL, 9, requestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeUsagePageCache(cachePath, usagePageCache{
		Version:  usagePageCacheVersion,
		CachedAt: time.Now().Add(-31 * 24 * time.Hour),
		Jobs:     []usageJob{{ID: 1, Name: "build", Duration: floatPointer(60), CreatedAt: "2026-09-01T00:00:00Z"}},
	}); err != nil {
		t.Fatal(err)
	}
	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}

	// Act
	jobs, _, cacheHit, stale, notice, err := c.usageJobPage(context.Background(), 9, requestPath, usageOptions{
		cacheTTL:           time.Minute,
		historicalCacheTTL: 30 * 24 * time.Hour,
	}, cacheDir, true)

	// Assert
	if err != nil {
		t.Fatalf("usageJobPage() error = %v", err)
	}
	if !cacheHit || !stale || len(jobs) != 1 || jobs[0].Name != "build" {
		t.Fatalf("usageJobPage() = jobs %#v, hit %v, stale %v; want stale cached build page", jobs, cacheHit, stale)
	}
	if notice == "" {
		t.Fatal("usageJobPage() returned no stale-cache notice")
	}
	if requests.Load() != maxAPIRequestRetries+1 {
		t.Fatalf("rate-limit requests = %d, want %d attempts", requests.Load(), maxAPIRequestRetries+1)
	}
}

func TestUsageCommandIsRegistered(t *testing.T) {
	cmd := newCommand()
	usage, _, err := cmd.Find([]string{"usage"})
	if err != nil {
		t.Fatalf("Find(usage) error = %v", err)
	}
	if usage.Use != "usage" {
		t.Fatalf("usage command = %q, want usage", usage.Use)
	}
}

func floatPointer(value float64) *float64 {
	return &value
}

func int64Pointer(value int64) *int64 {
	return &value
}

func TestCommandValidationRejectsPollingIntervalBeforeAPISetup(t *testing.T) {
	cmd := newCommand()
	cmd.SetArgs([]string{"--project", "group/project", "--live", "--interval", "0s"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--interval must be greater than zero") {
		t.Fatalf("command error = %v, want interval validation error", err)
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

func TestRequestWrapsTransportError(t *testing.T) {
	client := &client{
		baseURL: "http://127.0.0.1:1",
		token:   "test-token",
		http:    &http.Client{Timeout: time.Second},
	}

	var output pipeline
	err := client.get(context.Background(), "/api/v4/projects/1/pipelines/10", &output)
	if err == nil || !strings.Contains(err.Error(), "request /api/v4/projects/1/pipelines/10:") {
		t.Fatalf("client.get() error = %v, want request context", err)
	}
}

func TestAPIRequestTimeoutIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, http: &http.Client{Timeout: 10 * time.Millisecond}}
	var output pipeline
	start := time.Now()
	err := c.get(context.Background(), "/slow", &output)
	if err == nil {
		t.Fatal("client.get() succeeded for a timed-out request")
	}
	if elapsed := time.Since(start); elapsed > 90*time.Millisecond {
		t.Fatalf("request took %s, want timeout near 10ms", elapsed)
	}
}

func TestAPIErrorIncludesBoundedJSONMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		writeJSON(t, w, map[string]string{"message": "pipeline is invalid"})
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	_, err := c.retryJob(context.Background(), 1, 101)
	if err == nil || !strings.Contains(err.Error(), "pipeline is invalid") || !strings.Contains(err.Error(), "422 Unprocessable Entity") {
		t.Fatalf("retryJob() error = %v, want status and JSON message", err)
	}
}

func TestAPIErrorIncludesBoundedPlainTextWithoutSecretsOrHugeBody(t *testing.T) {
	secret := "test-token"
	body := strings.Repeat("x", maxAPIErrorMessage+100) + " PRIVATE-TOKEN=" + secret
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: secret, http: server.Client()}
	_, err := c.retryJob(context.Background(), 1, 101)
	if err == nil {
		t.Fatal("retryJob() succeeded for error response")
	}
	message := err.Error()
	if strings.Contains(message, secret) || len(message) > len("GitLab API /api/v4/projects/1/jobs/101/retry returned 502 Bad Gateway: ")+maxAPIErrorMessage+3 {
		t.Fatalf("API error leaked or exceeded bound: %q", message)
	}
}

func TestRefreshModelWithDeterministicAPIStopsPermanentError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		writeJSON(t, w, map[string]string{"message": "pipeline not found"})
	}))
	defer server.Close()

	c := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 10, ProjectID: 1, Status: "running"}})
	m.client, m.root, m.interval = c, pipeline{ID: 10, ProjectID: 1}, time.Second
	msg := refreshSnapshot(context.Background(), c, m.root, false)()
	updated, cmd := m.Update(msg)
	got := updated.(monitorModel)
	if cmd != nil || got.refreshFailures != 1 || !strings.Contains(got.err.Error(), "pipeline not found") {
		t.Fatalf("model = %#v, command = %v", got, cmd)
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
