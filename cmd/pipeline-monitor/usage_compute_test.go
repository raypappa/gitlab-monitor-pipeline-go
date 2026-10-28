package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHostedRunnerCostFactor(t *testing.T) {
	tests := []struct {
		name string
		job  usageJob
		host string
		want float64
		ok   bool
	}{
		{
			name: "hosted medium runner",
			job: usageJob{
				Tags:   []string{"docker", "saas-linux-medium-amd64"},
				Runner: &usageRunner{RunnerType: "instance_type"},
			},
			host: "https://gitlab.com",
			want: 2,
			ok:   true,
		},
		{
			name: "GPU runner",
			job: usageJob{
				Tags:   []string{"saas-linux-medium-amd64-gpu-standard"},
				Runner: &usageRunner{RunnerType: "instance_type"},
			},
			host: "https://gitlab.com",
			want: 7,
			ok:   true,
		},
		{
			name: "custom runner is unknown",
			job: usageJob{
				Tags:   []string{"linux-medium"},
				Runner: &usageRunner{RunnerType: "instance_type"},
			},
			host: "https://gitlab.com",
			ok:   false,
		},
		{
			name: "hosted tag without instance runner metadata",
			job:  usageJob{Tags: []string{"saas-linux-large-amd64"}},
			host: "https://gitlab.com",
			ok:   false,
		},
		{
			name: "conflicting hosted tags",
			job: usageJob{
				Tags:   []string{"saas-linux-small-amd64", "saas-linux-large-amd64"},
				Runner: &usageRunner{RunnerType: "instance_type"},
			},
			host: "https://gitlab.com",
			ok:   false,
		},
		{
			name: "untagged GitLab.com instance job uses default small runner",
			job:  usageJob{Runner: &usageRunner{RunnerType: "instance_type"}},
			host: "https://gitlab.com",
			want: 1,
			ok:   true,
		},
		{
			name: "untagged self-managed instance job stays unknown",
			job:  usageJob{Runner: &usageRunner{RunnerType: "instance_type"}},
			host: "https://gitlab.example.com",
			ok:   false,
		},
		{
			name: "untagged GitLab.com job with missing runner stays unknown",
			job:  usageJob{},
			host: "https://gitlab.com",
			ok:   false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := hostedRunnerCostFactor(test.job, test.host)
			if ok != test.ok || (ok && got != test.want) {
				t.Fatalf("hostedRunnerCostFactor() = %v, %v; want %v, %v", got, ok, test.want, test.ok)
			}
		})
	}
}

func TestEstimateJobComputeMinutes(t *testing.T) {
	if got := estimateJobComputeMinutes(14*60+48, 2); got != 29.6 {
		t.Fatalf("estimateJobComputeMinutes(888, 2) = %.2f, want 29.60", got)
	}
}

func TestProjectComputeNamespaceIDsIncludesSubgroupsWithoutDuplicates(t *testing.T) {
	projects := []groupProject{
		{ID: 1, Namespace: projectNamespace{ID: 20}},
		{ID: 2, Namespace: projectNamespace{ID: 30}},
		{ID: 3, Namespace: projectNamespace{ID: 20}},
	}

	got := projectComputeNamespaceIDs(20, projects)
	want := []int64{20, 30}
	if len(got) != len(want) {
		t.Fatalf("projectComputeNamespaceIDs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("projectComputeNamespaceIDs() = %v, want %v", got, want)
		}
	}
}

func TestProjectComputeMonthsRequiresExactMonthlyCoverage(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name            string
		since           time.Time
		until           time.Time
		untilWasDefault bool
		wantMonths      int
		wantAvailable   bool
	}{
		{
			name:            "default month to date",
			since:           time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
			until:           now,
			untilWasDefault: true,
			wantMonths:      1,
			wantAvailable:   true,
		},
		{
			name:          "complete two-month window",
			since:         time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			until:         time.Date(2026, 10, 31, 23, 59, 59, int(time.Second-time.Nanosecond), time.UTC),
			wantMonths:    2,
			wantAvailable: true,
		},
		{
			name:          "partial starting month",
			since:         time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
			until:         now,
			wantAvailable: false,
		},
		{
			name:          "partial past month end",
			since:         time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
			until:         time.Date(2026, 10, 15, 23, 59, 59, int(time.Second-time.Nanosecond), time.UTC),
			wantAvailable: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			months, available := projectComputeMonths(test.since, test.until, test.untilWasDefault, now)
			if available != test.wantAvailable || len(months) != test.wantMonths {
				t.Fatalf("projectComputeMonths() = %v, %v; want %d months, available %v", months, available, test.wantMonths, test.wantAvailable)
			}
		})
	}
}

func TestGroupProjectComputeUsagePaginatesProjectResults(t *testing.T) {
	// Arrange
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var request struct {
			Query     string            `json:"query"`
			Variables map[string]string `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode GraphQL request: %v", err)
			return
		}
		if !strings.Contains(request.Query, "ciMinutesProjectMonthlyUsage") {
			t.Errorf("query = %q, want project monthly usage field", request.Query)
		}
		if request.Variables["namespaceId"] != "gid://gitlab/Group/99" || request.Variables["date"] != "2026-10-01" {
			t.Errorf("query variables = %#v, want selected group and month", request.Variables)
		}
		if request.Variables["after"] == "cursor-1" {
			writeJSON(t, w, map[string]any{"data": map[string]any{"ciMinutesProjectMonthlyUsage": map[string]any{
				"nodes":    []any{map[string]any{"minutes": 7, "project": map[string]any{"id": "gid://gitlab/Project/2", "name": "two"}}},
				"pageInfo": map[string]any{"hasNextPage": false, "endCursor": "cursor-2"},
			}}})
			return
		}
		writeJSON(t, w, map[string]any{"data": map[string]any{"ciMinutesProjectMonthlyUsage": map[string]any{
			"nodes":    []any{map[string]any{"minutes": 5, "project": map[string]any{"id": "gid://gitlab/Project/1", "name": "one"}}},
			"pageInfo": map[string]any{"hasNextPage": true, "endCursor": "cursor-1"},
		}}})
	}))
	defer server.Close()
	client := &client{baseURL: server.URL, token: "test-token", http: server.Client()}
	months := []time.Time{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}

	// Act
	minutesByProject, err := client.groupProjectComputeUsage(context.Background(), []int64{99}, months)

	// Assert
	if err != nil {
		t.Fatalf("groupProjectComputeUsage() error = %v", err)
	}
	if requests != 2 || minutesByProject[1] != 5 || minutesByProject[2] != 7 {
		t.Fatalf("compute usage = %#v after %d requests; want projects 1 and 2 at 5 and 7 minutes", minutesByProject, requests)
	}
}

func TestUsageTableLabelsPartialComputeEstimateCoverage(t *testing.T) {
	// Arrange
	knownRunEstimate := 2682.85
	report := usageReport{
		Group: "example-group",
		Rows: []usageRow{{
			Project:                 "example-group/uno",
			Job:                     "gitlab-advanced-sast",
			Runs:                    229,
			DurationMinutes:         3029.1,
			EstimatedComputeMinutes: &knownRunEstimate,
			EstimatedComputeRuns:    97,
			UnestimatedComputeRuns:  132,
		}},
		TotalRows:    1,
		ReturnedRows: 1,
	}
	var output bytes.Buffer

	// Act
	if err := writeUsageReport(&output, nil, "table", report); err != nil {
		t.Fatalf("writeUsageReport() error = %v", err)
	}

	// Assert
	text := output.String()
	for _, want := range []string{"KNOWN-RUN EST. COMPUTE (MIN)", "EST. RUNS", "UNEST. RUNS", "97", "132"} {
		if !strings.Contains(text, want) {
			t.Errorf("table output is missing %q; got:\n%s", want, text)
		}
	}
}
