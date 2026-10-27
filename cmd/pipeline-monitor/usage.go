package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

const (
	usagePageSize                  = 100
	usageProjectConcurrency        = 2
	defaultUsageRowLimit           = 25
	defaultUsageCacheTTL           = 5 * time.Minute
	defaultUsageHistoricalCacheTTL = 30 * 24 * time.Hour
	usageRecentRefreshWindow       = 24 * time.Hour
)

type usageOptions struct {
	group              string
	projects           []string
	jobs               []string
	since              string
	until              string
	limit              int
	sortBy             string
	order              string
	output             string
	cacheDir           string
	cacheTTL           time.Duration
	historicalCacheTTL time.Duration
	noCache            bool
	refresh            bool
	includeArchived    bool
}

type groupProject struct {
	ID                int64  `json:"id"`
	PathWithNamespace string `json:"path_with_namespace"`
	Archived          bool   `json:"archived"`
}

type usageJob struct {
	ID        int64    `json:"id"`
	Name      string   `json:"name"`
	Duration  *float64 `json:"duration"`
	CreatedAt string   `json:"created_at"`
	Status    string   `json:"status"`
}

type usageKey struct {
	project string
	job     string
}

type usageTotal struct {
	runs       int
	duration   float64
	durations  []float64
	lastRun    time.Time
	hasLastRun bool
}

type usageRow struct {
	Project       string    `json:"project"`
	Job           string    `json:"job"`
	Runs          int       `json:"runs"`
	Minutes       float64   `json:"minutes"`
	MeanMinutes   float64   `json:"mean_minutes"`
	MedianMinutes float64   `json:"median_minutes"`
	P95Minutes    float64   `json:"p95_minutes"`
	MaxMinutes    float64   `json:"max_minutes"`
	LastRun       time.Time `json:"last_run"`
}

type usageReport struct {
	Group              string          `json:"group"`
	Since              time.Time       `json:"since"`
	Until              time.Time       `json:"until"`
	JobDurationMinutes float64         `json:"job_duration_minutes"`
	CIMinutesQuota     *ciMinutesQuota `json:"ci_minutes_quota,omitempty"`
	CachePagesRead     int             `json:"cache_pages_read"`
	CachePagesStale    int             `json:"cache_pages_stale"`
	CacheProjectsRead  int             `json:"cache_projects_read"`
	CacheProjectsStale int             `json:"cache_projects_stale"`
	CacheNotice        string          `json:"cache_notice,omitempty"`
	TotalRows          int             `json:"total_rows"`
	ReturnedRows       int             `json:"returned_rows"`
	Rows               []usageRow      `json:"rows"`
}

type ciMinutesQuota struct {
	Available        bool   `json:"available"`
	Month            string `json:"month,omitempty"`
	UsedMinutes      *int64 `json:"used_minutes,omitempty"`
	TotalMinutes     *int64 `json:"total_minutes,omitempty"`
	RemainingMinutes *int64 `json:"remaining_minutes,omitempty"`
	Reason           string `json:"reason,omitempty"`
}

type groupQuotaSettings struct {
	ID                             int64  `json:"id"`
	SharedRunnersMinutesLimit      *int64 `json:"shared_runners_minutes_limit"`
	ExtraSharedRunnersMinutesLimit *int64 `json:"extra_shared_runners_minutes_limit"`
}

type ciMinutesUsageResponse struct {
	Data struct {
		CIMinutesUsage struct {
			Nodes []struct {
				Minutes      *int64 `json:"minutes"`
				Month        string `json:"month"`
				MonthISO8601 string `json:"monthIso8601"`
			} `json:"nodes"`
		} `json:"ciMinutesUsage"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type projectUsageResult struct {
	project           groupProject
	jobs              []usageJob
	cachePagesRead    int
	cachePagesStale   int
	cacheProjectRead  bool
	cacheProjectStale bool
	cacheNotice       string
	err               error
}

func newUsageCommand(baseURL *string) *cobra.Command {
	opts := usageOptions{
		limit:              defaultUsageRowLimit,
		sortBy:             "minutes",
		order:              "desc",
		output:             "table",
		cacheTTL:           defaultUsageCacheTTL,
		historicalCacheTTL: defaultUsageHistoricalCacheTTL,
	}
	cmd := &cobra.Command{
		Use:   "usage",
		Short: "Summarize GitLab CI job duration usage",
		Long:  "Aggregate GitLab job durations by project and job across a group or selected projects.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := newAPIClient(*baseURL)
			if err != nil {
				return err
			}
			return runUsage(cmd.Context(), c, opts, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.group, "group", "", "GitLab group path or numeric ID; includes subgroups")
	flags.StringArrayVar(&opts.projects, "project", nil, "Limit to this exact project path or numeric ID; repeatable")
	flags.StringArrayVar(&opts.jobs, "job", nil, "Limit to this exact job name; repeatable")
	flags.StringVar(&opts.since, "since", "", "Include jobs created on or after this UTC date or RFC3339 timestamp; defaults to the first of this month")
	flags.StringVar(&opts.until, "until", "", "Include jobs created on or before this UTC date or RFC3339 timestamp; defaults to now")
	flags.IntVar(&opts.limit, "limit", opts.limit, "Maximum rows to return; use 0 for no limit")
	flags.StringVar(&opts.sortBy, "sort", opts.sortBy, "Sort rows by minutes, runs, project, job, or latest")
	flags.StringVar(&opts.order, "order", opts.order, "Sort order: desc or asc")
	flags.StringVar(&opts.output, "output", opts.output, "Output format: table, json, or csv")
	flags.BoolVar(&opts.includeArchived, "include-archived", false, "Include archived projects")
	flags.BoolVar(&opts.noCache, "no-cache", false, "Do not read or write the local job-page cache")
	flags.BoolVar(&opts.refresh, "refresh", false, "Fetch job pages from GitLab instead of reading cached pages")
	flags.DurationVar(&opts.cacheTTL, "cache-ttl", opts.cacheTTL, "Maximum age of cached job pages; use 0s to disable caching")
	flags.DurationVar(&opts.historicalCacheTTL, "historical-cache-ttl", opts.historicalCacheTTL, "Maximum age of cached job data older than one day")
	flags.StringVar(&opts.cacheDir, "cache-dir", "", "Cache directory; defaults to the user cache directory")
	flags.StringVar(baseURL, "host", *baseURL, "GitLab host URL")
	return cmd
}

func runUsage(ctx context.Context, c *client, opts usageOptions, output, summaryOutput io.Writer) error {
	if err := validateUsageOptions(opts); err != nil {
		return err
	}
	since, until, err := usageDateRange(opts.since, opts.until, time.Now().UTC())
	if err != nil {
		return err
	}
	projects, err := c.groupProjects(ctx, opts.group)
	if err != nil {
		return err
	}
	projects, err = selectUsageProjects(projects, opts.projects, opts.includeArchived)
	if err != nil {
		return err
	}
	cacheEnabled := !opts.noCache && opts.cacheTTL > 0
	cacheDir := ""
	cacheNotice := ""
	if cacheEnabled {
		cacheDir, err = usageCacheDirectory(opts.cacheDir)
		if err != nil {
			cacheEnabled = false
			cacheNotice = "cache disabled: " + err.Error()
		}
	}
	var quota *ciMinutesQuota
	if since.Year() != until.Year() || since.Month() != until.Month() {
		quota = &ciMinutesQuota{Reason: "the selected date range spans multiple calendar months"}
	} else {
		quota, err = c.groupCIMinutesQuota(ctx, opts.group, since)
		if err != nil {
			quota = &ciMinutesQuota{Reason: err.Error()}
		}
	}

	jobNames := make(map[string]bool, len(opts.jobs))
	for _, name := range opts.jobs {
		jobNames[name] = true
	}
	results := c.groupJobUsage(ctx, projects, since, until, opts, cacheDir, cacheEnabled)
	totals := make(map[usageKey]*usageTotal)
	var fetchErrors []error
	cachePagesRead := 0
	cachePagesStale := 0
	cacheProjectsRead := 0
	cacheProjectsStale := 0
	for result := range results {
		cachePagesRead += result.cachePagesRead
		cachePagesStale += result.cachePagesStale
		if result.cacheProjectRead {
			cacheProjectsRead++
		}
		if result.cacheProjectStale {
			cacheProjectsStale++
		}
		if result.cacheNotice != "" {
			cacheNotice = joinUsageNotices(cacheNotice, result.cacheNotice)
		}
		if result.err != nil {
			fetchErrors = append(fetchErrors, fmt.Errorf("%s: %w", result.project.PathWithNamespace, result.err))
			continue
		}
		for _, j := range result.jobs {
			if j.Duration == nil || (len(jobNames) > 0 && !jobNames[j.Name]) {
				continue
			}
			createdAt, parseErr := time.Parse(time.RFC3339Nano, j.CreatedAt)
			if parseErr != nil || createdAt.Before(since) || createdAt.After(until) {
				continue
			}
			key := usageKey{project: result.project.PathWithNamespace, job: j.Name}
			total := totals[key]
			if total == nil {
				total = &usageTotal{}
				totals[key] = total
			}
			total.runs++
			total.duration += *j.Duration
			total.durations = append(total.durations, *j.Duration)
			if !total.hasLastRun || createdAt.After(total.lastRun) {
				total.lastRun = createdAt
				total.hasLastRun = true
			}
		}
	}
	if err := errors.Join(fetchErrors...); err != nil {
		return fmt.Errorf("fetch job usage: %w", err)
	}

	rows := make([]usageRow, 0, len(totals))
	jobDurationMinutes := 0.0
	for key, total := range totals {
		jobDurationMinutes += total.duration / 60
		mean, median, p95, maximum := durationStatisticsMinutes(total.durations)
		rows = append(rows, usageRow{
			Project:       key.project,
			Job:           key.job,
			Runs:          total.runs,
			Minutes:       math.Round(total.duration/60*10) / 10,
			MeanMinutes:   math.Round(mean*100) / 100,
			MedianMinutes: math.Round(median*100) / 100,
			P95Minutes:    math.Round(p95*100) / 100,
			MaxMinutes:    math.Round(maximum*100) / 100,
			LastRun:       total.lastRun,
		})
	}
	sortUsageRows(rows, opts.sortBy, opts.order)

	totalRows := len(rows)
	if opts.limit > 0 && len(rows) > opts.limit {
		rows = rows[:opts.limit]
	}
	report := usageReport{
		Group:              opts.group,
		Since:              since,
		Until:              until,
		JobDurationMinutes: math.Round(jobDurationMinutes*10) / 10,
		CIMinutesQuota:     quota,
		CachePagesRead:     cachePagesRead,
		CachePagesStale:    cachePagesStale,
		CacheProjectsRead:  cacheProjectsRead,
		CacheProjectsStale: cacheProjectsStale,
		CacheNotice:        cacheNotice,
		TotalRows:          totalRows,
		ReturnedRows:       len(rows),
		Rows:               rows,
	}
	return writeUsageReport(output, summaryOutput, opts.output, report)
}

func durationStatisticsMinutes(durations []float64) (mean, median, p95, maximum float64) {
	if len(durations) == 0 {
		return 0, 0, 0, 0
	}
	sorted := append([]float64(nil), durations...)
	sort.Float64s(sorted)
	total := 0.0
	for _, duration := range sorted {
		total += duration
	}
	mean = total / float64(len(sorted)) / 60
	middle := len(sorted) / 2
	if len(sorted)%2 == 0 {
		median = (sorted[middle-1] + sorted[middle]) / 2 / 60
	} else {
		median = sorted[middle] / 60
	}
	p95Index := int(math.Ceil(0.95*float64(len(sorted)))) - 1
	p95 = sorted[p95Index] / 60
	maximum = sorted[len(sorted)-1] / 60
	return mean, median, p95, maximum
}

func validateUsageOptions(opts usageOptions) error {
	if strings.TrimSpace(opts.group) == "" {
		return errors.New("--group is required")
	}
	if opts.limit < 0 {
		return errors.New("--limit must be zero or greater")
	}
	if opts.cacheTTL < 0 {
		return errors.New("--cache-ttl must be zero or greater")
	}
	if opts.historicalCacheTTL < 0 {
		return errors.New("--historical-cache-ttl must be zero or greater")
	}
	if opts.sortBy != "minutes" && opts.sortBy != "runs" && opts.sortBy != "project" && opts.sortBy != "job" && opts.sortBy != "latest" {
		return errors.New("--sort must be minutes, runs, project, job, or latest")
	}
	if opts.order != "desc" && opts.order != "asc" {
		return errors.New("--order must be desc or asc")
	}
	if opts.output != "table" && opts.output != "json" && opts.output != "csv" {
		return errors.New("--output must be table, json, or csv")
	}
	return nil
}

func usageDateRange(sinceText, untilText string, now time.Time) (time.Time, time.Time, error) {
	now = now.UTC()
	since := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	if sinceText != "" {
		parsed, err := parseUsageTime(sinceText, false)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse --since: %w", err)
		}
		since = parsed
	}
	until := now
	if untilText != "" {
		parsed, err := parseUsageTime(untilText, true)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse --until: %w", err)
		}
		until = parsed
	}
	if until.Before(since) {
		return time.Time{}, time.Time{}, errors.New("--until must be on or after --since")
	}
	return since, until, nil
}

func parseUsageTime(value string, endOfDate bool) (time.Time, error) {
	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		if endOfDate {
			parsed = parsed.Add(24*time.Hour - time.Nanosecond)
		}
		return parsed.UTC(), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("use YYYY-MM-DD or RFC3339: %w", err)
	}
	return parsed.UTC(), nil
}

func (c *client) groupProjects(ctx context.Context, group string) ([]groupProject, error) {
	path := fmt.Sprintf("/api/v4/groups/%s/projects?include_subgroups=true&with_shared=false&per_page=%d", url.PathEscape(group), usagePageSize)
	return paginate[groupProject](c, ctx, path)
}

func (c *client) groupCIMinutesQuota(ctx context.Context, group string, month time.Time) (*ciMinutesQuota, error) {
	var settings groupQuotaSettings
	if err := c.get(ctx, fmt.Sprintf("/api/v4/groups/%s", url.PathEscape(group)), &settings); err != nil {
		return nil, err
	}
	if settings.ID == 0 {
		return nil, errors.New("GitLab group response did not include a numeric ID")
	}
	query := `query($namespaceId: NamespaceID!, $date: Date!) {
  ciMinutesUsage(namespaceId: $namespaceId, date: $date, first: 1) {
    nodes { minutes month monthIso8601 }
  }
}`
	request := map[string]any{
		"query": query,
		"variables": map[string]string{
			"namespaceId": fmt.Sprintf("gid://gitlab/Group/%d", settings.ID),
			"date":        time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
		},
	}
	var response ciMinutesUsageResponse
	if err := c.postJSON(ctx, "/api/graphql", request, &response); err != nil {
		return nil, err
	}
	if len(response.Errors) > 0 {
		messages := make([]string, 0, len(response.Errors))
		for _, gqlError := range response.Errors {
			if gqlError.Message != "" {
				messages = append(messages, gqlError.Message)
			}
		}
		if len(messages) == 0 {
			return nil, errors.New("GitLab GraphQL did not return CI minute usage")
		}
		return nil, fmt.Errorf("GitLab CI minute usage: %s", strings.Join(messages, "; "))
	}
	if len(response.Data.CIMinutesUsage.Nodes) == 0 || response.Data.CIMinutesUsage.Nodes[0].Minutes == nil {
		return nil, errors.New("GitLab did not return CI minute usage for the selected month")
	}
	node := response.Data.CIMinutesUsage.Nodes[0]
	monthName := node.MonthISO8601
	if monthName == "" {
		monthName = time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01")
	}
	quota := &ciMinutesQuota{
		Available:   true,
		Month:       monthName,
		UsedMinutes: node.Minutes,
	}
	if settings.SharedRunnersMinutesLimit == nil {
		quota.Reason = "GitLab returned usage but no group monthly minute limit"
		return quota, nil
	}
	total := *settings.SharedRunnersMinutesLimit
	if settings.ExtraSharedRunnersMinutesLimit != nil {
		total += *settings.ExtraSharedRunnersMinutesLimit
	}
	remaining := total - *node.Minutes
	quota.TotalMinutes = &total
	quota.RemainingMinutes = &remaining
	return quota, nil
}

func selectUsageProjects(projects []groupProject, filters []string, includeArchived bool) ([]groupProject, error) {
	if len(filters) == 0 {
		selected := make([]groupProject, 0, len(projects))
		for _, project := range projects {
			if includeArchived || !project.Archived {
				selected = append(selected, project)
			}
		}
		return selected, nil
	}
	selected := make([]groupProject, 0, len(filters))
	seen := make(map[int64]bool, len(filters))
	for _, filter := range filters {
		matched := false
		for _, project := range projects {
			if filter != project.PathWithNamespace && filter != strconv.FormatInt(project.ID, 10) {
				continue
			}
			matched = true
			if project.Archived && !includeArchived {
				return nil, fmt.Errorf("project %q is archived; use --include-archived to include it", filter)
			}
			if !seen[project.ID] {
				selected = append(selected, project)
				seen[project.ID] = true
			}
			break
		}
		if !matched {
			return nil, fmt.Errorf("project %q is not in the selected group", filter)
		}
	}
	return selected, nil
}

func (c *client) groupJobUsage(ctx context.Context, projects []groupProject, since, until time.Time, opts usageOptions, cacheDir string, cacheEnabled bool) <-chan projectUsageResult {
	results := make(chan projectUsageResult, len(projects))
	if len(projects) == 0 {
		close(results)
		return results
	}
	projectQueue := make(chan groupProject)
	var workers sync.WaitGroup
	workerCount := min(usageProjectConcurrency, len(projects))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for project := range projectQueue {
				jobs, cachePagesRead, cachePagesStale, cacheNotice, err := c.projectUsageJobs(ctx, project.ID, since, until, opts, cacheDir, cacheEnabled)
				results <- projectUsageResult{
					project:         project,
					jobs:            jobs,
					cachePagesRead:  cachePagesRead,
					cachePagesStale: cachePagesStale,
					cacheNotice:     cacheNotice,
					err:             err,
				}
			}
		}()
	}
	go func() {
		for _, project := range projects {
			select {
			case projectQueue <- project:
			case <-ctx.Done():
				close(projectQueue)
				workers.Wait()
				close(results)
				return
			}
		}
		close(projectQueue)
		workers.Wait()
		close(results)
	}()
	return results
}

func (c *client) projectUsageJobs(ctx context.Context, projectID int64, since, until time.Time, opts usageOptions, cacheDir string, cacheEnabled bool) ([]usageJob, int, int, string, error) {
	var matching []usageJob
	cachePagesRead := 0
	cachePagesStale := 0
	cacheNotice := ""
	requestPath := fmt.Sprintf("/api/v4/projects/%d/jobs?pagination=keyset&per_page=%d&order_by=id&sort=desc", projectID, usagePageSize)
	for requestPath != "" {
		page, nextPath, cacheHit, cacheStale, notice, err := c.usageJobPage(ctx, projectID, requestPath, opts, cacheDir, cacheEnabled)
		if cacheHit {
			cachePagesRead++
		}
		if cacheStale {
			cachePagesStale++
		}
		cacheNotice = joinUsageNotices(cacheNotice, notice)
		if err != nil {
			return nil, cachePagesRead, cachePagesStale, cacheNotice, err
		}
		if len(page) == 0 {
			return matching, cachePagesRead, cachePagesStale, cacheNotice, nil
		}
		oldest := time.Time{}
		for _, j := range page {
			createdAt, err := time.Parse(time.RFC3339Nano, j.CreatedAt)
			if err != nil {
				continue
			}
			if oldest.IsZero() || createdAt.Before(oldest) {
				oldest = createdAt
			}
			if !createdAt.Before(since) && !createdAt.After(until) {
				matching = append(matching, j)
			}
		}
		if !oldest.IsZero() && oldest.Before(since) {
			return matching, cachePagesRead, cachePagesStale, cacheNotice, nil
		}
		requestPath = nextPath
	}
	return matching, cachePagesRead, cachePagesStale, cacheNotice, nil
}

func sortUsageRows(rows []usageRow, sortBy, order string) {
	sort.SliceStable(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		comparison := 0
		switch sortBy {
		case "minutes":
			if left.Minutes < right.Minutes {
				comparison = -1
			} else if left.Minutes > right.Minutes {
				comparison = 1
			}
		case "runs":
			if left.Runs < right.Runs {
				comparison = -1
			} else if left.Runs > right.Runs {
				comparison = 1
			}
		case "project":
			comparison = strings.Compare(left.Project, right.Project)
		case "job":
			comparison = strings.Compare(left.Job, right.Job)
		case "latest":
			if left.LastRun.Before(right.LastRun) {
				comparison = -1
			} else if left.LastRun.After(right.LastRun) {
				comparison = 1
			}
		}
		if comparison == 0 {
			if left.Project != right.Project {
				comparison = strings.Compare(left.Project, right.Project)
			} else {
				comparison = strings.Compare(left.Job, right.Job)
			}
		}
		if order == "asc" {
			return comparison < 0
		}
		return comparison > 0
	})
}

func writeUsageReport(output, summaryOutput io.Writer, format string, report usageReport) error {
	switch format {
	case "json":
		return json.NewEncoder(output).Encode(report)
	case "csv":
		if summaryOutput != nil {
			if err := writeUsageSummary(summaryOutput, report); err != nil {
				return err
			}
		}
		writer := csv.NewWriter(output)
		if err := writer.Write([]string{"project", "job", "runs", "minutes", "mean_minutes", "median_minutes", "p95_minutes", "max_minutes", "last_run"}); err != nil {
			return err
		}
		for _, row := range report.Rows {
			if err := writer.Write([]string{
				row.Project,
				row.Job,
				strconv.Itoa(row.Runs),
				fmt.Sprintf("%.1f", row.Minutes),
				fmt.Sprintf("%.2f", row.MeanMinutes),
				fmt.Sprintf("%.2f", row.MedianMinutes),
				fmt.Sprintf("%.2f", row.P95Minutes),
				fmt.Sprintf("%.2f", row.MaxMinutes),
				row.LastRun.Format(time.RFC3339),
			}); err != nil {
				return err
			}
		}
		writer.Flush()
		return writer.Error()
	default:
		writer := tabwriter.NewWriter(output, 0, 2, 2, ' ', 0)
		if _, err := fmt.Fprintf(writer, "Group: %s\tWindow: %s through %s\tRows: %d of %d\tJob durations: %.1f minutes\n", report.Group, report.Since.Format(time.RFC3339), report.Until.Format(time.RFC3339), report.ReturnedRows, report.TotalRows, report.JobDurationMinutes); err != nil {
			return err
		}
		if report.CIMinutesQuota != nil {
			if _, err := fmt.Fprintln(writer, formatCIMinutesQuota(*report.CIMinutesQuota)); err != nil {
				return err
			}
		}
		if err := writeUsageCacheSummary(writer, report); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer, "PROJECT\tJOB\tRUNS\tMINUTES\tMEAN (MIN)\tMEDIAN (MIN)\tP95 (MIN)\tMAX (MIN)\tLAST RUN"); err != nil {
			return err
		}
		for _, row := range report.Rows {
			if _, err := fmt.Fprintf(writer, "%s\t%s\t%d\t%.1f\t%.2f\t%.2f\t%.2f\t%.2f\t%s\n", row.Project, row.Job, row.Runs, row.Minutes, row.MeanMinutes, row.MedianMinutes, row.P95Minutes, row.MaxMinutes, row.LastRun.Format(time.RFC3339)); err != nil {
				return err
			}
		}
		return writer.Flush()
	}
}

func writeUsageSummary(output io.Writer, report usageReport) error {
	if _, err := fmt.Fprintf(output, "Job durations for %s: %.1f minutes\n", report.Group, report.JobDurationMinutes); err != nil {
		return err
	}
	if report.CIMinutesQuota == nil {
		return writeUsageCacheSummary(output, report)
	}
	if _, err := fmt.Fprintln(output, formatCIMinutesQuota(*report.CIMinutesQuota)); err != nil {
		return err
	}
	return writeUsageCacheSummary(output, report)
}

func writeUsageCacheSummary(output io.Writer, report usageReport) error {
	if report.CachePagesRead > 0 {
		if _, err := fmt.Fprintf(output, "Cache: reused %d job pages (%d stale)\n", report.CachePagesRead, report.CachePagesStale); err != nil {
			return err
		}
	}
	if report.CacheNotice != "" {
		_, err := fmt.Fprintf(output, "Cache notice: %s\n", report.CacheNotice)
		return err
	}
	return nil
}

func joinUsageNotices(current, next string) string {
	if next == "" {
		return current
	}
	for _, notice := range strings.Split(current, "; ") {
		if notice == next {
			return current
		}
	}
	if current == "" {
		return next
	}
	return current + "; " + next
}

func formatCIMinutesQuota(quota ciMinutesQuota) string {
	if !quota.Available {
		if quota.Reason == "" {
			return "CI minutes quota: unavailable"
		}
		return "CI minutes quota: unavailable (" + quota.Reason + ")"
	}
	if quota.TotalMinutes == nil || quota.RemainingMinutes == nil || quota.UsedMinutes == nil {
		return fmt.Sprintf("CI minutes quota for %s: used %s; total and remaining unavailable (%s)", quota.Month, formatOptionalMinutes(quota.UsedMinutes), quota.Reason)
	}
	return fmt.Sprintf("CI minutes quota for %s: %s used / %s total; %s remaining", quota.Month, formatOptionalMinutes(quota.UsedMinutes), formatOptionalMinutes(quota.TotalMinutes), formatOptionalMinutes(quota.RemainingMinutes))
}

func formatOptionalMinutes(value *int64) string {
	if value == nil {
		return "unavailable"
	}
	return strconv.FormatInt(*value, 10)
}
