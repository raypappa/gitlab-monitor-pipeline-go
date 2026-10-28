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
	dbPath             string
	legacyCacheDir     string
	cacheTTL           time.Duration
	historicalCacheTTL time.Duration
	noCache            bool
	refresh            bool
	includeArchived    bool
}

type groupProject struct {
	ID                int64            `json:"id"`
	PathWithNamespace string           `json:"path_with_namespace"`
	Archived          bool             `json:"archived"`
	Namespace         projectNamespace `json:"namespace"`
}

type projectNamespace struct {
	ID int64 `json:"id"`
}

type usageJob struct {
	ID        int64        `json:"id"`
	Name      string       `json:"name"`
	Duration  *float64     `json:"duration"`
	CreatedAt string       `json:"created_at"`
	Status    string       `json:"status"`
	Tags      []string     `json:"tag_list"`
	Runner    *usageRunner `json:"runner"`
}

type usageRunner struct {
	RunnerType string `json:"runner_type"`
}

type usageKey struct {
	project string
	job     string
}

type usageTotal struct {
	runs                   int
	duration               float64
	durations              []float64
	estimatedCompute       float64
	estimatedComputeRuns   int
	unestimatedComputeRuns int
	lastRun                time.Time
	hasLastRun             bool
}

type usageRow struct {
	Project                 string    `json:"project"`
	Job                     string    `json:"job"`
	Runs                    int       `json:"runs"`
	DurationMinutes         float64   `json:"duration_minutes"`
	MeanDurationMinutes     float64   `json:"mean_duration_minutes"`
	MedianDurationMinutes   float64   `json:"median_duration_minutes"`
	P95DurationMinutes      float64   `json:"p95_duration_minutes"`
	MaxDurationMinutes      float64   `json:"max_duration_minutes"`
	EstimatedComputeMinutes *float64  `json:"known_run_estimated_compute_minutes,omitempty"`
	EstimatedComputeRuns    int       `json:"estimated_compute_runs"`
	UnestimatedComputeRuns  int       `json:"unestimated_compute_runs"`
	LastRun                 time.Time `json:"last_run"`
}

type projectComputeUsageRow struct {
	Project string `json:"project"`
	Minutes int64  `json:"compute_minutes"`
}

type usageReport struct {
	Group                    string                   `json:"group"`
	DatabasePath             string                   `json:"database_path"`
	Since                    time.Time                `json:"since"`
	Until                    time.Time                `json:"until"`
	JobDurationMinutes       float64                  `json:"job_duration_minutes"`
	EstimatedComputeMinutes  float64                  `json:"known_run_estimated_compute_minutes"`
	EstimatedComputeRuns     int                      `json:"estimated_compute_runs"`
	UnestimatedComputeRuns   int                      `json:"unestimated_compute_runs"`
	ComputeEstimateNotice    string                   `json:"compute_estimate_notice,omitempty"`
	ProjectComputeAvailable  bool                     `json:"project_compute_available"`
	ProjectComputePeriod     string                   `json:"project_compute_period,omitempty"`
	ProjectComputeMinutes    *int64                   `json:"project_compute_minutes,omitempty"`
	ProjectComputeByProject  []projectComputeUsageRow `json:"project_compute_by_project,omitempty"`
	ProjectComputeNotice     string                   `json:"project_compute_notice,omitempty"`
	CIMinutesQuota           *ciMinutesQuota          `json:"ci_minutes_quota,omitempty"`
	LegacyCachePagesImported int                      `json:"legacy_cache_pages_imported"`
	LegacyCacheJobsImported  int                      `json:"legacy_cache_jobs_imported"`
	ProjectsSynced           int                      `json:"projects_synced"`
	PagesSynced              int                      `json:"pages_synced"`
	ProjectsReadFromDatabase int                      `json:"projects_read_from_database"`
	ProjectsStale            int                      `json:"projects_stale"`
	CacheNotice              string                   `json:"cache_notice,omitempty"`
	TotalRows                int                      `json:"total_rows"`
	ReturnedRows             int                      `json:"returned_rows"`
	Rows                     []usageRow               `json:"rows"`
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

type ciMinutesProjectUsageResponse struct {
	Data struct {
		Usage struct {
			Nodes []struct {
				Minutes *int64 `json:"minutes"`
				Project struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"project"`
			} `json:"nodes"`
			PageInfo struct {
				EndCursor   string `json:"endCursor"`
				HasNextPage bool   `json:"hasNextPage"`
			} `json:"pageInfo"`
		} `json:"ciMinutesProjectMonthlyUsage"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type projectUsageResult struct {
	project groupProject
	jobs    []usageJob
	sync    usageSyncStats
	err     error
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
		Short: "Summarize GitLab CI compute usage",
		Long:  "Report GitLab's monthly compute minutes by project and show per-job duration with documented hosted-runner compute estimates.",
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
	flags.StringVar(&opts.sortBy, "sort", opts.sortBy, "Sort job rows by duration minutes, runs, project, job, or latest")
	flags.StringVar(&opts.order, "order", opts.order, "Sort order: desc or asc")
	flags.StringVar(&opts.output, "output", opts.output, "Output format: table, json, or csv")
	flags.BoolVar(&opts.includeArchived, "include-archived", false, "Include archived projects")
	flags.StringVar(&opts.dbPath, "db", "", "SQLite database path; defaults to the XDG user cache directory")
	flags.StringVar(&opts.legacyCacheDir, "legacy-cache-dir", "", "Import legacy page caches from this directory")
	flags.StringVar(&opts.legacyCacheDir, "cache-dir", "", "Deprecated alias for --legacy-cache-dir")
	flags.BoolVar(&opts.noCache, "no-cache", false, "Ignore stored sync state and force a full API sync")
	flags.BoolVar(&opts.refresh, "refresh", false, "Force a full API sync for selected projects")
	flags.DurationVar(&opts.cacheTTL, "cache-ttl", opts.cacheTTL, "Minimum interval between recent API syncs; use 0s to refresh every run")
	flags.DurationVar(&opts.historicalCacheTTL, "historical-cache-ttl", opts.historicalCacheTTL, "Interval between full historical API syncs; use 0s to rescan history every run")
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
	store, databasePath, err := openUsageStore(ctx, opts.dbPath, c.baseURL)
	if err != nil {
		return err
	}
	defer func() { _ = store.close() }()
	allProjects, err := c.groupProjects(ctx, opts.group)
	if err != nil {
		return err
	}
	if err := store.upsertProjects(ctx, allProjects); err != nil {
		return err
	}
	legacyPagesImported, legacyJobsImported := 0, 0
	cacheNotice := ""
	if !opts.noCache {
		legacyCacheDir, cacheDirErr := usageCacheDirectory(opts.legacyCacheDir)
		if cacheDirErr != nil {
			cacheNotice = "legacy cache import skipped: " + cacheDirErr.Error()
		} else {
			legacyPagesImported, legacyJobsImported, err = store.importLegacyCaches(ctx, legacyCacheDir, allProjects, since)
		}
		if err != nil {
			cacheNotice = joinUsageNotices(cacheNotice, "legacy cache import incomplete: "+err.Error())
		}
	}
	projects, err := selectUsageProjects(allProjects, opts.projects, opts.includeArchived)
	if err != nil {
		return err
	}
	settings, settingsErr := c.groupQuotaSettings(ctx, opts.group)
	var quota *ciMinutesQuota
	if since.Year() != until.Year() || since.Month() != until.Month() {
		quota = &ciMinutesQuota{Reason: "the selected date range spans multiple calendar months"}
	} else if settingsErr != nil {
		quota = &ciMinutesQuota{Reason: settingsErr.Error()}
	} else {
		quota, err = c.groupQuotaForSettings(ctx, settings, since)
		if err != nil {
			quota = &ciMinutesQuota{Reason: err.Error()}
		}
	}
	computeMonths, computeRangeSupported := projectComputeMonths(since, until, opts.until == "", time.Now().UTC())
	computeByID := make(map[int64]int64)
	projectComputeAvailable := false
	projectComputeNotice := ""
	projectComputePeriod := ""
	if !computeRangeSupported {
		projectComputeNotice = "GitLab exposes compute usage by calendar month, not for partial custom date ranges"
	} else if settingsErr != nil {
		projectComputeNotice = "could not load GitLab group settings for project compute usage: " + settingsErr.Error()
	} else {
		namespaceIDs := projectComputeNamespaceIDs(settings.ID, projects)
		computeByID, err = c.groupProjectComputeUsage(ctx, namespaceIDs, computeMonths)
		if err != nil {
			projectComputeNotice = "could not load GitLab project compute usage: " + err.Error()
		} else {
			projectComputeAvailable = true
			projectComputePeriod = formatComputeUsagePeriod(computeMonths)
		}
	}

	jobNames := make(map[string]bool, len(opts.jobs))
	for _, name := range opts.jobs {
		jobNames[name] = true
	}
	results := groupJobUsage(ctx, c, store, projects, since, until, opts)
	totals := make(map[usageKey]*usageTotal)
	var fetchErrors []error
	projectsSynced := 0
	pagesSynced := 0
	projectsReadFromDatabase := 0
	projectsStale := 0
	estimatedComputeMinutes := 0.0
	estimatedComputeRuns := 0
	unestimatedComputeRuns := 0
	for result := range results {
		if result.sync.synced {
			projectsSynced++
		}
		pagesSynced += result.sync.pages
		if result.sync.readFromDatabase {
			projectsReadFromDatabase++
		}
		if result.sync.stale {
			projectsStale++
		}
		if result.sync.notice != "" {
			cacheNotice = joinUsageNotices(cacheNotice, result.sync.notice)
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
			if costFactor, ok := hostedRunnerCostFactor(j, c.baseURL); ok {
				total.estimatedCompute += estimateJobComputeMinutes(*j.Duration, costFactor)
				total.estimatedComputeRuns++
			} else {
				total.unestimatedComputeRuns++
			}
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
		estimatedComputeMinutes += total.estimatedCompute
		estimatedComputeRuns += total.estimatedComputeRuns
		unestimatedComputeRuns += total.unestimatedComputeRuns
		mean, median, p95, maximum := durationStatisticsMinutes(total.durations)
		row := usageRow{
			Project:                key.project,
			Job:                    key.job,
			Runs:                   total.runs,
			DurationMinutes:        math.Round(total.duration/60*10) / 10,
			MeanDurationMinutes:    math.Round(mean*100) / 100,
			MedianDurationMinutes:  math.Round(median*100) / 100,
			P95DurationMinutes:     math.Round(p95*100) / 100,
			MaxDurationMinutes:     math.Round(maximum*100) / 100,
			EstimatedComputeRuns:   total.estimatedComputeRuns,
			UnestimatedComputeRuns: total.unestimatedComputeRuns,
			LastRun:                total.lastRun,
		}
		if total.estimatedComputeRuns > 0 {
			estimated := math.Round(total.estimatedCompute*100) / 100
			row.EstimatedComputeMinutes = &estimated
		}
		rows = append(rows, row)
	}
	projectComputeRows := make([]projectComputeUsageRow, 0, len(projects))
	var selectedProjectCompute int64
	if projectComputeAvailable {
		for _, project := range projects {
			minutes, found := computeByID[project.ID]
			if !found {
				continue
			}
			selectedProjectCompute += minutes
			projectComputeRows = append(projectComputeRows, projectComputeUsageRow{Project: project.PathWithNamespace, Minutes: minutes})
		}
		sort.Slice(projectComputeRows, func(i, j int) bool {
			return projectComputeRows[i].Project < projectComputeRows[j].Project
		})
	}
	sortUsageRows(rows, opts.sortBy, opts.order)

	totalRows := len(rows)
	if opts.limit > 0 && len(rows) > opts.limit {
		rows = rows[:opts.limit]
	}
	report := usageReport{
		Group:                    opts.group,
		DatabasePath:             databasePath,
		Since:                    since,
		Until:                    until,
		JobDurationMinutes:       math.Round(jobDurationMinutes*10) / 10,
		EstimatedComputeMinutes:  math.Round(estimatedComputeMinutes*100) / 100,
		EstimatedComputeRuns:     estimatedComputeRuns,
		UnestimatedComputeRuns:   unestimatedComputeRuns,
		ComputeEstimateNotice:    "Estimates cover only runs with recognized hosted-runner metadata, including data returned by GitLab; legacy cache entries and unknown/custom runners are not estimated. --refresh can backfill metadata where GitLab still provides it. Estimates assume the standard-project factor and do not account for discounts or historical factor changes.",
		ProjectComputeAvailable:  projectComputeAvailable,
		ProjectComputePeriod:     projectComputePeriod,
		ProjectComputeByProject:  projectComputeRows,
		ProjectComputeNotice:     projectComputeNotice,
		CIMinutesQuota:           quota,
		LegacyCachePagesImported: legacyPagesImported,
		LegacyCacheJobsImported:  legacyJobsImported,
		ProjectsSynced:           projectsSynced,
		PagesSynced:              pagesSynced,
		ProjectsReadFromDatabase: projectsReadFromDatabase,
		ProjectsStale:            projectsStale,
		CacheNotice:              cacheNotice,
		TotalRows:                totalRows,
		ReturnedRows:             len(rows),
		Rows:                     rows,
	}
	if projectComputeAvailable {
		report.ProjectComputeMinutes = &selectedProjectCompute
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

func (c *client) groupQuotaSettings(ctx context.Context, group string) (groupQuotaSettings, error) {
	var settings groupQuotaSettings
	if err := c.get(ctx, fmt.Sprintf("/api/v4/groups/%s", url.PathEscape(group)), &settings); err != nil {
		return groupQuotaSettings{}, err
	}
	if settings.ID == 0 {
		return groupQuotaSettings{}, errors.New("GitLab group response did not include a numeric ID")
	}
	return settings, nil
}

func (c *client) groupCIMinutesQuota(ctx context.Context, groupID int64, month time.Time) (*ciMinutesQuota, error) {
	query := `query($namespaceId: NamespaceID!, $date: Date!) {
  ciMinutesUsage(namespaceId: $namespaceId, date: $date, first: 1) {
    nodes { minutes month monthIso8601 }
  }
}`
	request := map[string]any{
		"query": query,
		"variables": map[string]string{
			"namespaceId": fmt.Sprintf("gid://gitlab/Group/%d", groupID),
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
	return quota, nil
}

func (c *client) groupQuotaForSettings(ctx context.Context, settings groupQuotaSettings, month time.Time) (*ciMinutesQuota, error) {
	quota, err := c.groupCIMinutesQuota(ctx, settings.ID, month)
	if err != nil {
		return nil, err
	}
	if quota.UsedMinutes == nil {
		return quota, nil
	}
	if settings.SharedRunnersMinutesLimit == nil {
		quota.Reason = "GitLab returned usage but no group monthly minute limit"
		return quota, nil
	}
	total := *settings.SharedRunnersMinutesLimit
	if settings.ExtraSharedRunnersMinutesLimit != nil {
		total += *settings.ExtraSharedRunnersMinutesLimit
	}
	remaining := total - *quota.UsedMinutes
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

func groupJobUsage(ctx context.Context, c *client, store *usageStore, projects []groupProject, since, until time.Time, opts usageOptions) <-chan projectUsageResult {
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
				syncStats, err := store.syncProject(ctx, c, project, since, until, opts)
				var jobs []usageJob
				if err == nil {
					jobs, err = store.queryUsageJobs(ctx, project, since, until)
				}
				results <- projectUsageResult{
					project: project,
					jobs:    jobs,
					sync:    syncStats,
					err:     err,
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

func sortUsageRows(rows []usageRow, sortBy, order string) {
	sort.SliceStable(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		comparison := 0
		switch sortBy {
		case "minutes":
			if left.DurationMinutes < right.DurationMinutes {
				comparison = -1
			} else if left.DurationMinutes > right.DurationMinutes {
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
		if err := writer.Write([]string{"project", "job", "runs", "duration_minutes", "mean_duration_minutes", "median_duration_minutes", "p95_duration_minutes", "max_duration_minutes", "known_run_estimated_compute_minutes", "estimated_compute_runs", "unestimated_compute_runs", "last_run"}); err != nil {
			return err
		}
		for _, row := range report.Rows {
			if err := writer.Write([]string{
				row.Project,
				row.Job,
				strconv.Itoa(row.Runs),
				fmt.Sprintf("%.1f", row.DurationMinutes),
				fmt.Sprintf("%.2f", row.MeanDurationMinutes),
				fmt.Sprintf("%.2f", row.MedianDurationMinutes),
				fmt.Sprintf("%.2f", row.P95DurationMinutes),
				fmt.Sprintf("%.2f", row.MaxDurationMinutes),
				formatOptionalFloat(row.EstimatedComputeMinutes),
				strconv.Itoa(row.EstimatedComputeRuns),
				strconv.Itoa(row.UnestimatedComputeRuns),
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
		if err := writeUsageComputeSummary(writer, report); err != nil {
			return err
		}
		if err := writeUsageCacheSummary(writer, report); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(writer, "PROJECT\tJOB\tRUNS\tDURATION (MIN)\tMEAN DURATION\tMEDIAN DURATION\tP95 DURATION\tMAX DURATION\tKNOWN-RUN EST. COMPUTE (MIN)\tEST. RUNS\tUNEST. RUNS\tLAST RUN"); err != nil {
			return err
		}
		for _, row := range report.Rows {
			estimated := "-"
			if row.EstimatedComputeMinutes != nil {
				estimated = fmt.Sprintf("%.2f", *row.EstimatedComputeMinutes)
			}
			if _, err := fmt.Fprintf(writer, "%s\t%s\t%d\t%.1f\t%.2f\t%.2f\t%.2f\t%.2f\t%s\t%d\t%d\t%s\n", row.Project, row.Job, row.Runs, row.DurationMinutes, row.MeanDurationMinutes, row.MedianDurationMinutes, row.P95DurationMinutes, row.MaxDurationMinutes, estimated, row.EstimatedComputeRuns, row.UnestimatedComputeRuns, row.LastRun.Format(time.RFC3339)); err != nil {
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
	if report.CIMinutesQuota != nil {
		if _, err := fmt.Fprintln(output, formatCIMinutesQuota(*report.CIMinutesQuota)); err != nil {
			return err
		}
	}
	if err := writeUsageComputeSummary(output, report); err != nil {
		return err
	}
	return writeUsageCacheSummary(output, report)
}

func writeUsageComputeSummary(output io.Writer, report usageReport) error {
	if report.ProjectComputeAvailable {
		if report.ProjectComputeMinutes == nil {
			return errors.New("project compute usage is marked available without a total")
		}
		if _, err := fmt.Fprintf(output, "GitLab-reported compute minutes (%s, selected projects): %d; these totals are monthly and are not split by job\n", report.ProjectComputePeriod, *report.ProjectComputeMinutes); err != nil {
			return err
		}
		for _, project := range report.ProjectComputeByProject {
			if _, err := fmt.Fprintf(output, "  %s: %d compute minutes\n", project.Project, project.Minutes); err != nil {
				return err
			}
		}
	} else if report.ProjectComputeNotice != "" {
		if _, err := fmt.Fprintf(output, "GitLab-reported project compute minutes unavailable: %s\n", report.ProjectComputeNotice); err != nil {
			return err
		}
	}
	if report.EstimatedComputeRuns > 0 || report.UnestimatedComputeRuns > 0 {
		if _, err := fmt.Fprintf(output, "Known-run estimated compute minutes: %.2f across %d estimated runs; %d runs could not be estimated\n", report.EstimatedComputeMinutes, report.EstimatedComputeRuns, report.UnestimatedComputeRuns); err != nil {
			return err
		}
		if report.ComputeEstimateNotice != "" {
			if _, err := fmt.Fprintf(output, "  Estimate caveat: %s\n", report.ComputeEstimateNotice); err != nil {
				return err
			}
		}
	}
	return nil
}

func formatOptionalFloat(value *float64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%.2f", *value)
}

func writeUsageCacheSummary(output io.Writer, report usageReport) error {
	if report.ProjectsReadFromDatabase > 0 || report.ProjectsSynced > 0 || report.LegacyCachePagesImported > 0 {
		if _, err := fmt.Fprintf(output, "SQLite: %d projects read, %d synced across %d pages; imported %d legacy cache pages (%d jobs)\n", report.ProjectsReadFromDatabase, report.ProjectsSynced, report.PagesSynced, report.LegacyCachePagesImported, report.LegacyCacheJobsImported); err != nil {
			return err
		}
	}
	if report.ProjectsStale > 0 {
		if _, err := fmt.Fprintf(output, "SQLite: used stored data for %d project(s) after refresh errors\n", report.ProjectsStale); err != nil {
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
