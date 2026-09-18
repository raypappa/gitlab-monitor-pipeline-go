package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"charm.land/bubbles/v2/list"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/fang/v2"
	"charm.land/lipgloss/v2"
	"github.com/spf13/cobra"
)

type client struct {
	baseURL string
	token   string
	http    *http.Client
}

type pipeline struct {
	ID         int64  `json:"id"`
	IID        int64  `json:"iid"`
	ProjectID  int64  `json:"project_id"`
	Status     string `json:"status"`
	Ref        string `json:"ref"`
	SHA        string `json:"sha"`
	WebURL     string `json:"web_url"`
	Name       string `json:"name"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
}

type job struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	Stage        string `json:"stage"`
	Status       string `json:"status"`
	AllowFailure bool   `json:"allow_failure"`
	Ref          string `json:"ref"`
	WebURL       string `json:"web_url"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
}

type menuItem struct {
	title string
	desc  string
}

func (i menuItem) FilterValue() string { return i.title }
func (i menuItem) Title() string       { return i.title }
func (i menuItem) Description() string { return i.desc }

type traceableJob struct {
	job       job
	projectID int64
	path      string
}

type menuDelegate struct{}

func (menuDelegate) Height() int                             { return 1 }
func (menuDelegate) Spacing() int                            { return 0 }
func (menuDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }
func (menuDelegate) Render(w io.Writer, m list.Model, index int, item list.Item) {
	menuItem := item.(menuItem)
	marker := "  "
	if index == m.Index() {
		marker = "> "
	}
	_, _ = fmt.Fprintf(w, "%s%s", marker, menuItem.Title())
}

type bridge struct {
	ID             int64 `json:"id"`
	DownstreamPipe *struct {
		ID        int64  `json:"id"`
		ProjectID int64  `json:"project_id"`
		Status    string `json:"status"`
		Ref       string `json:"ref"`
		SHA       string `json:"sha"`
		WebURL    string `json:"web_url"`
		Name      string `json:"name"`
	} `json:"downstream_pipeline"`
}

type monitoredPipeline struct {
	Pipeline pipeline             `json:"pipeline"`
	Jobs     []job                `json:"jobs"`
	Children []*monitoredPipeline `json:"children,omitempty"`
}

type snapshot struct {
	Root *monitoredPipeline `json:"root"`
}

type options struct {
	pipelineID int64
	branch     string
	project    string
	live       bool
	compact    bool
	noTUI      bool
	wait       bool
	include    bool
	output     string
	interval   time.Duration
	baseURL    string
}

type renderMode int

const (
	jsonMode renderMode = iota
	compactMode
	plainMode
	tuiMode
)

func selectRenderMode(opts options, terminal bool) renderMode {
	if opts.output == "json" {
		return jsonMode
	}
	if opts.compact {
		return compactMode
	}
	if opts.noTUI || !terminal {
		return plainMode
	}
	return tuiMode
}

func main() {
	if err := fang.Execute(context.Background(), newCommand()); err != nil {
		os.Exit(1)
	}
}

func newCommand() *cobra.Command {
	opts := options{include: true, interval: 3 * time.Second, baseURL: envOr("GITLAB_HOST", "https://gitlab.com")}
	cmd := &cobra.Command{
		Use:   "pipeline-monitor",
		Short: "Monitor a GitLab pipeline and its downstream pipelines",
		Long:  "Monitor a GitLab pipeline and recursively include downstream child pipelines.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd.Context(), opts)
		},
	}
	flags := cmd.Flags()
	flags.Int64Var(&opts.pipelineID, "pipeline-id", 0, "Monitor this pipeline ID instead of the latest pipeline for the branch")
	flags.StringVar(&opts.branch, "branch", "", "Branch to monitor; defaults to the current Git branch")
	flags.StringVar(&opts.project, "project", "", "GitLab project path or numeric ID; defaults to CI_PROJECT_PATH or git remote")
	flags.BoolVar(&opts.live, "live", false, "Refresh status in real time until all pipelines finish")
	flags.BoolVar(&opts.compact, "compact", false, "Show status in compact format")
	flags.BoolVar(&opts.noTUI, "no-tui", false, "Disable the interactive terminal UI and use plain text")
	flags.BoolVar(&opts.wait, "wait", false, "Wait until all pipelines finish")
	flags.BoolVar(&opts.include, "downstream", true, "Include downstream child pipelines")
	flags.StringVar(&opts.output, "output", "text", "Output format: text or json")
	flags.DurationVar(&opts.interval, "interval", opts.interval, "Polling interval for live updates")
	flags.StringVar(&opts.baseURL, "host", opts.baseURL, "GitLab host URL")
	return cmd
}

func run(ctx context.Context, opts options) error {
	if opts.wait {
		opts.live = true
	}
	if opts.output != "text" && opts.output != "json" {
		return errors.New("--output must be text or json")
	}
	if opts.output == "json" && (opts.live || opts.compact || opts.wait) {
		return errors.New("--output json cannot be used with --live, --wait, or --compact")
	}
	branchProvided := opts.branch != ""
	projectFromGit := false
	if opts.project == "" {
		opts.project = envOr("CI_PROJECT_PATH", "")
	}
	if opts.project == "" {
		opts.project = gitProject()
		projectFromGit = opts.project != ""
	}
	if opts.project == "" {
		return errors.New("project is required; use --project or CI_PROJECT_PATH")
	}
	token := envOr("GITLAB_TOKEN", envOr("GITLAB_PRIVATE_TOKEN", ""))
	if token == "" {
		return errors.New("GITLAB_TOKEN or GITLAB_PRIVATE_TOKEN is required")
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	c := &client{baseURL: strings.TrimRight(opts.baseURL, "/"), token: token, http: http.DefaultClient}
	if opts.pipelineID == 0 && !branchProvided {
		if projectFromGit || (func() bool {
			envProject := os.Getenv("CI_PROJECT_PATH")
			return envProject != "" && envProject == opts.project
		})() {
			opts.branch = gitBranch()
		}
		if opts.branch == "" {
			defaultBranch, err := c.defaultBranch(ctx, opts.project)
			if err != nil {
				return err
			}
			opts.branch = defaultBranch
		}
	}
	root, err := c.resolveRoot(ctx, opts.project, opts.branch, opts.pipelineID)
	if err != nil {
		return err
	}

	for {
		state, err := c.snapshot(ctx, root, opts.include, map[string]bool{})
		if err != nil {
			return err
		}
		if opts.output == "json" {
			if err := json.NewEncoder(os.Stdout).Encode(snapshot{Root: state}); err != nil {
				return err
			}
			return nil
		}
		if selectRenderMode(opts, interactiveTerminal()) == tuiMode && !opts.live {
			finalModel, err := runMonitorTUI(state)
			if err != nil {
				return err
			}
			if finalModel.state != nil && (finalModel.state.Pipeline.Status == "failed" || hasFailed(finalModel.state)) {
				return errors.New("pipeline failed")
			}
			return nil
		}
		fmt.Print("\033[H\033[2J")
		printPipeline(state, "", opts.compact)
		if !opts.live || !hasPollable(state) {
			if shouldOfferJobLogs(opts) {
				retried, err := offerJobLogs(ctx, c, state)
				if err != nil {
					return err
				}
				if retried {
					opts.live = true
					continue
				}
			}
			if state.Pipeline.Status == "failed" || hasFailed(state) {
				return errors.New("pipeline failed")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.interval):
		}
	}
}

func interactiveTerminal() bool {
	info, err := os.Stdout.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func shouldOfferJobLogs(opts options) bool {
	return opts.output == "text" && !opts.compact && !opts.wait
}

func (c *client) jobTrace(ctx context.Context, projectID, jobID int64) (string, error) {
	trace, err := c.requestText(ctx, fmt.Sprintf("/api/v4/projects/%d/jobs/%d/trace", projectID, jobID))
	if err != nil {
		return "", err
	}
	return trace, nil
}

func (c *client) retryJob(ctx context.Context, projectID, jobID int64) (job, error) {
	var retried job
	if err := c.request(ctx, http.MethodPost, fmt.Sprintf("/api/v4/projects/%d/jobs/%d/retry", projectID, jobID), &retried); err != nil {
		return job{}, err
	}
	return retried, nil
}

func (c *client) requestText(ctx context.Context, path string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("PRIVATE-TOKEN", c.token)
	req.Header.Set("Accept", "text/plain")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("GitLab API %s returned %s", path, resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func offerJobLogs(ctx context.Context, c *client, state *monitoredPipeline) (bool, error) {
	jobs := traceableJobs(state, "")
	if len(jobs) == 0 {
		return false, nil
	}
	items := []menuItem{{title: "View logs"}, {title: "Save logs"}, {title: "Retry"}, {title: "Exit"}}
	action, err := chooseMenu("Choose an action", items)
	if err != nil || action < 0 || action == 3 {
		return false, err
	}
	if action == 2 {
		return retrySelectedJob(ctx, c, state)
	}

	items = make([]menuItem, 0, len(jobs))
	for _, traced := range jobs {
		j := traced.job
		items = append(items, menuItem{
			title: fmt.Sprintf("%s%s (%d) - %s", traced.path, j.Name, j.ID, j.Status),
		})
	}
	selected, err := chooseMenu("Select pipeline job to trace", items)
	if err != nil {
		return false, err
	}
	if selected < 0 {
		return false, nil
	}
	traced := jobs[selected]
	job := traced.job
	trace, err := c.jobTrace(ctx, traced.projectID, job.ID)
	if err != nil {
		return false, err
	}
	if action == 1 {
		path, err := promptForPath(fmt.Sprintf("job-%d.txt", job.ID))
		if err != nil || path == "" {
			return false, err
		}
		if err := saveJobTrace(path, trace); err != nil {
			return false, fmt.Errorf("could not save logs to %s: %w", path, err)
		}
		fmt.Printf("Saved logs for %s (%d) to %s\n", job.Name, job.ID, path)
		return false, nil
	}
	fmt.Printf("\nGetting logs for %s (%d)...\n\n", job.Name, job.ID)
	fmt.Print(trace)
	if trace != "" && !strings.HasSuffix(trace, "\n") {
		fmt.Println()
	}
	return false, nil
}

func retrySelectedJob(ctx context.Context, c *client, state *monitoredPipeline) (bool, error) {
	jobs := traceableJobs(state, "")
	items := make([]menuItem, 0, len(jobs))
	for _, traced := range jobs {
		j := traced.job
		items = append(items, menuItem{
			title: fmt.Sprintf("%s%s (%d) - %s", traced.path, j.Name, j.ID, j.Status),
		})
	}
	selected, err := chooseMenu("Select pipeline job to retry", items)
	if err != nil || selected < 0 {
		return false, err
	}

	traced := jobs[selected]
	retried, err := c.retryJob(ctx, traced.projectID, traced.job.ID)
	if err != nil {
		return false, fmt.Errorf("could not retry job with ID %d: %w", traced.job.ID, err)
	}
	fmt.Printf("Retried job (ID: %d), status: %s, ref: %s, weburl: %s\n", retried.ID, retried.Status, retried.Ref, retried.WebURL)
	return true, nil
}

func traceableJobs(state *monitoredPipeline, path string) []traceableJob {
	jobs := make([]traceableJob, 0, len(state.Jobs))
	for _, j := range state.Jobs {
		jobs = append(jobs, traceableJob{job: j, projectID: state.Pipeline.ProjectID, path: path})
	}
	for _, child := range state.Children {
		jobs = append(jobs, traceableJobs(child, path+"[downstream] ")...)
	}
	return jobs
}

type menuModel struct {
	list     list.Model
	selected int
	canceled bool
}

func (m menuModel) Init() tea.Cmd { return nil }

func (m menuModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if window, ok := msg.(tea.WindowSizeMsg); ok {
		m.list.SetSize(window.Width, window.Height)
		return m, nil
	}
	if key, ok := msg.(tea.KeyPressMsg); ok {
		switch key.String() {
		case "enter":
			m.selected = m.list.Index()
			return m, tea.Quit
		case "esc", "ctrl+c":
			m.canceled = true
			return m, tea.Quit
		}
	}
	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m menuModel) View() tea.View { return tea.NewView(m.list.View()) }

func chooseMenu(title string, items []menuItem) (int, error) {
	listItems := make([]list.Item, 0, len(items))
	for _, item := range items {
		listItems = append(listItems, item)
	}
	model := list.New(listItems, menuDelegate{}, 120, len(items)+1)
	model.Title = title
	model.SetShowFilter(false)
	model.SetShowStatusBar(false)
	model.SetShowPagination(false)
	model.SetShowHelp(false)
	model.SetShowTitle(true)
	finalModel, err := tea.NewProgram(menuModel{list: model}).Run()
	if err != nil {
		return 0, err
	}
	result := finalModel.(menuModel)
	if result.canceled {
		return -1, nil
	}
	return result.selected, nil
}

type pathPromptModel struct {
	input    textinput.Model
	value    string
	canceled bool
}

func (m pathPromptModel) Init() tea.Cmd { return m.input.Focus() }

func (m pathPromptModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if window, ok := msg.(tea.WindowSizeMsg); ok {
		m.input.SetWidth(window.Width - 2)
		return m, nil
	}
	if key, ok := msg.(tea.KeyPressMsg); ok {
		switch key.String() {
		case "enter":
			m.value = strings.TrimSpace(m.input.Value())
			return m, tea.Quit
		case "esc", "q", "ctrl+c":
			m.canceled = true
			return m, tea.Quit
		}
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m pathPromptModel) View() tea.View {
	return tea.NewView("Save logs to: " + m.input.View())
}

func promptForPath(defaultPath string) (string, error) {
	input := textinput.New()
	input.Prompt = ""
	input.SetValue(defaultPath)
	input.SetWidth(118)
	finalModel, err := tea.NewProgram(pathPromptModel{input: input}).Run()
	if err != nil {
		return "", err
	}
	result := finalModel.(pathPromptModel)
	if result.canceled {
		return "", nil
	}
	return result.value, nil
}

func saveJobTrace(path, trace string) error {
	if path == "" {
		return errors.New("log path is required")
	}
	return os.WriteFile(path, []byte(trace), 0o600)
}

func (c *client) defaultBranch(ctx context.Context, project string) (string, error) {
	var p struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := c.get(ctx, fmt.Sprintf("/api/v4/projects/%s", url.PathEscape(project)), &p); err != nil {
		return "", err
	}
	if p.DefaultBranch == "" {
		return "", errors.New("GitLab project has no default branch")
	}
	return p.DefaultBranch, nil
}

func (c *client) resolveRoot(ctx context.Context, project, branch string, id int64) (pipeline, error) {
	if id != 0 {
		var p pipeline
		return p, c.get(ctx, fmt.Sprintf("/api/v4/projects/%s/pipelines/%d", url.PathEscape(project), id), &p)
	}
	var p pipeline
	path := fmt.Sprintf("/api/v4/projects/%s/pipelines/latest?ref=%s", url.PathEscape(project), url.QueryEscape(branch))
	return p, c.get(ctx, path, &p)
}

func (c *client) snapshot(ctx context.Context, p pipeline, include bool, seen map[string]bool) (*monitoredPipeline, error) {
	key := fmt.Sprintf("%d/%d", p.ProjectID, p.ID)
	if seen[key] {
		return nil, nil
	}
	seen[key] = true
	if err := c.get(ctx, fmt.Sprintf("/api/v4/projects/%d/pipelines/%d", p.ProjectID, p.ID), &p); err != nil {
		return nil, err
	}
	m := &monitoredPipeline{Pipeline: p}
	if err := c.list(ctx, fmt.Sprintf("/api/v4/projects/%d/pipelines/%d/jobs?per_page=100", p.ProjectID, p.ID), &m.Jobs); err != nil {
		return nil, err
	}
	if !include {
		return m, nil
	}
	var bridges []bridge
	if err := c.list(ctx, fmt.Sprintf("/api/v4/projects/%d/pipelines/%d/bridges?per_page=100", p.ProjectID, p.ID), &bridges); err != nil {
		return nil, err
	}
	for _, b := range bridges {
		if b.DownstreamPipe == nil {
			continue
		}
		d := pipeline{ID: b.DownstreamPipe.ID, ProjectID: b.DownstreamPipe.ProjectID, Status: b.DownstreamPipe.Status, Ref: b.DownstreamPipe.Ref, SHA: b.DownstreamPipe.SHA, WebURL: b.DownstreamPipe.WebURL, Name: b.DownstreamPipe.Name}
		if err := c.get(ctx, fmt.Sprintf("/api/v4/projects/%d/pipelines/%d", d.ProjectID, d.ID), &d); err != nil {
			return nil, err
		}
		child, err := c.snapshot(ctx, d, include, seen)
		if err != nil {
			return nil, err
		}
		if child != nil {
			m.Children = append(m.Children, child)
		}
	}
	sort.Slice(m.Children, func(i, j int) bool { return m.Children[i].Pipeline.ID < m.Children[j].Pipeline.ID })
	return m, nil
}

func (c *client) get(ctx context.Context, path string, out any) error {
	return c.request(ctx, http.MethodGet, path, out)
}

func (c *client) list(ctx context.Context, path string, out any) error {
	return c.request(ctx, http.MethodGet, path, out)
}

func (c *client) request(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("PRIVATE-TOKEN", c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GitLab API %s returned %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func printPipeline(m *monitoredPipeline, prefix string, compact bool) {
	status := m.Pipeline.Status
	name := m.Pipeline.Name
	if name == "" {
		name = fmt.Sprintf("pipeline %d", m.Pipeline.ID)
	}
	if compact {
		fmt.Printf("%s(%s) • %s [%s]", prefix, colorStatus(status), name, m.Pipeline.Ref)
	} else {
		fmt.Printf("%sPipeline %s [%s] (pipeline %d)", prefix, name, m.Pipeline.Ref, m.Pipeline.ID)
	}
	if len(m.Jobs) > 0 {
		fmt.Printf("\n")
		for _, j := range m.Jobs {
			if compact {
				fmt.Printf("%s  (%s) • %s [%s]\n", prefix, colorStatus(j.Status), j.Name, j.Stage)
			} else {
				fmt.Print(formatJobLine(prefix, j))
			}
		}
	} else {
		fmt.Printf("\n")
	}
	if !compact {
		fmt.Printf("%s\n%s%s\n%sSHA: %s\n%sPipeline state: %s\n%sElapsed time: %s\n", prefix, prefix, pipelineURL(m.Pipeline.WebURL), prefix, valueOrUnknown(m.Pipeline.SHA), prefix, status, prefix, formatPipelineDuration(m.Pipeline))
	}
	for _, child := range m.Children {
		if !compact {
			fmt.Printf("\n%sDownstream:\n", prefix)
		}
		printPipeline(child, prefix+"  ", compact)
	}
}

func formatPipelineDuration(p pipeline) string {
	if p.StartedAt == "" {
		return "-"
	}
	started, err := time.Parse(time.RFC3339Nano, p.StartedAt)
	if err != nil {
		return "-"
	}
	end := time.Now()
	if p.FinishedAt != "" {
		if finished, parseErr := time.Parse(time.RFC3339Nano, p.FinishedAt); parseErr == nil {
			end = finished
		}
	}
	if end.Before(started) {
		return "-"
	}
	return formatElapsed(end.Sub(started))
}

func formatJobLine(prefix string, j job) string {
	const statusWidth = 22
	const durationWidth = 11
	const stageWidth = 20

	statusPadding := statusWidth - len(j.Status) - 2
	if statusPadding < 0 {
		statusPadding = 0
	}
	return fmt.Sprintf("%s(%s)%s • %-*s %-*s %s\n", prefix, colorStatus(j.Status), strings.Repeat(" ", statusPadding), durationWidth, formatDuration(j), stageWidth, j.Stage, j.Name)
}

func formatDuration(j job) string {
	if j.StartedAt == "" {
		return "-"
	}
	started, err := time.Parse(time.RFC3339Nano, j.StartedAt)
	if err != nil {
		return "-"
	}
	end := time.Now()
	if j.FinishedAt != "" {
		if finished, parseErr := time.Parse(time.RFC3339Nano, j.FinishedAt); parseErr == nil {
			end = finished
		}
	}
	if end.Before(started) {
		return "-"
	}
	return formatElapsed(end.Sub(started))
}

func formatElapsed(duration time.Duration) string {
	seconds := int64(duration / time.Second)
	if seconds < 60 {
		return fmt.Sprintf("%02ds", seconds)
	}
	minutes := seconds / 60
	seconds %= 60
	if minutes < 60 {
		return fmt.Sprintf("%02dm %02ds", minutes, seconds)
	}
	hours := minutes / 60
	minutes %= 60
	return fmt.Sprintf("%02dh %02dm %02ds", hours, minutes, seconds)
}

func pipelineURL(raw string) string {
	if raw == "" {
		return "URL: unknown"
	}
	return raw
}

func valueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func colorStatus(status string) string {
	style := lipgloss.NewStyle().Foreground(lipgloss.Color("#A6A6A6"))
	switch status {
	case "success", "passed":
		style = style.Foreground(lipgloss.Color("#04B575"))
	case "failed", "error":
		style = style.Foreground(lipgloss.Color("#FF4672"))
	case "created", "waiting_for_resource", "preparing", "pending", "running", "scheduled", "waiting_for_callback", "canceling":
		style = style.Foreground(lipgloss.Color("#F2C94C"))
	}
	return style.Render(status)
}

func hasPollable(m *monitoredPipeline) bool {
	if pollable(m.Pipeline.Status) {
		return true
	}
	for _, child := range m.Children {
		if hasPollable(child) {
			return true
		}
	}
	return false
}

func hasFailed(m *monitoredPipeline) bool {
	if m.Pipeline.Status == "failed" {
		return true
	}
	for _, j := range m.Jobs {
		if j.Status == "failed" && !j.AllowFailure {
			return true
		}
	}
	for _, child := range m.Children {
		if hasFailed(child) {
			return true
		}
	}
	return false
}

func pollable(status string) bool {
	switch status {
	case "created", "waiting_for_resource", "preparing", "pending", "running", "scheduled", "waiting_for_callback", "canceling":
		return true
	default:
		return false
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func gitBranch() string {
	return gitOutput("rev-parse", "--abbrev-ref", "HEAD")
}

func gitProject() string {
	return projectFromRemote(gitOutput("remote", "get-url", "origin"))
}

func projectFromRemote(remote string) string {
	if remote == "" {
		return ""
	}
	if strings.Contains(remote, "://") {
		parsed, err := url.Parse(remote)
		if err == nil {
			return strings.TrimPrefix(strings.TrimSuffix(parsed.Path, ".git"), "/")
		}
	}
	if at := strings.Index(remote, ":"); at >= 0 {
		return strings.TrimSuffix(strings.TrimPrefix(remote[at+1:], "/"), ".git")
	}
	return strings.TrimSuffix(strings.TrimPrefix(remote, "/"), ".git")
}

func gitOutput(args ...string) string {
	command := exec.Command("git", args...)
	output, err := command.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}
