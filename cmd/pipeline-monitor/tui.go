package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

const maxRefreshFailures = 3

type monitorModel struct {
	state           *monitoredPipeline
	rows            []treeRow
	selected        int
	expanded        map[string]bool
	width           int
	height          int
	status          string
	err             error
	client          *client
	root            pipeline
	include         bool
	interval        time.Duration
	wait            bool
	loading         bool
	refreshFailures int
	ctx             context.Context
	view            string
	trace           string
	tracePos        int
}

type snapshotMsg struct {
	state *monitoredPipeline
}

type snapshotErrMsg struct {
	err error
}

type refreshTickMsg struct{}

type traceMsg struct {
	trace string
	job   *traceableJob
	save  bool
}

type traceErrMsg struct {
	err error
}

type retryMsg struct {
	job job
}

type retryErrMsg struct {
	err error
}

func fetchTrace(ctx context.Context, c *client, traced *traceableJob, save bool) tea.Cmd {
	return func() tea.Msg {
		trace, err := c.jobTrace(ctx, traced.projectID, traced.job.ID)
		if err != nil {
			return traceErrMsg{err: err}
		}
		return traceMsg{trace: trace, job: traced, save: save}
	}
}

func retrySelected(ctx context.Context, c *client, traced *traceableJob) tea.Cmd {
	return func() tea.Msg {
		retried, err := c.retryJob(ctx, traced.projectID, traced.job.ID)
		if err != nil {
			return retryErrMsg{err: err}
		}
		return retryMsg{job: retried}
	}
}

func refreshSnapshot(ctx context.Context, c *client, root pipeline, include bool) tea.Cmd {
	return func() tea.Msg {
		state, err := c.snapshot(ctx, root, include, map[string]bool{})
		if err != nil {
			return snapshotErrMsg{err: err}
		}
		return snapshotMsg{state: state}
	}
}

func scheduleRefresh(interval time.Duration) tea.Cmd {
	return tea.Tick(interval, func(time.Time) tea.Msg { return refreshTickMsg{} })
}

func newMonitorModel(state *monitoredPipeline) monitorModel {
	expanded := map[string]bool{}
	if state != nil {
		expanded[pipelineKey(state.Pipeline)] = true
	}
	m := monitorModel{state: state, expanded: expanded, status: "j/k or arrows: move  G: bottom  enter: expand  l: logs  s: save  r: retry  q: quit"}
	m.rebuildRows("")
	return m
}

func (m *monitorModel) rebuildRows(selectedKey string) {
	m.rows = flattenRows(m.state, m.expanded)
	if len(m.rows) == 0 {
		m.selected = 0
		return
	}
	if selectedKey != "" {
		for i, row := range m.rows {
			if rowKey(row) == selectedKey {
				m.selected = i
				return
			}
		}
	}
	if m.selected >= len(m.rows) {
		m.selected = len(m.rows) - 1
	}
	if m.selected < 0 {
		m.selected = 0
	}
}

func rowKey(row treeRow) string {
	if row.kind == pipelineRow {
		return "pipeline:" + row.pipelineKey
	}
	return fmt.Sprintf("job:%d/%d", row.job.projectID, row.job.job.ID)
}

func (m monitorModel) Init() tea.Cmd {
	if m.wait && m.state != nil && !hasPollable(m.state) {
		return tea.Quit
	}
	if m.client == nil || m.interval <= 0 || m.state == nil || !hasPollable(m.state) {
		return nil
	}
	return refreshSnapshot(m.ctx, m.client, m.root, m.include)
}

func (m monitorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case refreshTickMsg:
		if m.client == nil || m.loading || !hasPollable(m.state) {
			return m, nil
		}
		m.loading = true
		return m, refreshSnapshot(m.ctx, m.client, m.root, m.include)
	case snapshotMsg:
		selectedKey := ""
		if len(m.rows) > 0 && m.selected < len(m.rows) {
			selectedKey = rowKey(m.rows[m.selected])
		}
		m.state = msg.state
		m.loading = false
		m.refreshFailures = 0
		m.err = nil
		m.pruneExpansion()
		m.rebuildRows(selectedKey)
		if m.wait && !hasPollable(m.state) {
			return m, tea.Quit
		}
		if m.client != nil && hasPollable(m.state) {
			return m, scheduleRefresh(m.interval)
		}
		return m, nil
	case snapshotErrMsg:
		m.loading = false
		m.err = msg.err
		m.refreshFailures++
		if m.client != nil && m.interval > 0 && refreshRetryable(msg.err) && m.refreshFailures <= maxRefreshFailures {
			m.status = fmt.Sprintf("refresh failed (%d/%d); retrying", m.refreshFailures, maxRefreshFailures)
			return m, scheduleRefresh(m.interval)
		}
		if refreshRetryable(msg.err) {
			m.status = fmt.Sprintf("refresh stopped after %d failures; fix the error and restart", m.refreshFailures)
		} else {
			m.status = "refresh stopped; fix the error and restart"
		}
		return m, nil
	case traceMsg:
		m.err = nil
		if msg.save {
			path := fmt.Sprintf("job-%d.txt", msg.job.job.ID)
			if err := saveJobTrace(path, msg.trace); err != nil {
				m.err = err
				return m, nil
			}
			m.status = "saved logs to " + path
			return m, nil
		}
		m.view = "trace"
		m.trace = msg.trace
		m.tracePos = 0
		return m, nil
	case traceErrMsg:
		m.err = msg.err
		return m, nil
	case retryMsg:
		m.status = fmt.Sprintf("retried job %d (%s)", msg.job.ID, msg.job.Status)
		if m.client != nil {
			m.loading = true
			return m, refreshSnapshot(m.ctx, m.client, m.root, m.include)
		}
		return m, nil
	case retryErrMsg:
		m.err = msg.err
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyPressMsg:
		if m.view == "trace" {
			switch msg.String() {
			case "q", "esc", "ctrl+c":
				m.view = ""
			case "up", "k":
				if m.tracePos > 0 {
					m.tracePos--
				}
			case "down", "j":
				m.tracePos++
			case "f":
				m.tracePos = len(strings.Split(strings.TrimSuffix(m.trace, "\n"), "\n"))
			}
			return m, nil
		}
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			m.moveSelection(-1)
		case "down", "j":
			m.moveSelection(1)
		case "home":
			m.selected = 0
		case "end", "G":
			if len(m.rows) > 0 {
				m.selected = len(m.rows) - 1
			}
		case "enter":
			m.toggleSelected()
		case "l", "s", "r":
			if traced := m.selectedJob(); traced != nil && m.client != nil {
				if msg.String() == "r" {
					return m, retrySelected(m.ctx, m.client, traced)
				}
				return m, fetchTrace(m.ctx, m.client, traced, msg.String() == "s")
			}
		}
	}
	return m, nil
}

func (m *monitorModel) pruneExpansion() {
	keys := allPipelineKeys(m.state)
	for key := range m.expanded {
		if !keys[key] {
			delete(m.expanded, key)
		}
	}
}

func (m *monitorModel) moveSelection(delta int) {
	if len(m.rows) == 0 {
		return
	}
	m.selected += delta
	if m.selected < 0 {
		m.selected = 0
	}
	if m.selected >= len(m.rows) {
		m.selected = len(m.rows) - 1
	}
}

func (m *monitorModel) toggleSelected() {
	if len(m.rows) == 0 || m.rows[m.selected].kind != pipelineRow {
		return
	}
	row := m.rows[m.selected]
	m.expanded[row.pipelineKey] = !row.expanded
	m.rebuildRows(rowKey(row))
}

func (m monitorModel) View() tea.View {
	return tea.NewView(m.viewText())
}

func (m monitorModel) viewText() string {
	if m.width <= 0 || m.height <= 0 {
		return ""
	}
	if m.view == "trace" {
		return m.traceText()
	}
	lines := make([]string, 0, m.height)
	lines = append(lines, truncate("Pipeline Monitor", m.width))
	available := m.height - 2
	if available < 0 {
		available = 0
	}
	rowLimit := available
	if rowLimit > len(m.rows) {
		rowLimit = len(m.rows)
	}
	start := m.visibleStart(rowLimit)
	for i := start; i < start+rowLimit; i++ {
		lines = append(lines, truncate(m.renderLine(i), m.width))
	}
	for len(lines) < m.height-1 {
		lines = append(lines, "")
	}
	footer := m.status
	if m.err != nil {
		footer = "error: " + m.err.Error()
		if m.status != "" {
			footer += " | " + m.status
		}
	}
	lines = append(lines, truncate(footer, m.width))
	return strings.Join(lines, "\n")
}

func refreshRetryable(err error) bool {
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr.retryable()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func (m monitorModel) traceText() string {
	lines := strings.Split(m.trace, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	available := m.height - 2
	if available < 0 {
		available = 0
	}
	start := m.tracePos
	maxStart := len(lines) - available
	if maxStart < 0 {
		maxStart = 0
	}
	if start > maxStart {
		start = maxStart
	}
	if start < 0 {
		start = 0
	}
	result := []string{truncate("Job trace", m.width)}
	for i := start; i < start+available && i < len(lines); i++ {
		result = append(result, truncate(lines[i], m.width))
	}
	for len(result) < m.height-1 {
		result = append(result, "")
	}
	result = append(result, truncate("esc: back  j/k: scroll  f: follow  q: quit", m.width))
	return strings.Join(result, "\n")
}

func (m monitorModel) selectedJob() *traceableJob {
	if len(m.rows) == 0 || m.selected < 0 || m.selected >= len(m.rows) || m.rows[m.selected].kind != jobRow {
		return nil
	}
	return m.rows[m.selected].job
}

func (m monitorModel) renderLine(index int) string {
	if m.width >= 90 {
		leftWidth := m.width * 3 / 5
		left := truncate(m.renderRow(index), leftWidth)
		right := ""
		if index == m.selected {
			right = truncate(m.detailsText(), m.width-leftWidth-1)
		}
		return left + strings.Repeat(" ", maxInt(1, leftWidth-lipgloss.Width(left))) + " " + right
	}
	return m.renderRow(index)
}

func (m monitorModel) detailsText() string {
	if len(m.rows) == 0 {
		return "No pipeline data"
	}
	row := m.rows[m.selected]
	if row.kind == pipelineRow {
		p := row.pipeline.Pipeline
		name := p.Name
		if name == "" {
			name = fmt.Sprintf("pipeline %d", p.ID)
		}
		return fmt.Sprintf("%s | id %d | project %d | %s | jobs %d | children %d", name, p.ID, p.ProjectID, p.Status, len(row.pipeline.Jobs), len(row.pipeline.Children))
	}
	j := row.job.job
	return fmt.Sprintf("%s | id %d | project %d | %s | stage %s | %s", j.Name, j.ID, row.job.projectID, j.Status, j.Stage, row.pipeline.Pipeline.WebURL)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (m monitorModel) visibleStart(rowLimit int) int {
	if rowLimit <= 0 || len(m.rows) <= rowLimit {
		return 0
	}
	start := m.selected - rowLimit + 1
	if start < 0 {
		start = 0
	}
	return start
}

func (m monitorModel) renderRow(index int) string {
	row := m.rows[index]
	marker := "  "
	if index == m.selected {
		marker = "> "
	}
	indent := strings.Repeat("  ", row.depth)
	if row.kind == pipelineRow {
		name := row.pipeline.Pipeline.Name
		if name == "" {
			name = fmt.Sprintf("pipeline %d", row.pipeline.Pipeline.ID)
		}
		toggle := "▸"
		if row.expanded {
			toggle = "▾"
		}
		return fmt.Sprintf("%s%s%s %s", marker, indent, toggle, colorStatus(row.pipeline.Pipeline.Status)+" "+name)
	}
	return fmt.Sprintf("%s%s  %s %s", marker, indent, colorStatus(row.job.job.Status), row.job.job.Name)
}

func truncate(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if ansi.StringWidth(value) <= width {
		return value
	}
	if width == 1 {
		return ansi.Cut(value, 0, 1)
	}
	return ansi.Truncate(value, width, "…")
}

func runMonitorTUI(ctx context.Context, c *client, root pipeline, state *monitoredPipeline, include bool, interval time.Duration, wait bool) (monitorModel, error) {
	model := newMonitorModel(state)
	model.client = c
	model.root = root
	model.include = include
	model.interval = interval
	model.wait = wait
	model.ctx = ctx
	finalModel, err := tea.NewProgram(model).Run()
	if err != nil {
		return monitorModel{}, err
	}
	return finalModel.(monitorModel), nil
}
