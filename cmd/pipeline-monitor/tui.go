package main

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type monitorModel struct {
	state    *monitoredPipeline
	rows     []treeRow
	selected int
	expanded map[string]bool
	width    int
	height   int
	status   string
	err      error
}

func newMonitorModel(state *monitoredPipeline) monitorModel {
	expanded := map[string]bool{}
	if state != nil {
		expanded[pipelineKey(state.Pipeline)] = true
	}
	m := monitorModel{state: state, expanded: expanded, status: "j/k or arrows: move  enter: expand  q: quit"}
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

func (m monitorModel) Init() tea.Cmd { return nil }

func (m monitorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyPressMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "up", "k":
			m.moveSelection(-1)
		case "down", "j":
			m.moveSelection(1)
		case "home":
			m.selected = 0
		case "end":
			if len(m.rows) > 0 {
				m.selected = len(m.rows) - 1
			}
		case "enter":
			m.toggleSelected()
		}
	}
	return m, nil
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
		lines = append(lines, truncate(m.renderRow(i), m.width))
	}
	for len(lines) < m.height-1 {
		lines = append(lines, "")
	}
	footer := m.status
	if m.err != nil {
		footer = "error: " + m.err.Error()
	}
	lines = append(lines, truncate(footer, m.width))
	return strings.Join(lines, "\n")
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
	if lipgloss.Width(value) <= width {
		return value
	}
	runes := []rune(value)
	if width == 1 {
		return string(runes[:1])
	}
	return string(runes[:width-1]) + "…"
}

func runMonitorTUI(state *monitoredPipeline) (monitorModel, error) {
	finalModel, err := tea.NewProgram(newMonitorModel(state)).Run()
	if err != nil {
		return monitorModel{}, err
	}
	return finalModel.(monitorModel), nil
}
