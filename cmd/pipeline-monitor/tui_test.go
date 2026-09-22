package main

import (
	"context"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

func TestMonitorModelStartsAtRoot(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}, Jobs: []job{{ID: 2}}})
	if m.selected != 0 || len(m.rows) != 2 {
		t.Fatalf("model = %#v, want root selected with job visible", m)
	}
}

func TestMonitorModelNavigationIsBounded(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}, Jobs: []job{{ID: 2}, {ID: 3}}})
	m, _ = updateMonitor(t, m, tea.KeyPressMsg{Text: "up"})
	if m.selected != 0 {
		t.Fatalf("selected after up = %d", m.selected)
	}
	m, _ = updateMonitor(t, m, tea.KeyPressMsg{Text: "end"})
	m, _ = updateMonitor(t, m, tea.KeyPressMsg{Text: "down"})
	if m.selected != len(m.rows)-1 {
		t.Fatalf("selected after down at end = %d, want %d", m.selected, len(m.rows)-1)
	}
	m.selected = 0
	m, _ = updateMonitor(t, m, tea.KeyPressMsg{Text: "G"})
	if m.selected != len(m.rows)-1 {
		t.Fatalf("selected after G = %d, want %d", m.selected, len(m.rows)-1)
	}
}

func TestMonitorModelViewIsBounded(t *testing.T) {
	state := &monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}}
	for i := 0; i < 100; i++ {
		state.Jobs = append(state.Jobs, job{ID: int64(i), Name: strings.Repeat("long-name", 10)})
	}
	m := newMonitorModel(state)
	m.width, m.height = 24, 6
	if got := lipgloss.Height(m.viewText()); got > m.height {
		t.Fatalf("view height = %d, want <= %d", got, m.height)
	}
	if got := lipgloss.Width(m.viewText()); got > m.width {
		t.Fatalf("view width = %d, want <= %d", got, m.width)
	}
}

func TestMonitorModelDetailsOnlyRenderForSelectedRow(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{
		Pipeline: pipeline{ID: 1, ProjectID: 10},
		Jobs:     []job{{ID: 2, Name: "unit-tests"}},
	})
	m.width = 100

	selected := m.renderLine(0)
	job := m.renderLine(1)
	if !strings.Contains(selected, "| id 1 | project 10 |") {
		t.Fatalf("selected row is missing details: %q", selected)
	}
	if strings.Contains(job, "| id 1 | project 10 |") {
		t.Fatalf("job row repeats selected pipeline details: %q", job)
	}
}

func TestMonitorModelResize(t *testing.T) {
	m := newMonitorModel(nil)
	updated, _ := m.Update(tea.WindowSizeMsg{Width: 42, Height: 12})
	resized := updated.(monitorModel)
	if resized.width != 42 || resized.height != 12 {
		t.Fatalf("size = %dx%d", resized.width, resized.height)
	}
}

func TestMonitorModelQuit(t *testing.T) {
	m := newMonitorModel(nil)
	_, cmd := m.Update(tea.KeyPressMsg{Text: "q"})
	if cmd == nil {
		t.Fatal("q did not return quit command")
	}
}

func TestMonitorModelExpansion(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{
		Pipeline: pipeline{ID: 1, ProjectID: 10},
		Children: []*monitoredPipeline{{Pipeline: pipeline{ID: 2, ProjectID: 20}, Jobs: []job{{ID: 3}}}},
	})
	if len(m.rows) != 2 {
		t.Fatalf("initial rows = %d, want root and child", len(m.rows))
	}
	m.selected = 1
	m, _ = updateMonitor(t, m, tea.KeyPressMsg{Text: "enter"})
	if len(m.rows) != 3 || !m.rows[1].expanded {
		t.Fatalf("expanded child rows = %#v", m.rows)
	}
	m, _ = updateMonitor(t, m, tea.KeyPressMsg{Text: "enter"})
	if len(m.rows) != 2 || m.selected != 1 || m.rows[1].expanded {
		t.Fatalf("collapsed child rows = %#v, selected %d", m.rows, m.selected)
	}
}

func TestMonitorModelRefreshSuccessPreservesSelection(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}, Jobs: []job{{ID: 2, Name: "old"}}})
	m.selected = 1
	updated, _ := m.Update(snapshotMsg{state: &monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}, Jobs: []job{{ID: 2, Name: "new"}}}})
	got := updated.(monitorModel)
	if got.selected != 1 || got.rows[1].job.job.Name != "new" || got.err != nil {
		t.Fatalf("refreshed model = %#v", got)
	}
}

func TestMonitorModelWaitQuitsAfterTerminalSnapshot(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10, Status: "running"}})
	m.wait = true
	updated, cmd := m.Update(snapshotMsg{state: &monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10, Status: "success"}}})
	if cmd == nil {
		t.Fatal("wait mode did not quit after terminal snapshot")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("wait completion command = %T, want tea.QuitMsg", cmd())
	}
	if updated.(monitorModel).state.Pipeline.Status != "success" {
		t.Fatal("terminal snapshot was not retained")
	}
}

func TestMonitorModelLiveRemainsInteractiveAfterTerminalSnapshot(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10, Status: "running"}})
	m.wait = false
	updated, cmd := m.Update(snapshotMsg{state: &monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10, Status: "success"}}})
	if cmd != nil {
		t.Fatal("live mode quit or scheduled a command after terminal snapshot without a client")
	}
	if updated.(monitorModel).state.Pipeline.Status != "success" {
		t.Fatal("terminal snapshot was not retained")
	}
}

func TestMonitorModelWaitQuitsImmediatelyForFinalInitialSnapshot(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10, Status: "success"}})
	m.wait = true
	cmd := m.Init()
	if cmd == nil {
		t.Fatal("wait mode did not quit for an already-final snapshot")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("initial completion command = %T, want tea.QuitMsg", cmd())
	}
}

func TestMonitorModelRefreshErrorPreservesSnapshot(t *testing.T) {
	state := &monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}}
	m := newMonitorModel(state)
	updated, _ := m.Update(snapshotErrMsg{err: context.Canceled})
	got := updated.(monitorModel)
	if got.state != state || got.err != context.Canceled {
		t.Fatalf("error model = %#v", got)
	}
}

func TestMonitorModelRefreshPrunesExpansion(t *testing.T) {
	m := newMonitorModel(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}})
	m.expanded["20/2"] = true
	updated, _ := m.Update(snapshotMsg{state: &monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}}})
	if updated.(monitorModel).expanded["20/2"] {
		t.Fatal("removed pipeline expansion was retained")
	}
}

func TestMonitorModelSelectedDownstreamJobUsesOwningProject(t *testing.T) {
	state := &monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}, Children: []*monitoredPipeline{{Pipeline: pipeline{ID: 2, ProjectID: 20}, Jobs: []job{{ID: 3}}}}}
	m := newMonitorModel(state)
	m.expanded["20/2"] = true
	m.rebuildRows("")
	m.selected = 2
	traced := m.selectedJob()
	if traced == nil || traced.projectID != 20 || traced.job.ID != 3 {
		t.Fatalf("selected job = %#v", traced)
	}
}

func TestMonitorModelTraceViewIsBounded(t *testing.T) {
	m := newMonitorModel(nil)
	m.width, m.height, m.view = 20, 5, "trace"
	m.trace = strings.Repeat("long trace line\n", 20)
	if got := lipgloss.Height(m.viewText()); got > m.height {
		t.Fatalf("trace height = %d, want <= %d", got, m.height)
	}
}

func TestMonitorModelTraceFollowMovesToBottom(t *testing.T) {
	m := newMonitorModel(nil)
	m.width, m.height, m.view = 20, 5, "trace"
	m.trace = "one\ntwo\nthree\nfour\nfive\nsix"
	m.tracePos = 0
	updated, _ := m.Update(tea.KeyPressMsg{Text: "f"})
	got := updated.(monitorModel)
	if got.tracePos != 6 {
		t.Fatalf("trace position after f = %d, want 6", got.tracePos)
	}
	if !strings.Contains(got.traceText(), "six") {
		t.Fatalf("follow view does not show final trace line: %q", got.traceText())
	}
}

func TestSelectRenderMode(t *testing.T) {
	tests := []struct {
		name     string
		opts     options
		terminal bool
		want     renderMode
	}{
		{name: "json wins", opts: options{output: "json", compact: true}, terminal: true, want: jsonMode},
		{name: "compact wins", opts: options{output: "text", compact: true}, terminal: true, want: compactMode},
		{name: "no tui", opts: options{output: "text", noTUI: true}, terminal: true, want: plainMode},
		{name: "redirected", opts: options{output: "text"}, terminal: false, want: plainMode},
		{name: "interactive", opts: options{output: "text"}, terminal: true, want: tuiMode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := selectRenderMode(test.opts, test.terminal); got != test.want {
				t.Fatalf("selectRenderMode() = %v, want %v", got, test.want)
			}
		})
	}
}

func updateMonitor(t *testing.T, m monitorModel, msg tea.Msg) (monitorModel, tea.Cmd) {
	t.Helper()
	updated, cmd := m.Update(msg)
	return updated.(monitorModel), cmd
}
