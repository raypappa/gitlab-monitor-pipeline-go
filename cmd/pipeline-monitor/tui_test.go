package main

import (
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

func updateMonitor(t *testing.T, m monitorModel, msg tea.Msg) (monitorModel, tea.Cmd) {
	t.Helper()
	updated, cmd := m.Update(msg)
	return updated.(monitorModel), cmd
}
