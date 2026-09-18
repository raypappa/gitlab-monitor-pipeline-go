package main

import "testing"

func TestFlattenRowsRootOnly(t *testing.T) {
	rows := flattenRows(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}}, nil)
	if len(rows) != 1 || rows[0].kind != pipelineRow || rows[0].depth != 0 {
		t.Fatalf("rows = %#v, want one root row", rows)
	}
}

func TestFlattenRowsIncludesJobsAndNestedChildren(t *testing.T) {
	state := &monitoredPipeline{
		Pipeline: pipeline{ID: 1, ProjectID: 10},
		Jobs:     []job{{ID: 11, Name: "root"}},
		Children: []*monitoredPipeline{{
			Pipeline: pipeline{ID: 2, ProjectID: 20},
			Jobs:     []job{{ID: 22, Name: "child"}},
			Children: []*monitoredPipeline{{Pipeline: pipeline{ID: 3, ProjectID: 30}, Jobs: []job{{ID: 33}}}},
		}},
	}
	rows := flattenRows(state, map[string]bool{"10/1": true, "20/2": true, "30/3": true})
	if len(rows) != 6 {
		t.Fatalf("got %d rows, want 6", len(rows))
	}
	if rows[2].kind != pipelineRow || rows[2].pipelineKey != "20/2" || rows[2].depth != 1 {
		t.Fatalf("child pipeline row = %#v", rows[2])
	}
	if rows[3].job.projectID != 20 || rows[5].job.projectID != 30 {
		t.Fatalf("downstream project IDs not retained: %#v %#v", rows[3].job, rows[5].job)
	}
}

func TestFlattenRowsCollapsedChildHidesDescendants(t *testing.T) {
	state := &monitoredPipeline{
		Pipeline: pipeline{ID: 1, ProjectID: 10},
		Children: []*monitoredPipeline{{Pipeline: pipeline{ID: 2, ProjectID: 20}, Jobs: []job{{ID: 22}}}},
	}
	rows := flattenRows(state, map[string]bool{"10/1": true, "20/2": false})
	if len(rows) != 2 || rows[1].pipelineKey != "20/2" || rows[1].expanded {
		t.Fatalf("collapsed rows = %#v", rows)
	}
}

func TestFlattenRowsNilAndEmpty(t *testing.T) {
	if rows := flattenRows(nil, nil); rows != nil {
		t.Fatalf("nil state rows = %#v, want nil", rows)
	}
	rows := flattenRows(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}, Children: []*monitoredPipeline{nil}}, nil)
	if len(rows) != 1 {
		t.Fatalf("empty rows = %#v, want root only", rows)
	}
}

func TestPipelineKeyUsesProjectAndPipelineID(t *testing.T) {
	if got := pipelineKey(pipeline{ProjectID: 42, ID: 7, Name: "same"}); got != "42/7" {
		t.Fatalf("pipelineKey() = %q, want %q", got, "42/7")
	}
}

func TestAllPipelineKeysPrunesRemovedPipelines(t *testing.T) {
	keys := allPipelineKeys(&monitoredPipeline{Pipeline: pipeline{ID: 1, ProjectID: 10}, Children: []*monitoredPipeline{{Pipeline: pipeline{ID: 2, ProjectID: 20}}}})
	if !keys["10/1"] || !keys["20/2"] || len(keys) != 2 {
		t.Fatalf("keys = %#v", keys)
	}
}
