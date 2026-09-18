package main

import "fmt"

type rowKind int

const (
	pipelineRow rowKind = iota
	jobRow
)

type treeRow struct {
	kind        rowKind
	depth       int
	pipeline    *monitoredPipeline
	job         *traceableJob
	pipelineKey string
	expanded    bool
}

func pipelineKey(p pipeline) string {
	return fmt.Sprintf("%d/%d", p.ProjectID, p.ID)
}

func flattenRows(state *monitoredPipeline, expanded map[string]bool) []treeRow {
	if state == nil {
		return nil
	}
	rows := make([]treeRow, 0)
	appendPipelineRows(&rows, state, 0, "", expanded, true)
	return rows
}

func appendPipelineRows(rows *[]treeRow, state *monitoredPipeline, depth int, path string, expanded map[string]bool, root bool) {
	if state == nil {
		return
	}
	key := pipelineKey(state.Pipeline)
	isExpanded, exists := expanded[key]
	if !exists {
		isExpanded = root
	}
	*rows = append(*rows, treeRow{
		kind:        pipelineRow,
		depth:       depth,
		pipeline:    state,
		pipelineKey: key,
		expanded:    isExpanded,
	})
	if !isExpanded {
		return
	}
	for i := range state.Jobs {
		j := traceableJob{job: state.Jobs[i], projectID: state.Pipeline.ProjectID, path: path}
		*rows = append(*rows, treeRow{kind: jobRow, depth: depth + 1, pipeline: state, job: &j, pipelineKey: key})
	}
	for _, child := range state.Children {
		appendPipelineRows(rows, child, depth+1, path+"[downstream] ", expanded, false)
	}
}

func allPipelineKeys(state *monitoredPipeline) map[string]bool {
	keys := map[string]bool{}
	collectPipelineKeys(state, keys)
	return keys
}

func collectPipelineKeys(state *monitoredPipeline, keys map[string]bool) {
	if state == nil {
		return
	}
	keys[pipelineKey(state.Pipeline)] = true
	for _, child := range state.Children {
		collectPipelineKeys(child, keys)
	}
}
