# GitLab Pipeline Monitor

A small Go CLI for monitoring a GitLab pipeline and its downstream child pipelines. It preserves the useful status modes from `glab ci status` and adds explicit pipeline-ID targeting with recursive downstream traversal.

## Install

```bash
go install ./cmd/pipeline-monitor
```

## Authentication

Set `GITLAB_TOKEN` or `GITLAB_PRIVATE_TOKEN`. The token needs permission to read the root project, downstream projects, pipelines, jobs, and bridges.

## Examples

```bash
pipeline-monitor --pipeline-id 12345 --live --compact
pipeline-monitor --pipeline-id 12345 --live --compact --downstream=false
pipeline-monitor --branch main --live
pipeline-monitor --pipeline-id 12345 --output json
```

When a text-mode pipeline finishes, the monitor offers an interactive action menu. Choose `View logs` or `Save logs`, then select a job to retrieve its GitLab trace output. Saving prompts for a file path and writes the trace with restrictive permissions. Choose `Retry` to rerun a selected job. A successful retry refreshes the pipeline state and resumes monitoring. Use `--compact` or `--output json` to skip the interactive menu.

Project resolution uses `--project`, then `CI_PROJECT_PATH`, then the `origin` GitLab remote. Branch resolution uses `--branch`, then the current Git branch.

## Compatibility

The CLI supports the main status behaviors from `glab ci status`:

- latest pipeline lookup by branch
- explicit pipeline lookup with `--pipeline-id`
- `--live` polling
- `--wait` as non-interactive live polling
- `--compact` output
- `--output json` for a single snapshot
- non-zero exit status when a required job or pipeline fails
- interactive retry for root and downstream jobs

Unlike `glab ci status`, downstream pipelines are included by default and can cross project boundaries.

## Development

Install `pre-commit`, then enable the repository hooks once after cloning:

```bash
pre-commit install
```

The hooks format staged Go files with `gofmt`, then run `go vet ./...` and `go test ./...`.
