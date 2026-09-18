# GitLab Pipeline Monitor

GitLab Pipeline Monitor is a Go command-line application for inspecting a GitLab pipeline and its downstream child pipelines. It can monitor the latest pipeline for a branch or a specific pipeline ID, poll running pipelines, print a JSON snapshot, retrieve job traces, and retry jobs from its interactive text interface.

## Features

- Monitor a branch's latest pipeline or target a pipeline by ID.
- Recursively include downstream child pipelines, including pipelines in other projects.
- Display text, compact, live, wait, or JSON output.
- Return a non-zero status when a pipeline or required job fails.
- View or save job traces and retry selected root or downstream jobs from the text-mode menu.

## Getting the Code

This checkout does not have a configured Git remote, so the repository does not provide a verified clone URL. Obtain the repository through the Git source used by your team, then run the commands below from the repository root. The project has no submodules or other checkout steps represented in the repository.

## Prerequisites

- Go `1.27.0`. The required version is pinned in `mise.toml` and `mise.lock`.
- Access to a GitLab instance and a token that can read the root and downstream projects, pipelines, jobs, and bridges.

The repository also configures `pre-commit` as a development tool in `mise.toml`. It is needed only if you want to install and run the repository's local hooks.

Verify Go before building:

```bash
go version
```

## Setup and Usage

The Go toolchain downloads module dependencies when you build or run the application. From the repository root, run the CLI directly with:

```bash
go run ./cmd/pipeline-monitor --pipeline-id 12345 --compact
```

The executable is named `pipeline-monitor`. A minimal set of useful invocations is:

```bash
pipeline-monitor --pipeline-id 12345 --live --compact
pipeline-monitor --pipeline-id 12345 --live --compact --downstream=false
pipeline-monitor --branch main --live
pipeline-monitor --pipeline-id 12345 --output json
```

Use `--help` to view the command's available flags.

`--wait` enables live polling until all pipelines finish. `--live` refreshes status using the configured polling interval, which defaults to three seconds. `--compact` prints a condensed status view. `--output json` prints one snapshot and cannot be combined with `--live`, `--wait`, or `--compact`.

When a text-mode pipeline finishes, the application can offer actions to view or save a selected job trace, retry a selected job, or exit. Saved traces use restrictive file permissions. Compact mode and JSON output skip this interactive menu.

## Authentication and Configuration

Set one of these environment variables with a suitable GitLab token before running the CLI:

- `GITLAB_TOKEN`
- `GITLAB_PRIVATE_TOKEN`

The CLI uses `GITLAB_TOKEN` when both variables are set. The token must be able to read the root and downstream projects, pipelines, jobs, and bridges. The application sends the token to GitLab using the `PRIVATE-TOKEN` request header.

`GITLAB_HOST` changes the GitLab host URL. If it is not set, the default is `https://gitlab.com`.

Project and branch selection use these fallbacks:

- Project: `--project`, then `CI_PROJECT_PATH`, then the `origin` Git remote.
- Branch: `--branch`, then the current Git branch when the project is resolved from the current checkout, otherwise the GitLab project's default branch.

The `--host` flag can also set the GitLab host URL. The `--interval` flag controls the live polling interval.

## Build, Test, and Deploy

Build the binary with the repository's `mise` task:

```bash
mise run build
```

This creates the ignored artifact `build/pipeline-monitor`. The equivalent direct Go command is:

```bash
go build -o build/pipeline-monitor ./cmd/pipeline-monitor
```

Run the full test suite and static checks with:

```bash
go test ./...
go vet ./...
```

Run one focused test with:

```bash
go test ./cmd/pipeline-monitor -run TestName
```

The repository provides a local deployment task that copies an already-built binary to `$HOME/.local/bin/pipeline-monitor`:

```bash
mise run local-deploy
```

Run `mise run build` first. No production deployment configuration is included in this repository.

## Development

The executable and its tests are under `cmd/pipeline-monitor`. Install the repository hooks once if `pre-commit` is available:

```bash
pre-commit install
```

The configured hooks format staged Go files with `gofmt`, then run `go vet ./...` and `go test ./...`.

## Troubleshooting

- `GITLAB_TOKEN or GITLAB_PRIVATE_TOKEN is required`: set one of the authentication environment variables before running the CLI.
- `project is required`: provide `--project`, set `CI_PROJECT_PATH`, or run from a checkout with an `origin` remote that identifies the GitLab project.
- `--output json cannot be used with --live, --wait, or --compact`: use JSON for a single snapshot, or use text output for polling and compact display.
- GitLab API permission errors: ensure the token can read the root and every downstream project, including pipelines, jobs, and bridges. Cross-project downstream traversal requires access to those projects.
- The interactive menu does not appear: text mode must be non-compact and the pipeline must have traceable jobs. Use `--compact` or `--output json` intentionally when non-interactive output is required.

## Contributing

No repository-specific contribution guide or issue tracker URL is included. Before proposing a change, run `gofmt` on changed Go files, `go vet ./...`, and `go test ./...`. Keep changes focused on the Go module and its command under `cmd/pipeline-monitor`.

## License

No license file or license declaration is present in this repository.
