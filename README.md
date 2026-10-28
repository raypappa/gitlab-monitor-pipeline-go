# GitLab Pipeline Monitor

GitLab Pipeline Monitor is a Go command-line application for inspecting a GitLab pipeline and its downstream child pipelines. It can monitor the latest pipeline for a branch or a specific pipeline ID, poll running pipelines, print a JSON snapshot, retrieve job traces, and retry jobs from its bounded terminal interface.

## Features

- Monitor a branch's latest pipeline or target a pipeline by ID.
- Recursively include downstream child pipelines, including pipelines in other projects.
- Display a bounded interactive TUI in terminals, or plain text, compact, live, wait, or JSON output elsewhere.
- Return a non-zero status when a pipeline or required job fails.
- Expand and collapse child pipelines while navigating a scrollable pipeline/job tree.
- View selected pipeline or job details, job traces, save traces, and retry root or downstream jobs.
- Summarize CI job-duration usage by group, project, and job.

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
pipeline-monitor --pipeline-id 12345 --live
pipeline-monitor --pipeline-id 12345 --live --no-tui
pipeline-monitor --pipeline-id 12345 --output json
```

Use `--help` to view the command's available flags.

### CI Usage

The `usage` subcommand reports GitLab's monthly compute-minute totals by project, plus a project/job breakdown of job durations. It includes projects in subgroups. The per-job rows include duration totals and per-run mean, median, nearest-rank p95, and maximum duration. When GitLab's hosted-runner tag and runner type identify a documented cost factor, the report shows a **known-run estimate** using GitLab's [compute-minute formula and cost factors](https://docs.gitlab.com/ci/pipelines/compute_minutes/). Untagged instance-runner jobs on GitLab.com use the documented default small Linux factor of 1; the same assumption is not made for other GitLab hosts. Each row shows how many runs were estimated and how many were not; legacy cache entries and other runs without recognized runner metadata are not estimated. Use `--refresh` to ask GitLab for runner metadata again where it is still available. Estimates assume a standard project and do not apply open-source or community-contribution discounts or historical factor changes. GitLab's API does not expose exact compute-minute attribution by job, so these estimates can differ from GitLab's reported usage. Omit `--project` and `--job` to include all non-archived projects and job names in the selected group. Use `--include-archived` to include archived projects. The GitLab-reported project compute totals are authoritative monthly values from the [project monthly usage GraphQL query](https://docs.gitlab.com/api/graphql/reference/queries/#ciminutesprojectmonthlyusage), are integer minutes, and are not split by job. They are available for complete calendar-month windows and the default month-to-date window; partial custom ranges show an unavailable notice. `--job` filters the job rows and estimates, but does not change the GitLab project compute totals.

```bash
# Top 25 job/project rows across a group, ordered by total minutes (the default)
pipeline-monitor usage --group my-group

# All jobs in one project; return every row
pipeline-monitor usage --group my-group --project my-group/my-project --limit 0

# One job across the whole group
pipeline-monitor usage --group my-group --job build

# Filter multiple projects/jobs, change sort, and return the top 10
pipeline-monitor usage --group my-group --project my-group/service-a --project my-group/service-b \
  --job build --job test \
  --sort runs --order desc --limit 10

# Choose an inclusive date window and machine-readable output
pipeline-monitor usage --group my-group --since 2026-10-01 --until 2026-10-31 --output json
pipeline-monitor usage --group my-group --since 2026-10-01 --output csv
```

`--group` is required. `--project` accepts an exact project path or numeric project ID and can be repeated. `--job` accepts an exact job name and can be repeated. Supported sort keys are `minutes` (job duration), `runs`, `project`, `job`, and `latest`; the default is `minutes` descending. `--limit 0` disables the default 25-row limit. JSON includes the database path, date window, GitLab-reported project compute totals, known-run estimate fields and coverage counts, job-duration statistics, quota summary, sync/import counts, and total/returned row counts. CSV contains job rows on stdout and prints its summary to stderr.

Job data and project sync progress are stored in SQLite at the operating system's user cache location by default. Use `--db` to select another database file. On each run, the command imports recognized legacy JSON page caches from the old usage cache directory before syncing; `--legacy-cache-dir` selects another import directory (`--cache-dir` remains as a deprecated alias). Imported cache pages are tracked by file path, modification time, and size, so unchanged pages are not re-imported. The importer leaves the legacy files in place.

Keyset pagination keeps page cursors stable and avoids offset-page shifts as new jobs arrive. Successful pages are saved as they are fetched so an incomplete historical scan can resume. Recent jobs are refreshed at the `--cache-ttl` interval (five minutes by default); a full historical scan is refreshed every `--historical-cache-ttl` (30 days by default). Use `--refresh` to force a full sync for selected projects, `--no-cache` to force a full sync without using legacy caches, `--cache-ttl 0s` to refresh recent jobs every run, or `--historical-cache-ttl 0s` to rescan history every run. The database contains job names, durations, statuses, and creation timestamps; it does not contain the GitLab token. GitLab `429`, `408`, and `5xx` responses are retried up to four times, honoring `Retry-After` when present; transient network timeouts are retried once. Usage scanning uses a low request concurrency to reduce rate-limit pressure.

Job-duration breakdowns sum GitLab job durations, not pipeline wall-clock times. Retried jobs count as separate runs. Running jobs count when GitLab provides a duration, so duration totals can increase while they run. GitLab-reported compute minutes are taken from its monthly project-usage GraphQL API.

Interactive terminals use a full-screen Bubble Tea interface with a bounded viewport, so large job lists and deeply nested downstream pipelines do not continuously print lines. The root pipeline is expanded initially; child pipelines can be expanded or collapsed independently. Wide terminals show a details pane beside the tree, while narrow terminals prioritize the tree.

Use `up`/`down` or `j`/`k` to move, `home` and `end` to jump, `enter` to toggle a pipeline, `l` to view a selected job trace, `s` to save it as `job-<id>.txt`, `r` to retry it, and `q` or `ctrl+c` to quit. Trace viewing is bounded and scrollable with `j`/`k`; `esc` returns to the tree. Pipeline and job details include the owning project, IDs, status, duration-related metadata, and web URL where available.

`--wait` enables live polling until all pipelines finish. `--live` refreshes status using the configured polling interval, which defaults to three seconds. In the TUI, API refreshes run as Bubble Tea commands so keyboard input remains responsive. `--no-tui` forces the existing plain-text renderer even when stdout is a terminal. Redirected stdout and CI always use plain text without terminal control sequences.

`--compact` prints a condensed non-interactive status view. `--output json` prints one snapshot and cannot be combined with `--live`, `--wait`, or `--compact`. Downstream traversal remains enabled by default and can be disabled with `--downstream=false`.

For example, a pipeline with dozens of jobs and several nested child pipelines initially shows the root jobs and direct child pipeline rows. Select a child and press `enter` to inspect its jobs; collapse it again to return to the compact tree without losing descendant expansion state.

## Authentication and Configuration

Set one of these environment variables with a suitable GitLab token before running the CLI:

- `GITLAB_TOKEN`
- `GITLAB_PRIVATE_TOKEN`

The CLI uses `GITLAB_TOKEN` when both variables are set. The token must be able to read the root and downstream projects, pipelines, jobs, and bridges. The `usage` subcommand also needs permission to list projects in the selected group and read jobs from each included project. The quota summary requires permission to read the group and its monthly CI-minute usage. The application sends the token to GitLab using the `PRIVATE-TOKEN` request header.

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
- The TUI does not appear: it is selected only for interactive stdout and is disabled by `--no-tui`, `--compact`, and `--output json`. Use `--no-tui` for deterministic plain text in a terminal.

## Contributing

No repository-specific contribution guide or issue tracker URL is included. Before proposing a change, run `gofmt` on changed Go files, `go vet ./...`, and `go test ./...`. Keep changes focused on the Go module and its command under `cmd/pipeline-monitor`.

## License

No license file or license declaration is present in this repository.
