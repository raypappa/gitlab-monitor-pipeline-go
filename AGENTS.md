# Agent Guidance

## Tooling and Commands

- Use Go `1.27.0`; the version is pinned in `mise.toml` and `mise.lock`.
- This is a single Go module. The executable and its tests are under `cmd/pipeline-monitor`.
- Run `go test ./...` for the full test suite; run `go test ./cmd/pipeline-monitor -run TestName` for a focused test.
- Run `go vet ./...` for static verification and `gofmt -w` on changed Go files before finishing.
- `mise run build` creates the ignored binary at `build/pipeline-monitor`; `mise run local-deploy` copies that binary to `$HOME/.local/bin/pipeline-monitor` and requires the build first.
- Pre-commit runs in this order: `gofmt`, `go vet ./...`, then `go test ./...`; install hooks with `pre-commit install`.

## Runtime Constraints

- CLI API calls require `GITLAB_TOKEN` or `GITLAB_PRIVATE_TOKEN`; `GITLAB_HOST` overrides the default `https://gitlab.com` host.
- Project resolution is `--project`, then `CI_PROJECT_PATH`, then the `origin` Git remote. Branch resolution normally uses `--branch`, then the current Git branch; otherwise the GitLab project default branch is queried.
- The CLI recursively includes downstream pipelines by default and can cross project boundaries, so API tokens must read root/downstream projects, pipelines, jobs, and bridges.
- Text mode can enter an interactive log/retry menu after completion. Use `--compact` or `--output json` in automation; JSON cannot be combined with `--live`, `--wait`, or `--compact`.
