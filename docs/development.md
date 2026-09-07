# Development

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```bash
uv sync --locked
make check
make format
make build
make docs
uv run --locked python -m examples.quickstart
uv run --locked python -m examples.streaming
uv run --locked python -m examples.http_client
uv run --locked python -m examples.service_shutdown
```

`make check` runs Ruff formatting and lint checks, ty, pytest, and a strict documentation build.
The example tests verify returned values, failures, cleanup, and executable entry points. Commands use `uv.lock`; update dependencies with `uv lock --upgrade`
and verify them with `make check`. CI tests Python 3.11, 3.12, 3.13, and 3.14.

Release CI checks the candidate, sets its version from the published GitHub release
tag (optional `v` prefix), builds with `uv_build`, and publishes through GitHub's
trusted PyPI identity. See [LICENSE](https://github.com/insomnes/aqute/blob/main/LICENSE).

Run commands from the repository root. `make docs` builds the site in `site/`;
`uv run --locked mkdocs serve` provides a local preview. The documentation includes
the actual files in `examples/`. Missing source files fail the build. No site
deployment is configured.
