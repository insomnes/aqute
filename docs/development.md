# Development

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```bash
uv sync --locked
make check
make format
make build
make docs
make wheel-smoke SMOKE_PYTHON=3.11
uv run --locked python -m examples.quickstart
uv run --locked python -m examples.streaming
uv run --locked python -m examples.http_client
uv run --locked python -m examples.service_shutdown
```

`make check` runs Ruff formatting and lint checks, ty, pytest with coverage, and a
strict documentation build.
The example tests verify returned values, failures, cleanup, and executable entry points. Commands use `uv.lock`; update dependencies with `uv lock --upgrade`
and verify them with `make check`. CI tests Python 3.11, 3.12, 3.13, and 3.14.

`make wheel-smoke SMOKE_PYTHON=3.14` builds a wheel, installs it without dependencies
in a temporary virtual environment, and runs the quickstart outside the checkout.
The probe checks the import path and expected processing results. Python runs in
isolated mode to exclude the checkout and user site packages. CI runs this check
on every supported Python version in addition to the source tests.

Release CI checks the candidate, sets its version from the published GitHub release
tag (optional `v` prefix), builds with `uv_build`, and publishes through GitHub's
trusted PyPI identity. See [LICENSE](https://github.com/insomnes/aqute/blob/main/LICENSE).

Run commands from the repository root. `make docs` builds the site in `site/`;
`uv run --locked mkdocs serve` provides a local preview. The documentation includes
the actual files in `examples/`. Missing source files fail the build. No site
deployment is configured.

## Coverage

`make coverage` runs the tests with line and branch coverage of `aqute/`. It prints
missing lines and branches and generates these files under `reports/coverage/`:

- `coverage.xml` and `coverage.json` for automated consumers.
- `html/index.html` for per-file inspection.
- `coverage.svg`, generated locally by `genbadge` from the measured XML report.

The badge percentage combines covered lines and branches. Tests and examples are
excluded from the coverage denominator. The main pytest process is measured;
the subprocess quickstart checks verify wheel and entrypoint behavior separately.
`make test` remains available for an uninstrumented run. No minimum percentage is
enforced; use missing coverage to investigate supported behavior.

CI collects coverage on Python 3.11 and uploads the report and badge together as
`coverage-python-3.11`. Open a [CI run](https://github.com/insomnes/aqute/actions/workflows/ci.yml)
and download its artifact to view them. Each run regenerates its badge; there is
no checked-in percentage or external coverage account. Reports follow GitHub's
artifact retention policy. Local files are overwritten by `make coverage`.
