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
uv run --locked python -m examples.retry_progress
uv run --locked python -m examples.http_client
uv run --locked python -m examples.llm_inference
uv run --locked python -m examples.request_pool
uv run --locked python -m examples.manual_drain
uv run --locked python -m examples.service_shutdown
```

`make check` runs Ruff formatting and lint checks, ty, pytest with coverage, and a
strict documentation build.
The example tests verify returned values, failures, and cleanup. Entrypoint checks
execute `quickstart`, `streaming`, `retry_progress`, `http_client`, `llm_inference`, `request_pool`,
and `manual_drain`.
Commands use `uv.lock`; update dependencies with `uv lock --upgrade` and verify them with
`make check`. CI tests Python 3.11, 3.12, 3.13, 3.14, and 3.15.

`make wheel-smoke SMOKE_PYTHON=3.15` builds a wheel, installs it without dependencies
in a temporary virtual environment, and runs the quickstart outside the checkout.
The probe checks the import path and expected processing results. Python runs in
isolated mode to exclude the checkout and user site packages. CI runs this check
on every supported Python version in addition to the source tests.

Release CI checks the candidate, sets its version from the published GitHub release
tag (optional `v` prefix), builds with `uv_build`, and publishes through GitHub's
trusted PyPI identity. See [LICENSE](https://github.com/insomnes/aqute/blob/main/LICENSE).

The site uses [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)
with system fonts. Its dependencies are included in the locked development environment.
Run commands from the repository root. `make docs` builds the site in `site/`;
`uv run --locked mkdocs serve` provides a local preview at
`http://127.0.0.1:8000/aqute/`. The Markdown files contain full copies of the source
in `examples/`, so the code is also visible when reading the files on GitHub.
HTML `example` and `/example` comments mark each generated block; the opening
comment names its source path, such as `examples/quickstart.py`. Keep each marker
on its own line. Edit the Python source, then run `make sync-examples` to update
every marked copy in `README.md` and `docs/**/*.md`. Text outside those blocks is
preserved. `make check-examples` checks without writing and reports outdated
copies, missing sources, or malformed markers. Both `make docs` and `make check`
run it, including in CI.

## API documentation

The [API reference](api/index.md) uses
[mkdocstrings-python](https://mkdocstrings.github.io/python/) to read the local
`aqute/` source. Keep types and defaults in Python signatures. Write Google-style
docstrings with a short summary and `Args:`, `Returns:`, and `Raises:` sections
where applicable. Describe each parameter, return value, and relevant error
condition. Document public fields with a docstring immediately after the
annotated assignment. Use backticks for identifiers.

Put constructor parameter documentation in `__init__`; the renderer merges it
into the class entry. For `AquteTask`, keep the construction example and explicit
field entries instead of displaying the generated dataclass constructor, which
also contains private parameters.

To add an API entry:

1. Complete the symbol's docstring and annotations in Python.
2. Add a `:::` directive to the appropriate page under `docs/api/`. Use its public
   import path, such as `aqute.Aqute`.
3. Select public members with an explicit `members` list. Use `members: []` for
   entries without members. Keep worker internals and inherited implementation
   methods excluded.
4. Add new pages to `mkdocs.yml` and the API landing page. Link longer behavioral
   explanations to `usage.md` or `reference.md`; preserve existing guide anchors.
5. Run `make docs` and `make check`. Preview `/aqute/api/` with `mkdocs serve` and
   check signatures, links, search results, and narrow-screen layout.

Internal anchor validation is set to `warn`, which fails strict builds for broken
anchor links. The reference describes the build checkout; production builds
continue to follow `main`.

## Documentation publication

The documentation site is [insomnes.github.io/aqute](https://insomnes.github.io/aqute/).
The [Documentation workflow](https://github.com/insomnes/aqute/actions/workflows/docs.yml)
runs `make docs` with the locked development environment on pull requests and
pushes to `main`. Pull requests only build the site. Successful builds from `main`
in `insomnes/aqute` upload `site/` and deploy it through the `github-pages`
environment. Only the deployment job receives Pages and OIDC write permissions.
Runs for the same Git ref are serialized without cancelling an active run.

Before the first deployment, a repository administrator must select **GitHub
Actions** under **Settings > Pages > Build and deployment > Source**. Configure the
`github-pages` environment to allow deployments only from the `main` branch.
See GitHub's [publishing source instructions](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site).

To redeploy, open the Documentation workflow, select **Run workflow**, and choose
`main`. Manual runs from other refs skip both jobs. The deployment job links to
the published site. After deployment, check the homepage, navigation, example
pages, and static assets under `/aqute/`. `make docs` verifies the local build;
it does not verify publication.

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
