#!/usr/bin/env bash
set -euo pipefail

smoke_dir=$(mktemp -d)
trap 'rm -rf -- "$smoke_dir"' EXIT

uv build --no-sources --wheel --out-dir "$smoke_dir/dist"
uv venv --python "${1:-3.11}" "$smoke_dir/venv"
uv pip install --python "$smoke_dir/venv/bin/python" --no-deps "$smoke_dir"/dist/aqute-*.whl
cp examples/quickstart.py "$smoke_dir/quickstart.py"

cd "$smoke_dir"
venv/bin/python -I -c 'import pathlib, sys, aqute; path = pathlib.Path(aqute.__file__).resolve(); assert path.is_relative_to(pathlib.Path(sys.prefix).resolve()), path; print(sys.version); print(path)'
venv/bin/python -I quickstart.py
