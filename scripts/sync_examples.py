"""Synchronize marked Markdown blocks with complete examples/*.py files."""

import argparse
import logging
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BLOCK = re.compile(
    r"^<!-- example: (?P<source>examples/[a-z_]+\.py) -->\n"
    r".*?^<!-- /example -->$",
    re.MULTILINE | re.DOTALL,
)


def sync_document(path: Path, *, check: bool) -> bool:
    """Return True when a check finds an outdated block; updates preserve prose."""
    text = path.read_text(encoding="utf-8")
    blocks = list(BLOCK.finditer(text))
    if text.count("<!-- example:") != len(blocks) or text.count(
        "<!-- /example -->"
    ) != len(blocks):
        raise ValueError(f"{path}: malformed example markers")

    def render(match: re.Match[str]) -> str:
        source = match["source"]
        code = (ROOT / source).read_text(encoding="utf-8").rstrip("\n")
        return f"<!-- example: {source} -->\n```python\n{code}\n```\n<!-- /example -->"

    updated = BLOCK.sub(render, text)
    if updated == text:
        return False
    if check:
        logging.error("%s: examples differ; run make sync-examples", path)
        return True
    path.write_text(updated, encoding="utf-8")
    logging.info("Updated %s", path)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail without writing")
    args = parser.parse_args()
    failed = False
    try:
        for path in [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))]:
            if sync_document(path, check=args.check):
                failed = True
    except (OSError, ValueError) as error:
        logging.error("%s", error)
        return 1
    return int(failed)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
