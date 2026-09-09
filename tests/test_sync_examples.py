import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def checkout(tmp_path):
    (tmp_path / "scripts").mkdir()
    (tmp_path / "examples").mkdir()
    (tmp_path / "docs").mkdir()
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / "scripts/sync_examples.py",
        tmp_path / "scripts/sync_examples.py",
    )
    return tmp_path


def run_sync(checkout, *args):
    return subprocess.run(
        [sys.executable, str(checkout / "scripts/sync_examples.py"), *args],
        cwd=checkout,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )


def test_sync_checks_and_updates_all_copies_without_changing_prose(checkout):
    """CLI checks must detect drift without writing; sync updates every marked copy."""
    source = checkout / "examples/demo.py"
    source.write_text("run()\n")
    block = (
        "<!-- example: examples/demo.py -->\n"
        "```python\nstale()\n```\n"
        "<!-- /example -->\n"
    )
    documents = {
        checkout / "README.md": "# Intro\n\n" + block + "\nSecond copy:\n" + block,
        checkout / "docs/demo.md": block + "\nUnmarked:\n```python\nstale()\n```\n",
    }
    for path, text in documents.items():
        path.write_text(text)

    checked = run_sync(checkout, "--check")
    assert checked.returncode == 1
    assert "make sync-examples" in checked.stderr
    assert {path: path.read_text() for path in documents} == documents

    assert run_sync(checkout).returncode == 0
    expected = {
        path: text.replace(block, block.replace("stale()", "run()"))
        for path, text in documents.items()
    }
    assert {path: path.read_text() for path in documents} == expected
    assert run_sync(checkout, "--check").returncode == 0
    assert run_sync(checkout).returncode == 0
    assert {path: path.read_text() for path in documents} == expected

    source.write_text("updated()\n")
    assert run_sync(checkout, "--check").returncode == 1
    assert {path: path.read_text() for path in documents} == expected


@pytest.mark.parametrize("args", [(), ("--check",)])
def test_missing_source_fails_without_replacing_the_copy(checkout, args):
    """A removed or renamed source must fail the command with its path reported."""
    readme = checkout / "README.md"
    text = (
        "<!-- example: examples/missing.py -->\n"
        "```python\nkept()\n```\n<!-- /example -->\n"
    )
    readme.write_text(text)
    result = run_sync(checkout, *args)
    assert result.returncode == 1
    assert "missing.py" in result.stderr
    assert readme.read_text() == text


@pytest.mark.parametrize(
    "text",
    [
        "<!-- example: examples/demo.py -->\n```python\nrun()\n```\n",
        "<!-- /example -->\n",
    ],
)
def test_incomplete_markers_fail_instead_of_silently_skipping(checkout, text):
    """Deleting one marker must not silently remove the example from CI checks."""
    readme = checkout / "README.md"
    readme.write_text(text)
    result = run_sync(checkout, "--check")
    assert result.returncode == 1
    assert "malformed example markers" in result.stderr
    assert readme.read_text() == text
