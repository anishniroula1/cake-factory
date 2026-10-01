"""Refresh generate_project.py from the current portable project files."""

import base64
import json
import re
import textwrap
import zlib
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
GENERATOR = PROJECT_ROOT / "generate_project.py"
EXCLUDED_PARTS = {
    ".git",
    ".venv",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
}


def portableFiles():
    """Read the text files that belong in a generated standalone copy."""

    files = {}

    for path in sorted(PROJECT_ROOT.rglob("*")):
        if not path.is_file() or path.name == GENERATOR.name:
            continue

        relativePath = path.relative_to(PROJECT_ROOT)

        if any(
            part in EXCLUDED_PARTS or part.endswith(".egg-info")
            for part in relativePath.parts
        ):
            continue

        files[relativePath.as_posix()] = path.read_text(encoding="utf-8")

    return files


def refreshGenerator():
    """Replace the embedded snapshot without changing generator behavior."""

    snapshot = json.dumps(
        portableFiles(),
        sort_keys=True,
        separators=(",", ":"),
    )
    compressed = zlib.compress(snapshot.encode("utf-8"), level=9)
    encoded = base64.b64encode(compressed).decode("ascii")
    constant = (
        "PROJECT_FILES = (\n"
        + "\n".join(
            f"    {chunk!r}"
            for chunk in textwrap.wrap(encoded, width=88)
        )
        + "\n)"
    )

    source = GENERATOR.read_text(encoding="utf-8")
    refreshedSource, replacementCount = re.subn(
        r"PROJECT_FILES = \(\n.*?\n\)",
        constant,
        source,
        count=1,
        flags=re.DOTALL,
    )

    if replacementCount != 1:
        raise RuntimeError("Could not find PROJECT_FILES in generate_project.py")

    GENERATOR.write_text(refreshedSource, encoding="utf-8")


if __name__ == "__main__":
    refreshGenerator()
    print(f"Refreshed {GENERATOR}")
