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
    ".venv",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
}


def portable_files() -> dict[str, str]:
    files = {}
    for path in sorted(PROJECT_ROOT.rglob("*")):
        if not path.is_file() or path.name == GENERATOR.name or path.suffix == ".pyc":
            continue
        relative_path = path.relative_to(PROJECT_ROOT)
        if any(
            part in EXCLUDED_PARTS or part.endswith(".egg-info")
            for part in relative_path.parts
        ):
            continue
        files[relative_path.as_posix()] = path.read_text(encoding="utf-8")
    return files


def refresh_generator() -> None:
    snapshot = json.dumps(portable_files(), sort_keys=True, separators=(",", ":"))
    encoded = base64.b64encode(zlib.compress(snapshot.encode("utf-8"), level=9)).decode(
        "ascii"
    )
    constant = (
        "PROJECT_FILES = (\n"
        + "\n".join(f'    "{chunk}"' for chunk in textwrap.wrap(encoded, width=88))
        + "\n)"
    )

    source = GENERATOR.read_text(encoding="utf-8")
    refreshed, replacements = re.subn(
        r"PROJECT_FILES = \(\n.*?\n\)",
        constant,
        source,
        count=1,
        flags=re.DOTALL,
    )
    if replacements != 1:
        raise RuntimeError("Could not locate PROJECT_FILES in generate_project.py")
    GENERATOR.write_text(refreshed, encoding="utf-8")


if __name__ == "__main__":
    refresh_generator()
    print(f"Refreshed {GENERATOR}")
