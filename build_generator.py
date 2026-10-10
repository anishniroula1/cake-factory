"""Build a portable generate_project.py for any project folder.

Run this file from the workspace root and pass the project folder:

    python3 build_generator.py sentence-similarity-service

An absolute path also works:

    python3 build_generator.py /path/to/my-project

The command creates or replaces generate_project.py inside the selected
project. Copy that generated file anywhere and run it to recreate the project:

    python3 generate_project.py
    python3 generate_project.py /optional/destination

The real .env file is never included. When .env.example exists, its safe
contents become .env in the recreated project.
"""

import base64
import json
import sys
import textwrap
import zlib
from pathlib import Path

EXCLUDED_NAMES = {
    ".git",
    ".idea",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build_generator.py",
    "build_word_generator.py",
    "generate_project.py",
    "generate_word_project.py",
}


def read_project_files(project_root: Path) -> dict[str, str]:
    """Read the portable text files that belong to one project."""

    files = {}

    for path in sorted(project_root.rglob("*")):
        if not path.is_file():
            continue

        relative_path = path.relative_to(project_root)
        if any(part in EXCLUDED_NAMES for part in relative_path.parts):
            continue

        # Never place real environment credentials inside the generated file.
        if relative_path.as_posix() == ".env":
            continue

        try:
            files[relative_path.as_posix()] = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as error:
            raise ValueError(
                f"{relative_path} is not a text file and cannot be embedded"
            ) from error

    env_example = project_root / ".env.example"
    if env_example.exists():
        files[".env"] = env_example.read_text(encoding="utf-8")

    return files


def build_generator(project_root: Path) -> Path:
    """Create a standalone generator inside the selected project."""

    project_files = read_project_files(project_root)
    if not project_files:
        raise ValueError(f"No portable project files found in {project_root}")

    project_json = json.dumps(project_files, sort_keys=True, separators=(",", ":"))
    compressed = zlib.compress(project_json.encode("utf-8"), level=9)
    encoded = base64.b64encode(compressed).decode("ascii")
    encoded_parts = "".join(
        f"    {part!r}\n" for part in textwrap.wrap(encoded, width=100)
    )

    generator_code = f'''import base64
import json
import sys
import zlib
from pathlib import Path

PROJECT_NAME = {project_root.name!r}

# Compressed copy of every portable project file.
PROJECT_FILES = (
{encoded_parts})


def read_project_files() -> dict:
    """Read the project files stored inside this generator."""

    compressed = base64.b64decode(PROJECT_FILES)
    content = zlib.decompress(compressed).decode("utf-8")
    return json.loads(content)


def write_project(destination) -> Path:
    """Create the project folder and write every stored file."""

    project_directory = Path(destination).expanduser().resolve() / PROJECT_NAME
    project_directory.mkdir(parents=True, exist_ok=True)

    for relative_path, content in read_project_files().items():
        target = project_directory / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    generator_copy = project_directory / "generate_project.py"
    generator_copy.write_text(
        Path(__file__).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return project_directory


if __name__ == "__main__":
    destination = sys.argv[1] if len(sys.argv) > 1 else "."
    output = write_project(destination)
    print(f"Created project: {{output}}")
'''

    output_path = project_root / "generate_project.py"
    output_path.write_text(generator_code, encoding="utf-8")
    return output_path


def main() -> None:
    """Validate the requested folder and build its generator."""

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 build_generator.py PROJECT_FOLDER")

    project_root = Path(sys.argv[1]).expanduser().resolve()
    if not project_root.is_dir():
        raise SystemExit(f"Project folder does not exist: {project_root}")

    output_path = build_generator(project_root)
    print(f"Generated: {output_path}")


if __name__ == "__main__":
    main()
