"""Build a portable generate_project.py for a project folder.

Create the smaller Base64 version:

    python3 build_generator.py FOLDER_PATH

Create the English-word version instead:

    python3 build_generator.py FOLDER_PATH --words

The command creates or replaces generate_project.py inside FOLDER_PATH. Copy
it anywhere and run it to recreate the project:

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

BASE64_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
ENCODING_WORDS = (
    "apple",
    "bridge",
    "cloud",
    "dream",
    "earth",
    "flame",
    "garden",
    "harbor",
    "island",
    "jungle",
    "kite",
    "lemon",
    "meadow",
    "night",
    "ocean",
    "pebble",
    "quiet",
    "river",
    "stone",
    "tree",
    "unity",
    "valley",
    "water",
    "xenon",
    "yellow",
    "zebra",
    "amber",
    "breeze",
    "cedar",
    "dawn",
    "ember",
    "forest",
    "grape",
    "hill",
    "ivory",
    "joy",
    "kingdom",
    "lake",
    "moon",
    "north",
    "olive",
    "pine",
    "queen",
    "rain",
    "sun",
    "trail",
    "urban",
    "violet",
    "willow",
    "xray",
    "youth",
    "zenith",
    "acorn",
    "brook",
    "coral",
    "dune",
    "elm",
    "frost",
    "grove",
    "honey",
    "iris",
    "jade",
    "kelp",
    "lotus",
    "padding",
)
WORDS_PER_LINE = 20

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


def encode_as_words(data: bytes) -> list[str]:
    """Convert bytes to a reversible list of English words."""

    character_to_word = dict(zip(BASE64_CHARACTERS, ENCODING_WORDS, strict=True))
    encoded = base64.b64encode(data).decode("ascii")
    return [character_to_word[character] for character in encoded]


def decode_words(words: list[str]) -> bytes:
    """Convert words created by encode_as_words back to bytes."""

    word_to_character = dict(zip(ENCODING_WORDS, BASE64_CHARACTERS, strict=True))
    encoded = "".join(word_to_character[word] for word in words)
    return base64.b64decode(encoded)


def format_word_lines(words: list[str]) -> str:
    """Place a small number of words on each generated-file line."""

    return "\n".join(
        " ".join(words[start : start + WORDS_PER_LINE])
        for start in range(0, len(words), WORDS_PER_LINE)
    )


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


def build_base64_generator(project_root: Path) -> Path:
    """Create a compact Base64 generator inside the selected project."""

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


def build_word_generator(project_root: Path) -> Path:
    """Create an English-word generator inside the selected project."""

    project_files = read_project_files(project_root)
    if not project_files:
        raise ValueError(f"No portable project files found in {project_root}")

    project_json = json.dumps(project_files, sort_keys=True, separators=(",", ":"))
    compressed = zlib.compress(project_json.encode("utf-8"), level=9)
    project_words = format_word_lines(encode_as_words(compressed))
    encoding_words = " ".join(ENCODING_WORDS)

    generator_code = f'''import base64
import json
import sys
import zlib
from pathlib import Path

PROJECT_NAME = {project_root.name!r}
BASE64_CHARACTERS = {BASE64_CHARACTERS!r}
ENCODING_WORDS = {encoding_words!r}.split()

# Compressed project data represented by reversible English words.
PROJECT_WORDS = """\
{project_words}
"""


def read_project_files() -> dict:
    """Convert the words back into the original project files."""

    word_to_character = dict(zip(ENCODING_WORDS, BASE64_CHARACTERS, strict=True))
    encoded = "".join(word_to_character[word] for word in PROJECT_WORDS.split())
    compressed = base64.b64decode(encoded)
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


def build_generator(project_root: Path, use_words: bool = False) -> Path:
    """Create the requested generator format."""

    if use_words:
        return build_word_generator(project_root)
    return build_base64_generator(project_root)


def main() -> None:
    """Validate FOLDER_PATH and build its generator."""

    if len(sys.argv) not in {2, 3} or (len(sys.argv) == 3 and sys.argv[2] != "--words"):
        raise SystemExit("Usage: python3 build_generator.py FOLDER_PATH [--words]")

    project_root = Path(sys.argv[1]).expanduser().resolve()
    if not project_root.is_dir():
        raise SystemExit(f"Project folder does not exist: {project_root}")

    output_path = build_generator(project_root, use_words="--words" in sys.argv[2:])
    print(f"Generated: {output_path}")


if __name__ == "__main__":
    main()
