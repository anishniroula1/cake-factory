"""Create a portable project generator whose payload uses English words.

Build it:
    python3 build_word_generator.py PROJECT_FOLDER

Recreate the project:
    python3 PROJECT_FOLDER/generate_word_project.py [DESTINATION]

The words are reversible encoded data, not meaningful sentences. The real
.env is excluded, and .env.example becomes .env in the recreated project.
"""

import base64
import json
import sys
import zlib
from pathlib import Path

from build_generator import read_project_files

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
OUTPUT_NAME = "generate_word_project.py"


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


def build_word_generator(project_root: Path) -> Path:
    """Create a standalone word-based generator for one project."""

    project_files = read_project_files(project_root)
    project_files.pop(OUTPUT_NAME, None)
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

    generator_copy = project_directory / {OUTPUT_NAME!r}
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

    output_path = project_root / OUTPUT_NAME
    output_path.write_text(generator_code, encoding="utf-8")
    return output_path


def main() -> None:
    """Validate the project path and create its word-based generator."""

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 build_word_generator.py PROJECT_FOLDER")

    project_root = Path(sys.argv[1]).expanduser().resolve()
    if not project_root.is_dir():
        raise SystemExit(f"Project folder does not exist: {project_root}")

    output_path = build_word_generator(project_root)
    print(f"Generated: {output_path}")


if __name__ == "__main__":
    main()
