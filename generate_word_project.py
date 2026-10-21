import base64
import json
import sys
import zlib
from pathlib import Path

PROJECT_NAME = 'old-doc2vec'
BASE64_CHARACTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/='
ENCODING_WORDS = 'apple bridge cloud dream earth flame garden harbor island jungle kite lemon meadow night ocean pebble quiet river stone tree unity valley water xenon yellow zebra amber breeze cedar dawn ember forest grape hill ivory joy kingdom lake moon north olive pine queen rain sun trail urban violet willow xray youth zenith acorn brook coral dune elm frost grove honey iris jade kelp lotus padding'.split()

# Compressed project data represented by reversible English words.
PROJECT_WORDS = """

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

    generator_copy = project_directory / 'generate_word_project.py'
    generator_copy.write_text(
        Path(__file__).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return project_directory


if __name__ == "__main__":
    destination = sys.argv[1] if len(sys.argv) > 1 else "."
    output = write_project(destination)
    print(f"Created project: {output}")
