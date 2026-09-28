import base64
import json
import zlib
from pathlib import Path

PROJECT_NAME = "doc2vec-service"
OUTPUT_FILE = "doc2vec_service_project.py"
SKIP_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "htmlcov",
}
SKIP_FILES = {".env", ".coverage", OUTPUT_FILE}


def projectFiles(root: Path) -> dict:
    """Read every portable project file into a JSON-safe dictionary."""

    files = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(
            part in SKIP_PARTS or part.endswith(".egg-info")
            for part in relative.parts
        ):
            continue
        if relative.name in SKIP_FILES:
            continue
        files[str(relative)] = base64.b64encode(path.read_bytes()).decode("ascii")
    return files


def generatedSource(payload: str) -> str:
    """Create one Python file that can rebuild the standalone project."""

    return f'''import base64
import json
import sys
import zlib
from pathlib import Path

PROJECT_NAME = {PROJECT_NAME!r}
PAYLOAD = {payload!r}


def main():
    targetRoot = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    projectRoot = targetRoot / PROJECT_NAME
    files = json.loads(zlib.decompress(base64.b64decode(PAYLOAD)))
    for relativeName, encoded in files.items():
        destination = projectRoot / relativeName
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(base64.b64decode(encoded))
    print(f"Created {{projectRoot}} with {{len(files)}} files")


if __name__ == "__main__":
    main()
'''


def main():
    """Write the self-extracting project beside this generator."""

    root = Path(__file__).resolve().parent
    raw = json.dumps(projectFiles(root), separators=(",", ":")).encode("utf-8")
    payload = base64.b64encode(zlib.compress(raw, level=9)).decode("ascii")
    output = root / OUTPUT_FILE
    output.write_text(generatedSource(payload), encoding="utf-8")
    print(f"Created {output}")


if __name__ == "__main__":
    main()
