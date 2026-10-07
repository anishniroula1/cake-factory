import runpy
import sys
from pathlib import Path


def main():
    """Run a generated self-extracting project file."""

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python decode_project.py PROJECT_FILE [TARGET]")
    generated_file = Path(sys.argv[1]).resolve()
    target = sys.argv[2] if len(sys.argv) > 2 else "."
    sys.argv = [str(generated_file), target]
    runpy.run_path(str(generated_file), run_name="__main__")


if __name__ == "__main__":
    main()
