import sys
from pathlib import Path


def main() -> None:
    """Run a generated portable project file in the requested directory."""

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python decode_project.py PROJECT_FILE [TARGET]")
    generated_file = Path(sys.argv[1]).resolve()
    target = sys.argv[2] if len(sys.argv) > 2 else "."
    sys.argv = [str(generated_file), target]
    exec(compile(generated_file.read_text(), str(generated_file), "exec"), {"__name__": "__main__"})


if __name__ == "__main__":
    main()
