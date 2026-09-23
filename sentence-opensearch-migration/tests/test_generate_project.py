from pathlib import Path

import generate_project


def test_generator_contains_and_recreates_portable_project(tmp_path):
    embedded = generate_project._read_project_files()
    project_root = Path(__file__).resolve().parent.parent
    excluded = {
        ".venv",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "build",
        "dist",
    }
    portable_paths = {
        path.relative_to(project_root).as_posix()
        for path in project_root.rglob("*")
        if path.is_file()
        and path.name != "generate_project.py"
        and path.suffix != ".pyc"
        and not any(
            part in excluded or part.endswith(".egg-info")
            for part in path.relative_to(project_root).parts
        )
    }

    assert "Dockerfile" in embedded
    assert "docker-compose.yml" in embedded
    assert "sentence_migration/main.py" in embedded
    assert ".venv" not in embedded
    assert set(embedded) == portable_paths
    for relative_path, content in embedded.items():
        assert (project_root / relative_path).read_text(encoding="utf-8") == content

    output = generate_project.write_project(tmp_path)
    assert output == tmp_path / "sentence-opensearch-migration"
    assert (output / "README.md").read_text(encoding="utf-8") == embedded["README.md"]
    assert (output / "generate_project.py").is_file()
