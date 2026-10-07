"""Compatibility import for code that still imports the old module name."""

from similarity_completed.opensearch.DatabaseConnection import DatabaseConnection

Database = DatabaseConnection

__all__ = ["Database", "DatabaseConnection"]
