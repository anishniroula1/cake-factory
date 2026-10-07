"""Compatibility import for code that still imports the old module name."""

from sentence_similarity.DatabaseConnection import DatabaseConnection

Database = DatabaseConnection

__all__ = ["Database", "DatabaseConnection"]
