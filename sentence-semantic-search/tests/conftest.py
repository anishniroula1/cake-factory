import os

os.environ.setdefault("OPENSEARCH_HOST", "search.example.test")
os.environ.setdefault("OPENSEARCH_IVF_NPROBES_ENABLED", "true")
os.environ.setdefault("OPENSEARCH_IVF_NPROBES", "64")
os.environ.setdefault(
    "OPENSEARCH_ASYLEE_CATALOG_ALIAS",
    "sentence-semantic-catalog-asylee-test",
)
os.environ.setdefault(
    "OPENSEARCH_REFUGEE_CATALOG_ALIAS",
    "sentence-semantic-catalog-refugee-test",
)
