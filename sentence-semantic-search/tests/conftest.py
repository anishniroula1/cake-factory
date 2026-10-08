import os

os.environ.setdefault("OPENSEARCH_HOST", "search.example.test")
os.environ.setdefault("OPENSEARCH_IVF_NPROBES", "64")
os.environ.setdefault(
    "OPENSEARCH_TRACER_FORM_LANGUAGE_ALIAS",
    "tracer-catalog",
)
