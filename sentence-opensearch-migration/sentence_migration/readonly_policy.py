import re

from sentence_migration.models import ReadOnlyMethod

# Any occurrence of these API segments is refused before allow-list matching.
BLOCKED_SEGMENTS = {
    "_bulk",
    "_cache",
    "_close",
    "_delete_by_query",
    "_flush",
    "_forcemerge",
    "_open",
    "_refresh",
    "_reindex",
    "_restore",
    "_rollover",
    "_shrink",
    "_split",
    "_update",
    "_update_by_query",
}

GET_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"^/$",
        r"^/_cat(?:/[a-z0-9_.-]+)?$",
        r"^/_cluster/(?:health|state|stats|settings)(?:/[a-z0-9*._,-]+)?$",
        r"^/_cluster/(?:pending_tasks|allocation/explain)$",
        r"^/_nodes(?:/[a-z0-9*._,-]+)*$",
        r"^/_tasks(?:/[a-z0-9*._:-]+)*$",
        r"^/_stats(?:/[a-z0-9*._,-]+)*$",
        r"^/_(?:alias|aliases)(?:/[a-z0-9*._,-]+)?$",
        r"^/_mapping(?:/[a-z0-9*._,-]+)?$",
        r"^/_settings(?:/[a-z0-9*._,-]+)?$",
        r"^/_resolve/index/[a-z0-9*._,-]+$",
        r"^/_(?:index_template|component_template|ingest/pipeline)(?:/[a-z0-9*._,-]+)?$",
        r"^/_snapshot(?:/[a-z0-9*._,-]+)*$",
        r"^/_(?:remote/info|dangling)$",
        r"^/_(?:scripts|search/pipeline)(?:/[^/]+)?$",
        r"^/_plugins/_(?:ml|knn)/(?:models|tasks|profile|stats)(?:/[a-z0-9*._:-]+)*$",
        r"^/_(?:search|count|field_caps|terms_enum)$",
        r"^/[a-z0-9*._,-]+$",
        r"^/[a-z0-9*._,-]+/_(?:alias|mapping|settings|stats|segments|recovery|shard_stores)(?:/[a-z0-9*._,-]+)*$",
        r"^/[a-z0-9*._,-]+/_(?:search|count|field_caps|terms_enum|validate/query)$",
        r"^/[a-z0-9*._,-]+/_(?:doc|source|explain|termvectors)/[^/]+$",
    )
)

POST_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"^/(?:[a-z0-9*._,-]+/)?_(?:search|msearch|count|mget|field_caps|terms_enum|rank_eval)$",
        r"^/(?:[a-z0-9*._,-]+/)?_msearch/template$",
        r"^/(?:[a-z0-9*._,-]+/)?_search/template$",
        r"^/(?:[a-z0-9*._,-]+/)?_validate/query$",
        r"^/(?:[a-z0-9*._,-]+/)?_analyze$",
        r"^/_cluster/allocation/explain$",
        r"^/_render/template$",
        r"^/_search/scroll$",
        r"^/[a-z0-9*._,-]+/_(?:explain|termvectors)/[^/]+$",
    )
)


def validate_read_only_request(method: ReadOnlyMethod | str, endpoint: str) -> str:
    """Return a normalized safe path or reject the request."""

    method = ReadOnlyMethod(str(method))
    path = endpoint.strip()
    if not path.startswith("/"):
        path = f"/{path}"
    if any(token in path for token in ("://", "?", "#", "\\", "%", "\x00")):
        raise ValueError(
            "endpoint must be an OpenSearch path only; send query values in params"
        )
    if "//" in path or any(segment in {".", ".."} for segment in path.split("/")):
        raise ValueError("endpoint contains an unsafe path segment")

    normalized_path = path.rstrip("/") or "/"
    lower_path = normalized_path.lower()
    segments = set(lower_path.split("/"))
    blocked = sorted(segments & BLOCKED_SEGMENTS)
    if blocked:
        raise ValueError(f"endpoint is blocked by the read-only policy: {blocked[0]}")

    patterns = (
        GET_PATTERNS
        if method in {ReadOnlyMethod.GET, ReadOnlyMethod.HEAD}
        else POST_PATTERNS
    )
    if not any(pattern.fullmatch(lower_path) for pattern in patterns):
        raise ValueError(
            f"{method.value} {lower_path} is not in the read-only OpenSearch allow-list"
        )
    return normalized_path
