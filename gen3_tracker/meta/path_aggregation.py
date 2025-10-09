import re
from collections import defaultdict


def normalize_path(p: str) -> str:
    """Normalize a POSIX-like path: keep a leading slash, collapse //, strip trailing slash (except root)."""
    if not p:
        return ""
    if not p.startswith("/"):
        p = "/" + p
    p = re.sub(r"/+", "/", p)
    if p != "/":
        p = p.rstrip("/")
    return p


def path_prefixes(p: str):
    """
    Yield directory prefixes excluding the filename.
    '/a/b/c.txt' -> '/a', '/a/b'
    """
    p = normalize_path(p)
    if not p or p == "/":
        return
    parts = p.lstrip("/").split("/")
    if len(parts) < 2:
        return
    for i in range(1, len(parts)):
        if i == len(parts) - 1:
            yield "/" + "/".join(parts[:i])


aggregator: dict[str, tuple[int, int]] = defaultdict(lambda: (0, 0))
"""
In-memory aggregation of (file_count, total_size) by (path).
"""
