import re
from pathlib import Path

CACHE_ROOT = Path.home() / ".ghrag"

_REPO_RE = re.compile(r"^[A-Za-z0-9_.\-]+/[A-Za-z0-9_.\-]+$")


def validate_repo(repo: str) -> str:
    """Validate that *repo* looks like 'owner/name' and return it unchanged."""
    if not _REPO_RE.match(repo):
        raise ValueError(
            f"Invalid repo format: {repo!r}. Expected 'owner/repo' "
            "(alphanumeric, hyphens, dots, underscores only)."
        )
    return repo


def get_cache_dir(repo: str) -> Path:
    """Return ~/.ghrag/<owner>/<repo>/, creating it if needed."""
    validate_repo(repo)
    cache_dir = CACHE_ROOT / repo
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
