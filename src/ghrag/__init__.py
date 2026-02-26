import re
import shutil
from pathlib import Path

CACHE_ROOT = Path.home() / ".ghrag"
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


def validate_repo(repo: str) -> str:
    """Validate that *repo* looks like 'owner/name' with no traversal segments."""
    repo = repo.strip()
    parts = repo.split("/")
    if (
        len(parts) != 2
        or any(p in ("", ".", "..") for p in parts)
        or not all(_SEGMENT_RE.match(p) for p in parts)
    ):
        raise ValueError(
            f"Invalid repo format: {repo!r}. Expected 'owner/repo'."
        )
    return repo


def _resolve_cache_dir(repo: str) -> Path:
    """Return the resolved cache path for *repo* without creating it."""
    owner, name = validate_repo(repo).split("/")
    cache_dir = (CACHE_ROOT / owner / name).resolve()
    if not cache_dir.is_relative_to(CACHE_ROOT.resolve()):
        raise ValueError(f"Invalid repo format: {repo!r}.")
    return cache_dir


def get_cache_dir(repo: str) -> Path:
    """Return ~/.ghrag/<owner>/<repo>/, creating it if needed."""
    cache_dir = _resolve_cache_dir(repo)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def delete(repo: str) -> None:
    """Delete the local database for *repo*."""
    cache_dir = _resolve_cache_dir(repo)
    if not cache_dir.exists():
        print(f"No local database found for {repo}.")
        return
    shutil.rmtree(cache_dir)
    print(f"Deleted local database for {repo}.")
