from pathlib import Path

CACHE_ROOT = Path.home() / ".ghrag"


def get_cache_dir(repo: str) -> Path:
    """Return ~/.ghrag/<owner>/<repo>/, creating it if needed."""
    cache_dir = CACHE_ROOT / repo
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
