from pathlib import Path

CACHE_ROOT = Path.home() / ".ghrag"


def validate_repo(repo: str) -> str:
    """Validate that *repo* looks like 'owner/name' with no traversal segments."""
    parts = repo.split("/")
    if len(parts) != 2 or any(p in ("", ".", "..") for p in parts):
        raise ValueError(
            f"Invalid repo format: {repo!r}. Expected 'owner/repo'."
        )
    return repo


def get_cache_dir(repo: str) -> Path:
    """Return ~/.ghrag/<owner>/<repo>/, creating it if needed."""
    owner, name = validate_repo(repo).split("/")
    cache_dir = (CACHE_ROOT / owner / name).resolve()
    if not cache_dir.is_relative_to(CACHE_ROOT.resolve()):
        raise ValueError(f"Invalid repo format: {repo!r}.")
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
