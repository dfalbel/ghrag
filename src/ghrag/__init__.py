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


def list_repos() -> list[str]:
    """Return a list of repos that have a local database."""
    repos: list[str] = []
    if not CACHE_ROOT.exists():
        return repos
    for owner_dir in sorted(CACHE_ROOT.iterdir()):
        if not owner_dir.is_dir():
            continue
        for repo_dir in sorted(owner_dir.iterdir()):
            if not repo_dir.is_dir():
                continue
            repos.append(f"{owner_dir.name}/{repo_dir.name}")
    return repos


_CACHE_FILES = {"issues.jsonl", "cache_last_update.txt"}


def delete(repo: str, keep_cache: bool = False) -> None:
    """Delete the local database for *repo*.

    Args:
        repo: GitHub repository in "owner/repo" format.
        keep_cache: When *True*, only remove the vector store and preserve
            the issues cache and sync metadata so a subsequent ``sync`` can
            rebuild without re-fetching.
    """
    cache_dir = _resolve_cache_dir(repo)
    if not cache_dir.exists():
        raise ValueError(f"No local database found for {repo}.")

    if keep_cache:
        for child in cache_dir.iterdir():
            if child.name in _CACHE_FILES:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        print(f"Deleted vector store for {repo} (kept issues cache).")
    else:
        shutil.rmtree(cache_dir)
        print(f"Deleted local database for {repo}.")
