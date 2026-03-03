"""Centralized store creation and connection logic."""

from pathlib import Path


def create_store(repo: str, cache_dir: Path, store_type: str = "duckdb"):
    """Create or connect to a vector store for syncing/ingesting.

    If the store already exists on disk, connects to it.  Otherwise creates
    a fresh one and unlinks ``store_last_update.txt`` so a full ingest is
    triggered.

    Args:
        repo: GitHub repository in "owner/repo" format.
        cache_dir: Local cache directory for this repo.
        store_type: ``"duckdb"`` or ``"chroma"``.

    Returns:
        A ``BaseStore`` instance ready for upserts.
    """
    from raghilda.embedding import EmbeddingOpenAI

    store_meta_path = cache_dir / f"store_last_update_{store_type}.txt"

    if store_type == "duckdb":
        from raghilda.store import DuckDBStore

        location = str(cache_dir / "store.duckdb")
        if Path(location).exists():
            return DuckDBStore.connect(location)

        # Store was (re)created — force a full ingest
        if store_meta_path.exists():
            store_meta_path.unlink()

        return DuckDBStore.create(
            location=location,
            embed=EmbeddingOpenAI(),
            overwrite=True,
            name="github_issues",
            title=f"GitHub Issues: {repo}",
            attributes={
                "item_number": int,
                "state": str,
                "labels": str,
                "updated_at": int,
            },
        )

    if store_type == "chroma":
        from raghilda.store import ChromaDBStore

        store_path = str(cache_dir / "chroma")
        if Path(store_path).exists():
            return ChromaDBStore.connect("github_issues", location=store_path)

        # Store was (re)created — force a full ingest
        if store_meta_path.exists():
            store_meta_path.unlink()

        return ChromaDBStore.create(
            location=store_path,
            embed=EmbeddingOpenAI(),
            overwrite=True,
            name="github_issues",
            title=f"GitHub Issues: {repo}",
            attributes={
                "item_number": int,
                "state": str,
                "labels": str,
                "updated_at": int,
            },
        )

    raise ValueError(f"Unknown store type: {store_type!r}. Use 'duckdb' or 'chroma'.")


def connect_store(repo: str, cache_dir: Path, store_type: str = "duckdb"):
    """Connect to an existing vector store (read-only access for chat/mcp).

    Args:
        repo: GitHub repository in "owner/repo" format.
        cache_dir: Local cache directory for this repo.
        store_type: ``"duckdb"`` or ``"chroma"``.

    Returns:
        A ``BaseStore`` instance ready for retrieval.

    Raises:
        ValueError: If the expected store path does not exist.
    """
    if store_type == "duckdb":
        from raghilda.store import DuckDBStore

        location = str(cache_dir / "store.duckdb")
        if not Path(location).exists():
            raise ValueError(
                f"No store found for {repo}. Run 'ghrag sync {repo}' first."
            )
        return DuckDBStore.connect(location)

    if store_type == "chroma":
        from raghilda.store import ChromaDBStore

        store_path = str(cache_dir / "chroma")
        if not Path(store_path).exists():
            raise ValueError(
                f"No store found for {repo}. Run 'ghrag sync {repo}' first."
            )
        return ChromaDBStore.connect("github_issues", location=store_path)

    raise ValueError(f"Unknown store type: {store_type!r}. Use 'duckdb' or 'chroma'.")
