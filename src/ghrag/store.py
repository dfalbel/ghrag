"""Centralized store creation and connection logic."""

import json
from datetime import datetime
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


def retrieve(
    store,
    query: str,
    *,
    top_k: int = 20,
    state: str | None = None,
    labels: str | None = None,
    updated_after: str | None = None,
) -> str:
    """Retrieve matching chunks from the store and return formatted JSON.

    Args:
        store: A connected BaseStore instance.
        query: The search query text.
        top_k: Maximum number of chunks to retrieve.
        state: Filter by issue state ("open" or "closed").
        labels: Filter by issue label.
        updated_after: Only include items updated after this ISO date.

    Returns:
        A JSON string containing a list of result dicts with "text",
        "context", and optionally "attributes" keys.
    """
    filters = []
    if state:
        filters.append({"type": "eq", "key": "state", "value": state})
    if labels:
        filters.append({"type": "eq", "key": "labels", "value": labels})
    if updated_after:
        ts = int(datetime.fromisoformat(updated_after).timestamp())
        filters.append({"type": "gte", "key": "updated_at", "value": ts})

    if len(filters) == 0:
        attributes_filter = None
    elif len(filters) == 1:
        attributes_filter = filters[0]
    else:
        attributes_filter = {"type": "and", "filters": filters}

    chunks = store.retrieve(query, top_k=top_k, attributes_filter=attributes_filter)

    results = []
    for chunk in chunks:
        result = {"text": chunk.text, "context": chunk.context}
        if hasattr(chunk, "attributes") and chunk.attributes:
            result["attributes"] = chunk.attributes
        results.append(result)
    return json.dumps(results, default=str)
