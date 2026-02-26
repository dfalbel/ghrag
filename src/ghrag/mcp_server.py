"""MCP server exposing GitHub issue retrieval tools."""

import contextlib
import io
import json
import logging
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from ghrag import get_cache_dir

logger = logging.getLogger(__name__)


def _background_sync(repo: str, interval_minutes: int):
    """Run sync periodically, suppressing output to avoid corrupting stdio."""
    from ghrag.github import sync

    while True:
        time.sleep(interval_minutes * 60)
        try:
            with contextlib.redirect_stdout(io.StringIO()), \
                 contextlib.redirect_stderr(io.StringIO()):
                sync(repo)
            logger.info("Background sync completed for %s", repo)
        except Exception:
            logger.exception("Background sync failed for %s", repo)


def _parse_sync_interval(sync_interval: int | str | None = None) -> int | None:
    """Resolve and validate sync interval from arg or ``--sync-interval``."""
    if sync_interval is None:
        if "--sync-interval" not in sys.argv:
            return None
        idx = sys.argv.index("--sync-interval")
        if idx + 1 >= len(sys.argv):
            raise ValueError("--sync-interval requires a value (minutes).")
        sync_interval = sys.argv[idx + 1]

    try:
        interval = int(sync_interval)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"--sync-interval must be a positive integer, got {sync_interval!r}."
        ) from exc

    if interval <= 0:
        raise ValueError(
            f"--sync-interval must be a positive integer, got {interval!r}."
        )

    return interval


def serve(repo: str, sync_interval: int | None = None):
    """Start an MCP server with issue retrieval tools for the given repo.

    Args:
        repo: GitHub repository in "owner/repo" format.
        sync_interval: If set, sync issues in the background every N minutes.
            When *None*, the value is parsed from ``--sync-interval`` on the
            command line (if present).
    """
    try:
        sync_interval = _parse_sync_interval(sync_interval)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    cache_dir = get_cache_dir(repo)
    store_path = str(cache_dir / "chroma")
    if not Path(store_path).exists():
        print(f"No store found at {store_path}. Run 'ghrag sync {repo}' first.", file=sys.stderr)
        sys.exit(1)

    from raghilda.store import ChromaDBStore

    store = ChromaDBStore.connect("github_issues", location=store_path)

    if sync_interval is not None:
        thread = threading.Thread(
            target=_background_sync,
            args=(repo, sync_interval),
            daemon=True,
        )
        thread.start()

    mcp = FastMCP(f"ghrag - {repo}")

    @mcp.tool()
    def retrieve(
        query: str,
        state: str | None = None,
        labels: str | None = None,
        updated_after: str | None = None,
    ) -> str:
        """Search GitHub issues and PRs for relevant information.

        Args:
            query: The search query to find relevant issues/PRs.
            state: Filter by state - "open" or "closed".
            labels: Filter issues that have this label.
            updated_after: Only include items updated after this ISO date (e.g. "2024-01-15").
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

        chunks = store.retrieve(query, top_k=20, attributes_filter=attributes_filter)

        results = []
        for chunk in chunks:
            result = {"text": chunk.text, "context": chunk.context}
            if hasattr(chunk, "attributes") and chunk.attributes:
                result["attributes"] = chunk.attributes
            results.append(result)
        return json.dumps(results, default=str)

    mcp.run(transport="stdio")
