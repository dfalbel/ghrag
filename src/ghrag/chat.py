"""Interactive chat with RAG context from GitHub issues."""

import json
import sys
from datetime import datetime
from pathlib import Path

from ghrag import get_cache_dir


def chat(repo: str):
    """Interactive chat with RAG context from GitHub issues."""
    from chatlas import ChatOpenAI
    from raghilda.store import ChromaDBStore

    cache_dir = get_cache_dir(repo)
    store_path = str(cache_dir / "chroma")
    if not Path(store_path).exists():
        print(f"No store found at {store_path}. Run 'ghrag sync {repo}' first.")
        sys.exit(1)

    store = ChromaDBStore.connect("github_issues", location=store_path)

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

    def current_date() -> str:
        """Return today's date in ISO format (e.g. '2024-06-15')."""
        return datetime.now().strftime("%Y-%m-%d")

    chat_model = ChatOpenAI()
    chat_model.register_tool(retrieve)
    chat_model.register_tool(current_date)
    chat_model.console()
