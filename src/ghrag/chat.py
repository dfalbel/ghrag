"""Interactive chat with RAG context from GitHub issues."""

from datetime import datetime

from ghrag import get_cache_dir


def chat(repo: str, store_type: str = "duckdb"):
    """Interactive chat with RAG context from GitHub issues."""
    from chatlas import ChatOpenAI

    from ghrag.store import connect_store

    cache_dir = get_cache_dir(repo)
    store = connect_store(repo, cache_dir, store_type)

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
        from ghrag.store import retrieve as _retrieve

        return _retrieve(
            store, query, state=state, labels=labels, updated_after=updated_after,
        )

    def current_date() -> str:
        """Return today's date in ISO format (e.g. '2024-06-15')."""
        return datetime.now().strftime("%Y-%m-%d")

    chat_model = ChatOpenAI()
    chat_model.register_tool(retrieve)
    chat_model.register_tool(current_date)
    chat_model.console()
