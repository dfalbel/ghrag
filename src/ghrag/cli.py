"""ghrag CLI entry point."""

from typing import Optional

import typer

app = typer.Typer(help="GitHub Issues & PRs RAG tool")


@app.command("list")
def list_repos():
    """List all repositories with a local database."""
    from ghrag import list_repos as _list_repos

    repos = _list_repos()
    if not repos:
        print("No local databases found.")
        raise typer.Exit()
    for repo in repos:
        print(repo)


@app.command()
def sync(
    repo: str = typer.Argument(help="GitHub repo in owner/repo format"),
    store: str = typer.Option(
        "duckdb", "--store", help="Vector store backend: 'duckdb' or 'chroma'"
    ),
    force: bool = typer.Option(
        False, "--force", help="Discard cache and refetch all issues from GitHub"
    ),
):
    """Download & ingest issues from a GitHub repository."""
    try:
        from ghrag.ingest import sync as _sync

        _sync(repo, store_type=store, force=force)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def retrieve(
    repo: str = typer.Argument(help="GitHub repo in owner/repo format"),
    query: str = typer.Argument(help="Search query to find relevant issues/PRs"),
    store: str = typer.Option(
        "duckdb", "--store", help="Vector store backend: 'duckdb' or 'chroma'"
    ),
    state: Optional[str] = typer.Option(
        None, "--state", help='Filter by state: "open" or "closed"'
    ),
    labels: Optional[str] = typer.Option(
        None, "--labels", help="Filter issues that have this label"
    ),
    updated_after: Optional[str] = typer.Option(
        None,
        "--updated-after",
        help="Only include items updated after this ISO date (e.g. 2024-01-15)",
    ),
):
    """Retrieve relevant issues/PRs matching a query."""
    try:
        from ghrag import get_cache_dir
        from ghrag.store import connect_store, retrieve as _retrieve

        cache_dir = get_cache_dir(repo)
        st = connect_store(repo, cache_dir, store)
        result = _retrieve(
            st, query, state=state, labels=labels, updated_after=updated_after,
        )
        print(result)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def chat(
    repo: str = typer.Argument(help="GitHub repo in owner/repo format"),
    store: str = typer.Option(
        "duckdb", "--store", help="Vector store backend: 'duckdb' or 'chroma'"
    ),
):
    """Interactive chat over ingested GitHub issues."""
    try:
        from ghrag.chat import chat as _chat

        _chat(repo, store_type=store)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def mcp(
    repo: str = typer.Argument(help="GitHub repo in owner/repo format"),
    store: str = typer.Option(
        "duckdb", "--store", help="Vector store backend: 'duckdb' or 'chroma'"
    ),
    sync_interval: Optional[int] = typer.Option(
        None,
        "--sync-interval",
        help="Sync issues in the background every N minutes",
    ),
):
    """Start an MCP server for querying issues."""
    try:
        from ghrag.mcp_server import serve

        serve(repo, store_type=store, sync_interval=sync_interval)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def delete(
    repo: str = typer.Argument(help="GitHub repo in owner/repo format"),
    keep_cache: bool = typer.Option(
        False,
        "--keep-cache",
        help="Keep the issues cache, only delete the vector store",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompt",
    ),
):
    """Delete the local database for a repository."""
    try:
        if not yes:
            what = "vector store" if keep_cache else "local database"
            confirmed = typer.confirm(
                f"Delete {what} for {repo}?"
            )
            if not confirmed:
                raise typer.Abort()

        from ghrag import delete as _delete

        _delete(repo, keep_cache=keep_cache)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


def main():
    app()
