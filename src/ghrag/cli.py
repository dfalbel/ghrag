"""ghrag CLI entry point."""

from typing import Optional

import typer

app = typer.Typer(help="GitHub Issues & PRs RAG tool")


@app.command()
def sync(repo: str = typer.Argument(help="GitHub repo in owner/repo format")):
    """Download & ingest issues from a GitHub repository."""
    try:
        from ghrag.github import sync as _sync

        _sync(repo)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def chat(repo: str = typer.Argument(help="GitHub repo in owner/repo format")):
    """Interactive chat over ingested GitHub issues."""
    try:
        from ghrag.chat import chat as _chat

        _chat(repo)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def mcp(
    repo: str = typer.Argument(help="GitHub repo in owner/repo format"),
    sync_interval: Optional[int] = typer.Option(
        None,
        "--sync-interval",
        help="Sync issues in the background every N minutes",
    ),
):
    """Start an MCP server for querying issues."""
    try:
        from ghrag.mcp_server import serve

        serve(repo, sync_interval=sync_interval)
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
):
    """Delete the local database for a repository."""
    try:
        from ghrag import delete as _delete

        _delete(repo, keep_cache=keep_cache)
    except ValueError as e:
        print(f"Error: {e}")
        raise typer.Exit(code=1)


def main():
    app()
