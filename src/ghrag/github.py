"""Pure helper functions for GitHub issue fetching and conversion."""

import os
import subprocess
from datetime import datetime


def get_github_token() -> str:
    """Get GitHub token from environment or gh CLI."""
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token
    try:
        result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True)
    except FileNotFoundError:
        raise RuntimeError("No GitHub token found. Set GITHUB_TOKEN or install the gh CLI and run 'gh auth login'")
    if result.returncode == 0:
        return result.stdout.strip()
    raise RuntimeError("No GitHub token found. Set GITHUB_TOKEN or run 'gh auth login'")


def issue_to_dict(issue) -> dict:
    """Convert a PyGithub issue object to a serializable dict."""
    comments = []
    for comment in issue.get_comments():
        comments.append({
            "author": comment.user.login if comment.user else None,
            "body": comment.body,
            "created_at": comment.created_at.isoformat(),
        })
    return {
        "number": issue.number,
        "title": issue.title,
        "body": issue.body,
        "url": issue.html_url,
        "state": issue.state,
        "author": issue.user.login if issue.user else None,
        "labels": [label.name for label in issue.labels],
        "created_at": issue.created_at.isoformat(),
        "updated_at": issue.updated_at.isoformat(),
        "is_pull_request": issue.pull_request is not None,
        "comments": comments,
    }


def issue_to_document(issue: dict):
    """Convert an issue dict into a chunked MarkdownDocument ready for ingestion.

    Builds a Markdown string from the issue fields (title, body, comments),
    attaches filterable attributes, and splits into smaller chunks using
    MarkdownChunker. We use chunk_size=800 because GitHub issues often contain
    code blocks which tokenize inefficiently.
    """
    from raghilda.chunker import MarkdownChunker
    from raghilda.document import MarkdownDocument

    item_type = "PR" if issue.get("is_pull_request") else "Issue"
    labels = ", ".join(issue.get("labels", []))

    lines = [
        f"# {item_type} #{issue['number']}: {issue['title']}",
        "",
        f"**State:** {issue['state']}",
        f"**Author:** {issue.get('author') or 'unknown'}",
        f"**Labels:** {labels or 'none'}",
        "",
    ]

    body = issue.get("body") or ""
    if body.strip():
        lines.append(body)
        lines.append("")

    for comment in issue.get("comments", []):
        comment_body = comment.get("body") or ""
        if comment_body.strip():
            comment_author = comment.get("author") or "unknown"
            lines.append(f"## Comment by {comment_author}")
            lines.append("")
            lines.append(comment_body)
            lines.append("")

    content = "\n".join(lines)

    updated_at = int(datetime.fromisoformat(issue["updated_at"]).timestamp())
    doc = MarkdownDocument(
        content=content,
        origin=issue["url"],
        attributes={
            "item_number": issue["number"],
            "state": issue["state"],
            "labels": labels if labels else "",
            "updated_at": updated_at,
        },
    )

    chunker = MarkdownChunker(chunk_size=800)
    doc = chunker.chunk_document(doc)
    return doc
