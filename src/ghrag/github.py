"""Fetch and cache GitHub issues/PRs."""

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

from ghrag import get_cache_dir


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


def sync(repo: str):
    """Download issues from GitHub and build the RAG store (incremental)."""
    from github import Auth, Github
    from raghilda.embedding import EmbeddingOpenAI
    from raghilda.store import ChromaDBStore

    cache_dir = get_cache_dir(repo)
    store_path = str(cache_dir / "chroma")
    jsonl_path = cache_dir / "issues.jsonl"
    meta_path = cache_dir / "issues.meta.json"

    metadata = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    last_update = metadata.get("last_update")

    if Path(store_path).exists() and last_update:
        store = ChromaDBStore.connect("github_issues", location=store_path)
    else:
        store = ChromaDBStore.create(
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
        # Seed from JSONL cache if available (e.g. store was deleted)
        if jsonl_path.exists():
            all_issues = (json.loads(l) for l in open(jsonl_path) if l.strip())
            deduped = {i["number"]: i for i in all_issues}
            store.ingest(list(deduped.values()), prepare=issue_to_document)
            print(f"Rebuilt store from cache ({len(deduped)} issues).")

    # Fetch issues from GitHub (incremental if we have a last_update)
    token = get_github_token()
    g = Github(auth=Auth.Token(token))
    github_repo = g.get_repo(repo)

    if last_update:
        print(f"Fetching updates since {last_update}...")
        since = datetime.fromisoformat(last_update)
        issues_iter = github_repo.get_issues(state="all", sort="updated", since=since)
    else:
        print("Fetching all issues...")
        issues_iter = github_repo.get_issues(state="all", sort="updated")

    # Fetch issues: append to JSONL cache and collect for ingestion
    update_time = datetime.now(timezone.utc)
    new_issues = []
    with open(jsonl_path, "a") as f:
        for issue in tqdm(issues_iter, total=issues_iter.totalCount, desc="Fetching"):
            issue_dict = issue_to_dict(issue)
            new_issues.append(issue_dict)
            f.write(json.dumps(issue_dict) + "\n")

    # Ingest only the new/updated issues (ChromaDB upserts by document ID)
    if new_issues:
        print(f"Ingesting {len(new_issues)} issues...")
        store.ingest(new_issues, prepare=issue_to_document)

    # Save sync timestamp so next run only fetches new/updated issues
    metadata["last_update"] = update_time.isoformat()
    meta_path.write_text(json.dumps(metadata, indent=2, default=str))

    print(f"Done! Store contains {store.size()} documents.")
