# /// script
# dependencies = ["chatlas[bedrock-anthropic]", "ghrag"]
#
# [tool.uv.sources]
# ghrag = { path = "..", editable = true }
# ///
"""
Generate an eval dataset from GitHub issues/PRs.

Reads the issues JSONL from the shared cache at ~/.ghrag/<owner>/<repo>/,
randomly samples N items, and uses an LLM to generate short search queries.
Each run appends to the existing dataset.json in evals/<repo>/.

Requires running `ghrag sync <repo>` first to populate the cache.

Usage:
    uv run evals/generate_dataset.py --repo posit-dev/positron
    uv run evals/generate_dataset.py --repo posit-dev/positron --limit 50 --seed 42
"""

import argparse
import json
import re
import random
from pathlib import Path

from ghrag import get_cache_dir


def load_issues_from_jsonl(path: Path) -> list[dict]:
    """Load all issues from a JSONL file."""
    issues = []
    with open(path) as f:
        for line in f:
            if line.strip():
                issues.append(json.loads(line))
    return issues


def load_existing_dataset(dataset_path: Path) -> list[dict]:
    """Load existing dataset.json if it exists."""
    if dataset_path.exists():
        with open(dataset_path) as f:
            return json.load(f)
    return []


def clean_body(body: str | None, max_chars: int = 2000) -> str:
    if not body:
        return ""
    # Strip HTML comments (issue templates often have <!-- ... -->)
    body = re.sub(r"<!--.*?-->", "", body, flags=re.DOTALL)
    body = body.strip()
    if len(body) > max_chars:
        body = body[:max_chars] + "..."
    return body


def generate_search_query(chat, title: str, body: str, is_pr: bool) -> str:
    item_type = "pull request" if is_pr else "issue"
    prompt = (
        f"Below is a GitHub {item_type}.\n\n"
        f"Title: {title}\n\n"
        f"Body:\n{body}\n\n"
        "---\n"
        "Write a very short (5-6 words) search query that a developer would "
        f"type into a search box when looking for this {item_type}. "
        "Imagine a user who knows the general context but doesn't know "
        "exactly how the issue is framed. "
        "The query should capture the core problem or feature request. "
        f"Do NOT mention the {item_type} number. "
        "Do NOT quote the title verbatim — rephrase it naturally. "
        "Be concise and natural, as if a human is quickly searching. "
        "Reply with ONLY the search query, nothing else."
    )
    response = chat.chat(prompt, echo="none")
    return str(response).strip()


def generate_dataset(issues: list[dict], output_dir: Path, repo_name: str, limit: int, seed: int):
    from chatlas import ChatBedrockAnthropic

    dataset_path = output_dir / "dataset.json"

    existing_samples = load_existing_dataset(dataset_path)
    print(f"Existing dataset has {len(existing_samples)} samples")

    to_sample = min(limit, len(issues))
    rng = random.Random(seed)
    sampled = rng.sample(issues, to_sample)

    print(f"Sampled {len(sampled)} items for dataset generation")

    chat = ChatBedrockAnthropic(
        model="us.anthropic.claude-opus-4-6-v1",
        max_tokens=8192,
    )

    new_samples = []
    for i, item in enumerate(sampled):
        title = item["title"]
        body = clean_body(item.get("body"))
        is_pr = item.get("is_pull_request", False)
        item_type = "pr" if is_pr else "issue"
        number = item["number"]

        print(f"  [{i + 1}/{len(sampled)}] #{number}: {title[:60]}...")

        query = generate_search_query(chat, title, body, is_pr)

        new_samples.append({
            "input": query,
            "target": str(number),
            "id": f"{repo_name}-{number}",
            "metadata": {
                "title": title,
                "url": item.get("url", ""),
                "labels": item.get("labels", []),
                "type": item_type,
                "state": item.get("state", ""),
            },
        })

    all_samples = existing_samples + new_samples

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(dataset_path, "w") as f:
        json.dump(all_samples, f, indent=2)

    print(f"Added {len(new_samples)} new samples (total: {len(all_samples)}) to {dataset_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate an eval dataset from GitHub issues/PRs."
    )
    parser.add_argument(
        "--repo", required=True,
        help="GitHub repository in owner/name format (e.g. posit-dev/positron)",
    )
    parser.add_argument(
        "--limit", type=int, default=50,
        help="Number of items to randomly sample (default: 50)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility (default: random)",
    )
    args = parser.parse_args()

    # Read from shared cache at ~/.ghrag/<owner>/<repo>/issues.jsonl
    cache_dir = get_cache_dir(args.repo)
    jsonl_path = cache_dir / "issues.jsonl"

    if not jsonl_path.exists():
        print(f"Error: No issues cache found at {jsonl_path}")
        print(f"Run 'ghrag sync {args.repo}' first to download issues.")
        raise SystemExit(1)

    # Write dataset to evals/<repo-name>/
    repo_name = args.repo.split("/")[-1]
    evals_dir = Path(__file__).parent
    output_dir = evals_dir / repo_name

    print(f"=== Loading issues from {jsonl_path} ===")
    issues = load_issues_from_jsonl(jsonl_path)
    print(f"Loaded {len(issues)} issues")

    print(f"\n=== Generating dataset ({args.limit} samples) ===")
    generate_dataset(issues, output_dir, repo_name, args.limit, args.seed)

    print("\nDone!")


if __name__ == "__main__":
    main()
