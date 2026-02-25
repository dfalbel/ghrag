# /// script
# dependencies = ["inspect-ai", "ghrag"]
#
# [tool.uv.sources]
# ghrag = { path = "../..", editable = true }
# ///
"""
Inspect AI eval: identify Positron GitHub issues/PRs from search queries.

Retrieval via GitHub search API (PyGithub).

Usage:
    inspect eval evals/positron/task_gh.py --model anthropic/claude-sonnet-4-20250514
"""

import json
import time
from pathlib import Path

from inspect_ai import Task, task
from inspect_ai.dataset import json_dataset
from inspect_ai.scorer import match
from inspect_ai.solver import generate, system_message, use_tools
from inspect_ai.tool import tool

from github import Auth, Github, RateLimitExceededException
from ghrag.github import get_github_token

SYSTEM_MESSAGE = (
    "You are given a search query about a Positron IDE GitHub issue or pull request. "
    "Positron is an open-source IDE built by Posit (repo: posit-dev/positron). "
    "Your job is to identify the GitHub issue or PR number that best matches the query. "
    "You have access to a search tool that can search GitHub issues and PRs. "
    "Use it to find the relevant issue, then respond with ONLY the issue/PR number "
    "(e.g. 1234). Do not include '#' or any other text."
)

DATASET_PATH = Path(__file__).parent / "dataset.json"


@tool
def gh_search_issues():
    g = Github(auth=Auth.Token(get_github_token()))

    async def execute(query: str) -> str:
        """Search GitHub issues and PRs in the posit-dev/positron repository.

        Args:
            query: The search query to find relevant issues/PRs.
        """
        full_query = f"{query} repo:posit-dev/positron"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                results = g.search_issues(full_query)
                items = []
                for i, issue in enumerate(results):
                    if i >= 20:
                        break
                    items.append({
                        "number": issue.number,
                        "title": issue.title,
                        "url": issue.html_url,
                        "state": issue.state,
                        "labels": [l.name for l in issue.labels],
                    })
                return json.dumps(items)
            except RateLimitExceededException:
                reset_time = g.get_rate_limit().search.reset.timestamp()
                wait = max(reset_time - time.time(), 1) + 1
                time.sleep(wait)

        return json.dumps({"error": "Rate limit exceeded after retries"})

    return execute


@task
def positron_issues_with_gh():
    return Task(
        dataset=json_dataset(str(DATASET_PATH)),
        solver=[
            system_message(SYSTEM_MESSAGE),
            use_tools([gh_search_issues()]),
            generate(),
        ],
        scorer=match(),
    )
