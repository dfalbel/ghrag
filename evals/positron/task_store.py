# /// script
# dependencies = ["inspect-ai", "ghrag"]
#
# [tool.uv.sources]
# ghrag = { path = "../..", editable = true }
# ///
"""
Inspect AI eval: identify Positron GitHub issues/PRs from search queries.

Retrieval-augmented task — uses a retrieval tool to search the issue database.

Usage:
    inspect eval evals/positron/task_store.py --model anthropic/claude-sonnet-4-20250514
"""

from pathlib import Path

from inspect_ai import Task, task
from inspect_ai.dataset import json_dataset
from inspect_ai.scorer import match
from inspect_ai.solver import generate, system_message, use_tools
from inspect_ai.tool import tool

from ghrag import get_cache_dir
from ghrag.store import connect_store, retrieve as ghrag_retrieve

SYSTEM_MESSAGE = (
    "You are given a search query about a Positron IDE GitHub issue or pull request. "
    "Positron is an open-source IDE built by Posit (repo: posit-dev/positron). "
    "Your job is to identify the GitHub issue or PR number that best matches the query. "
    "You have access to a retrieval tool that can search the issue database. "
    "Use it to find the relevant issue, then respond with ONLY the issue/PR number "
    "(e.g. 1234). Do not include '#' or any other text."
)

DATASET_PATH = Path(__file__).parent / "dataset.json"

REPO = "posit-dev/positron"


@tool
def retrieve():
    cache_dir = get_cache_dir(REPO)
    store = connect_store(REPO, cache_dir)

    async def execute(query: str) -> str:
        """Search GitHub issues and PRs for relevant information.

        Args:
            query: The search query to find relevant issues/PRs.
        """
        return ghrag_retrieve(store, query, top_k=20)

    return execute


@task
def positron_issues_with_retrieval():
    return Task(
        dataset=json_dataset(str(DATASET_PATH)),
        solver=[
            system_message(SYSTEM_MESSAGE),
            use_tools([retrieve()]),
            generate(),
        ],
        scorer=match(),
    )
