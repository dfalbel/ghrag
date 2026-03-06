# Evals

Evaluation suite for ghrag using [Inspect AI](https://inspect.aisi.org.uk/).

## Setup

Install the eval dependencies:

```bash
uv sync --extra eval
```

## Running evals

Run both tasks as an eval set:

```bash
uv run inspect eval-set evals/positron/task_gh.py evals/positron/task_store.py \
  --model bedrock/us.anthropic.claude-sonnet-4-6 \
  --log-dir logs-positron
```

Or run a single task:

```bash
uv run inspect eval evals/positron/task_store.py --model bedrock/us.anthropic.claude-sonnet-4-6
uv run inspect eval evals/positron/task_gh.py --model bedrock/us.anthropic.claude-sonnet-4-6
```

## Tasks

### `positron_issues_with_retrieval` (`task_store.py`)

Uses a local vector store (DuckDB by default) to retrieve relevant GitHub issues/PRs. Requires running `ghrag sync posit-dev/positron` first to populate the store.

### `positron_issues_with_gh` (`task_gh.py`)

Uses the GitHub search API directly to find relevant issues/PRs. Requires a valid `GITHUB_TOKEN`.

## Generating the dataset

```bash
uv run evals/generate_dataset.py --repo posit-dev/positron --limit 50 --seed 42
```

This samples issues from the local cache (`~/.ghrag/posit-dev/positron/`) and uses an LLM to generate search queries. Each run appends to the existing `dataset.json`.
