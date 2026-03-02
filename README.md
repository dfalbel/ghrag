# ghrag

RAG over GitHub issues and PRs. Syncs a repository's issues into a local vector store and lets you query them via chat or an MCP server.

## Usage

Requires a GitHub token (`GITHUB_TOKEN` env var or `gh auth login`) and an OpenAI API key (`OPENAI_API_KEY`) for embeddings.

```bash
# Sync issues from a repo into the local store
uvx ghrag sync owner/repo

# Interactive chat with RAG context
uvx ghrag chat owner/repo

# Start an MCP server for querying issues
uvx ghrag mcp owner/repo

# MCP server with background sync every 15 minutes
uvx ghrag mcp owner/repo --sync-interval 15
```

## MCP server configuration

To use ghrag as an MCP server (e.g. with Claude Code), add it to your MCP settings:

```json
{
  "mcpServers": {
    "ghrag": {
      "command": "uvx",
      "args": ["ghrag", "mcp", "owner/repo", "--sync-interval", "15"]
    }
  }
}
```
