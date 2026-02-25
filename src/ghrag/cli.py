"""ghrag CLI entry point."""

import sys


def main():
    if len(sys.argv) < 3:
        print("Usage:")
        print("  ghrag sync <owner/repo>    # Download & ingest issues")
        print("  ghrag chat <owner/repo>    # Interactive chat")
        print("  ghrag mcp  <owner/repo>    # Start MCP server")
        sys.exit(1)

    command = sys.argv[1]
    repo = sys.argv[2]

    try:
        if command == "sync":
            from ghrag.github import sync
            sync(repo)
        elif command == "chat":
            from ghrag.chat import chat
            chat(repo)
        elif command == "mcp":
            from ghrag.mcp_server import serve
            serve(repo)
        else:
            print(f"Unknown command: {command}. Use 'sync', 'chat', or 'mcp'.")
            sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
