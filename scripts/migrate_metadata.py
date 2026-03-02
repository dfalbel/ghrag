"""Migrate existing ~/.ghrag stores to the new two-file metadata format.

Replaces the old ``issues.meta.json`` (with ``cache_last_update`` and
``store_last_update`` keys) with two plain-text files:

- ``cache_last_update.txt``
- ``store_last_update.txt``

Each contains a single ISO timestamp string.
"""

import json
from pathlib import Path

CACHE_ROOT = Path.home() / ".ghrag"


def migrate_repo(repo_dir: Path):
    repo_name = f"{repo_dir.parent.name}/{repo_dir.name}"
    meta_path = repo_dir / "issues.meta.json"
    cache_txt = repo_dir / "cache_last_update.txt"
    store_txt = repo_dir / "store_last_update.txt"
    jsonl_path = repo_dir / "issues.jsonl"

    # Already migrated to txt files
    if cache_txt.exists():
        print(f"  {repo_name}: already migrated (txt files exist), skipping")
        return

    # Migrate from issues.meta.json
    if meta_path.exists():
        metadata = json.loads(meta_path.read_text())

        # Handle old single-key format
        if "last_update" in metadata:
            ts = metadata["last_update"]
            cache_txt.write_text(ts)
            store_txt.write_text(ts)
            meta_path.unlink()
            print(f"  {repo_name}: migrated from last_update={ts}")
            return

        # Handle two-key JSON format
        cache_val = metadata.get("cache_last_update")
        store_val = metadata.get("store_last_update")

        if cache_val:
            cache_txt.write_text(cache_val)
        if store_val:
            store_txt.write_text(store_val)

        meta_path.unlink()
        print(f"  {repo_name}: migrated from issues.meta.json "
              f"(cache={cache_val}, store={store_val})")
        return

    # No metadata at all — derive from JSONL
    if not jsonl_path.exists():
        print(f"  {repo_name}: no JSONL cache, skipping")
        return

    max_updated = None
    count = 0
    for line in open(jsonl_path):
        line = line.strip()
        if not line:
            continue
        issue = json.loads(line)
        updated = issue.get("updated_at")
        if updated and (max_updated is None or updated > max_updated):
            max_updated = updated
        count += 1

    if not max_updated:
        print(f"  {repo_name}: empty JSONL, skipping")
        return

    cache_txt.write_text(max_updated)
    # No store_last_update.txt — force a full re-ingest from the JSONL cache
    print(f"  {repo_name}: created metadata from {count} cached issues "
          f"(cache_last_update={max_updated})")


def main():
    if not CACHE_ROOT.exists():
        print("No ~/.ghrag directory found.")
        return

    print("Migrating ~/.ghrag stores to new metadata format...")
    for owner_dir in sorted(CACHE_ROOT.iterdir()):
        if not owner_dir.is_dir():
            continue
        for repo_dir in sorted(owner_dir.iterdir()):
            if not repo_dir.is_dir():
                continue
            migrate_repo(repo_dir)
    print("Done.")


if __name__ == "__main__":
    main()
