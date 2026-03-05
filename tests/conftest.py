"""Shared fixtures for ghrag tests."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def patched_cache_root(tmp_path, monkeypatch):
    """Redirect ghrag.CACHE_ROOT to a temp directory."""
    import ghrag

    monkeypatch.setattr(ghrag, "CACHE_ROOT", tmp_path)
    return tmp_path


@pytest.fixture()
def sample_issue_dict():
    """A realistic issue dict as produced by issue_to_dict."""
    return {
        "number": 42,
        "title": "Fix login redirect",
        "body": "The login page redirects to the wrong URL.\n\n```python\nredirect('/home')\n```",
        "url": "https://github.com/owner/repo/issues/42",
        "state": "open",
        "author": "alice",
        "labels": ["bug", "auth"],
        "created_at": "2025-01-10T12:00:00",
        "updated_at": "2025-02-15T08:30:00",
        "is_pull_request": False,
        "comments": [
            {
                "author": "bob",
                "body": "I can reproduce this on Chrome.",
                "created_at": "2025-01-11T09:00:00",
            }
        ],
    }


@pytest.fixture()
def mock_store():
    """A MagicMock pretending to be a raghilda BaseStore."""
    from raghilda.store import BaseStore

    return MagicMock(spec=BaseStore)
