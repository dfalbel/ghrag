"""Tests for ghrag.ingest — IssuesCache, Inbox, Fetcher (replay), Ingester, sync."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ghrag.ingest import Event, IssuesCache, Inbox, Progress, Fetcher, Ingester


class TestIssuesCache:
    def test_roundtrip(self, tmp_path):
        path = tmp_path / "issues.jsonl"
        cache = IssuesCache(path)
        cache.put({"number": 1, "title": "first"})
        cache.put({"number": 2, "title": "second"})
        cache.save()

        cache2 = IssuesCache(path)
        vals = {i["number"]: i for i in cache2.values()}
        assert 1 in vals
        assert 2 in vals
        assert vals[1]["title"] == "first"

    def test_dedup_by_number(self, tmp_path):
        path = tmp_path / "issues.jsonl"
        cache = IssuesCache(path)
        cache.put({"number": 1, "title": "old"})
        cache.put({"number": 1, "title": "new"})
        assert len(cache.values()) == 1
        assert cache.values()[0]["title"] == "new"

    def test_empty_file(self, tmp_path):
        path = tmp_path / "issues.jsonl"
        path.write_text("")
        cache = IssuesCache(path)
        assert cache.values() == []

    def test_nonexistent_file(self, tmp_path):
        path = tmp_path / "issues.jsonl"
        cache = IssuesCache(path)
        assert cache.values() == []


class TestInbox:
    def test_errors_first(self):
        inbox = Inbox()
        inbox.put(Event.Fetch.Ok(issue={"number": 1, "updated_at": "2025-01-01"}))
        inbox.put(Event.Error(exp=RuntimeError("fail")))
        inbox.put(Event.Fetch.Done())

        first = inbox.pop()
        assert isinstance(first, Event.Error)

    def test_ingest_before_fetch(self):
        inbox = Inbox()
        inbox.put(Event.Fetch.Ok(issue={"number": 1, "updated_at": "2025-01-01"}))
        inbox.put(Event.Ingest.Ok(issue={"number": 1}))
        # After ingest.ok, pending goes to 0. But fetch not done so no Done yet.
        # Pop should return the fetch.ok first (since ingest.ok decremented pending
        # from the fetch.ok we added).
        # Actually: ingest results come before fetch issues in priority.
        # But we have ingest.ok and fetch.ok both queued.
        # Priority: errors > ingest > fetch > done
        first = inbox.pop()
        assert isinstance(first, Event.Ingest.Ok)

    def test_done_after_all_settled(self):
        inbox = Inbox()
        inbox.put(Event.Fetch.Ok(issue={"number": 1, "updated_at": "2025-01-01"}))
        inbox.put(Event.Ingest.Ok(issue={"number": 1}))
        inbox.put(Event.Fetch.Done())

        # Drain ingest and fetch
        inbox.pop()  # Ingest.Ok
        inbox.pop()  # Fetch.Ok (but pending is 0 already since ingest.ok decremented)

        # Actually, let me reconsider. After put(Fetch.Ok), pending=1.
        # After put(Ingest.Ok), pending=0. After put(Fetch.Done), fetch_done=True.
        # Pop 1: ingest results first -> Ingest.Ok
        # Pop 2: fetch issues next -> Fetch.Ok
        # Pop 3: fetch_done=True, pending=0 (but we just popped a Fetch.Ok without
        #         a corresponding ingest... wait, pending tracks put counts, not pop).
        # pending went 0 -> +1 (fetch.ok put) -> 0 (ingest.ok put). So pending=0.
        # After popping ingest and fetch, fetch_done=True and pending=0 -> Done.
        done = inbox.pop()
        assert isinstance(done, Event.Done)

    def test_fetch_ordered_by_updated_at(self):
        inbox = Inbox()
        inbox.put(Event.Fetch.Ok(issue={"number": 2, "updated_at": "2025-03-01"}))
        inbox.put(Event.Fetch.Ok(issue={"number": 1, "updated_at": "2025-01-01"}))
        inbox.put(Event.Fetch.Done())

        # Need to drain them all: 2 fetch.ok, then 2 ingest results expected
        # But we won't get Done until pending=0. pending is now 2.
        # For this test, just check order of the two pops.
        first = inbox.pop()
        second = inbox.pop()
        assert isinstance(first, Event.Fetch.Ok)
        assert isinstance(second, Event.Fetch.Ok)
        assert first.issue["number"] == 1  # older first
        assert second.issue["number"] == 2


class TestProgress:
    def test_counters(self):
        p = Progress()
        assert p.fetched == 0
        assert p.ingested == 0
        p.on_fetch()
        assert p.fetched == 1
        p.on_ingest()
        assert p.ingested == 1


class TestFetcherReplayCache:
    def test_replay_all(self, tmp_path):
        jsonl = tmp_path / "issues.jsonl"
        issues = [
            {"number": 1, "updated_at": "2025-01-01T00:00:00"},
            {"number": 2, "updated_at": "2025-02-01T00:00:00"},
        ]
        jsonl.write_text("\n".join(json.dumps(i) for i in issues))

        fetcher = Fetcher("owner/repo", tmp_path, since=None)
        inbox = Inbox()
        fetcher._replay_cache(inbox)

        events = []
        # Drain: 2 items, pending=2, fetch not done, so we just pop what's available.
        # Use a non-blocking approach: check internal state
        assert len(inbox._fetch_issues) == 2

    def test_replay_filtered_by_since(self, tmp_path):
        jsonl = tmp_path / "issues.jsonl"
        issues = [
            {"number": 1, "updated_at": "2025-01-01T00:00:00"},
            {"number": 2, "updated_at": "2025-03-01T00:00:00"},
        ]
        jsonl.write_text("\n".join(json.dumps(i) for i in issues))

        since = datetime(2025, 2, 1)
        fetcher = Fetcher("owner/repo", tmp_path, since=since)
        inbox = Inbox()
        fetcher._replay_cache(inbox)

        assert len(inbox._fetch_issues) == 1
        _, _, event = inbox._fetch_issues[0]
        assert event.issue["number"] == 2


class TestIngester:
    def test_submit_and_ingest(self, sample_issue_dict, mock_store, tmp_path):
        store_meta = tmp_path / "store_last_update.txt"
        ingester = Ingester(mock_store, store_meta, num_workers=1)
        inbox = Inbox()
        ingester.start(inbox)

        try:
            ingester.submit(sample_issue_dict)
            # Wait for the ingest result
            event = inbox.pop()
            assert isinstance(event, (Event.Ingest.Ok, Event.Error))
            if isinstance(event, Event.Error):
                raise event.exp
            assert event.issue["number"] == 42
            mock_store.upsert.assert_called_once()
        finally:
            ingester.stop()

    def test_error_on_bad_issue(self, mock_store, tmp_path):
        store_meta = tmp_path / "store_last_update.txt"
        ingester = Ingester(mock_store, store_meta, num_workers=1)
        inbox = Inbox()
        ingester.start(inbox)

        try:
            # Missing required fields should cause an error
            ingester.submit({"number": 99})
            event = inbox.pop()
            assert isinstance(event, Event.Error)
            assert event.issue["number"] == 99
        finally:
            ingester.stop()


class TestSync:
    def test_sync_with_cached_issues(self, tmp_path, sample_issue_dict):
        cache_dir = tmp_path / "owner" / "repo"
        cache_dir.mkdir(parents=True)

        # Pre-populate JSONL cache
        jsonl = cache_dir / "issues.jsonl"
        jsonl.write_text(json.dumps(sample_issue_dict) + "\n")

        mock_store = MagicMock()
        mock_store.size.return_value = 1

        with patch("ghrag.store.create_store", return_value=mock_store), \
             patch("ghrag.get_cache_dir", return_value=cache_dir), \
             patch.object(Fetcher, "_fetch_github"):
            from ghrag.ingest import sync

            sync("owner/repo", store_type="duckdb", num_workers=1)

        mock_store.upsert.assert_called_once()
        mock_store.build_index.assert_called_once()
