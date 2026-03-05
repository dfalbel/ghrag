"""Tests for ghrag.store — retrieve filter logic, create_store/connect_store branching."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ghrag.store import retrieve, create_store, connect_store


class TestRetrieve:
    def _make_chunk(self, text="chunk text", context="some context", attributes=None):
        chunk = MagicMock()
        chunk.text = text
        chunk.context = context
        chunk.attributes = attributes or {"item_number": 1, "state": "open"}
        return chunk

    def test_no_filters(self, mock_store):
        mock_store.retrieve.return_value = [self._make_chunk()]
        result = retrieve(mock_store, "search query")
        mock_store.retrieve.assert_called_once_with(
            "search query", top_k=20, attributes_filter=None
        )
        data = json.loads(result)
        assert len(data) == 1
        assert data[0]["text"] == "chunk text"

    def test_state_filter(self, mock_store):
        mock_store.retrieve.return_value = []
        retrieve(mock_store, "query", state="open")
        call_kwargs = mock_store.retrieve.call_args[1]
        assert call_kwargs["attributes_filter"] == {
            "type": "eq", "key": "state", "value": "open"
        }

    def test_multiple_filters(self, mock_store):
        mock_store.retrieve.return_value = []
        retrieve(mock_store, "query", state="closed", labels="bug")
        call_kwargs = mock_store.retrieve.call_args[1]
        f = call_kwargs["attributes_filter"]
        assert f["type"] == "and"
        assert len(f["filters"]) == 2

    def test_updated_after_filter(self, mock_store):
        mock_store.retrieve.return_value = []
        retrieve(mock_store, "query", updated_after="2025-06-01")
        call_kwargs = mock_store.retrieve.call_args[1]
        f = call_kwargs["attributes_filter"]
        assert f["type"] == "gte"
        assert f["key"] == "updated_at"
        expected_ts = int(datetime.fromisoformat("2025-06-01").timestamp())
        assert f["value"] == expected_ts

    def test_all_filters(self, mock_store):
        mock_store.retrieve.return_value = []
        retrieve(mock_store, "q", state="open", labels="bug", updated_after="2025-01-01")
        call_kwargs = mock_store.retrieve.call_args[1]
        f = call_kwargs["attributes_filter"]
        assert f["type"] == "and"
        assert len(f["filters"]) == 3

    def test_attributes_in_output(self, mock_store):
        chunk = self._make_chunk(attributes={"item_number": 5, "state": "closed"})
        mock_store.retrieve.return_value = [chunk]
        data = json.loads(retrieve(mock_store, "q"))
        assert data[0]["attributes"]["item_number"] == 5


class TestCreateStore:
    def test_duckdb_existing(self, tmp_path):
        store_file = tmp_path / "store.duckdb"
        store_file.write_text("exists")
        with patch("raghilda.store.DuckDBStore") as MockDuckDB:
            result = create_store("owner/repo", tmp_path, "duckdb")
            MockDuckDB.connect.assert_called_once_with(str(store_file))

    def test_duckdb_new(self, tmp_path):
        mock_create = MagicMock()
        with patch("raghilda.store.DuckDBStore") as MockDuckDB, \
             patch("raghilda.embedding.EmbeddingOpenAI") as MockEmbed:
            MockDuckDB.create = mock_create
            result = create_store("owner/repo", tmp_path, "duckdb")
            mock_create.assert_called_once()

    def test_chroma_existing(self, tmp_path):
        chroma_dir = tmp_path / "chroma"
        chroma_dir.mkdir()
        with patch("raghilda.store.ChromaDBStore") as MockChroma:
            result = create_store("owner/repo", tmp_path, "chroma")
            MockChroma.connect.assert_called_once()

    def test_chroma_new(self, tmp_path):
        with patch("raghilda.store.ChromaDBStore") as MockChroma, \
             patch("raghilda.embedding.EmbeddingOpenAI") as MockEmbed:
            result = create_store("owner/repo", tmp_path, "chroma")
            MockChroma.create.assert_called_once()

    def test_unknown_store_type(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown store type"):
            create_store("owner/repo", tmp_path, "postgres")


class TestConnectStore:
    def test_duckdb_existing(self, tmp_path):
        store_file = tmp_path / "store.duckdb"
        store_file.write_text("exists")
        with patch("raghilda.store.DuckDBStore") as MockDuckDB:
            connect_store("owner/repo", tmp_path, "duckdb")
            MockDuckDB.connect.assert_called_once_with(str(store_file))

    def test_duckdb_missing(self, tmp_path):
        with pytest.raises(ValueError, match="No store found"):
            connect_store("owner/repo", tmp_path, "duckdb")

    def test_chroma_existing(self, tmp_path):
        (tmp_path / "chroma").mkdir()
        with patch("raghilda.store.ChromaDBStore") as MockChroma:
            connect_store("owner/repo", tmp_path, "chroma")
            MockChroma.connect.assert_called_once()

    def test_chroma_missing(self, tmp_path):
        with pytest.raises(ValueError, match="No store found"):
            connect_store("owner/repo", tmp_path, "chroma")

    def test_unknown_type(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown store type"):
            connect_store("owner/repo", tmp_path, "redis")
