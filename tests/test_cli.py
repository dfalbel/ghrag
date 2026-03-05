"""CLI smoke tests via typer's CliRunner."""

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from ghrag.cli import app

runner = CliRunner()


class TestListCommand:
    def test_list_empty(self):
        with patch("ghrag.list_repos", return_value=[]):
            result = runner.invoke(app, ["list"])
            assert "No local databases found" in result.output

    def test_list_repos(self):
        with patch("ghrag.list_repos", return_value=["alice/proj", "bob/tool"]):
            result = runner.invoke(app, ["list"])
            assert "alice/proj" in result.output
            assert "bob/tool" in result.output


class TestRetrieveCommand:
    def test_retrieve(self):
        with patch("ghrag.get_cache_dir") as mock_cache, \
             patch("ghrag.store.connect_store") as mock_connect, \
             patch("ghrag.store.retrieve", return_value='[{"text":"hello"}]'):
            mock_cache.return_value = "/tmp/fake"
            mock_connect.return_value = MagicMock()
            result = runner.invoke(app, ["retrieve", "owner/repo", "search query"])
            assert result.exit_code == 0
            assert "hello" in result.output

    def test_retrieve_missing_store(self):
        with patch("ghrag.get_cache_dir"), \
             patch("ghrag.store.connect_store", side_effect=ValueError("No store found")):
            result = runner.invoke(app, ["retrieve", "owner/repo", "query"])
            assert result.exit_code == 1
            assert "No store found" in result.output


class TestDeleteCommand:
    def test_delete_with_yes(self):
        with patch("ghrag.delete") as mock_delete:
            result = runner.invoke(app, ["delete", "owner/repo", "--yes"])
            assert result.exit_code == 0
            mock_delete.assert_called_once_with("owner/repo", keep_cache=False)

    def test_delete_declined(self):
        result = runner.invoke(app, ["delete", "owner/repo"], input="n\n")
        assert result.exit_code != 0  # Aborted

    def test_delete_missing_repo(self):
        with patch("ghrag.delete", side_effect=ValueError("No local database found")):
            result = runner.invoke(app, ["delete", "owner/repo", "--yes"])
            assert result.exit_code == 1
            assert "No local database found" in result.output
