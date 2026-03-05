"""Tests for ghrag.github — get_github_token, issue_to_dict, issue_to_document."""

from unittest.mock import MagicMock, patch
from datetime import datetime

import pytest

from ghrag.github import get_github_token, issue_to_dict, issue_to_document


class TestGetGithubToken:
    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_test123")
        assert get_github_token() == "ghp_test123"

    def test_from_gh_cli(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        result = MagicMock(returncode=0, stdout="ghp_fromcli\n")
        with patch("subprocess.run", return_value=result) as mock_run:
            assert get_github_token() == "ghp_fromcli"
            mock_run.assert_called_once_with(
                ["gh", "auth", "token"], capture_output=True, text=True
            )

    def test_raises_when_gh_not_found(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        with patch("subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(RuntimeError, match="No GitHub token found"):
                get_github_token()

    def test_raises_when_gh_fails(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        result = MagicMock(returncode=1, stdout="")
        with patch("subprocess.run", return_value=result):
            with pytest.raises(RuntimeError, match="No GitHub token found"):
                get_github_token()


class TestIssueToDict:
    def _make_mock_issue(self):
        issue = MagicMock()
        issue.number = 10
        issue.title = "Test issue"
        issue.body = "Body text"
        issue.html_url = "https://github.com/o/r/issues/10"
        issue.state = "open"
        issue.user.login = "alice"
        issue.labels = [MagicMock(name="bug"), MagicMock(name="help wanted")]
        issue.created_at = datetime(2025, 1, 1, 12, 0, 0)
        issue.updated_at = datetime(2025, 2, 1, 12, 0, 0)
        issue.pull_request = None

        comment = MagicMock()
        comment.user.login = "bob"
        comment.body = "Nice find!"
        comment.created_at = datetime(2025, 1, 2, 9, 0, 0)
        issue.get_comments.return_value = [comment]

        return issue

    def test_basic_structure(self):
        issue = self._make_mock_issue()
        d = issue_to_dict(issue)

        assert d["number"] == 10
        assert d["title"] == "Test issue"
        assert d["body"] == "Body text"
        assert d["state"] == "open"
        assert d["author"] == "alice"
        assert d["is_pull_request"] is False
        assert d["url"] == "https://github.com/o/r/issues/10"

    def test_comments(self):
        issue = self._make_mock_issue()
        d = issue_to_dict(issue)

        assert len(d["comments"]) == 1
        assert d["comments"][0]["author"] == "bob"
        assert d["comments"][0]["body"] == "Nice find!"

    def test_pull_request_detection(self):
        issue = self._make_mock_issue()
        issue.pull_request = MagicMock()
        d = issue_to_dict(issue)
        assert d["is_pull_request"] is True

    def test_dates_are_isoformat(self):
        issue = self._make_mock_issue()
        d = issue_to_dict(issue)
        # Should not raise
        datetime.fromisoformat(d["created_at"])
        datetime.fromisoformat(d["updated_at"])


class TestIssueToDocument:
    def test_returns_chunked_document(self, sample_issue_dict):
        doc = issue_to_document(sample_issue_dict)
        assert len(doc.chunks) > 0

    def test_attributes(self, sample_issue_dict):
        doc = issue_to_document(sample_issue_dict)
        attrs = doc.attributes
        assert attrs["item_number"] == 42
        assert attrs["state"] == "open"
        assert attrs["labels"] == "bug, auth"
        assert isinstance(attrs["updated_at"], int)

    def test_pr_label(self, sample_issue_dict):
        sample_issue_dict["is_pull_request"] = True
        doc = issue_to_document(sample_issue_dict)
        assert doc.chunks[0].text.startswith("# PR #42")

    def test_issue_label(self, sample_issue_dict):
        doc = issue_to_document(sample_issue_dict)
        assert doc.chunks[0].text.startswith("# Issue #42")
