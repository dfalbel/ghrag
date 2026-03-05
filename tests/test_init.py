"""Tests for ghrag.__init__ — validate_repo, get_cache_dir, list_repos, delete."""

import pytest

from ghrag import validate_repo, get_cache_dir, list_repos, delete


class TestValidateRepo:
    def test_valid(self):
        assert validate_repo("owner/repo") == "owner/repo"

    def test_strips_whitespace(self):
        assert validate_repo("  owner/repo  ") == "owner/repo"

    def test_missing_slash(self):
        with pytest.raises(ValueError):
            validate_repo("ownerrepo")

    def test_traversal_dots(self):
        with pytest.raises(ValueError):
            validate_repo("../evil")

    def test_empty_segment(self):
        with pytest.raises(ValueError):
            validate_repo("/repo")

    def test_empty_name(self):
        with pytest.raises(ValueError):
            validate_repo("owner/")

    def test_too_many_segments(self):
        with pytest.raises(ValueError):
            validate_repo("a/b/c")

    def test_special_chars(self):
        with pytest.raises(ValueError):
            validate_repo("owner/repo name")


class TestGetCacheDir:
    def test_creates_directory(self, patched_cache_root):
        d = get_cache_dir("owner/repo")
        assert d.exists()
        assert d.is_dir()

    def test_correct_path(self, patched_cache_root):
        d = get_cache_dir("owner/repo")
        assert d == patched_cache_root / "owner" / "repo"

    def test_idempotent(self, patched_cache_root):
        d1 = get_cache_dir("owner/repo")
        d2 = get_cache_dir("owner/repo")
        assert d1 == d2


class TestListRepos:
    def test_empty(self, patched_cache_root):
        assert list_repos() == []

    def test_one_repo(self, patched_cache_root):
        (patched_cache_root / "alice" / "project").mkdir(parents=True)
        assert list_repos() == ["alice/project"]

    def test_multiple_repos(self, patched_cache_root):
        (patched_cache_root / "alice" / "aaa").mkdir(parents=True)
        (patched_cache_root / "alice" / "zzz").mkdir(parents=True)
        (patched_cache_root / "bob" / "tool").mkdir(parents=True)
        assert list_repos() == ["alice/aaa", "alice/zzz", "bob/tool"]

    def test_ignores_files(self, patched_cache_root):
        (patched_cache_root / "somefile.txt").write_text("hi")
        assert list_repos() == []


class TestDelete:
    def test_removes_directory(self, patched_cache_root):
        d = patched_cache_root / "owner" / "repo"
        d.mkdir(parents=True)
        (d / "store.duckdb").write_text("data")
        delete("owner/repo")
        assert not d.exists()

    def test_keep_cache_preserves_issues(self, patched_cache_root):
        d = patched_cache_root / "owner" / "repo"
        d.mkdir(parents=True)
        (d / "issues.jsonl").write_text("{}\n")
        (d / "cache_last_update.txt").write_text("2025-01-01")
        (d / "store.duckdb").write_text("data")
        (d / "extra").mkdir()

        delete("owner/repo", keep_cache=True)

        assert (d / "issues.jsonl").exists()
        assert (d / "cache_last_update.txt").exists()
        assert not (d / "store.duckdb").exists()
        assert not (d / "extra").exists()

    def test_raises_on_missing(self, patched_cache_root):
        with pytest.raises(ValueError, match="No local database found"):
            delete("owner/nonexistent")
