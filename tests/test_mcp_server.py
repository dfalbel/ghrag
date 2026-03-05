"""Tests for ghrag.mcp_server — _parse_sync_interval."""

import pytest

from ghrag.mcp_server import _parse_sync_interval


class TestParseSyncInterval:
    def test_valid_positive(self):
        assert _parse_sync_interval(30) == 30

    def test_none(self):
        assert _parse_sync_interval(None) is None

    def test_zero_raises(self):
        with pytest.raises(ValueError, match="positive integer"):
            _parse_sync_interval(0)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="positive integer"):
            _parse_sync_interval(-5)
