"""Unit tests for the polite HTTP client.

Scope: retry policy, rate-limit math, User-Agent discipline.
Mocks urllib.request.urlopen directly; no network.
"""

from __future__ import annotations

import io
import urllib.error
from unittest.mock import patch

import pytest

from arrow.ingest.common.http import HttpClient, RateLimit


class _FakeHttpResponse:
    def __init__(
        self,
        body: bytes = b'{"ok":true}',
        status: int = 200,
        content_type: str = "application/json",
    ) -> None:
        self._body = body
        self.status = status
        self._ct = content_type

    def read(self) -> bytes:
        return self._body

    def getheaders(self) -> list[tuple[str, str]]:
        return [("Content-Type", self._ct)]

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, *a: object) -> bool:
        return False


def _http_error(url: str, code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url, code, f"err{code}", {}, io.BytesIO(b""))


def test_get_returns_body_headers_and_status() -> None:
    with patch(
        "urllib.request.urlopen",
        return_value=_FakeHttpResponse(b"hi", 200, "application/json"),
    ):
        c = HttpClient(user_agent="test-ua")
        resp = c.get("https://example.com/x")

    assert resp.status == 200
    assert resp.body == b"hi"
    assert resp.content_type == "application/json"
    assert resp.url == "https://example.com/x"
    assert resp.headers["content-type"] == "application/json"


def test_sends_user_agent_header() -> None:
    captured: dict[str, str] = {}

    def _urlopen(req, timeout=None):
        captured["ua"] = req.get_header("User-agent") or ""
        return _FakeHttpResponse()

    with patch("urllib.request.urlopen", side_effect=_urlopen):
        c = HttpClient(user_agent="Arrow Research info@exascale.capital")
        c.get("https://example.com/x")

    assert captured["ua"] == "Arrow Research info@exascale.capital"


def test_retries_on_500_then_succeeds() -> None:
    calls = {"n": 0}

    def _urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 2:
            raise _http_error(req.full_url, 500)
        return _FakeHttpResponse(b"ok")

    with patch("urllib.request.urlopen", side_effect=_urlopen), patch(
        "time.sleep"
    ):
        c = HttpClient(user_agent="test", max_retries=3)
        resp = c.get("https://example.com/x")

    assert resp.body == b"ok"
    assert calls["n"] == 2


def test_retries_on_429_then_succeeds() -> None:
    calls = {"n": 0}

    def _urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 2:
            raise _http_error(req.full_url, 429)
        return _FakeHttpResponse(b"ok")

    with patch("urllib.request.urlopen", side_effect=_urlopen), patch(
        "time.sleep"
    ):
        c = HttpClient(user_agent="test", max_retries=3)
        resp = c.get("https://example.com/x")

    assert resp.body == b"ok"
    assert calls["n"] == 2


def test_surfaces_404_without_retry() -> None:
    calls = {"n": 0}

    def _urlopen(req, timeout=None):
        calls["n"] += 1
        raise _http_error(req.full_url, 404)

    with patch("urllib.request.urlopen", side_effect=_urlopen):
        c = HttpClient(user_agent="test", max_retries=3)
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            c.get("https://example.com/x")

    assert exc_info.value.code == 404
    assert calls["n"] == 1  # 4xx is not retried


def test_exhausted_retries_raises_last_error() -> None:
    def _urlopen(req, timeout=None):
        raise _http_error(req.full_url, 503)

    with patch("urllib.request.urlopen", side_effect=_urlopen), patch(
        "time.sleep"
    ):
        c = HttpClient(user_agent="test", max_retries=2)
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            c.get("https://example.com/x")

    assert exc_info.value.code == 503


def test_rate_limit_sleeps_between_requests() -> None:
    sleeps: list[float] = []

    with patch(
        "urllib.request.urlopen", return_value=_FakeHttpResponse()
    ), patch("time.sleep", side_effect=lambda s: sleeps.append(s)):
        c = HttpClient(user_agent="test", rate_limit=RateLimit(min_interval_s=0.25))
        c.get("https://example.com/a")
        c.get("https://example.com/b")

    # First request has no prior — no rate-limit sleep.
    # Second request should sleep ~0.25s (modulo tiny elapsed between calls).
    assert any(0.2 < s <= 0.25 for s in sleeps), f"expected a rate-limit sleep near 0.25, got {sleeps}"


def test_no_rate_limit_means_no_sleep() -> None:
    sleeps: list[float] = []

    with patch(
        "urllib.request.urlopen", return_value=_FakeHttpResponse()
    ), patch("time.sleep", side_effect=lambda s: sleeps.append(s)):
        c = HttpClient(user_agent="test")  # no rate_limit
        c.get("https://example.com/a")
        c.get("https://example.com/b")

    assert sleeps == []


def test_query_params_are_urlencoded() -> None:
    captured: dict[str, str] = {}

    def _urlopen(req, timeout=None):
        captured["url"] = req.full_url
        return _FakeHttpResponse()

    with patch("urllib.request.urlopen", side_effect=_urlopen):
        c = HttpClient(user_agent="test")
        c.get("https://example.com/x", params={"symbol": "NVDA", "period": "quarter"})

    assert captured["url"].startswith("https://example.com/x?")
    assert "symbol=NVDA" in captured["url"]
    assert "period=quarter" in captured["url"]
