"""Polite HTTP client — User-Agent, rate limit, retries, timeouts.

Used by every vendor ingest module. Wraps stdlib urllib.request to avoid a
new dependency; sufficient for v1 scope (bounded, serial fetches). The
public surface is small on purpose — swap to httpx behind this module if a
concrete perf problem arises.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RateLimit:
    min_interval_s: float


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str]
    body: bytes
    content_type: str
    url: str


class HttpClient:
    def __init__(
        self,
        *,
        user_agent: str,
        rate_limit: RateLimit | None = None,
        timeout_s: float = 30.0,
        max_retries: int = 3,
    ) -> None:
        self._user_agent = user_agent
        self._rate_limit = rate_limit
        self._timeout_s = timeout_s
        self._max_retries = max_retries
        self._last_fetch_at: float | None = None

    def get(self, url: str, params: dict[str, Any] | None = None) -> Response:
        full_url = url if not params else f"{url}?{urllib.parse.urlencode(params)}"

        last_err: Exception | None = None
        for attempt in range(self._max_retries):
            self._maybe_wait()
            try:
                req = urllib.request.Request(
                    full_url,
                    headers={
                        "User-Agent": self._user_agent,
                        "Accept": "application/json, */*",
                    },
                )
                with urllib.request.urlopen(req, timeout=self._timeout_s) as resp:
                    body = resp.read()
                    headers = {k.lower(): v for k, v in resp.getheaders()}
                    return Response(
                        status=resp.status,
                        headers=headers,
                        body=body,
                        content_type=headers.get("content-type", ""),
                        url=full_url,
                    )
            except urllib.error.HTTPError as e:
                # Retry transient server-side errors; surface client errors.
                if e.code == 429 or 500 <= e.code < 600:
                    last_err = e
                    self._backoff(attempt)
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last_err = e
                self._backoff(attempt)

        assert last_err is not None
        raise last_err

    def _maybe_wait(self) -> None:
        if self._rate_limit is None:
            return
        now = time.monotonic()
        if self._last_fetch_at is not None:
            wait = self._rate_limit.min_interval_s - (now - self._last_fetch_at)
            if wait > 0:
                time.sleep(wait)
        self._last_fetch_at = time.monotonic()

    def _backoff(self, attempt: int) -> None:
        time.sleep(min(0.5 * (2**attempt), 4.0))
