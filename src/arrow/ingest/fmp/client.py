"""FMP API client — thin wrapper around ingest/common/http.

Handles:
  - Base URL (FMP stable endpoint)
  - apikey query parameter (from FMP_API_KEY env)
  - Polite User-Agent
  - Conservative rate limit (4 req/sec)

The apikey is stripped from the returned `Response.url` so callers can
safely write it to raw_responses.request_url without leaking the secret.
Callers pair this with ingest/common/raw_responses.write_raw_response
to persist the payload.
"""

from __future__ import annotations

import os
import urllib.parse
from typing import Any

from arrow.ingest.common.http import HttpClient, RateLimit, Response

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_USER_AGENT = "Arrow Research info@exascale.capital"
# Conservative: 4 req/sec. FMP paid tiers allow more; starting tight to
# avoid backing ourselves into a throttle ban.
FMP_RATE_LIMIT = RateLimit(min_interval_s=0.25)


class FMPClient:
    def __init__(self, *, api_key: str | None = None) -> None:
        key = api_key or os.environ.get("FMP_API_KEY") or os.environ.get("FMP_API")
        if not key:
            raise RuntimeError(
                "FMP_API_KEY is not set. Add it to .env or the environment."
            )
        self._api_key = key
        self._http = HttpClient(
            user_agent=FMP_USER_AGENT,
            rate_limit=FMP_RATE_LIMIT,
        )

    def get(self, endpoint: str, **params: Any) -> Response:
        """Fetch a stable FMP endpoint with apikey authentication.

        Returns a Response whose `url` is sanitized (apikey removed) so it
        can be safely recorded in raw_responses.request_url.
        """
        path = endpoint.lstrip("/")
        base_url = f"{FMP_BASE}/{path}"
        auth_params = {**params, "apikey": self._api_key}
        resp = self._http.get(base_url, params=auth_params)

        if params:
            sanitized_url = f"{base_url}?{urllib.parse.urlencode(params)}"
        else:
            sanitized_url = base_url
        return Response(
            status=resp.status,
            headers=resp.headers,
            body=resp.body,
            content_type=resp.content_type,
            url=sanitized_url,
        )
