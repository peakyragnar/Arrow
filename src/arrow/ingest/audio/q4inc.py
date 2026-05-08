"""Q4Inc earnings-webcast audio adapter.

Background:
- Q4Inc is the dominant US IR webcast vendor. Once a call ends and the
  edited replay posts (typically within a few hours), the audio is
  served from `static.events.q4inc.com/edited-recordings/{event_id}/{uuid}.mp4`.
- That CloudFront URL is publicly reachable from any machine with no auth
  and no Cloudflare challenge — verified empirically against multiple
  Q4-hosted issuers.
- The challenge is finding the UUID. Q4's player at
  `events.q4inc.com/attendee/{event_id}` is a React SPA that gates content
  behind a name+email registration form. We use Playwright to drive a
  real Chromium, fill the form with research credentials, and watch the
  network for the `.mp4` URL the player triggers when it starts.

Manual fallback:
- If `discover_audio_url(...)` returns None (registration form changed,
  Cloudflare flagged the headless browser, network capture timed out,
  etc.), the orchestrator falls through to `accept_pasted_url(...)`,
  which is the path the operator's manual DevTools paste went through
  during the proof. That fallback always works as long as the operator
  can load the page in their browser.
"""

from __future__ import annotations

import re
import time
from typing import Any

from .contracts import AudioRef

_Q4_AUDIO_PATTERN = re.compile(
    r"https://static\.events\.q4inc\.com/edited-recordings/(\d+)/([0-9a-f-]+)\.mp4",
    re.IGNORECASE,
)

DEFAULT_REGISTRATION = {
    "first_name": "Arrow",
    "last_name": "Research",
    "email": "research@arrow.example",
    "company": "Arrow Research",
    "title": "Analyst",
    "phone": "555-555-5555",
}


def attendee_url(event_id: str) -> str:
    """The canonical attendee URL for a Q4 event."""
    return f"https://events.q4inc.com/attendee/{event_id}"


def parse_q4_audio_url(url: str) -> tuple[str, str] | None:
    """Extract (event_id, uuid) from a Q4 edited-recording URL.

    Returns None if the URL doesn't match Q4's pattern.
    """
    m = _Q4_AUDIO_PATTERN.match(url)
    if not m:
        return None
    return m.group(1), m.group(2)


def accept_pasted_url(url: str, *, expected_event_id: str | None = None) -> AudioRef:
    """Manual fallback: the operator pasted a URL from their browser.

    Validates that it matches Q4's edited-recording pattern and that
    its event_id matches what we expected (if provided).
    """
    parsed = parse_q4_audio_url(url)
    if parsed is None:
        raise ValueError(
            f"Pasted URL does not match Q4 edited-recording pattern. "
            f"Expected something like "
            f"https://static.events.q4inc.com/edited-recordings/<event_id>/<uuid>.mp4 "
            f"but got: {url}"
        )
    event_id, uuid = parsed
    if expected_event_id is not None and event_id != expected_event_id:
        raise ValueError(
            f"Pasted URL is for event_id={event_id}, expected {expected_event_id}. "
            f"Did you paste the wrong call's URL?"
        )
    return AudioRef(
        vendor="q4inc",
        event_id=event_id,
        source_url=url,
        source_uuid=uuid,
        discovered_via="manual_paste",
    )


def discover_audio_url(
    event_id: str,
    *,
    timeout_sec: float = 90.0,
    headless: bool = True,
    register_with: dict[str, str] | None = None,
) -> AudioRef | None:
    """Drive Playwright to discover the public CloudFront audio URL.

    Returns None on failure — caller falls back to accept_pasted_url().
    Reasons for None:
    - Playwright not installed / Chromium not present
    - Registration form fields unrecognized (vendor changed UI)
    - Cloudflare flagged the headless browser
    - 90 sec elapsed without seeing the .mp4 request

    Setting `headless=False` runs a visible Chromium window — useful when
    debugging why discovery failed for a specific issuer.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    except ImportError:
        return None

    register_with = {**DEFAULT_REGISTRATION, **(register_with or {})}
    discovered: list[str] = []  # nonlocal-friendly mutable container
    page_url = attendee_url(event_id)

    def on_request(request: Any) -> None:
        url = request.url
        if _Q4_AUDIO_PATTERN.match(url):
            discovered.append(url)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/132.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()
            page.on("request", on_request)

            try:
                page.goto(page_url, timeout=int(timeout_sec * 1000), wait_until="domcontentloaded")
            except PlaywrightTimeout:
                browser.close()
                return None

            # Try to fill + submit a registration form, if one is present.
            # If form can't be filled, we still wait — some Q4 customers don't
            # require registration.
            _try_fill_q4_registration(page, register_with)

            # Try clicking any "Play" / "Listen" button that might be present
            _try_click_play(page)

            # Wait for the .mp4 request to fire, up to remaining timeout
            deadline = time.time() + timeout_sec
            while not discovered and time.time() < deadline:
                page.wait_for_timeout(500)

            browser.close()
    except Exception:
        return None

    if not discovered:
        return None

    parsed = parse_q4_audio_url(discovered[0])
    if parsed is None:
        return None
    parsed_event_id, parsed_uuid = parsed
    return AudioRef(
        vendor="q4inc",
        event_id=parsed_event_id,
        source_url=discovered[0],
        source_uuid=parsed_uuid,
        discovered_via="playwright",
    )


def _try_fill_q4_registration(page: Any, info: dict[str, str]) -> None:
    """Best-effort Q4 registration form fill. Silent on any failure."""
    field_map = [
        # (info_key, list of common label/placeholder/name patterns)
        ("first_name", ["first name", "firstname", "first"]),
        ("last_name", ["last name", "lastname", "last"]),
        ("email", ["email", "e-mail"]),
        ("company", ["company", "organization", "firm"]),
        ("title", ["title", "job title", "role"]),
        ("phone", ["phone", "telephone"]),
    ]
    for info_key, patterns in field_map:
        value = info.get(info_key, "")
        if not value:
            continue
        for pat in patterns:
            try:
                # Try by label
                loc = page.get_by_label(pat, exact=False)
                if loc.count() > 0:
                    loc.first.fill(value, timeout=2000)
                    break
            except Exception:
                pass
            try:
                # Try by placeholder
                loc = page.get_by_placeholder(pat, exact=False)
                if loc.count() > 0:
                    loc.first.fill(value, timeout=2000)
                    break
            except Exception:
                pass

    # Try clicking a submit / register / continue button
    for label in ("Submit", "Register", "Continue", "Enter", "Join"):
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.I))
            if btn.count() > 0:
                btn.first.click(timeout=2000)
                page.wait_for_timeout(1500)
                break
        except Exception:
            pass


def _try_click_play(page: Any) -> None:
    """Best-effort attempt to start playback so the .mp4 request fires."""
    for label in ("Play", "Listen", "Start"):
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.I))
            if btn.count() > 0:
                btn.first.click(timeout=2000)
                page.wait_for_timeout(1500)
                return
        except Exception:
            pass
    # Last resort: hit space (HTML5 audio/video play shortcut)
    try:
        page.keyboard.press("Space", timeout=1000)
    except Exception:
        pass
