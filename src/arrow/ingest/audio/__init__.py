"""Audio acquisition layer for the ASR transcripts vertical.

See docs/architecture/asr_transcripts_ingest_plan.md.

Design summary:
- Acquisition vendors live in their own module (q4inc.py, youtube.py, ...).
- Each vendor exposes the AudioSource protocol from contracts.
- A single download() helper handles the actual byte transfer once a URL
  is known — most vendor URLs are unauthenticated public CloudFront once
  you know them, so download is uniform.
- Manual fallback: any vendor adapter can return None from discover_*,
  and the orchestrator falls through to accepting an operator-pasted URL.
"""
