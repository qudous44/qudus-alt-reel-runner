"""Keep the cloud publisher moving between unreliable scheduled events."""
from __future__ import annotations

import datetime as dt
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STATE_PATH = ROOT / "upload" / "scheduler_state.json"
LOG_PATH = ROOT / "upload" / "scheduler.log"


def last_log_line() -> str:
    if not LOG_PATH.exists():
        return ""
    lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
    return lines[-1] if lines else ""


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))

def delay_seconds(state: dict) -> int:
    override = os.getenv("CHAIN_DELAY_SECONDS", "").strip()
    if override:
        delay = int(override)
        if not 30 <= delay <= 1200:
            raise ValueError("CHAIN_DELAY_SECONDS must be between 30 and 1200")
        return delay
    due_at = state.get("next_due_at")
    if due_at:
        try:
            due = dt.datetime.fromisoformat(due_at)
            remaining = (due - dt.datetime.now(dt.timezone.utc)).total_seconds()
            if remaining > 0:
                return max(30, min(1200, int(remaining) + 10))
        except (TypeError, ValueError):
            pass
    return 300


def dispatch() -> None:
    token = os.environ["GITHUB_TOKEN"]
    repo = os.environ["GITHUB_REPOSITORY"]
    url = f"https://api.github.com/repos/{repo}/actions/workflows/publish.yml/dispatches"
    request = urllib.request.Request(
        url,
        data=b'{"ref":"main"}',
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "qudus-alt-reel-chain",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            if response.status not in (200, 204):
                raise RuntimeError(f"Unexpected dispatch status: {response.status}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Workflow dispatch failed: HTTP {exc.code}") from None
    print("Next cloud publisher run dispatched", flush=True)


def main() -> None:
    line = last_log_line()
    state = load_state()
    if "queue complete" in line:
        print("Queue complete; stopping chain", flush=True)
        return
    if "ambiguous publish" in line:
        print("Ambiguous publish; chain paused for manual reconciliation", flush=True)
        return
    if "daily limit reached" in line or "rolling quota reached" in line:
        print("Publish limit reached; scheduled recovery will resume later", flush=True)
        return
    delay = delay_seconds(state)
    print(f"Next cloud check in {delay}s", flush=True)
    time.sleep(delay)
    dispatch()


if __name__ == "__main__":
    main()
