from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
BATCH_PATH = ROOT / "upload" / "batches" / "batch_001_first100" / "batch_001.jsonl"
STATE_PATH = ROOT / "upload" / "scheduler_state.json"
LOCK_PATH = ROOT / "upload" / "scheduler.lock"
LOG_PATH = ROOT / "upload" / "scheduler.log"

DEFAULT_IG_ID = ""
DEFAULT_FB_PAGE_ID = ""
DEFAULT_GRAPH_VERSION = "v24.0"
DEFAULT_DAILY_LIMIT = 12
DEFAULT_MIN_INTERVAL_MINUTES = 90


class SchedulerError(RuntimeError):
    pass


class AmbiguousPublish(SchedulerError):
    """A publish may have reached Meta but the response was lost. Never blind-retry."""


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utcnow().isoformat(timespec="seconds")


def load_dotenv() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_dotenv()

TOKEN = os.getenv("META_SYSTEM_USER_TOKEN", "")
IG_ID = os.getenv("META_IG_USER_ID", DEFAULT_IG_ID)
FB_PAGE_ID = os.getenv("META_FB_PAGE_ID", DEFAULT_FB_PAGE_ID)
GRAPH_VERSION = os.getenv("META_GRAPH_VERSION", DEFAULT_GRAPH_VERSION)
GRAPH_BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"
DAILY_LIMIT = int(os.getenv("REELS_DAILY_LIMIT", str(DEFAULT_DAILY_LIMIT)))
MIN_INTERVAL_MINUTES = int(
    os.getenv("REELS_MIN_INTERVAL_MINUTES", str(DEFAULT_MIN_INTERVAL_MINUTES))
)
FB_PUBLISH_ENABLED = os.getenv("FB_PUBLISH_ENABLED", "1").strip().lower() not in {
    "0", "false", "no", "off"
}

CLOUDINARY_CLOUD_NAME = os.getenv("CLOUDINARY_CLOUD_NAME", "")
CLOUDINARY_API_KEY = os.getenv("CLOUDINARY_API_KEY", "")
CLOUDINARY_API_SECRET = os.getenv("CLOUDINARY_API_SECRET", "")
CLOUD_STATE_ENABLED = os.getenv("CLOUD_STATE_ENABLED", "1").strip().lower() not in {
    "0", "false", "no", "off"
}
CLOUD_BATCH_PUBLIC_ID = os.getenv(
    "CLOUD_BATCH_PUBLIC_ID", "qudus_alt/state/batch_001.jsonl"
)
CLOUD_STATE_PUBLIC_ID = os.getenv(
    "CLOUD_STATE_PUBLIC_ID", "qudus_alt/state/scheduler_state.json"
)


def log(message: str) -> None:
    line = f"{iso_now()} {message}"
    print(line, flush=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def _cloud_ready() -> bool:
    return bool(
        CLOUD_STATE_ENABLED
        and CLOUDINARY_CLOUD_NAME
        and CLOUDINARY_API_KEY
        and CLOUDINARY_API_SECRET
    )


def _cloud_signature(params: dict[str, str]) -> str:
    unsigned = "&".join(f"{key}={params[key]}" for key in sorted(params))
    return hashlib.sha1((unsigned + CLOUDINARY_API_SECRET).encode("utf-8")).hexdigest()


def cloud_upload_file(path: Path, public_id: str) -> None:
    if not _cloud_ready():
        if CLOUD_STATE_ENABLED:
            raise SchedulerError("Cloudinary state backend credentials are missing")
        return
    timestamp = str(int(time.time()))
    signed = {
        "invalidate": "true",
        "overwrite": "true",
        "public_id": public_id,
        "timestamp": timestamp,
    }
    data = {
        **signed,
        "api_key": CLOUDINARY_API_KEY,
        "signature": _cloud_signature(signed),
    }
    with path.open("rb") as fh:
        response = requests.post(
            f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD_NAME}/raw/upload",
            data=data,
            files={"file": fh},
            timeout=60,
        )
    if not response.ok:
        raise SchedulerError(
            f"Cloudinary state upload failed HTTP {response.status_code}: "
            f"{response.text[:300]}"
        )


def cloud_download_file(path: Path, public_id: str) -> bool:
    if not _cloud_ready():
        if CLOUD_STATE_ENABLED:
            raise SchedulerError("Cloudinary state backend credentials are missing")
        return False
    url = (
        f"https://res.cloudinary.com/{CLOUDINARY_CLOUD_NAME}/raw/upload/"
        f"{public_id}?cb={int(time.time())}"
    )
    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        return False
    if not response.ok:
        raise SchedulerError(
            f"Cloudinary state download failed HTTP {response.status_code}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".cloud.tmp")
    tmp.write_bytes(response.content)
    os.replace(tmp, path)
    return True


def cloud_sync_from_remote() -> None:
    if not CLOUD_STATE_ENABLED:
        return
    got_batch = cloud_download_file(BATCH_PATH, CLOUD_BATCH_PUBLIC_ID)
    got_state = cloud_download_file(STATE_PATH, CLOUD_STATE_PUBLIC_ID)
    if not got_batch:
        cloud_upload_file(BATCH_PATH, CLOUD_BATCH_PUBLIC_ID)
    if not got_state and STATE_PATH.exists():
        cloud_upload_file(STATE_PATH, CLOUD_STATE_PUBLIC_ID)


def load_rows() -> list[dict]:
    if not BATCH_PATH.exists():
        raise SchedulerError(f"manifest not found: {BATCH_PATH}")
    rows = []
    for n, raw in enumerate(BATCH_PATH.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            rows.append(json.loads(raw))
        except json.JSONDecodeError as exc:
            raise SchedulerError(f"invalid JSONL line {n}: {exc}") from exc
    return rows


def checkpoint(rows: list[dict]) -> None:
    BATCH_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = BATCH_PATH.with_suffix(".jsonl.scheduler.tmp")
    backup = BATCH_PATH.with_suffix(".jsonl.scheduler.bak")
    payload = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
    if BATCH_PATH.exists():
        with contextlib.suppress(OSError):
            shutil.copy2(BATCH_PATH, backup)
    tmp.write_text(payload, encoding="utf-8")
    last_exc = None
    for attempt in range(8):
        try:
            os.replace(tmp, BATCH_PATH)
            cloud_upload_file(BATCH_PATH, CLOUD_BATCH_PUBLIC_ID)
            return
        except PermissionError as exc:
            last_exc = exc
            time.sleep(0.35 * (attempt + 1))
    # Some Windows programs keep a handle open with delete/rename denied.
    # In that case os.replace() fails even though ordinary writes are allowed.
    # We already made a backup above, so fall back to an in-place rewrite.
    for attempt in range(8):
        try:
            with BATCH_PATH.open("w", encoding="utf-8", newline="") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            with contextlib.suppress(OSError):
                tmp.unlink()
            cloud_upload_file(BATCH_PATH, CLOUD_BATCH_PUBLIC_ID)
            return
        except PermissionError as exc:
            last_exc = exc
            time.sleep(0.35 * (attempt + 1))
    recovery = BATCH_PATH.with_name(
        BATCH_PATH.stem + f".recovery.{int(time.time())}.jsonl"
    )
    recovery.write_text(payload, encoding="utf-8")
    raise SchedulerError(
        f"manifest locked for replace and write; recovery written to {recovery}"
    ) from last_exc


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    payload = json.dumps(state, indent=2, ensure_ascii=False)
    tmp.write_text(payload, encoding="utf-8")
    try:
        os.replace(tmp, STATE_PATH)
    except PermissionError:
        STATE_PATH.write_text(payload, encoding="utf-8")
        with contextlib.suppress(OSError):
            tmp.unlink()
    cloud_upload_file(STATE_PATH, CLOUD_STATE_PUBLIC_ID)


@contextlib.contextmanager
def single_instance():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        age = time.time() - LOCK_PATH.stat().st_mtime
        if age < 6 * 3600:
            raise SchedulerError("another scheduler instance is active")
        with contextlib.suppress(OSError):
            LOCK_PATH.unlink()
    try:
        LOCK_PATH.write_text(str(os.getpid()), encoding="ascii")
        yield
    finally:
        with contextlib.suppress(OSError):
            LOCK_PATH.unlink()


def media_fingerprint(row: dict) -> str:
    paths = row.get("media_paths") or []
    if paths:
        local = ROOT / paths[0]
        if local.exists() and local.is_file():
            h = hashlib.sha256()
            with local.open("rb") as fh:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    h.update(chunk)
            return h.hexdigest()
    value = row.get("cloudinary_video_url") or row.get("queue_id") or ""
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def transformed_video_url(url: str, *, facebook: bool) -> str:
    if "/upload/" not in url:
        raise SchedulerError("Cloudinary URL is missing /upload/")
    left, right = url.split("/upload/", 1)
    base = "f_mp4,vc_h264,ac_aac,fps_30"
    if facebook:
        base = "c_pad,w_1080,h_1920,b_black/" + base
    return left + "/upload/" + base + "/" + right


def http_json(
    method: str,
    url: str,
    *,
    retry_safe: bool,
    timeout: int = 60,
    **kwargs,
) -> dict:
    attempts = 3 if retry_safe else 1
    last_exc = None
    for attempt in range(attempts):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt + 1 < attempts:
                time.sleep(2 ** attempt)
                continue
            raise SchedulerError(f"network error: {type(exc).__name__}") from None
        if response.ok:
            try:
                return response.json()
            except ValueError as exc:
                raise SchedulerError("Meta returned non-JSON success response") from exc
        if retry_safe and (response.status_code == 429 or response.status_code >= 500):
            if attempt + 1 < attempts:
                retry_after = response.headers.get("Retry-After")
                delay = int(retry_after) if (retry_after or "").isdigit() else 2 ** attempt
                time.sleep(max(1, delay))
                continue
        try:
            body = response.json()
        except ValueError:
            body = {"raw": response.text[:500]}
        raise SchedulerError(f"Meta HTTP {response.status_code}: {body}")
    raise SchedulerError(f"request failed: {last_exc}")


def ig_quota() -> tuple[int | None, int | None, int | None]:
    data = http_json(
        "GET",
        f"{GRAPH_BASE}/{IG_ID}/content_publishing_limit",
        retry_safe=True,
        params={"fields": "quota_usage,config", "access_token": TOKEN},
    )
    entry = (data.get("data") or [{}])[0]
    config = entry.get("config") or {}
    return entry.get("quota_usage"), config.get("quota_total"), config.get("quota_duration")


def page_token() -> str:
    data = http_json(
        "GET",
        f"{GRAPH_BASE}/{FB_PAGE_ID}",
        retry_safe=True,
        params={"fields": "access_token", "access_token": TOKEN},
    )
    token = data.get("access_token")
    if not token:
        raise SchedulerError("Page access token was not returned")
    return token


def poll_ig_container(container_id: str) -> str:
    for _ in range(45):
        data = http_json(
            "GET",
            f"{GRAPH_BASE}/{container_id}",
            retry_safe=True,
            params={"fields": "status_code,status", "access_token": TOKEN},
        )
        status = data.get("status_code") or "UNKNOWN"
        if status in {"FINISHED", "ERROR", "EXPIRED"}:
            return status
        time.sleep(4)
    return "TIMEOUT"


def ensure_ig(row: dict, rows: list[dict]) -> bool:
    if row.get("published_ig_id"):
        return False
    if row.get("ig_publish_ambiguous"):
        raise AmbiguousPublish(
            f"{row.get('queue_id')} Instagram publish is ambiguous; refusing blind retry"
        )
    source = row.get("cloudinary_video_url")
    if not source:
        raise SchedulerError("missing cloudinary_video_url")
    video_url = transformed_video_url(source, facebook=False)
    row["meta_video_url"] = video_url
    if not row.get("ig_creation_id"):
        data = {
            "media_type": "REELS",
            "video_url": video_url,
            "caption": row.get("caption_draft") or "",
            "share_to_feed": "true",
            "access_token": TOKEN,
        }
        cover = row.get("cloudinary_cover_url")
        if cover:
            data["cover_url"] = cover
        created = http_json(
            "POST", f"{GRAPH_BASE}/{IG_ID}/media", retry_safe=True, data=data
        )
        row["ig_creation_id"] = created["id"]
        row["batch_status"] = "ig_processing"
        checkpoint(rows)
    status = poll_ig_container(row["ig_creation_id"])
    if status != "FINISHED":
        row["ig_last_status"] = status
        checkpoint(rows)
        raise SchedulerError(f"Instagram container status: {status}")
    try:
        published = http_json(
            "POST",
            f"{GRAPH_BASE}/{IG_ID}/media_publish",
            retry_safe=False,
            data={"creation_id": row["ig_creation_id"], "access_token": TOKEN},
        )
    except SchedulerError:
        row["ig_publish_ambiguous"] = True
        row["ig_publish_ambiguous_at"] = iso_now()
        checkpoint(rows)
        raise AmbiguousPublish(
            f"{row.get('queue_id')} Instagram publish response lost; stopped to prevent duplicate"
        )
    row["published_ig_id"] = published["id"]
    row["publisher"] = "meta_graph_api"
    row["ig_published_at"] = iso_now()
    row["batch_status"] = "ig_published"
    checkpoint(rows)
    return True


def poll_fb_status(video_id: str, token: str) -> str:
    for _ in range(18):
        try:
            data = http_json(
                "GET",
                f"{GRAPH_BASE}/{video_id}",
                retry_safe=True,
                params={"fields": "status", "access_token": token},
            )
        except SchedulerError:
            return "UNKNOWN"
        status_obj = data.get("status") or {}
        raw = status_obj.get("video_status") or "unknown"
        if raw == "ready":
            return "FINISHED"
        if raw in {"error", "upload_failed"}:
            return "ERROR"
        if raw == "expired":
            return "EXPIRED"
        time.sleep(5)
    return "PROCESSING"


def ensure_fb(row: dict, rows: list[dict]) -> bool:
    if row.get("published_fb_id"):
        return False
    if row.get("fb_finish_ambiguous"):
        raise AmbiguousPublish(
            f"{row.get('queue_id')} Facebook finish is ambiguous; refusing blind retry"
        )
    source = row.get("cloudinary_video_url")
    if not source:
        raise SchedulerError("missing cloudinary_video_url")
    token = page_token()
    video_url = transformed_video_url(source, facebook=True)
    row["fb_video_url"] = video_url
    if not row.get("fb_video_id"):
        started = http_json(
            "POST",
            f"{GRAPH_BASE}/{FB_PAGE_ID}/video_reels",
            retry_safe=True,
            data={"upload_phase": "start", "access_token": token},
        )
        row["fb_video_id"] = started["video_id"]
        row["fb_upload_url"] = started.get("upload_url")
        row["batch_status"] = "fb_started"
        checkpoint(rows)
    if not row.get("fb_upload_done"):
        upload_url = row.get("fb_upload_url") or (
            f"https://rupload.facebook.com/video-upload/{GRAPH_VERSION}/{row['fb_video_id']}"
        )
        uploaded = http_json(
            "POST",
            upload_url,
            retry_safe=True,
            headers={"Authorization": f"OAuth {token}", "file_url": video_url},
            timeout=120,
        )
        if uploaded.get("success") is not True:
            raise SchedulerError(f"Facebook upload refused: {uploaded}")
        row["fb_upload_done"] = True
        row["batch_status"] = "fb_uploaded"
        checkpoint(rows)
    try:
        finished = http_json(
            "POST",
            f"{GRAPH_BASE}/{FB_PAGE_ID}/video_reels",
            retry_safe=False,
            data={
                "upload_phase": "finish",
                "video_id": row["fb_video_id"],
                "video_state": "PUBLISHED",
                "description": row.get("caption_draft") or "",
                "access_token": token,
            },
        )
    except SchedulerError:
        row["fb_finish_ambiguous"] = True
        row["fb_finish_ambiguous_at"] = iso_now()
        checkpoint(rows)
        raise AmbiguousPublish(
            f"{row.get('queue_id')} Facebook finish response lost; stopped to prevent duplicate"
        )
    if finished.get("success") is not True:
        raise SchedulerError(f"Facebook finish refused: {finished}")
    # Facebook is already committed at this point. Record the id BEFORE any status poll.
    row["published_fb_id"] = row["fb_video_id"]
    row["fb_published_at"] = iso_now()
    row["batch_status"] = "published_both" if row.get("published_ig_id") else "fb_published"
    checkpoint(rows)
    row["fb_last_status"] = poll_fb_status(row["fb_video_id"], token)
    checkpoint(rows)
    return True


def reset_daily_state(state: dict) -> None:
    today = utcnow().date().isoformat()
    if state.get("day") != today:
        state["day"] = today
        state["jobs_today"] = 0


def seconds_until_due(state: dict) -> int:
    last = state.get("last_action_at")
    if not last:
        return 0
    try:
        when = dt.datetime.fromisoformat(last)
    except ValueError:
        return 0
    due = when + dt.timedelta(minutes=MIN_INTERVAL_MINUTES)
    return max(0, int((due - utcnow()).total_seconds()))


def pending_row(rows: list[dict]) -> dict | None:
    # Paired mode: when Facebook publishing is enabled, finish both destinations
    # for each queue item before advancing to the next item.
    for row in rows:
        if row.get("scheduler_blocked"):
            continue
        if FB_PUBLISH_ENABLED:
            if not row.get("published_ig_id") or not row.get("published_fb_id"):
                return row
        elif not row.get("published_ig_id"):
            return row
    return None


def preflight(rows: list[dict]) -> dict:
    if not TOKEN:
        raise SchedulerError("META_SYSTEM_USER_TOKEN is missing")
    usage, total, duration = ig_quota()
    row = pending_row(rows)
    result = {
        "ig_id": IG_ID,
        "fb_page_id": FB_PAGE_ID,
        "quota_usage": usage,
        "quota_total": total,
        "quota_duration": duration,
        "pending_queue_id": row.get("queue_id") if row else None,
        "daily_limit": DAILY_LIMIT,
        "min_interval_minutes": MIN_INTERVAL_MINUTES,
        "fb_publish_enabled": FB_PUBLISH_ENABLED,
    }
    return result


def run_once(*, live: bool) -> int:
    with single_instance():
        rows = load_rows()
        state = load_state()
        reset_daily_state(state)
        info = preflight(rows)
        log("preflight " + json.dumps(info, ensure_ascii=False))
        row = pending_row(rows)
        if row is None:
            state["last_result"] = "queue_complete"
            save_state(state)
            log("queue complete")
            return 0
        row["media_fingerprint"] = row.get("media_fingerprint") or media_fingerprint(row)
        if not live:
            log(
                f"DRY RUN next={row.get('queue_id')} pos={row.get('batch_position')} "
                f"ig_done={bool(row.get('published_ig_id'))} fb_done={bool(row.get('published_fb_id'))}"
            )
            return 0
        wait = seconds_until_due(state)
        if wait:
            log(f"not due yet; {wait}s remaining")
            return 0
        if int(state.get("jobs_today", 0)) >= DAILY_LIMIT:
            log(f"self-imposed daily limit reached: {DAILY_LIMIT}")
            return 0
        usage = info.get("quota_usage")
        total = info.get("quota_total")
        if (
            not row.get("published_ig_id")
            and isinstance(usage, int)
            and isinstance(total, int)
            and usage >= total
        ):
            log(f"Meta Instagram rolling quota reached: {usage}/{total}")
            return 0
        changed = False
        try:
            changed = ensure_ig(row, rows) or changed
            if FB_PUBLISH_ENABLED:
                changed = ensure_fb(row, rows) or changed
        except AmbiguousPublish as exc:
            state["last_error"] = str(exc)
            state["last_error_at"] = iso_now()
            save_state(state)
            log(f"HALT ambiguous publish: {exc}")
            return 3
        except SchedulerError as exc:
            row["scheduler_last_error"] = str(exc)
            row["scheduler_last_error_at"] = iso_now()
            checkpoint(rows)
            state["last_error"] = str(exc)
            state["last_error_at"] = iso_now()
            save_state(state)
            log(f"ERROR {exc}")
            return 2
        if changed:
            state["jobs_today"] = int(state.get("jobs_today", 0)) + 1
            state["last_action_at"] = iso_now()
            state["last_queue_id"] = row.get("queue_id")
            state["last_result"] = "published"
            state.pop("last_error", None)
            save_state(state)
            log(
                f"published {row.get('queue_id')} "
                f"IG={row.get('published_ig_id')} FB={row.get('published_fb_id')}"
            )
        return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Allow one due publish. Without this flag the worker is read-only/dry-run.",
    )
    args = parser.parse_args()
    try:
        if args.preflight:
            print(json.dumps(preflight(load_rows()), indent=2, ensure_ascii=False))
            return 0
        return run_once(live=args.live)
    except SchedulerError as exc:
        log(f"FATAL {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
