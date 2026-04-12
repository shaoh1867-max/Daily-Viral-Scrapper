#!/usr/bin/env python3
"""
Instagram Reels scraper using the Bright Data Web Scraper API.

Setup — add to .env in this folder:
  BRIGHTDATA_TOKEN=your-brightdata-api-token
  NETLIFY_SITE_ID=your-site-id
  NETLIFY_TOKEN=your-personal-access-token

Normal mode (cron):   python scrape_reels.py
  → runs Bright Data scrape for today's posts, appends to reels_history.json, redeploys Netlify

Backfill mode (once): python scrape_reels.py --backfill
  → runs Bright Data scrape for last 30 days, organises by date, merges into reels_history.json, redeploys Netlify

Dry-run mode (local): python scrape_reels.py --dry-run
  → skips all Bright Data API calls; loads raw items from test_data.json (if present) instead
  → still filters, ranks, writes reels_history.json, and deploys to Netlify as normal
  → combine with --backfill for a free end-to-end test: python scrape_reels.py --backfill --dry-run
"""

import hashlib
import io
import json
import os
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone

# Force UTF-8 output so emoji in captions don't crash on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE    = os.path.join(SCRIPT_DIR, "reels_history.json")
NETLIFY_DIR     = os.path.join(SCRIPT_DIR, "netlify-deploy")

# Load .env if present (local dev). In GitHub Actions the secrets are already
# injected as environment variables, so a missing .env file is fine.
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=False)
BRIGHTDATA_TOKEN      = os.getenv("BRIGHTDATA_TOKEN", "")
NETLIFY_TOKEN         = os.getenv("NETLIFY_TOKEN", "")
NETLIFY_SITE_ID       = os.getenv("NETLIFY_SITE_ID", "")

BRIGHTDATA_DATASET_ID = "gd_lyclm20il4r5helnj"
BRIGHTDATA_BASE       = "https://api.brightdata.com/datasets/v3"
POLL_INTERVAL         = 15        # seconds between snapshot-status checks
MAX_WAIT              = 45 * 60   # 45 minutes — give up if snapshot never becomes ready

TODAY         = datetime.now(timezone.utc).date()
YESTERDAY     = TODAY - timedelta(days=1)
BACKFILL_DAYS = 30

BACKFILL_MODE = "--backfill" in sys.argv
DRY_RUN       = "--dry-run"  in sys.argv


def load_accounts() -> list[str]:
    """Read accounts.txt and return a list of Instagram usernames.
    Blank lines and lines starting with # are ignored.
    """
    path = os.path.join(SCRIPT_DIR, "accounts.txt")
    if not os.path.exists(path):
        print("ERROR: accounts.txt not found. Create it in the same folder as this script,")
        print("       with one Instagram username per line.")
        sys.exit(1)
    accounts = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            accounts.append(line)
    return accounts


ACCOUNTS     = load_accounts()
ACCOUNT_URLS = [f"https://www.instagram.com/{a}/" for a in ACCOUNTS]


# ── Bright Data API helpers ───────────────────────────────────────────────────
def bd_headers(content_type: str = "application/json") -> dict:
    if not BRIGHTDATA_TOKEN:
        print("ERROR: BRIGHTDATA_TOKEN not set. Add it to your .env file.")
        sys.exit(1)
    return {"Authorization": f"Bearer {BRIGHTDATA_TOKEN}", "Content-Type": content_type}


def start_snapshot(payload: list[dict]) -> str:
    """Trigger a Bright Data scrape and return the snapshot_id."""
    url = (
        f"{BRIGHTDATA_BASE}/scrape"
        f"?dataset_id={BRIGHTDATA_DATASET_ID}"
        f"&notify=false&include_errors=true&type=discover_new&discover_by=url_all_reels"
    )
    resp = requests.post(url, headers=bd_headers(), json=payload, timeout=120)
    resp.raise_for_status()
    return resp.json()["snapshot_id"]


def wait_for_snapshot(snapshot_id: str) -> None:
    """Poll until the snapshot status is 'ready', with a 45-minute hard timeout.
    If the API returns a list instead of a status dict, the snapshot is already
    complete — treat it as ready immediately.
    """
    url = f"{BRIGHTDATA_BASE}/snapshot/{snapshot_id}?format=json"
    print(f"  Waiting for Bright Data snapshot {snapshot_id}", end="", flush=True)
    elapsed = 0
    while True:
        resp = requests.get(url, headers=bd_headers(), timeout=120)
        resp.raise_for_status()
        data = resp.json()
        # Bright Data sometimes returns the results list directly when the
        # snapshot is already complete rather than {"status": "ready"}.
        if isinstance(data, list):
            print("  ready (results returned directly)")
            return
        status = data.get("status", "")
        if status == "ready":
            print("  ready")
            return
        if status in ("failed", "error"):
            print(f"\n  [ERROR] Snapshot ended with status: {status}")
            raise RuntimeError(f"Bright Data snapshot failed: {status}")
        # Log actual status each poll so hangs are visible in the logs
        print(f" [{status}]", end="", flush=True)
        if elapsed >= MAX_WAIT:
            raise RuntimeError(
                f"Bright Data snapshot {snapshot_id} still '{status}' after "
                f"{elapsed // 60}m — giving up."
            )
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL


def fetch_snapshot(snapshot_id: str) -> list[dict]:
    """Download the completed snapshot results."""
    url = f"{BRIGHTDATA_BASE}/snapshot/{snapshot_id}?format=json"
    resp = requests.get(url, headers=bd_headers(), timeout=120)
    resp.raise_for_status()
    data = resp.json()
    # Results may be wrapped in a list or returned directly
    return data if isinstance(data, list) else data.get("results", [])


# ── Data processing ───────────────────────────────────────────────────────────
def parse_timestamp(ts: str):
    """Parse an ISO timestamp string to a date object. Returns None on failure."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def normalise_item(item: dict) -> dict | None:
    """
    Convert a raw Bright Data result into our standard record dict.
    Returns None if the item should be skipped (bad date, no timestamp, etc.).
    Note: we don't filter by media_type here because we use the url_all_reels
    discovery endpoint which only returns reels — the media_type field from
    Bright Data doesn't reliably contain the word "video".
    """
    # Timestamp: use date_posted (actual post date); timestamp is Bright Data's
    # internal scrape time and is always today — do NOT use it as the post date.
    ts = item.get("date_posted") or ""
    upload_date = parse_timestamp(ts)
    if not upload_date:
        return None

    # Username: Bright Data returns 'user_posted'; keep others as fallback
    owner = (
        item.get("user_posted")
        or item.get("owner_username")
        or item.get("ownerUsername")
        or item.get("profile_url", "").rstrip("/").rsplit("/", 1)[-1]
        or ""
    )

    caption_raw = item.get("description") or item.get("caption") or ""
    caption = caption_raw.split("\n")[0][:120].strip() or "[No caption]"

    return {
        "ownerUsername":  owner,
        "caption":        caption,
        "url":            item.get("url") or "",
        "videoViewCount": (item.get("views") or item.get("video_play_count")
                           or item.get("video_view_count") or item.get("plays")
                           or item.get("videoViewCount") or 0),
        "likesCount":     item.get("likes") or item.get("likesCount") or 0,
        "commentsCount":  (item.get("num_comments") or item.get("comments")
                           or item.get("commentsCount") or 0),
        "timestamp":      ts,
        "displayUrl":     item.get("image_url") or item.get("thumbnail") or item.get("displayUrl") or "",
        "upload_date":    upload_date.isoformat(),
    }


def run_brightdata_scrape(since_date, results_limit: int) -> list[dict]:
    """
    Run the Bright Data scrape for all accounts and return normalised, filtered items.
    since_date: date object — only keep posts on or after this date (and before today).
    """
    payload = [
        {"url": url, "num_of_posts": results_limit}
        for url in ACCOUNT_URLS
    ]

    print(f"  Dataset        : {BRIGHTDATA_DATASET_ID}")
    print(f"  Accounts       : {len(ACCOUNT_URLS)}")
    print(f"  Posts per acct : {results_limit}")
    print(f"  Newer than     : {since_date.isoformat()}")

    if DRY_RUN:
        test_path = os.path.join(SCRIPT_DIR, "test_data.json")
        if os.path.exists(test_path):
            with open(test_path, "r", encoding="utf-8") as f:
                raw_items = json.load(f)
            print(f"  [DRY RUN] Loaded {len(raw_items)} item(s) from test_data.json")
        else:
            print("  [DRY RUN] test_data.json not found — using empty dataset")
            raw_items = []
    else:
        print(f"  Starting Bright Data scrape...")
        snapshot_id = start_snapshot(payload)
        wait_for_snapshot(snapshot_id)
        print(f"  Downloading snapshot {snapshot_id}...")
        raw_items = fetch_snapshot(snapshot_id)
        print(f"  Raw items returned: {len(raw_items)}")

    # Normalise, filter to videos, filter to date window, exclude today
    results = []
    for item in raw_items:
        record = normalise_item(item)
        if record is None:
            continue
        upload_date_obj = datetime.strptime(record["upload_date"], "%Y-%m-%d").date()
        if upload_date_obj < since_date or upload_date_obj > TODAY:
            continue
        results.append(record)

    return results


# ── History helpers ───────────────────────────────────────────────────────────
def load_history() -> list[dict]:
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []


def save_history(history: list[dict]) -> None:
    history.sort(key=lambda e: e["date"], reverse=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def upsert_day(history: list[dict], date_str: str, reels: list[dict]) -> None:
    """Insert or replace a single day's entry (in-place). Safe to re-run."""
    history[:] = [e for e in history if e.get("date") != date_str]
    history.append({
        "date":        date_str,
        "scraped_at":  datetime.now(timezone.utc).isoformat(),
        "total_reels": len(reels),
        "reels":       reels,
    })


# ── Display ───────────────────────────────────────────────────────────────────
def print_ranked(reels: list[dict], heading: str) -> None:
    print(f"\n{'='*80}")
    print(f"  {heading}  ({len(reels)} total)")
    print(f"{'='*80}")
    if not reels:
        print("  No reels found.")
        return
    for rank, reel in enumerate(reels, 1):
        views = reel.get("videoViewCount") or 0
        likes = reel.get("likesCount") or 0
        views_fmt = f"{views:,}" if views else "N/A"
        likes_fmt = f"{likes:,}" if likes else "N/A"
        caption = reel["caption"][:60] + ("..." if len(reel["caption"]) > 60 else "")
        print(f"\n  #{rank:>3}  @{reel['ownerUsername']}")
        print(f"        Caption : {caption}")
        print(f"        Date    : {reel['upload_date']}")
        print(f"        Views   : {views_fmt}   Likes : {likes_fmt}")
        print(f"        URL     : {reel['url']}")
    print(f"\n{'='*80}\n")


# ── Netlify deploy ───────────────────────────────────────────────────────────
def _sha1(path: str) -> str:
    """Return the hex SHA1 digest of a file — used by Netlify's file-based deploy API."""
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def deploy_to_netlify() -> None:
    """
    Copy reels_history.json into netlify-deploy/, then push all files to Netlify
    via the file-based deploy API so the live site updates automatically.
    """
    if not NETLIFY_TOKEN or not NETLIFY_SITE_ID:
        print("  [SKIP] Netlify deploy — NETLIFY_TOKEN or NETLIFY_SITE_ID not set in .env")
        return

    print("\nDeploying to Netlify...")

    # 1. Sync the latest reels_history.json into the deploy folder
    shutil.copy2(HISTORY_FILE, os.path.join(NETLIFY_DIR, "reels_history.json"))

    # 2. Build file manifest: { "/filename": "sha1hex", ... }
    deploy_files = {}
    sha1_to_path = {}
    for filename in os.listdir(NETLIFY_DIR):
        filepath = os.path.join(NETLIFY_DIR, filename)
        if not os.path.isfile(filepath):
            continue
        digest = _sha1(filepath)
        deploy_files[f"/{filename}"] = digest
        sha1_to_path[digest] = filepath

    headers = {"Authorization": f"Bearer {NETLIFY_TOKEN}", "Content-Type": "application/json"}
    netlify_base = "https://api.netlify.com/api/v1"

    # 3. Create the deploy — Netlify responds with which files it still needs
    resp = requests.post(
        f"{netlify_base}/sites/{NETLIFY_SITE_ID}/deploys",
        headers=headers,
        json={"files": deploy_files},
        timeout=120,
    )
    resp.raise_for_status()
    deploy = resp.json()
    deploy_id = deploy["id"]
    required  = deploy.get("required", [])
    print(f"  Deploy created  : {deploy_id}  ({len(required)} file(s) to upload)")

    # 4. Upload only the files Netlify says it needs (skips unchanged files)
    upload_headers = {"Authorization": f"Bearer {NETLIFY_TOKEN}", "Content-Type": "application/octet-stream"}
    for digest in required:
        filepath = sha1_to_path.get(digest)
        if not filepath:
            print(f"  [WARN] Required digest {digest} not found locally — skipping")
            continue
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as fh:
            put_resp = requests.put(
                f"{netlify_base}/deploys/{deploy_id}/files/{filename}",
                headers=upload_headers,
                data=fh,
                timeout=120,
            )
            put_resp.raise_for_status()
        print(f"  Uploaded        : /{filename}")

    # 5. Poll until the deploy is live
    print("  Waiting for deploy to go live", end="", flush=True)
    while True:
        status_resp = requests.get(f"{netlify_base}/deploys/{deploy_id}", headers=headers, timeout=120)
        status_resp.raise_for_status()
        state = status_resp.json().get("state", "")
        if state == "ready":
            print("  live")
            url = status_resp.json().get("deploy_ssl_url") or status_resp.json().get("deploy_url", "")
            print(f"  Site updated    : {url}")
            return
        if state in ("error", "failed"):
            print(f"\n  [ERROR] Deploy ended in state: {state}")
            return
        print(".", end="", flush=True)
        time.sleep(5)


# ── Modes ─────────────────────────────────────────────────────────────────────
def main_normal() -> None:
    """Daily cron mode — scrape today's posts only."""
    print(f"Instagram Reels Scraper — {TODAY.isoformat()}")
    print(f"Fetching video posts from today ({TODAY.isoformat()})...\n")

    reels = run_brightdata_scrape(since_date=TODAY, results_limit=30)
    reels.sort(key=lambda r: r["videoViewCount"], reverse=True)

    print_ranked(reels, f"RANKED REELS — Today ({TODAY.isoformat()})")

    snapshot = os.path.join(SCRIPT_DIR, f"reels_{TODAY.isoformat()}.json")
    with open(snapshot, "w", encoding="utf-8") as f:
        json.dump({"scraped_at": datetime.now(timezone.utc).isoformat(),
                   "date": TODAY.isoformat(), "total_reels": len(reels),
                   "reels": reels}, f, indent=2, ensure_ascii=False)
    print(f"Dated snapshot saved to : {snapshot}")

    history = load_history()
    upsert_day(history, TODAY.isoformat(), reels)
    save_history(history)
    print(f"History updated         : {HISTORY_FILE}")

    deploy_to_netlify()


def main_backfill() -> None:
    """One-time backfill — scrape last 30 days, organise by date."""
    cutoff = TODAY - timedelta(days=BACKFILL_DAYS)
    print(f"Instagram Reels Scraper — BACKFILL MODE")
    print(f"Fetching video posts from {cutoff.isoformat()} to {YESTERDAY.isoformat()}...\n")

    # Higher results limit per account so we catch all posts over 30 days
    reels = run_brightdata_scrape(since_date=cutoff, results_limit=100)

    # Group by date, sort each day by videoViewCount
    by_date: dict[str, list[dict]] = {}
    for reel in reels:
        by_date.setdefault(reel["upload_date"], []).append(reel)
    for date_str in by_date:
        by_date[date_str].sort(key=lambda r: r["videoViewCount"], reverse=True)

    dates_found = sorted(by_date.keys(), reverse=True)
    total = sum(len(v) for v in by_date.values())
    print(f"Found {total} video posts across {len(dates_found)} day(s).\n")

    for date_str in dates_found:
        print_ranked(by_date[date_str], f"RANKED REELS — {date_str}")

    history = load_history()
    for date_str, day_reels in by_date.items():
        upsert_day(history, date_str, day_reels)
    save_history(history)
    print(f"Backfill complete: {len(dates_found)} day(s) written to {HISTORY_FILE}")

    deploy_to_netlify()


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    if BACKFILL_MODE:
        main_backfill()
    else:
        main_normal()


if __name__ == "__main__":
    main()
