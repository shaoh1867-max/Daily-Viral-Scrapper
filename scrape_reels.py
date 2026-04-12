#!/usr/bin/env python3
"""
Instagram Reels scraper using the Apify Instagram API Scraper.

Setup — add to .env in this folder:
  APIFY_TOKEN=your-apify-api-token
  NETLIFY_SITE_ID=your-site-id
  NETLIFY_TOKEN=your-netlify-personal-access-token

Normal mode (cron):   python scrape_reels.py
  → runs Apify scrape for today's posts, appends to reels_history.json, redeploys Netlify

Backfill mode (once): python scrape_reels.py --backfill
  → runs Apify scrape for last 30 days, organises by date, merges into reels_history.json, redeploys Netlify

Dry-run mode (local): python scrape_reels.py --dry-run
  → skips all Apify API calls; loads raw items from test_data.json (if present) instead
  → still filters, ranks, writes reels_history.json, and deploys to Netlify as normal
  → combine with --backfill for a free end-to-end test: python scrape_reels.py --backfill --dry-run
"""

import hashlib
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
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(SCRIPT_DIR, "reels_history.json")
NETLIFY_DIR  = os.path.join(SCRIPT_DIR, "netlify-deploy")

# Load .env if present (local dev). In GitHub Actions the secrets are already
# injected as environment variables, so a missing .env file is fine.
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=False)
APIFY_TOKEN   = os.getenv("APIFY_TOKEN", "")
NETLIFY_TOKEN = os.getenv("NETLIFY_TOKEN", "")
NETLIFY_SITE_ID = os.getenv("NETLIFY_SITE_ID", "")

APIFY_ACTOR   = "apify~instagram-api-scraper"
APIFY_BASE    = "https://api.apify.com/v2"
POLL_INTERVAL = 15       # seconds between run-status checks
MAX_WAIT      = 45 * 60  # 45 minutes — give up if run never finishes

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


# ── Apify API helpers ─────────────────────────────────────────────────────────
def start_apify_run(payload: dict) -> str:
    """Trigger an Apify actor run and return the run ID."""
    if not APIFY_TOKEN:
        print("ERROR: APIFY_TOKEN not set. Add it to your .env file.")
        sys.exit(1)
    url = f"{APIFY_BASE}/acts/{APIFY_ACTOR}/runs?token={APIFY_TOKEN}"
    resp = requests.post(url, json=payload, timeout=120)
    resp.raise_for_status()
    return resp.json()["data"]["id"]


def wait_for_run(run_id: str) -> None:
    """Poll until the Apify run status is SUCCEEDED, with a 45-minute hard timeout."""
    url = f"{APIFY_BASE}/actor-runs/{run_id}?token={APIFY_TOKEN}"
    print(f"  Waiting for Apify run {run_id}", end="", flush=True)
    elapsed = 0
    while True:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        status = resp.json()["data"]["status"]
        if status == "SUCCEEDED":
            print("  done")
            return
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"\n  [ERROR] Apify run ended with status: {status}")
            raise RuntimeError(f"Apify run failed: {status}")
        # Log actual status each poll so hangs are visible in the logs
        print(f" [{status}]", end="", flush=True)
        if elapsed >= MAX_WAIT:
            raise RuntimeError(
                f"Apify run {run_id} still '{status}' after {elapsed // 60}m — giving up."
            )
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL


def fetch_run_results(run_id: str) -> list[dict]:
    """Download the completed run's dataset items."""
    url = f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items?token={APIFY_TOKEN}"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.json()


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
    Convert a raw Apify result into our standard record dict.
    Returns None if the item should be skipped (bad date, no timestamp, etc.).
    """
    ts = item.get("timestamp") or ""
    upload_date = parse_timestamp(ts)
    if not upload_date:
        return None

    caption_raw = item.get("caption") or ""
    caption = caption_raw.split("\n")[0][:120].strip() or "[No caption]"

    return {
        "ownerUsername":  item.get("ownerUsername") or "",
        "caption":        caption,
        "url":            item.get("url") or "",
        "videoViewCount": item.get("videoViewCount") or 0,
        "likesCount":     item.get("likesCount") or 0,
        "commentsCount":  item.get("commentsCount") or 0,
        "timestamp":      ts,
        "displayUrl":     item.get("displayUrl") or "",
        "upload_date":    upload_date.isoformat(),
    }


def run_apify_scrape(since_date, results_limit: int, newer_than: str) -> list[dict]:
    """
    Run the Apify scrape for all accounts and return normalised, filtered items.
    since_date:    date object — only keep posts on or after this date
    results_limit: max posts per account to request from Apify
    newer_than:    Apify server-side pre-filter ("1 day" or "YYYY-MM-DD")
    """
    payload = {
        "directUrls":   ACCOUNT_URLS,
        "resultsType":  "posts",
        "resultsLimit": results_limit,
        "newerThan":    newer_than,
    }

    print(f"  Actor          : {APIFY_ACTOR}")
    print(f"  Accounts       : {len(ACCOUNT_URLS)}")
    print(f"  Posts per acct : {results_limit}")
    print(f"  Newer than     : {newer_than}")

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
        print("  Starting Apify run...")
        run_id = start_apify_run(payload)
        wait_for_run(run_id)
        print(f"  Fetching results for run {run_id}...")
        raw_items = fetch_run_results(run_id)
        print(f"  Raw items returned: {len(raw_items)}")

    # Normalise and filter to date window
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
def fetch_netlify_history() -> list[dict]:
    """Download reels_history.json from the live Netlify site.
    Used by GitHub Actions where the file is not in the repo.
    Returns [] on any failure so the run still continues cleanly.
    """
    if not NETLIFY_TOKEN or not NETLIFY_SITE_ID:
        return []
    try:
        site_resp = requests.get(
            f"https://api.netlify.com/api/v1/sites/{NETLIFY_SITE_ID}",
            headers={"Authorization": f"Bearer {NETLIFY_TOKEN}"},
            timeout=30,
        )
        site_resp.raise_for_status()
        site_url = site_resp.json().get("ssl_url") or site_resp.json().get("url", "")
        if not site_url:
            return []
        hist_resp = requests.get(f"{site_url}/reels_history.json", timeout=30)
        hist_resp.raise_for_status()
        data = hist_resp.json()
        print(f"  Fetched {len(data)} day(s) of history from Netlify.")
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  [WARN] Could not fetch history from Netlify: {e}")
        return []


def load_history() -> list[dict]:
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    # File missing (e.g. GitHub Actions) — pull from live Netlify site
    print("  No local reels_history.json — fetching from Netlify...")
    return fetch_netlify_history()


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


# ── Netlify deploy ────────────────────────────────────────────────────────────
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

    reels = run_apify_scrape(since_date=TODAY, results_limit=10, newer_than="1 day")
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
    print(f"Fetching video posts from {cutoff.isoformat()} to {TODAY.isoformat()}...\n")

    reels = run_apify_scrape(since_date=cutoff, results_limit=50, newer_than=cutoff.isoformat())

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
