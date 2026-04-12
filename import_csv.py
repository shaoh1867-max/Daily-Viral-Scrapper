#!/usr/bin/env python3
"""
One-off script: import backfill CSV from Apify into reels_history.json
and redeploy to Netlify.

Usage:
  python import_csv.py <path-to-csv>

The CSV is merged with the existing Netlify history. Existing days are
preserved; duplicate reels (same URL) within a day are deduplicated.
Each day is re-sorted by videoViewCount descending.
"""

import csv
import json
import os
import sys
import hashlib
import shutil
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=False)

NETLIFY_TOKEN   = os.getenv("NETLIFY_TOKEN", "")
NETLIFY_SITE_ID = os.getenv("NETLIFY_SITE_ID", "")
NETLIFY_DIR     = os.path.join(SCRIPT_DIR, "netlify-deploy")
HISTORY_FILE    = os.path.join(SCRIPT_DIR, "reels_history.json")


# ── Parse CSV ────────────────────────────────────────────────────────────────
def parse_csv(path: str) -> dict[str, list[dict]]:
    """Read the Apify CSV and return records grouped by upload_date."""
    by_date: dict[str, list[dict]] = {}
    skipped = 0
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = row.get("timestamp", "")
            if not ts:
                skipped += 1
                continue
            try:
                upload_date = datetime.fromisoformat(ts.replace("Z", "+00:00")).date().isoformat()
            except ValueError:
                skipped += 1
                continue

            caption_raw = row.get("caption") or ""
            caption = caption_raw.split("\n")[0][:120].strip() or "[No caption]"

            def intval(key):
                try:
                    return int(float(row.get(key) or 0))
                except (ValueError, TypeError):
                    return 0

            record = {
                "ownerUsername":  row.get("ownerUsername") or "",
                "caption":        caption,
                "url":            row.get("url") or "",
                "videoViewCount": intval("videoViewCount"),
                "likesCount":     intval("likesCount"),
                "commentsCount":  intval("commentsCount"),
                "timestamp":      ts,
                "displayUrl":     row.get("displayUrl") or "",
                "upload_date":    upload_date,
            }
            by_date.setdefault(upload_date, []).append(record)

    total = sum(len(v) for v in by_date.values())
    print(f"  CSV parsed: {total} records across {len(by_date)} days ({skipped} skipped)")
    return by_date


# ── Fetch existing Netlify history ────────────────────────────────────────────
def fetch_netlify_history() -> list[dict]:
    if not NETLIFY_TOKEN or not NETLIFY_SITE_ID:
        print("  [WARN] No Netlify credentials — starting with empty history")
        return []
    try:
        site_resp = requests.get(
            f"https://api.netlify.com/api/v1/sites/{NETLIFY_SITE_ID}",
            headers={"Authorization": f"Bearer {NETLIFY_TOKEN}"},
            timeout=30,
        )
        site_resp.raise_for_status()
        site_url = site_resp.json().get("ssl_url") or site_resp.json().get("url", "")
        hist_resp = requests.get(f"{site_url}/reels_history.json", timeout=30)
        hist_resp.raise_for_status()
        data = hist_resp.json()
        result = data if isinstance(data, list) else []
        print(f"  Fetched {len(result)} existing day(s) from Netlify")
        return result
    except Exception as e:
        print(f"  [WARN] Could not fetch from Netlify: {e}")
        return []


# ── Netlify deploy ────────────────────────────────────────────────────────────
def _sha1(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def deploy_to_netlify() -> None:
    if not NETLIFY_TOKEN or not NETLIFY_SITE_ID:
        print("  [SKIP] No Netlify credentials")
        return

    print("\nDeploying to Netlify...")
    shutil.copy2(HISTORY_FILE, os.path.join(NETLIFY_DIR, "reels_history.json"))

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

    resp = requests.post(
        f"{netlify_base}/sites/{NETLIFY_SITE_ID}/deploys",
        headers=headers,
        json={"files": deploy_files},
        timeout=120,
    )
    resp.raise_for_status()
    deploy = resp.json()
    deploy_id = deploy["id"]
    required = deploy.get("required", [])
    print(f"  Deploy created : {deploy_id}  ({len(required)} file(s) to upload)")

    upload_headers = {"Authorization": f"Bearer {NETLIFY_TOKEN}", "Content-Type": "application/octet-stream"}
    for digest in required:
        filepath = sha1_to_path.get(digest)
        if not filepath:
            continue
        filename = os.path.basename(filepath)
        with open(filepath, "rb") as fh:
            requests.put(
                f"{netlify_base}/deploys/{deploy_id}/files/{filename}",
                headers=upload_headers,
                data=fh,
                timeout=120,
            ).raise_for_status()
        print(f"  Uploaded       : /{filename}")

    print("  Waiting for deploy to go live", end="", flush=True)
    while True:
        status_resp = requests.get(f"{netlify_base}/deploys/{deploy_id}", headers=headers, timeout=120)
        status_resp.raise_for_status()
        state = status_resp.json().get("state", "")
        if state == "ready":
            url = status_resp.json().get("deploy_ssl_url") or ""
            print(f"  live\n  Site updated   : {url}")
            return
        if state in ("error", "failed"):
            print(f"\n  [ERROR] Deploy state: {state}")
            return
        print(".", end="", flush=True)
        time.sleep(5)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Usage: python import_csv.py <path-to-csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    print(f"Reading CSV: {csv_path}")
    csv_by_date = parse_csv(csv_path)

    print("Fetching current history from Netlify...")
    history = fetch_netlify_history()

    # Build a lookup of existing entries keyed by date
    existing: dict[str, dict] = {e["date"]: e for e in history}

    # Merge: for each date in CSV, combine with existing reels, deduplicate by URL
    for date_str, csv_reels in csv_by_date.items():
        existing_reels = existing.get(date_str, {}).get("reels", [])
        # Build URL → record map; CSV fills gaps, existing data takes precedence
        merged: dict[str, dict] = {r["url"]: r for r in csv_reels}
        for r in existing_reels:
            merged[r["url"]] = r  # existing (e.g. today's normal run) wins
        sorted_reels = sorted(merged.values(), key=lambda r: r["videoViewCount"], reverse=True)
        existing[date_str] = {
            "date":        date_str,
            "scraped_at":  existing.get(date_str, {}).get("scraped_at",
                           datetime.now(timezone.utc).isoformat()),
            "total_reels": len(sorted_reels),
            "reels":       sorted_reels,
        }

    # Sort history newest-first and save
    final_history = sorted(existing.values(), key=lambda e: e["date"], reverse=True)
    print(f"\nMerged history: {len(final_history)} days total")

    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_history, f, indent=2, ensure_ascii=False)
    print(f"Saved to: {HISTORY_FILE}")

    deploy_to_netlify()


if __name__ == "__main__":
    main()
