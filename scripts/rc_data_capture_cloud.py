#!/usr/bin/env python3
"""
rc_data_capture_cloud.py -- Cloud version of RC data capture
Runs on forge-local (Fly.io). Reads credentials from env vars.

Fetches RingCentral SMS and call logs, upserts to Supabase tables:
  - rc_sms_archive
  - rc_call_archive
  - rc_contact_export (rebuilt from archive data)

Each page of results is persisted immediately so partial runs still save data.

Usage:
    python3 scripts/rc_data_capture_cloud.py [--days 30]
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests

# -- Config from env vars -------------------------------------------------------

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RC_CLIENT_ID = os.environ["RC_CLIENT_ID"]
RC_CLIENT_SECRET = os.environ["RC_CLIENT_SECRET"]
RC_JWT = os.environ["RC_JWT"]
RC_SERVER = "https://platform.ringcentral.com"

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

KAI_NUMBER = "4708574325"
MAX_RETRIES = 3
RETRY_BASE_WAIT = 60  # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("rc_data_capture")

# -- RingCentral auth -----------------------------------------------------------

def get_rc_token():
    resp = requests.post(
        f"{RC_SERVER}/restapi/oauth/token",
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": RC_JWT,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

# -- Retry wrapper ---------------------------------------------------------------

def rc_get_with_retry(url, headers, params=None):
    """GET with exponential backoff on 429. Waits 60s, 120s, 240s."""
    for attempt in range(MAX_RETRIES + 1):
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 429:
            resp.raise_for_status()
            return resp
        if attempt == MAX_RETRIES:
            log.error(f"429 rate limit after {MAX_RETRIES} retries, giving up")
            resp.raise_for_status()
        wait = RETRY_BASE_WAIT * (2 ** attempt)
        log.warning(f"429 rate limited, waiting {wait}s (attempt {attempt + 1}/{MAX_RETRIES})")
        time.sleep(wait)

# -- Phone normalization ---------------------------------------------------------

def clean_phone(phone):
    if not phone:
        return ""
    return phone.replace("+1", "").replace("-", "").replace("(", "").replace(")", "").replace(" ", "")

# -- Candidate matching ----------------------------------------------------------

def load_candidate_map():
    """Load phone -> candidate_id map from Supabase candidates table."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/candidates",
        headers=SB_HEADERS,
        params={"select": "id,phone"},
    )
    resp.raise_for_status()
    phone_map = {}
    for row in resp.json():
        if row.get("phone"):
            phone_map[clean_phone(row["phone"])] = row["id"]
    return phone_map

# -- Supabase upsert helpers -----------------------------------------------------

def upsert_batch(table, rows, batch_size=200):
    """Upsert rows to a Supabase table in batches. Returns total upserted count."""
    if not rows:
        return 0
    headers = {
        **SB_HEADERS,
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=headers,
            json=batch,
        )
        resp.raise_for_status()
        total += len(batch)
    return total

# -- SMS row transform -----------------------------------------------------------

def transform_sms_page(records, phone_map):
    """Convert a page of RC SMS records to rc_sms_archive rows."""
    rows = []
    for msg in records:
        from_num = clean_phone(msg.get("from", {}).get("phoneNumber", ""))
        to_list = msg.get("to", [])
        to_num = clean_phone(to_list[0].get("phoneNumber", "")) if to_list else ""
        direction = msg.get("direction", "").lower()
        other_num = from_num if direction == "inbound" else to_num
        rows.append({
            "message_id": msg["id"],
            "from_number": from_num,
            "to_number": to_num,
            "body": msg.get("subject", ""),
            "direction": direction,
            "received_at": msg.get("creationTime"),
            "read_status": msg.get("readStatus", ""),
            "candidate_id": phone_map.get(other_num),
        })
    return rows

# -- Call row transform ----------------------------------------------------------

def transform_call_page(records, phone_map):
    """Convert a page of RC call-log records to rc_call_archive rows."""
    rows = []
    for call in records:
        from_num = clean_phone(call.get("from", {}).get("phoneNumber", ""))
        to_num = clean_phone(call.get("to", {}).get("phoneNumber", ""))
        direction = call.get("direction", "").lower()
        other_num = from_num if direction == "inbound" else to_num
        rows.append({
            "call_id": call["id"],
            "from_number": from_num,
            "to_number": to_num,
            "direction": direction,
            "duration_seconds": call.get("duration", 0),
            "start_time": call.get("startTime"),
            "result": call.get("result", ""),
            "candidate_id": phone_map.get(other_num),
        })
    return rows

# -- Fetch + persist per page ----------------------------------------------------

def fetch_and_persist_sms(token, date_from, phone_map):
    """Fetch SMS page by page, upsert each page immediately. Returns all rows."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{RC_SERVER}/restapi/v1.0/account/~/extension/~/message-store"
    params = {"messageType": "SMS", "dateFrom": date_from, "perPage": 100}
    all_rows = []
    page = 1
    total_persisted = 0
    while url:
        resp = rc_get_with_retry(url, headers, params=params if page == 1 else None)
        data = resp.json()
        records = data.get("records", [])
        rows = transform_sms_page(records, phone_map)
        if rows:
            upsert_batch("rc_sms_archive", rows)
            total_persisted += len(rows)
            log.info(f"SMS page {page}: fetched {len(records)}, persisted {total_persisted} total")
        all_rows.extend(rows)
        nav = data.get("navigation", {})
        next_page = nav.get("nextPage", {})
        url = next_page.get("uri") if next_page else None
        page += 1
    return all_rows, total_persisted


def fetch_and_persist_calls(token, date_from, phone_map):
    """Fetch call logs page by page, upsert each page immediately. Returns all rows."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{RC_SERVER}/restapi/v1.0/account/~/extension/~/call-log"
    params = {"dateFrom": date_from, "perPage": 100, "view": "Simple"}
    all_rows = []
    page = 1
    total_persisted = 0
    while url:
        resp = rc_get_with_retry(url, headers, params=params if page == 1 else None)
        data = resp.json()
        records = data.get("records", [])
        rows = transform_call_page(records, phone_map)
        if rows:
            upsert_batch("rc_call_archive", rows)
            total_persisted += len(rows)
            log.info(f"Call page {page}: fetched {len(records)}, persisted {total_persisted} total")
        all_rows.extend(rows)
        nav = data.get("navigation", {})
        next_page = nav.get("nextPage", {})
        url = next_page.get("uri") if next_page else None
        page += 1
    return all_rows, total_persisted

# -- Main pipeline ---------------------------------------------------------------

def run(days):
    date_from = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )
    log.info(f"Capturing RC data from {date_from} ({days} days back)")

    # Auth
    token = get_rc_token()
    log.info("RC auth OK")

    # Load candidate phone map
    phone_map = load_candidate_map()
    log.info(f"Loaded {len(phone_map)} candidate phone mappings")

    # Fetch + persist SMS (page by page)
    sms_rows, sms_count = fetch_and_persist_sms(token, date_from, phone_map)
    log.info(f"SMS complete: {sms_count} records persisted")

    # Fetch + persist calls (page by page)
    try:
        call_rows, call_count = fetch_and_persist_calls(token, date_from, phone_map)
        log.info(f"Calls complete: {call_count} records persisted")
    except Exception as e:
        log.warning(f"Call fetch failed, continuing without call data: {e}")
        call_rows = []
        call_count = 0

    # Build rc_contact_export
    contacts = defaultdict(lambda: {
        "first_seen": None,
        "last_seen": None,
        "message_count": 0,
        "call_count": 0,
        "candidate_id": None,
    })

    for row in sms_rows:
        other = row["from_number"] if row["direction"] == "inbound" else row["to_number"]
        if not other or other == KAI_NUMBER:
            continue
        c = contacts[other]
        ts = row["received_at"]
        if ts:
            if c["first_seen"] is None or ts < c["first_seen"]:
                c["first_seen"] = ts
            if c["last_seen"] is None or ts > c["last_seen"]:
                c["last_seen"] = ts
        c["message_count"] += 1
        if row["candidate_id"]:
            c["candidate_id"] = row["candidate_id"]

    for row in call_rows:
        other = row["from_number"] if row["direction"] == "inbound" else row["to_number"]
        if not other or other == KAI_NUMBER:
            continue
        c = contacts[other]
        ts = row["start_time"]
        if ts:
            if c["first_seen"] is None or ts < c["first_seen"]:
                c["first_seen"] = ts
            if c["last_seen"] is None or ts > c["last_seen"]:
                c["last_seen"] = ts
        c["call_count"] += 1
        if row["candidate_id"]:
            c["candidate_id"] = row["candidate_id"]

    contact_rows = []
    for phone, data in contacts.items():
        contact_rows.append({
            "phone_number": phone,
            **data,
        })

    contact_count = upsert_batch("rc_contact_export", contact_rows)
    log.info(f"Upserted {contact_count} contacts to rc_contact_export")

    log.info("RC data capture complete")
    print(f"SMS: {sms_count} | Calls: {call_count} | Contacts: {contact_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RC data capture (cloud)")
    parser.add_argument("--days", type=int, default=30, help="Lookback days (default 30)")
    args = parser.parse_args()
    run(args.days)
