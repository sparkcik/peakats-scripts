#!/usr/bin/env python3
"""
twilio_sms_send.py -- Send pending SMS via Twilio REST API

Reads from sms_send_queue WHERE status = 'pending' AND migration_status != 'rc_active',
sends via Twilio, updates status on success/failure.

Rate limited to 1 message per second.

Usage:
    python3 scripts/twilio_sms_send.py [--dry-run] [--limit 50]
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime, timezone

import requests
from requests.auth import HTTPBasicAuth

# -- Config from env vars -------------------------------------------------------

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TWILIO_ACCOUNT_SID = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_FROM_NUMBER = os.environ.get("TWILIO_FROM_NUMBER", "+14704704766")

TWILIO_API_URL = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
TWILIO_AUTH = HTTPBasicAuth(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("twilio_sms_send")

# -- Supabase helpers ------------------------------------------------------------

def fetch_pending(limit):
    """Fetch pending messages that are not RC-active."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue",
        headers=SB_HEADERS,
        params={
            "status": "eq.pending",
            "migration_status": "neq.rc_active",
            "select": "id,candidate_id,to_number,body,template_id,template_name",
            "order": "scheduled_for.asc",
            "limit": str(limit),
        },
    )
    resp.raise_for_status()
    return resp.json()


def update_queue(msg_id, payload):
    """Update a row in sms_send_queue."""
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{msg_id}",
        headers=SB_HEADERS,
        json=payload,
    )
    resp.raise_for_status()

# -- Twilio send -----------------------------------------------------------------

def send_sms(to_number, body):
    """Send an SMS via Twilio. Returns (sid, None) on success, (None, error) on failure."""
    clean = to_number.replace("+1", "").replace("-", "").replace("(", "").replace(")", "").replace(" ", "")
    resp = requests.post(
        TWILIO_API_URL,
        auth=TWILIO_AUTH,
        data={
            "From": TWILIO_FROM_NUMBER,
            "To": f"+1{clean}",
            "Body": body,
        },
    )
    if resp.status_code in (200, 201):
        return resp.json().get("sid"), None
    else:
        error = resp.json().get("message", resp.text[:200])
        return None, error

# -- Main ------------------------------------------------------------------------

def run(dry_run, limit):
    messages = fetch_pending(limit)
    log.info(f"Found {len(messages)} pending messages")

    if not messages:
        print("No pending messages")
        return

    sent = 0
    failed = 0
    now = datetime.now(timezone.utc).isoformat()

    for msg in messages:
        msg_id = msg["id"]
        to_number = msg["to_number"]
        body = msg["body"]

        if dry_run:
            log.info(f"[DRY RUN] Would send to {to_number}: {body[:50]}...")
            sent += 1
            continue

        sid, error = send_sms(to_number, body)

        if sid:
            update_queue(msg_id, {
                "status": "sent",
                "sent_at": now,
                "twilio_message_id": sid,
                "migration_status": "twilio_active",
                "delivery_status": "delivered",
                "updated_at": now,
            })
            log.info(f"Sent {msg_id} to {to_number} -> {sid}")
            sent += 1
        else:
            update_queue(msg_id, {
                "status": "failed",
                "delivery_error": error,
                "delivery_status": "failed",
                "updated_at": now,
            })
            log.warning(f"Failed {msg_id} to {to_number}: {error}")
            failed += 1

        # Rate limit: 1 msg/sec
        time.sleep(1)

    log.info(f"Done. Sent: {sent}, Failed: {failed}")
    print(f"Sent: {sent} | Failed: {failed}")

    # Write heartbeat to forge_memory so monitor can detect stale poller
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        hb = requests.post(
            f"{SUPABASE_URL}/rest/v1/forge_memory",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json={
                "category": "heartbeat",
                "subject": "poller_heartbeat",
                "content": f"sent={sent} failed={failed} total={sent+failed}",
                "session_date": now_iso[:10],
                "target_thread": "PEAK Infra",
            },
            timeout=5,
        )
        if hb.status_code not in (200, 201, 204):
            log.warning(f"Heartbeat write failed: {hb.status_code}")
    except Exception as hb_err:
        log.warning(f"Heartbeat write exception: {hb_err}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twilio SMS sender")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    parser.add_argument("--limit", type=int, default=500, help="Max messages to process (default 500)")
    args = parser.parse_args()
    run(args.dry_run, args.limit)
