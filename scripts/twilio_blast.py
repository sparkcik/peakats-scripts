#!/usr/bin/env python3
"""
twilio_blast.py -- Blast Template 39 to all rc_contact_export numbers not yet sent via Twilio

1. Reads template body from message_templates WHERE id = 39
2. Reads all phone numbers from rc_contact_export WHERE twilio_sent = false
3. Inserts each into sms_send_queue with migration_status = 'twilio_active'
4. Sends each immediately via Twilio
5. On success: marks rc_contact_export.twilio_sent = true

Usage:
    python3 scripts/twilio_blast.py [--dry-run] [--limit 500]
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
TWILIO_FROM_CLEAN = "4704704766"

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
log = logging.getLogger("twilio_blast")

# -- Supabase helpers ------------------------------------------------------------

def fetch_template(template_id):
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/message_templates",
        headers=SB_HEADERS,
        params={"id": f"eq.{template_id}", "select": "id,name,body"},
    )
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        log.error(f"Template {template_id} not found")
        sys.exit(1)
    return rows[0]


def fetch_unsent_contacts(limit):
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/rc_contact_export",
        headers=SB_HEADERS,
        params={
            "twilio_sent": "eq.false",
            "select": "phone_number,candidate_id",
            "limit": str(limit),
        },
    )
    resp.raise_for_status()
    return resp.json()


def insert_queue_row(to_number, body, candidate_id, template_id, template_name):
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "to_number": to_number,
        "from_number": TWILIO_FROM_CLEAN,
        "body": body,
        "candidate_id": candidate_id,
        "template_id": template_id,
        "template_name": template_name,
        "scheduled_for": now,
        "status": "pending",
        "migration_status": "twilio_active",
        "created_by": "twilio_blast",
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue",
        headers={**SB_HEADERS, "Prefer": "return=representation"},
        json=payload,
    )
    resp.raise_for_status()
    rows = resp.json()
    return rows[0]["id"] if rows else None


def update_queue(msg_id, payload):
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{msg_id}",
        headers=SB_HEADERS,
        json=payload,
    )
    resp.raise_for_status()


def mark_contact_sent(phone_number):
    now = datetime.now(timezone.utc).isoformat()
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/rc_contact_export?phone_number=eq.{phone_number}",
        headers=SB_HEADERS,
        json={"twilio_sent": True, "twilio_sent_at": now},
    )
    resp.raise_for_status()

# -- Twilio send -----------------------------------------------------------------

def send_sms(to_number, body):
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
    template = fetch_template(39)
    body = template["body"]
    log.info(f"Template 39 ({template['name']}): {body[:60]}...")

    contacts = fetch_unsent_contacts(limit)
    log.info(f"Found {len(contacts)} unsent contacts")

    if not contacts:
        print("No unsent contacts")
        return

    sent = 0
    failed = 0
    now = datetime.now(timezone.utc).isoformat()

    for contact in contacts:
        phone = contact["phone_number"]
        candidate_id = contact.get("candidate_id")

        # Insert into queue
        queue_id = insert_queue_row(phone, body, candidate_id, 39, template["name"])

        if dry_run:
            log.info(f"[DRY RUN] Would send to {phone}")
            sent += 1
            continue

        # Send immediately
        sid, error = send_sms(phone, body)

        if sid:
            update_queue(queue_id, {
                "status": "sent",
                "sent_at": now,
                "twilio_message_id": sid,
                "delivery_status": "delivered",
                "updated_at": now,
            })
            mark_contact_sent(phone)
            log.info(f"Sent to {phone} -> {sid}")
            sent += 1
        else:
            update_queue(queue_id, {
                "status": "failed",
                "delivery_error": error,
                "delivery_status": "failed",
                "updated_at": now,
            })
            log.warning(f"Failed {phone}: {error}")
            failed += 1

        # Rate limit: 1 msg/sec
        time.sleep(1)

    log.info(f"Blast complete. Sent: {sent}, Failed: {failed}")
    print(f"Sent: {sent} | Failed: {failed} | Total: {len(contacts)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twilio blast sender (Template 39)")
    parser.add_argument("--dry-run", action="store_true", help="Insert queue rows but don't actually send")
    parser.add_argument("--limit", type=int, default=500, help="Max contacts to process (default 500)")
    args = parser.parse_args()
    run(args.dry_run, args.limit)
