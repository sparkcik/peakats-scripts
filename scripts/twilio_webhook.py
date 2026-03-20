#!/usr/bin/env python3
"""
twilio_webhook.py -- Inbound SMS webhook handler for Twilio

Flask app that receives Twilio inbound SMS POSTs, matches candidates,
logs to candidate_comms, and returns TwiML 200.

Port: 8080 (standalone) -- but see NOTE below.

NOTE: Fly.io forge-local only exposes port 8080 (used by forge_runner).
This script cannot run standalone on Fly. Options:
  1. Mount routes in forge_runner.py
  2. Deploy as a Supabase Edge Function
  3. Add a second Fly service

Usage (local testing):
    python3 scripts/twilio_webhook.py
"""

import os
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, Response

# -- Config from env vars -------------------------------------------------------

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TWILIO_FROM_NUMBER = os.environ.get("TWILIO_FROM_NUMBER", "+14704704766")

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
log = logging.getLogger("twilio_webhook")

app = Flask(__name__)

# -- Phone normalization ---------------------------------------------------------

def clean_phone(phone):
    if not phone:
        return ""
    return phone.replace("+1", "").replace("-", "").replace("(", "").replace(")", "").replace(" ", "")

# -- Candidate matching ----------------------------------------------------------

def match_candidate(phone):
    clean = clean_phone(phone)
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/candidates",
        headers=SB_HEADERS,
        params={
            "phone": f"eq.{clean}",
            "select": "id,first_name,last_name,client_id,status",
        },
    )
    resp.raise_for_status()
    rows = resp.json()
    return rows[0] if rows else None

# -- Logging to candidate_comms --------------------------------------------------

def log_inbound_sms(from_number, body, candidate):
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "candidate_id": candidate["id"],
        "client_id": candidate["client_id"],
        "channel": "sms",
        "direction": "inbound",
        "body": body,
        "sent_at": now,
        "sent_by": "twilio_webhook",
        "send_mode": "automated",
        "from_number": clean_phone(from_number),
        "to_number": clean_phone(TWILIO_FROM_NUMBER),
        "delivery_status": "delivered",
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/candidate_comms",
        headers={**SB_HEADERS, "Prefer": "return=minimal"},
        json=payload,
    )
    if resp.status_code >= 400:
        log.error(f"Failed to log comms: {resp.status_code} {resp.text[:200]}")
    else:
        log.info(f"Logged inbound SMS from {from_number} -> candidate {candidate['id']}")

# -- TwiML response --------------------------------------------------------------

TWIML_OK = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'

# -- Routes ----------------------------------------------------------------------

@app.route("/twilio/sms", methods=["POST"])
def inbound_sms():
    from_number = request.form.get("From", "")
    body = request.form.get("Body", "")
    to_number = request.form.get("To", "")

    log.info(f"Inbound SMS from {from_number}: {body[:80]}")

    candidate = match_candidate(from_number)

    if candidate:
        log_inbound_sms(from_number, body, candidate)
    else:
        log.warning(f"No candidate match for {from_number}")

    return Response(TWIML_OK, mimetype="application/xml")


@app.route("/twilio/sms/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "twilio_webhook"}


if __name__ == "__main__":
    port = int(os.environ.get("TWILIO_SMS_PORT", 8080))
    log.info(f"Twilio SMS webhook listening on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
