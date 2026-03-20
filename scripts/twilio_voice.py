#!/usr/bin/env python3
"""
twilio_voice.py -- Inbound voice webhook handler for Twilio

Flask app that handles Twilio voice calls with a greeting + voicemail,
then logs the recording URL to candidate_comms.

Port: 8081 (standalone) -- but see NOTE below.

NOTE: Fly.io forge-local only exposes port 8080 (used by forge_runner).
This script cannot run standalone on Fly. Options:
  1. Mount routes in forge_runner.py
  2. Deploy as a Supabase Edge Function
  3. Add a second Fly service

Usage (local testing):
    python3 scripts/twilio_voice.py
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
log = logging.getLogger("twilio_voice")

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

# -- TwiML responses -------------------------------------------------------------

TWIML_GREETING = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="alice">You have reached Kai at PEAK recruiting. Please leave a message after the tone.</Say>
    <Record maxLength="120" action="/twilio/voice/recording" transcribe="false" />
    <Say voice="alice">We did not receive a recording. Goodbye.</Say>
</Response>"""

TWIML_RECORDING_ACK = '<?xml version="1.0" encoding="UTF-8"?><Response><Say voice="alice">Thank you. Goodbye.</Say><Hangup/></Response>'

# -- Routes ----------------------------------------------------------------------

@app.route("/twilio/voice", methods=["POST"])
def inbound_call():
    from_number = request.form.get("From", "")
    log.info(f"Inbound call from {from_number}")
    return Response(TWIML_GREETING, mimetype="application/xml")


@app.route("/twilio/voice/recording", methods=["POST"])
def recording_callback():
    from_number = request.form.get("From", "")
    recording_url = request.form.get("RecordingUrl", "")
    recording_duration = request.form.get("RecordingDuration", "0")

    log.info(f"Recording from {from_number}: {recording_url} ({recording_duration}s)")

    candidate = match_candidate(from_number)

    if candidate:
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "candidate_id": candidate["id"],
            "client_id": candidate["client_id"],
            "channel": "voice",
            "direction": "inbound",
            "body": f"Voicemail ({recording_duration}s): {recording_url}",
            "sent_at": now,
            "sent_by": "twilio_voice",
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
            log.error(f"Failed to log voicemail: {resp.status_code} {resp.text[:200]}")
        else:
            log.info(f"Logged voicemail from {from_number} -> candidate {candidate['id']}")
    else:
        log.warning(f"No candidate match for voicemail from {from_number}")

    return Response(TWIML_RECORDING_ACK, mimetype="application/xml")


@app.route("/twilio/voice/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "twilio_voice"}


if __name__ == "__main__":
    port = int(os.environ.get("TWILIO_VOICE_PORT", 8081))
    log.info(f"Twilio voice webhook listening on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
