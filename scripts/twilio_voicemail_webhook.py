#!/usr/bin/env python3
"""
twilio_voicemail_webhook.py -- Twilio voicemail webhook handler

Receives Twilio recording callbacks, matches the caller to a candidate,
and writes the voicemail record to the twilio_voicemail table in Supabase.

Deployed as a route in forge_runner.py (/voicemail).
"""

import os
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def _clean_phone(phone):
    if not phone:
        return ""
    return phone.replace("+1", "").replace("-", "").replace("(", "").replace(")", "").replace(" ", "")[-10:]


def match_candidate(phone):
    clean = _clean_phone(phone)
    if not SUPABASE_URL or not clean:
        return None
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/candidates",
        headers=_sb_headers(),
        params={"phone": f"eq.{clean}", "select": "id,first_name,last_name"},
    )
    rows = resp.json() if resp.status_code == 200 else []
    return rows[0] if rows else None


def handle_voicemail(from_number, call_sid, recording_url, duration, transcript=""):
    """Process a voicemail and write to twilio_voicemail table."""
    clean = _clean_phone(from_number)
    cand = match_candidate(from_number)
    payload = {
        "call_sid": call_sid,
        "recording_url": (recording_url + ".mp3") if recording_url else None,
        "from_number": clean,
        "duration_seconds": duration,
        "transcript": transcript or None,
        "candidate_id": cand["id"] if cand else None,
        "candidate_name": f"{cand['first_name']} {cand['last_name']}" if cand else None,
    }
    requests.post(
        f"{SUPABASE_URL}/rest/v1/twilio_voicemail",
        headers=_sb_headers(),
        json=payload,
    )
    return payload
