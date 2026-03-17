#!/usr/bin/env python3
"""
sms_queue_poller.py
Polls sms_send_queue in Supabase, fires pending messages via RC API at scheduled time.
Runs on forge-local (Fly.io) — invoke via cron or forge-bridge.

Usage:
  python3 sms_queue_poller.py          # process all due messages
  python3 sms_queue_poller.py --dry-run # preview without sending
"""

import sys
import json
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
RC_CLIENT_ID     = '1QDQiRjk50kfxvIVYTT3IA'
RC_CLIENT_SECRET = 'aTMprgZe1Safik4e4qDBnHaKcnA6o9gb3cafm1xQtJxo'
RC_JWT           = 'eyJraWQiOiI4NzYyZjU5OGQwNTk0NGRiODZiZjVjYTk3ODA0NzYwOCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJhdWQiOiJodHRwczovL3BsYXRmb3JtLnJpbmdjZW50cmFsLmNvbS9yZXN0YXBpL29hdXRoL3Rva2VuIiwic3ViIjoiNTM2NDg0MDMwIiwiaXNzIjoiaHR0cHM6Ly9wbGF0Zm9ybS5yaW5nY2VudHJhbC5jb20iLCJleHAiOjM5MjEyMTIzNTksImlhdCI6MTc3MzcyODcxMiwianRpIjoib3IxQTdEUTFULUdLTU5MY0lPSDRzQSJ9.Jwz40n4cSYp5Ke7j3jGJSeY1g-nPPsUbXS8PMw_gmKRGVTp22O4BzCMxSZIFkAde_FOVEOjtqrHCwYha7fj04WPDPI528z1fyTbavivHTb5pYRlQRDVboeL-3GBftOdS4EFt1cDWhA-qDfUO_9ClpxbnBUbWZbWnSNE4oLoZyf8TeC86GvHvftQTljTFzYlKNNA7wHhNAGCygDVMq6NVDIacXB81XADVpJ2DPtRI58M5CvJphnmqzeoYsIVNaQC8C5n-GyQxSGGXleIO6VVxeQ4LMraUWu_JS52Lhswu-Fb8otWft8ephnWDybhaRcjiCkG1uXQX1yOkOWMYrmTKiA'
RC_SERVER        = 'https://platform.ringcentral.com'
KAI_NUMBER       = '+14708574325'

SUPABASE_URL     = 'https://eyopvsmsvbgfuffscfom.supabase.co'
SUPABASE_KEY     = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4'

DRY_RUN = '--dry-run' in sys.argv

# ── RC Auth ───────────────────────────────────────────────────────────────────
def get_rc_token():
    resp = requests.post(
        f'{RC_SERVER}/restapi/oauth/token',
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': RC_JWT
        }
    )
    resp.raise_for_status()
    return resp.json()['access_token']

# ── Supabase helpers ──────────────────────────────────────────────────────────
SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

def get_due_messages():
    """Fetch all pending messages where scheduled_for <= now."""
    now = datetime.now(timezone.utc).isoformat()
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue',
        headers=SB_HEADERS,
        params={
            'status': 'eq.pending',
            'scheduled_for': f'lte.{now}',
            'order': 'scheduled_for.asc'
        }
    )
    resp.raise_for_status()
    return resp.json()

def mark_sent(msg_id, rc_message_id):
    now = datetime.now(timezone.utc).isoformat()
    requests.patch(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{msg_id}',
        headers=SB_HEADERS,
        json={
            'status': 'sent',
            'sent_at': now,
            'rc_message_id': rc_message_id,
            'delivery_status': 'delivered',
            'updated_at': now
        }
    )

def mark_failed(msg_id, error):
    now = datetime.now(timezone.utc).isoformat()
    requests.patch(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{msg_id}',
        headers=SB_HEADERS,
        json={
            'status': 'failed',
            'delivery_error': str(error)[:500],
            'updated_at': now
        }
    )

def update_comms_log(candidate_id, rc_message_id, body):
    """Update candidate_comms sent_at and external_message_id for this send."""
    if not candidate_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    # Match on candidate_id + body to find the right comms record
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/candidate_comms',
        headers=SB_HEADERS,
        params={
            'candidate_id': f'eq.{candidate_id}',
            'direction': 'eq.outbound',
            'external_message_id': 'is.null',
            'order': 'created_at.desc',
            'limit': '1'
        }
    )
    records = resp.json()
    if records:
        comms_id = records[0]['id']
        requests.patch(
            f'{SUPABASE_URL}/rest/v1/candidate_comms?id=eq.{comms_id}',
            headers=SB_HEADERS,
            json={
                'external_message_id': rc_message_id,
                'delivery_status': 'delivered',
                'updated_at': now
            }
        )

# ── RC Send ───────────────────────────────────────────────────────────────────
def send_sms(token, to_number, body):
    clean = to_number.replace('+1','').replace('-','').replace('(','').replace(')','').replace(' ','')
    resp = requests.post(
        f'{RC_SERVER}/restapi/v1.0/account/~/extension/~/sms',
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        json={
            'from': {'phoneNumber': KAI_NUMBER},
            'to': [{'phoneNumber': f'+1{clean}'}],
            'text': body
        }
    )
    resp.raise_for_status()
    return resp.json().get('id', 'unknown')

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f'[SMS Poller] {"DRY RUN — " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    messages = get_due_messages()
    print(f'[SMS Poller] {len(messages)} message(s) due')

    if not messages:
        print('[SMS Poller] Nothing to send. Exiting.')
        return

    token = get_rc_token() if not DRY_RUN else None
    sent = 0
    failed = 0

    for msg in messages:
        msg_id       = msg['id']
        to_number    = msg['to_number']
        body         = msg['body']
        candidate_id = msg.get('candidate_id')
        template     = msg.get('template_name', 'manual')

        print(f'\n[{msg_id}] To: {to_number} | Template: {template}')
        print(f'         Preview: {body[:80]}...')

        if DRY_RUN:
            print(f'         [DRY RUN] Would send — skipping')
            continue

        try:
            rc_id = send_sms(token, to_number, body)
            mark_sent(msg_id, rc_id)
            update_comms_log(candidate_id, rc_id, body)
            print(f'         SENT — RC ID: {rc_id}')
            sent += 1
        except Exception as e:
            mark_failed(msg_id, e)
            print(f'         FAILED — {e}')
            failed += 1

    print(f'\n[SMS Poller] Complete. Sent: {sent} | Failed: {failed}')

if __name__ == '__main__':
    main()
