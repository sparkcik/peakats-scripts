#!/usr/bin/env python3
"""
sms_queue_poller.py
Polls sms_send_queue in Supabase, fires pending messages via Twilio.
Runs on forge-local (Fly.io) -- invoke via cron or forge-bridge.

Usage:
  python3 sms_queue_poller.py          # process all due messages
  python3 sms_queue_poller.py --dry-run # preview without sending
"""

import os
import sys
import json
import requests
from datetime import datetime, timezone

# ── Twilio Config ─────────────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID', 'AC7c95b5dfb1d6bda35b75cc16186e653c')
TWILIO_AUTH_TOKEN  = os.environ.get('TWILIO_AUTH_TOKEN', '609a3c093480bbe58382ac8ac1afe468')
TWILIO_FROM_NUMBER = os.environ.get('TWILIO_FROM_NUMBER', '+14704704766')

SUPABASE_URL     = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY     = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')

DRY_RUN = '--dry-run' in sys.argv

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

def mark_sent(msg_id, twilio_sid, candidate_id=None, template_name=''):
    now = datetime.now(timezone.utc).isoformat()
    requests.patch(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{msg_id}',
        headers=SB_HEADERS,
        json={
            'status': 'sent',
            'sent_at': now,
            'rc_message_id': twilio_sid,
            'delivery_status': 'delivered',
            'updated_at': now
        }
    )
    # Write-back to candidates table based on template type
    if candidate_id:
        try:
            tpl = (template_name or '').upper()
            patch_data = None
            if 'MEC' in tpl:
                patch_data = {
                    'mec_dl_outreach_sent_at': now,
                    'mec_dl_collection_stage': 'OUTREACH_SENT'
                }
            elif 'GCIC' in tpl:
                patch_data = {'gcic_text_sent': 1}
            if patch_data:
                patch_data['updated_at'] = now
                requests.patch(
                    f'{SUPABASE_URL}/rest/v1/candidates?id=eq.{candidate_id}',
                    headers=SB_HEADERS,
                    json=patch_data
                )
                print(f'         [write-back] Updated candidate {candidate_id}: {list(patch_data.keys())}')
        except Exception as e:
            print(f'         [write-back] Failed for candidate {candidate_id}: {e}')

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

# ── Twilio Send ──────────────────────────────────────────────────────────────
def send_sms(to_number, body):
    """Send SMS via Twilio REST API. Returns Twilio message SID."""
    clean = to_number.replace('+1','').replace('-','').replace('(','').replace(')','').replace(' ','')
    resp = requests.post(
        f'https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json',
        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        data={
            'From': TWILIO_FROM_NUMBER,
            'To': f'+1{clean}',
            'Body': body
        }
    )
    resp.raise_for_status()
    return resp.json().get('sid', 'unknown')

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f'[SMS Poller] {"DRY RUN — " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    messages = get_due_messages()
    print(f'[SMS Poller] {len(messages)} message(s) due')

    if not messages:
        print('[SMS Poller] Nothing to send. Exiting.')
        return

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
            print(f'         [DRY RUN] Would send -- skipping')
            continue

        try:
            # Normalize line breaks -- DB stores literal \n as escape sequence
            if body:
                body = body.replace('\\n', '\n')
                body = body.replace('\\r\\n', '\n')
                body = body.replace('\\r', '\n')
                # If still no real newlines, try unicode_escape decode
                if '\\n' in body:
                    try:
                        body = bytes(body, 'utf-8').decode('unicode_escape')
                    except Exception:
                        pass
            print(f'         Body repr: {repr(body[:200])}')
            twilio_sid = send_sms(to_number, body)
            mark_sent(msg_id, twilio_sid, candidate_id, template)
            update_comms_log(candidate_id, twilio_sid, body)
            print(f'         SENT -- Twilio SID: {twilio_sid}')
            sent += 1
        except Exception as e:
            mark_failed(msg_id, e)
            print(f'         FAILED -- {e}')
            failed += 1

    print(f'\n[SMS Poller] Complete. Sent: {sent} | Failed: {failed}')

if __name__ == '__main__':
    main()
