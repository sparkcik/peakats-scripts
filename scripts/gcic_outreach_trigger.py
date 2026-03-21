#!/usr/bin/env python3
"""
gcic_outreach_trigger.py
Detects candidates with background_status='In Progress' and no GCIC outreach sent,
queues Template 2 SMS via sms_send_queue.

Runs on forge-local (Fly.io) every 30 minutes via forge_runner.py scheduler.

Usage:
    python3 scripts/gcic_outreach_trigger.py            # process all eligible
    python3 scripts/gcic_outreach_trigger.py --dry-run   # preview without sending
"""

import os
import sys
import requests
from datetime import datetime, timezone

# -- Config -------------------------------------------------------------------
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')
TEMPLATE_ID = 2
TEMPLATE_NAME = 'GCIC Outreach SMS -- Form Not Submitted'
FROM_NUMBER = '+14704704766'
CREATED_BY = 'gcic_outreach_trigger'

DRY_RUN = '--dry-run' in sys.argv

TEMPLATE_BODY = (
    "Your background check is on hold -- complete both steps below to move forward.\n\n"
    "Step 1 -- Submit your info (2 min):\n"
    "https://bit.ly/gcic-esiginfo\n\n"
    "Step 2 -- Sign your background form:\n"
    "https://bit.ly/gcic-esig\n\n"
    "Important: After signing, Adobe will send you an email asking you to verify "
    "your email address. You must click that link to complete your signature -- "
    "check your inbox right after you sign.\n\n"
    "Both steps take under 5 minutes. I am trying to get you started this week -- "
    "please do both now.\n\n"
    "Kai\nPEAKrecruiting\nQuestions? (470) 470-4766"
)

# -- Supabase helpers ---------------------------------------------------------
SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}


def sb_get(path, params):
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/{path}',
        headers={**SB_HEADERS, 'Prefer': 'return=representation'},
        params=params
    )
    resp.raise_for_status()
    return resp.json()


def sb_patch(path, params, body):
    qs = '&'.join(f'{k}={v}' for k, v in params.items())
    resp = requests.patch(
        f'{SUPABASE_URL}/rest/v1/{path}?{qs}',
        headers=SB_HEADERS,
        json=body
    )
    resp.raise_for_status()


def sb_insert(path, body):
    resp = requests.post(
        f'{SUPABASE_URL}/rest/v1/{path}',
        headers=SB_HEADERS,
        json=body
    )
    resp.raise_for_status()


# -- Phone formatting --------------------------------------------------------
def format_phone(phone):
    """Ensure phone has +1 prefix."""
    if not phone:
        return None
    clean = phone.replace('-', '').replace('(', '').replace(')', '').replace(' ', '')
    if clean.startswith('+1'):
        return clean
    if len(clean) == 10 and clean.isdigit():
        return '+1' + clean
    if len(clean) == 11 and clean.startswith('1') and clean.isdigit():
        return '+' + clean
    return '+1' + clean


# -- Main ---------------------------------------------------------------------
def run_gcic_outreach():
    print(f'[GCIC Outreach] {"DRY RUN -- " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    # Step 1: Query candidates needing outreach
    candidates = sb_get('candidates', {
        'select': 'id,first_name,last_name,client_id,phone',
        'background_status': 'eq.In Progress',
        'or': '(gcic_text_sent.is.null,gcic_text_sent.eq.0)',
        'and': '(gcic_stage.is.null,gcic_stage.eq.)',
        'status': 'not.in.(Rejected,Hired,Transferred)',
        'phone': 'neq.0000000000',
        'order': 'id.asc'
    })

    # Filter out null/empty phones client-side (belt-and-suspenders)
    candidates = [c for c in candidates if c.get('phone') and c['phone'] != '0000000000']

    print(f'[GCIC Outreach] {len(candidates)} candidate(s) eligible for outreach.')

    if not candidates:
        print('[GCIC Outreach] Nothing to send. Exiting.')
        return

    count = 0
    now = datetime.now(timezone.utc).isoformat()

    for c in candidates:
        cid = c['id']
        first = c.get('first_name', '')
        last = c.get('last_name', '')
        client = c.get('client_id', 'unknown')
        phone = format_phone(c.get('phone', ''))

        if not phone:
            print(f'  [{cid}] {first} {last} -- skipping, no valid phone')
            continue

        print(f'  [{cid}] {first} {last} ({client}) -> {phone}')

        if DRY_RUN:
            print(f'         [DRY RUN] Would queue SMS -- skipping')
            count += 1
            continue

        # Step 2a: Insert into sms_send_queue
        try:
            sb_insert('sms_send_queue', {
                'candidate_id': cid,
                'to_number': phone,
                'from_number': FROM_NUMBER,
                'body': TEMPLATE_BODY,
                'template_id': TEMPLATE_ID,
                'template_name': TEMPLATE_NAME,
                'status': 'pending',
                'scheduled_for': now,
                'created_by': CREATED_BY
            })
        except Exception as e:
            print(f'         FAILED to queue SMS: {e}')
            continue

        # Step 2b: Update candidate
        try:
            sb_patch('candidates', {'id': f'eq.{cid}'}, {
                'gcic_text_sent': 1,
                'gcic_stage': 'FORM_SENT',
                'updated_at': now
            })
        except Exception as e:
            print(f'         FAILED to update candidate: {e}')

        print(f'         [GCIC Outreach] Queued SMS for {first} {last} ({client}) -> {phone}')
        count += 1

    # Step 3: Summary
    print(f'\n[GCIC Outreach] Done. {count} candidates queued.')


if __name__ == '__main__':
    run_gcic_outreach()
