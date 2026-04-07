#!/usr/bin/env python3
"""
gcic_outreach_trigger.py
Detects candidates with background_status IN ('In Progress','Needs Further Review') and no GCIC outreach sent,
queues Template 2 SMS via sms_send_queue (or T52 for cold candidates pre-Feb 2026).

Runs on forge-local (Fly.io) every 30 minutes via forge_runner.py scheduler.

Usage:
    python3 scripts/gcic_outreach_trigger.py            # process all eligible
    python3 scripts/gcic_outreach_trigger.py --dry-run   # preview without sending
"""

import os
import sys
import requests
from datetime import datetime, timedelta, timezone

# -- Config -------------------------------------------------------------------
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')
TEMPLATE_ID = 2
TEMPLATE_NAME = 'GCIC Outreach SMS -- Form Not Submitted'
FROM_NUMBER = '+14704704766'
CREATED_BY = 'gcic_outreach_trigger'

DRY_RUN = '--dry-run' in sys.argv

COLD_CUTOFF = datetime(2026, 2, 1, tzinfo=timezone.utc)

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


# -- Template fetch -----------------------------------------------------------
def get_template_body(template_id):
    """Fetch template body from message_templates table."""
    rows = sb_get('message_templates', {
        'select': 'body',
        'id': f'eq.{template_id}'
    })
    if rows:
        return rows[0]['body']
    return None


def substitute_first_name(body, first_name):
    result = body
    for token in ['[FIRST]', '{FIRST}', '[FIRST_NAME]']:
        result = result.replace(token, first_name)
    return result


# -- Main ---------------------------------------------------------------------
def run_gcic_outreach():
    print(f'[GCIC Outreach] {"DRY RUN -- " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    # Query candidates needing outreach
    candidates = sb_get('candidates', {
        'select': 'id,first_name,last_name,client_id,phone,created_at,gcic_status,gcic_outreach_sent_at',
        'background_status': 'in.(In Progress,Needs Further Review)',
        'or': '(gcic_outreach_sent_at.is.null,gcic_status.eq.NOT_SENT)',
        'status': 'not.in.(Rejected,Hired,Transferred)',
        'phone': 'neq.0000000000',
        'order': 'id.asc'
    })

    # Filter out null/empty phones client-side
    candidates = [c for c in candidates if c.get('phone') and c['phone'] != '0000000000']

    print(f'[GCIC Outreach] {len(candidates)} candidate(s) eligible for outreach.')

    if not candidates:
        print('[GCIC Outreach] Nothing to send. Exiting.')
        return

    # Fetch T52 for cold candidates
    cold_template_body = get_template_body(52)
    if not cold_template_body:
        print('[GCIC Outreach] WARNING: Template 52 not found in message_templates.')

    base_time = datetime.now(timezone.utc)
    now = base_time.isoformat()
    send_index = 0
    count = 0

    for c in candidates:
        cid = c['id']
        first = c.get('first_name', '')
        last = c.get('last_name', '')
        client = c.get('client_id', 'unknown')
        phone = format_phone(c.get('phone', ''))
        created_at_raw = c.get('created_at')

        if not phone:
            print(f'  [{cid}] {first} {last} -- skipping, no valid phone')
            continue

        # Date split
        is_cold = False
        if created_at_raw:
            try:
                created_dt = datetime.fromisoformat(created_at_raw.replace('Z', '+00:00'))
                is_cold = created_dt < COLD_CUTOFF
            except (ValueError, TypeError):
                pass

        if is_cold:
            if not cold_template_body:
                print(f'  [{cid}] {first} {last} -- skipping cold, template 52 not found')
                continue
            tpl_id = 52
            tpl_name = 'GCIC Cold Re-engagement'
            body = substitute_first_name(cold_template_body, first)
            print(f'  [{cid}] {first} {last} ({client}) -> T52 Cold -> {phone}')
        else:
            tpl_id = TEMPLATE_ID
            tpl_name = TEMPLATE_NAME
            body = TEMPLATE_BODY
            print(f'  [{cid}] {first} {last} ({client}) -> T2 -> {phone}')

        if DRY_RUN:
            print(f'         [DRY RUN] Would queue T{tpl_id} -- skipping')
            count += 1
            continue

        scheduled_for = (base_time + timedelta(minutes=3 * send_index)).isoformat()

        # Insert into sms_send_queue
        try:
            sb_insert('sms_send_queue', {
                'candidate_id': cid,
                'to_number': phone,
                'from_number': FROM_NUMBER,
                'body': body,
                'template_id': tpl_id,
                'template_name': tpl_name,
                'status': 'pending',
                'migration_status': 'twilio_active',
                'scheduled_for': scheduled_for,
                'created_by': CREATED_BY
            })
        except Exception as e:
            print(f'         FAILED to queue SMS -- NOT stamping candidate: {e}')
            continue

        # Update candidate
        update_fields = {
            'gcic_text_sent': 1,
            'gcic_stage': 'FORM_SENT',
            'gcic_outreach_sent_at': now,
            'gcic_sms_sent_at': now,
            'updated_at': now
        }
        if is_cold:
            update_fields['gcic_reminder_count'] = 1

        try:
            sb_patch('candidates', {'id': f'eq.{cid}'}, update_fields)
        except Exception as e:
            print(f'         FAILED to update candidate: {e}')

        print(f'         [GCIC Outreach] Queued T{tpl_id} for {first} {last} ({client}) -> {phone}')
        send_index += 1
        count += 1

    # Summary
    print(f'\n[GCIC Outreach] Done. {count} candidates queued.')


if __name__ == '__main__':
    run_gcic_outreach()
