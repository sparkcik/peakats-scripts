#!/usr/bin/env python3
"""
fadv_profile_escalation.py
Escalation cadence for candidates submitted to FADV who have NOT yet
completed their background check profile (invitation link from FADV).

Trigger: fadv_submitted_at IS NOT NULL AND fadv_profile_completed_at IS NULL
         AND status = Active AND background_status IN (In Progress, Needs Further Review)

Day 0: Template 41 -- Initial (check your FADV email)
Day 1: Template 42 -- FUP 1
Day 2: Template 43 -- FUP 2
Day 3: Template 44 -- Final warning
Stop:  Template 45 -- Resolved (fired by FADV email parser on completion)

{link}         = instruction to check FADV email / spam
{expiry_date}  = fadv_submitted_at + 7 days
{reason}       = "First Advantage needs you to complete your background check profile."

Usage:
    python3 scripts/fadv_profile_escalation.py
    python3 scripts/fadv_profile_escalation.py --dry-run
"""

import os
import sys
import requests
from datetime import datetime, timedelta, timezone

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')
FROM_NUMBER = '+14704704766'
CREATED_BY  = 'fadv_profile_escalation'
DRY_RUN     = '--dry-run' in sys.argv

LINK_TEXT = (
    "Check your email from First Advantage (do_not_reply@fadv.com) -- "
    "the link to complete your profile is in that email. "
    "Check your spam folder if you don't see it."
)
REASON_TEXT = "First Advantage needs you to complete your background check profile."

DAY_TEMPLATE_MAP = {0: 41, 1: 42, 2: 43, 3: 44}
MAX_DAY = 3

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

def sb_patch(path, filter_qs, body):
    resp = requests.patch(
        f'{SUPABASE_URL}/rest/v1/{path}?{filter_qs}',
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

def format_phone(phone):
    if not phone:
        return None
    clean = phone.replace('-','').replace('(','').replace(')','').replace(' ','')
    if clean.startswith('+1'):
        return clean
    if len(clean) == 10 and clean.isdigit():
        return '+1' + clean
    if len(clean) == 11 and clean.startswith('1') and clean.isdigit():
        return '+' + clean
    return None

def get_template(template_id):
    rows = sb_get('message_templates', {
        'id': f'eq.{template_id}',
        'select': 'id,name,body'
    })
    return rows[0] if rows else None

def build_body(template_body, candidate, expiry_date):
    first = candidate.get('first_name', '')
    body = template_body
    body = body.replace('{First}', first)
    body = body.replace('{first}', first)
    body = body.replace('{link}', LINK_TEXT)
    body = body.replace('{expiry_date}', expiry_date)
    body = body.replace('{reason}', REASON_TEXT)
    return body

def main():
    now = datetime.now(timezone.utc)
    print(f'[FADV Profile Escalation] Starting at {now.strftime("%Y-%m-%d %H:%M:%S UTC")}')

    candidates = sb_get('candidates', {
        'select': 'id,first_name,last_name,phone,client_id,fadv_submitted_at,fadv_profile_escalation_day,fadv_profile_last_escalation_at',
        'fadv_submitted_at': 'not.is.null',
        'fadv_profile_completed_at': 'is.null',
        'status': 'eq.Active',
        'background_status': 'in.(In Progress,Needs Further Review)',
        'order': 'fadv_submitted_at.asc'
    })

    print(f'[FADV Profile Escalation] {len(candidates)} candidate(s) in pool.')

    queued = 0
    skipped = 0

    for c in candidates:
        cid        = c['id']
        first      = c.get('first_name', '')
        phone      = format_phone(c.get('phone', ''))
        client_id  = c.get('client_id', '')
        submitted  = c.get('fadv_submitted_at')
        esc_day    = c.get('fadv_profile_escalation_day') or 0
        last_esc   = c.get('fadv_profile_last_escalation_at')

        if not phone:
            print(f'  [{cid}] {first} -- no phone, skip')
            skipped += 1
            continue

        if not submitted:
            skipped += 1
            continue

        submitted_dt = datetime.fromisoformat(submitted.replace('Z', '+00:00'))
        expiry_date  = (submitted_dt + timedelta(days=7)).strftime('%B %d, %Y')

        # Skip if already at max day
        if esc_day > MAX_DAY:
            skipped += 1
            continue

        # Skip if already escalated today
        if last_esc:
            last_esc_dt = datetime.fromisoformat(last_esc.replace('Z', '+00:00'))
            hours_since = (now - last_esc_dt).total_seconds() / 3600
            if hours_since < 20:
                skipped += 1
                continue

        # Determine which day to send based on days since submission
        days_since = (now - submitted_dt).days

        # Day 0 fires immediately on first run after submission
        # Day 1+ fires once per day cadence
        target_day = min(days_since, MAX_DAY)

        # If we've already sent up to esc_day, send the next one
        send_day = esc_day
        if send_day > MAX_DAY:
            skipped += 1
            continue

        template_id = DAY_TEMPLATE_MAP.get(send_day)
        if not template_id:
            skipped += 1
            continue

        template = get_template(template_id)
        if not template:
            print(f'  [{cid}] Template {template_id} not found, skip')
            skipped += 1
            continue

        body = build_body(template['body'], c, expiry_date)

        print(f'  [{cid}] {first} ({client_id}) -- Day {send_day} T{template_id}')
        print(f'         Phone: {phone} | Submitted: {submitted_dt.date()} | Expires: {expiry_date}')
        print(f'         Preview: {body[:80]}...')

        if DRY_RUN:
            print(f'         [DRY RUN] skipping insert')
            queued += 1
            continue

        # Insert to sms_send_queue
        sb_insert('sms_send_queue', {
            'candidate_id':     cid,
            'to_number':        phone.replace('+1',''),
            'from_number':      FROM_NUMBER,
            'body':             body,
            'template_id':      template_id,
            'template_name':    template['name'],
            'status':           'pending',
            'migration_status': 'twilio_active',
            'scheduled_for':    now.isoformat(),
            'created_by':       CREATED_BY
        })

        # Stamp escalation fields
        sb_patch('candidates', f'id=eq.{cid}', {
            'fadv_profile_escalation_day':    send_day + 1,
            'fadv_profile_last_escalation_at': now.isoformat(),
            'updated_at':                     now.isoformat()
        })

        queued += 1

    print(f'\n[FADV Profile Escalation] Done. Queued: {queued} | Skipped: {skipped}')

if __name__ == '__main__':
    main()
