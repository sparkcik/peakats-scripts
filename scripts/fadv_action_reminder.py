#!/usr/bin/env python3
"""
fadv_action_reminder.py
Automated 3-day reminder cadence for candidates whose background check
needs further review and action has been requested but not resolved.

Day 1: Template 42 (gentle nudge)
Day 2: Template 43 (firmer follow-up)
Day 3: Template 44 (final reminder + escalation to PEAK Ops)

Runs on forge-local (Fly.io) via forge_runner.py scheduler.

Usage:
    python3 scripts/fadv_action_reminder.py            # process all eligible
    python3 scripts/fadv_action_reminder.py --dry-run   # preview without sending
"""

import os
import sys
import requests
from datetime import datetime, timezone

# -- Config -------------------------------------------------------------------
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')
FROM_NUMBER = '+14704704766'
CREATED_BY = 'fadv_action_reminder'

DRY_RUN = '--dry-run' in sys.argv

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
def run_fadv_action_reminder():
    print(f'[FADV Action Reminder] {"DRY RUN -- " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    # Fetch candidates with FADV action sent but not resolved
    candidates = sb_get('candidates', {
        'select': 'id,first_name,last_name,client_id,phone,fadv_action_reminder_count,fadv_action_last_reminder_at,fadv_action_sent_at',
        'background_status': 'eq.Needs Further Review',
        'fadv_action_sent_at': 'not.is.null',
        'fadv_action_resolved_at': 'is.null',
        'status': 'not.in.(Rejected,Hired,Transferred)',
        'order': 'id.asc'
    })

    candidates = [c for c in candidates if c.get('phone') and c['phone'] != '0000000000']

    print(f'[FADV Action Reminder] {len(candidates)} candidate(s) in reminder pool.')

    if not candidates:
        print('[FADV Action Reminder] Nothing to send. Exiting.')
        return

    # Pre-fetch templates
    templates = {}
    for tpl_id in (42, 43, 44):
        body = get_template_body(tpl_id)
        if body:
            templates[tpl_id] = body
        else:
            print(f'[FADV Action Reminder] WARNING: Template {tpl_id} not found in message_templates.')

    now_iso = datetime.now(timezone.utc).isoformat()
    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    count = 0
    skipped = 0

    for c in candidates:
        cid = c['id']
        first = c.get('first_name', '')
        last = c.get('last_name', '')
        client = c.get('client_id', 'unknown')
        phone = format_phone(c.get('phone', ''))
        reminder_count = c.get('fadv_action_reminder_count') or 0
        last_reminder = c.get('fadv_action_last_reminder_at')
        action_sent = c.get('fadv_action_sent_at')

        if not phone:
            skipped += 1
            continue

        # Determine which day reminder to send
        tpl_id = None
        compare_time = None

        if reminder_count == 0:
            tpl_id = 42
            compare_time = action_sent
        elif reminder_count == 1:
            tpl_id = 43
            compare_time = last_reminder
        elif reminder_count == 2:
            tpl_id = 44
            compare_time = last_reminder
        else:
            skipped += 1
            continue

        if not compare_time:
            skipped += 1
            continue

        # Check if at least 1 day has passed
        try:
            sent_dt = datetime.fromisoformat(compare_time.replace('Z', '+00:00'))
            elapsed = (datetime.now(timezone.utc) - sent_dt).total_seconds()
            if elapsed < 86400:
                skipped += 1
                continue
        except (ValueError, TypeError):
            skipped += 1
            continue

        if tpl_id not in templates:
            print(f'  [{cid}] {first} {last} -- skipping, template {tpl_id} not found')
            skipped += 1
            continue

        body = substitute_first_name(templates[tpl_id], first)

        print(f'  [{cid}] {first} {last} ({client}) -> T{tpl_id} Day {reminder_count + 1} -> {phone}')

        if DRY_RUN:
            print(f'         [DRY RUN] Would queue T{tpl_id} -- skipping')
            count += 1
            continue

        # Queue SMS
        try:
            sb_insert('sms_send_queue', {
                'candidate_id': cid,
                'to_number': phone,
                'from_number': FROM_NUMBER,
                'body': body,
                'template_id': tpl_id,
                'template_name': f'FADV Action Reminder Day {reminder_count + 1}',
                'status': 'pending',
                'scheduled_for': now_iso,
                'created_by': CREATED_BY
            })
        except Exception as e:
            print(f'         FAILED to queue SMS: {e}')
            continue

        # Update candidate
        update_fields = {
            'fadv_action_reminder_count': reminder_count + 1,
            'fadv_action_last_reminder_at': now_iso,
            'updated_at': now_iso
        }

        # Day 3 escalation
        if tpl_id == 44:
            try:
                sb_insert('forge_memory', {
                    'category': 'ops_note',
                    'subject': f'FADV Action Day 3 Escalation - {first} {last}',
                    'target_thread': 'PEAK Ops',
                    'content': f'Candidate {cid} ({first} {last}) client={client} phone={phone} -- 3 FADV action reminders sent with no resolution. Escalating.',
                    'created_at': now_iso
                })
            except Exception as e:
                print(f'         FAILED to insert escalation forge_memory: {e}')

        try:
            sb_patch('candidates', {'id': f'eq.{cid}'}, update_fields)
        except Exception as e:
            print(f'         FAILED to update candidate: {e}')

        count += 1

    # Run summary log to forge_memory
    summary = f'FADV Action Reminder run at {now_str}: {count} sent, {skipped} skipped out of {len(candidates)} eligible.'
    print(f'\n[FADV Action Reminder] Done. {count} queued, {skipped} skipped.')

    if not DRY_RUN and count > 0:
        try:
            sb_insert('forge_memory', {
                'category': 'ops_note',
                'subject': f'FADV Action Reminder Run Summary - {now_str}',
                'target_thread': 'PEAK Ops',
                'content': summary,
                'created_at': now_iso
            })
        except Exception as e:
            print(f'[FADV Action Reminder] FAILED to log summary to forge_memory: {e}')


if __name__ == '__main__':
    run_fadv_action_reminder()
