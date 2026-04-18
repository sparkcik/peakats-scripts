#!/usr/bin/env python3
"""
drug_screen_reminder.py
Automated 3-day reminder cadence for candidates who received drug screen outreach
but have not yet completed their drug test.

Day 1: Template 48 (gentle nudge)
Day 2: Template 49 (firmer follow-up)
Day 3: Template 50 (final reminder + escalation to PEAK Ops)

Runs on forge-local (Fly.io) via forge_runner.py scheduler.

Usage:
    python3 scripts/drug_screen_reminder.py            # process all eligible
    python3 scripts/drug_screen_reminder.py --dry-run   # preview without sending
"""

import os
import sys
import requests
import pytz
from datetime import datetime, timedelta, timezone

# -- Config -------------------------------------------------------------------
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')
FROM_NUMBER = '+14704704766'
CREATED_BY = 'drug_screen_reminder'

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


def enforce_blackout(dt):
    """Push any send time in blackout window (9PM-7:29AM ET) to 7:30AM ET same or next day."""
    ET = pytz.timezone('America/New_York')
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    dt_et = dt.astimezone(ET)
    hour = dt_et.hour
    minute = dt_et.minute
    in_blackout = (hour < 7) or (hour == 7 and minute < 30) or (hour >= 21)
    if in_blackout:
        delivery = dt_et.replace(hour=7, minute=30, second=0, microsecond=0)
        if dt_et.hour >= 21:
            delivery = delivery + timedelta(days=1)
        return delivery.astimezone(pytz.utc)
    return dt


def already_sent(sb_url, sb_key, candidate_id, template_name, hours=20):
    """Return True if this template was already sent/pending to this candidate in last N hours."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    r = requests.get(
        f'{sb_url}/rest/v1/sms_send_queue',
        headers={'apikey': sb_key, 'Authorization': f'Bearer {sb_key}'},
        params={
            'candidate_id': f'eq.{candidate_id}',
            'template_name': f'eq.{template_name}',
            'status': 'in.(sent,pending)',
            'created_at': f'gte.{cutoff}',
            'limit': '1',
            'select': 'id'
        }
    )
    try:
        return len(r.json()) > 0
    except Exception:
        return False


# -- Main ---------------------------------------------------------------------
def run_drug_screen_reminder():
    print(f'[Drug Screen Reminder] {"DRY RUN -- " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    # Fetch candidates with drug outreach sent but test not started
    candidates = sb_get('candidates', {
        'select': 'id,first_name,last_name,client_id,phone,drug_reminder_count,drug_last_reminder_at,drug_outreach_sent_at,background_status',
        'drug_test_status': 'eq.Not Started',
        'drug_outreach_sent_at': 'not.is.null',
        'background_status': 'neq.Ineligible',
        'status': 'not.in.(Rejected,Hired,Transferred)',
        'compliance_override': 'neq.true',
        'order': 'id.asc'
    })

    candidates = [c for c in candidates if c.get('phone') and c['phone'] != '0000000000']

    print(f'[Drug Screen Reminder] {len(candidates)} candidate(s) in reminder pool.')

    if not candidates:
        print('[Drug Screen Reminder] Nothing to send. Exiting.')
        return

    # Pre-fetch templates
    templates = {}
    for tpl_id in (48, 49, 50):
        body = get_template_body(tpl_id)
        if body:
            templates[tpl_id] = body
        else:
            print(f'[Drug Screen Reminder] WARNING: Template {tpl_id} not found in message_templates.')

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
        reminder_count = c.get('drug_reminder_count') or 0
        last_reminder = c.get('drug_last_reminder_at')
        outreach_sent = c.get('drug_outreach_sent_at')

        if not phone:
            skipped += 1
            continue

        # Determine which day reminder to send
        tpl_id = None
        compare_time = None

        if reminder_count == 0:
            tpl_id = 48
            compare_time = outreach_sent
        elif reminder_count == 1:
            tpl_id = 49
            compare_time = last_reminder
        elif reminder_count == 2:
            tpl_id = 50
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

        template_name = f'Drug Screen Reminder Day {reminder_count + 1}'
        if already_sent(SUPABASE_URL, SUPABASE_KEY, cid, template_name):
            print(f'  [dedup] Skipping {template_name} for candidate {cid} -- already sent')
            skipped += 1
            continue

        scheduled_for = enforce_blackout(datetime.now(timezone.utc)).isoformat()

        # Queue SMS
        try:
            sb_insert('sms_send_queue', {
                'candidate_id': cid,
                'to_number': phone,
                'from_number': FROM_NUMBER,
                'body': body,
                'template_id': tpl_id,
                'template_name': template_name,
                'status': 'pending',
                'scheduled_for': scheduled_for,
                'created_by': CREATED_BY
            })
        except Exception as e:
            print(f'         FAILED to queue SMS: {e}')
            continue

        # Update candidate
        update_fields = {
            'drug_reminder_count': reminder_count + 1,
            'drug_last_reminder_at': now_iso,
            'updated_at': now_iso
        }

        # Day 3 escalation
        if tpl_id == 50:
            update_fields['drug_escalated_at'] = now_iso
            try:
                sb_insert('forge_memory', {
                    'category': 'ops_note',
                    'subject': f'Drug Screen Day 3 Escalation - {first} {last}',
                    'target_thread': 'PEAK Ops',
                    'content': f'Candidate {cid} ({first} {last}) client={client} phone={phone} -- 3 drug screen reminders sent with no completion. Escalating.',
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
    summary = f'Drug Screen Reminder run at {now_str}: {count} sent, {skipped} skipped out of {len(candidates)} eligible.'
    print(f'\n[Drug Screen Reminder] Done. {count} queued, {skipped} skipped.')

    if not DRY_RUN and count > 0:
        try:
            sb_insert('forge_memory', {
                'category': 'ops_note',
                'subject': f'Drug Screen Reminder Run Summary - {now_str}',
                'target_thread': 'PEAK Ops',
                'content': summary,
                'created_at': now_iso
            })
        except Exception as e:
            print(f'[Drug Screen Reminder] FAILED to log summary to forge_memory: {e}')


if __name__ == '__main__':
    run_drug_screen_reminder()
