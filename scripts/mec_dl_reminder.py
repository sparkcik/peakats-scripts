#!/usr/bin/env python3
"""
mec_dl_reminder.py
Automated reminder cadence for candidates who received MEC/DL outreach
but have not yet uploaded their documents.

Date split logic:
  - created_at BEFORE 2026-02-01: Template 52 (cold re-engagement), one-shot
  - created_at 2026-02-01+: Day 1 T16, Day 2 T17, Day 3 T18 (escalation)

Runs on forge-local (Fly.io) via forge_runner.py scheduler.

Usage:
    python3 scripts/mec_dl_reminder.py            # process all eligible
    python3 scripts/mec_dl_reminder.py --dry-run   # preview without sending
"""

import os
import sys
import time
import requests
from datetime import datetime, timezone

# -- Config -------------------------------------------------------------------
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')
FROM_NUMBER = '+14704704766'
CREATED_BY = 'mec_dl_reminder'

DRY_RUN = '--dry-run' in sys.argv

COLD_CUTOFF = datetime(2026, 2, 1, tzinfo=timezone.utc)

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
    """Fetch template body from message_templates table."""
    rows = sb_get('message_templates', {
        'select': 'body',
        'id': f'eq.{template_id}'
    })
    if rows:
        return rows[0]['body']
    return None


def substitute_first_name(body, first_name):
    """Replace [FIRST], {FIRST}, [FIRST_NAME] with candidate first_name."""
    result = body
    for token in ['[FIRST]', '{FIRST}', '[FIRST_NAME]']:
        result = result.replace(token, first_name)
    return result


# -- Main ---------------------------------------------------------------------
def run_mec_dl_reminder():
    print(f'[MEC/DL Reminder] {"DRY RUN -- " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    # Fetch candidates with outreach sent but docs not uploaded
    candidates = sb_get('candidates', {
        'select': 'id,first_name,last_name,client_id,phone,mec_reminder_count,mec_last_reminder_at,mec_dl_outreach_sent_at,mec_uploaded,dl_verified,created_at',
        'mec_dl_collection_stage': 'eq.OUTREACH_SENT',
        'or': '(mec_uploaded.is.null,mec_uploaded.eq.0,dl_verified.is.null,dl_verified.eq.0)',
        'status': 'not.in.(Rejected,Hired,Transferred)',
        'order': 'id.asc'
    })

    # Filter out candidates without valid phones
    candidates = [c for c in candidates if c.get('phone') and c['phone'] != '0000000000']

    print(f'[MEC/DL Reminder] {len(candidates)} candidate(s) in reminder pool.')

    if not candidates:
        print('[MEC/DL Reminder] Nothing to send. Exiting.')
        return

    # Pre-fetch templates (Day 1/2/3 + cold re-engagement)
    templates = {}
    for tpl_id in (16, 17, 18, 52):
        body = get_template_body(tpl_id)
        if body:
            templates[tpl_id] = body
        else:
            print(f'[MEC/DL Reminder] WARNING: Template {tpl_id} not found in message_templates.')

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
        reminder_count = c.get('mec_reminder_count') or 0
        last_reminder = c.get('mec_last_reminder_at')
        outreach_sent = c.get('mec_dl_outreach_sent_at')
        created_at_raw = c.get('created_at')

        if not phone:
            skipped += 1
            continue

        # Parse created_at for date split
        is_cold = False
        if created_at_raw:
            try:
                created_dt = datetime.fromisoformat(created_at_raw.replace('Z', '+00:00'))
                is_cold = created_dt < COLD_CUTOFF
            except (ValueError, TypeError):
                pass

        # -- Cold re-engagement (pre-Feb 2026) --
        if is_cold:
            if reminder_count > 0:
                # Already sent cold re-engagement
                skipped += 1
                continue

            if not outreach_sent:
                skipped += 1
                continue

            # Check 1-day elapsed since outreach
            try:
                sent_dt = datetime.fromisoformat(outreach_sent.replace('Z', '+00:00'))
                if (datetime.now(timezone.utc) - sent_dt).total_seconds() < 86400:
                    skipped += 1
                    continue
            except (ValueError, TypeError):
                skipped += 1
                continue

            tpl_id = 52
            if tpl_id not in templates:
                print(f'  [{cid}] {first} {last} -- skipping, template 52 not found')
                skipped += 1
                continue

            body = substitute_first_name(templates[tpl_id], first)
            print(f'  [{cid}] {first} {last} ({client}) -> T52 Cold Re-engagement -> {phone}')

            if DRY_RUN:
                print(f'         [DRY RUN] Would queue T52 -- skipping')
                count += 1
                continue

            # Pace sends
            if count > 0:
                time.sleep(180)

            try:
                sb_insert('sms_send_queue', {
                    'candidate_id': cid,
                    'to_number': phone,
                    'from_number': FROM_NUMBER,
                    'body': body,
                    'template_id': tpl_id,
                    'template_name': 'MEC/DL Cold Re-engagement',
                    'status': 'pending',
                    'channel': 'rc',
                    'scheduled_for': now_iso,
                    'created_by': CREATED_BY
                })
            except Exception as e:
                print(f'         FAILED to queue SMS: {e}')
                continue

            try:
                sb_patch('candidates', {'id': f'eq.{cid}'}, {
                    'mec_reminder_count': 1,
                    'mec_last_reminder_at': now_iso,
                    'updated_at': now_iso
                })
            except Exception as e:
                print(f'         FAILED to update candidate: {e}')

            count += 1
            continue

        # -- Active candidates (Feb 2026+): Day 1/2/3 logic --
        tpl_id = None
        compare_time = None

        if reminder_count == 0:
            tpl_id = 16
            compare_time = outreach_sent
        elif reminder_count == 1:
            tpl_id = 17
            compare_time = last_reminder
        elif reminder_count == 2:
            tpl_id = 18
            compare_time = last_reminder
        else:
            # Already sent all 3 reminders
            skipped += 1
            continue

        if not compare_time:
            skipped += 1
            continue

        # Check if at least 1 day has passed
        try:
            sent_dt = datetime.fromisoformat(compare_time.replace('Z', '+00:00'))
            elapsed = (datetime.now(timezone.utc) - sent_dt).total_seconds()
            if elapsed < 86400:  # less than 1 day
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

        # Pace sends
        if count > 0:
            time.sleep(180)

        # Queue SMS
        try:
            sb_insert('sms_send_queue', {
                'candidate_id': cid,
                'to_number': phone,
                'from_number': FROM_NUMBER,
                'body': body,
                'template_id': tpl_id,
                'template_name': f'MEC/DL Reminder Day {reminder_count + 1}',
                'status': 'pending',
                'channel': 'rc',
                'scheduled_for': now_iso,
                'created_by': CREATED_BY
            })
        except Exception as e:
            print(f'         FAILED to queue SMS: {e}')
            continue

        # Update candidate
        update_fields = {
            'mec_reminder_count': reminder_count + 1,
            'mec_last_reminder_at': now_iso,
            'updated_at': now_iso
        }

        # Day 3 escalation
        if tpl_id == 18:
            update_fields['mec_escalated_at'] = now_iso
            try:
                sb_insert('forge_memory', {
                    'category': 'ops_note',
                    'subject': f'MEC/DL Day 3 Escalation - {first} {last}',
                    'target_thread': 'PEAK Ops',
                    'content': f'Candidate {cid} ({first} {last}) client={client} phone={phone} -- 3 MEC/DL reminders sent with no upload. Escalating.',
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
    summary = f'MEC/DL Reminder run at {now_str}: {count} sent, {skipped} skipped out of {len(candidates)} eligible.'
    print(f'\n[MEC/DL Reminder] Done. {count} queued, {skipped} skipped.')

    if not DRY_RUN and count > 0:
        try:
            sb_insert('forge_memory', {
                'category': 'ops_note',
                'subject': f'MEC/DL Reminder Run Summary - {now_str}',
                'target_thread': 'PEAK Ops',
                'content': summary,
                'created_at': now_iso
            })
        except Exception as e:
            print(f'[MEC/DL Reminder] FAILED to log summary to forge_memory: {e}')


if __name__ == '__main__':
    run_mec_dl_reminder()
