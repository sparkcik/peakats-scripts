#!/usr/bin/env python3
"""
fadv_profile_reminder.py
Sequential daily SMS escalation for candidates who have not completed their
FADV background screening profile after submission.

Cadence (anchor = fadv_submitted_at):
  T66 (id=66)  Day 0 -- fires at import time (handled by Forge, NOT this script)
  T67 (id=67)  Day 1 -- fadv_submitted_at <= NOW() - 1 day, t66 sent, t67 not sent
  T68 (id=68)  Day 2 -- fadv_submitted_at <= NOW() - 2 days, t67 sent, t68 not sent
  Day 3        No SMS -- insert action_item flagging candidate for manual review

Stop condition: background_status advances past Not Started / Intake.
Dedup guard: stamp columns fadv_profile_t67_sent_at / fadv_profile_t68_sent_at.

Runs on forge-local (Fly.io) daily via forge_runner.py scheduler.

Usage:
    python3 scripts/fadv_profile_reminder.py            # process all eligible
    python3 scripts/fadv_profile_reminder.py --dry-run  # preview without sending
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
CREATED_BY = 'fadv_profile_reminder'

DRY_RUN = '--dry-run' in sys.argv

# Background statuses that mean the candidate completed their FADV profile
COMPLETED_STATUSES = {
    'In Progress', 'Needs Further Review', 'Eligible', 'Ineligible',
    'Collection Event Review', 'Case Canceled', 'Adverse Action'
}

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


# -- Helpers ------------------------------------------------------------------
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


def get_template_body(template_id):
    rows = sb_get('message_templates', {'select': 'body', 'id': f'eq.{template_id}'})
    return rows[0]['body'] if rows else None


def substitute_first_name(body, first_name):
    result = body
    for token in ['[FIRST]', '{FIRST}', '[FIRST_NAME]']:
        result = result.replace(token, first_name)
    return result


def enforce_blackout(dt):
    ET = pytz.timezone('America/New_York')
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    dt_et = dt.astimezone(ET)
    hour, minute = dt_et.hour, dt_et.minute
    in_blackout = (hour < 7) or (hour == 7 and minute < 30) or (hour >= 21)
    if in_blackout:
        delivery = dt_et.replace(hour=7, minute=30, second=0, microsecond=0)
        if hour >= 21:
            delivery = delivery + timedelta(days=1)
        return delivery.astimezone(pytz.utc)
    return dt


def already_sent(candidate_id, template_name):
    """Dedup guard: check sms_send_queue for any sent/pending record."""
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue',
        headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}'},
        params={
            'candidate_id': f'eq.{candidate_id}',
            'template_name': f'eq.{template_name}',
            'status': 'in.(sent,pending)',
            'limit': '1',
            'select': 'id'
        }
    )
    try:
        return len(r.json()) > 0
    except Exception:
        return False


def elapsed_days(ts_str):
    """Return float days since a timestamp string, or None if unparseable."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 86400
    except (ValueError, TypeError):
        return None


# -- Main ---------------------------------------------------------------------
def run_fadv_profile_reminder():
    print(f'[FADV Profile Reminder] {"DRY RUN -- " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')

    # Fetch candidates: Active, fadv submitted, profile not yet completed
    candidates = sb_get('candidates', {
        'select': (
            'id,first_name,last_name,client_id,phone,'
            'fadv_submitted_at,background_status,'
            'fadv_profile_t66_sent_at,fadv_profile_t67_sent_at,fadv_profile_t68_sent_at'
        ),
        'status': 'eq.Active',
        'fadv_submitted_at': 'not.is.null',
        'background_status': 'in.(Not Started,Intake)',
        'compliance_override': 'neq.true',
        'order': 'fadv_submitted_at.asc'
    })

    candidates = [c for c in candidates if c.get('phone') and c['phone'] != '0000000000']
    print(f'[FADV Profile Reminder] {len(candidates)} candidate(s) in reminder pool.')

    if not candidates:
        print('[FADV Profile Reminder] Nothing to process. Exiting.')
        return

    # Pre-fetch templates
    templates = {}
    for tpl_id in (67, 68):
        body = get_template_body(tpl_id)
        if body:
            templates[tpl_id] = body
        else:
            print(f'[FADV Profile Reminder] WARNING: Template {tpl_id} not found.')

    base_time = datetime.now(timezone.utc)
    now_iso = base_time.isoformat()
    now_str = base_time.strftime('%Y-%m-%d %H:%M:%S UTC')
    send_index = 0
    count_t67 = count_t68 = count_flag = count_skip = 0

    for c in candidates:
        cid = c['id']
        first = c.get('first_name', '')
        last = c.get('last_name', '')
        client = c.get('client_id', 'unknown')
        phone = format_phone(c.get('phone', ''))
        submitted = c.get('fadv_submitted_at')
        t66_sent = c.get('fadv_profile_t66_sent_at')
        t67_sent = c.get('fadv_profile_t67_sent_at')
        t68_sent = c.get('fadv_profile_t68_sent_at')
        bg = (c.get('background_status') or '').strip()

        # Stop condition: profile completed
        if bg in COMPLETED_STATUSES:
            count_skip += 1
            continue

        if not phone:
            count_skip += 1
            continue

        days_since_submit = elapsed_days(submitted)
        if days_since_submit is None:
            count_skip += 1
            continue

        # -- Day 3: flag for manual review, no SMS --
        if days_since_submit >= 3 and t67_sent and t68_sent:
            if DRY_RUN:
                print(f'  [{cid}] {first} {last} ({client}) -> Day 3 FLAG (dry run)')
                count_flag += 1
                continue
            # Only flag once -- check action_items to avoid dupe
            existing = sb_get('action_items', {
                'select': 'id',
                'task': f'ilike.%FADV Profile Day 3%{cid}%',
                'status': 'eq.PENDING',
                'limit': '1'
            })
            if not existing:
                try:
                    sb_insert('action_items', {
                        'task': f'FADV Profile Day 3 -- {first} {last} ({client}) cid={cid} -- 3 reminders sent, profile still not completed. Manual follow-up required.',
                        'priority': '🔴',
                        'category': 'OPS',
                        'domain': 'PEAK Ops',
                        'status': 'PENDING',
                        'created_at': now_iso
                    })
                    print(f'  [{cid}] {first} {last} ({client}) -> Day 3 FLAG inserted')
                    count_flag += 1
                except Exception as e:
                    print(f'  [{cid}] FAILED to insert Day 3 action_item: {e}')
            else:
                count_skip += 1
            continue

        # -- Day 2: T68 --
        if days_since_submit >= 2 and t66_sent and t67_sent and not t68_sent:
            if 68 not in templates:
                count_skip += 1
                continue
            tpl_name = 'FADV Profile Reminder Day 2'
            print(f'  [{cid}] {first} {last} ({client}) -> T68 Day 2 -> {phone}')
            if DRY_RUN:
                print(f'         [DRY RUN] Would queue T68 -- skipping')
                count_t68 += 1
                continue
            if already_sent(cid, tpl_name):
                print(f'  [dedup] Skipping {tpl_name} for {cid} -- already in queue')
                count_skip += 1
                continue
            body = substitute_first_name(templates[68], first)
            scheduled = enforce_blackout(base_time + timedelta(minutes=3 * send_index)).isoformat()
            try:
                sb_insert('sms_send_queue', {
                    'candidate_id': cid,
                    'to_number': phone,
                    'from_number': FROM_NUMBER,
                    'body': body,
                    'template_id': 68,
                    'template_name': tpl_name,
                    'status': 'pending',
                    'channel': 'twilio',
                    'scheduled_for': scheduled,
                    'created_by': CREATED_BY
                })
            except Exception as e:
                print(f'         FAILED to queue T68: {e}')
                continue
            try:
                sb_patch('candidates', {'id': f'eq.{cid}'}, {
                    'fadv_profile_t68_sent_at': now_iso,
                    'updated_at': now_iso
                })
            except Exception as e:
                print(f'         FAILED to stamp t68: {e}')
            send_index += 1
            count_t68 += 1
            continue

        # -- Day 1: T67 --
        if days_since_submit >= 1 and t66_sent and not t67_sent:
            if 67 not in templates:
                count_skip += 1
                continue
            tpl_name = 'FADV Profile Reminder Day 1'
            print(f'  [{cid}] {first} {last} ({client}) -> T67 Day 1 -> {phone}')
            if DRY_RUN:
                print(f'         [DRY RUN] Would queue T67 -- skipping')
                count_t67 += 1
                continue
            if already_sent(cid, tpl_name):
                print(f'  [dedup] Skipping {tpl_name} for {cid} -- already in queue')
                count_skip += 1
                continue
            body = substitute_first_name(templates[67], first)
            scheduled = enforce_blackout(base_time + timedelta(minutes=3 * send_index)).isoformat()
            try:
                sb_insert('sms_send_queue', {
                    'candidate_id': cid,
                    'to_number': phone,
                    'from_number': FROM_NUMBER,
                    'body': body,
                    'template_id': 67,
                    'template_name': tpl_name,
                    'status': 'pending',
                    'channel': 'twilio',
                    'scheduled_for': scheduled,
                    'created_by': CREATED_BY
                })
            except Exception as e:
                print(f'         FAILED to queue T67: {e}')
                continue
            try:
                sb_patch('candidates', {'id': f'eq.{cid}'}, {
                    'fadv_profile_t67_sent_at': now_iso,
                    'updated_at': now_iso
                })
            except Exception as e:
                print(f'         FAILED to stamp t67: {e}')
            send_index += 1
            count_t67 += 1
            continue

        count_skip += 1

    summary = (
        f'FADV Profile Reminder run at {now_str}: '
        f'T67={count_t67}, T68={count_t68}, Day3 flags={count_flag}, skipped={count_skip}'
    )
    print(f'\n[FADV Profile Reminder] Done. {summary}')

    if not DRY_RUN and (count_t67 + count_t68 + count_flag) > 0:
        try:
            sb_insert('forge_memory', {
                'category': 'ops_note',
                'subject': f'FADV Profile Reminder Run -- {now_str}',
                'target_thread': 'PEAK Ops',
                'content': summary,
                'created_at': now_iso
            })
        except Exception as e:
            print(f'[FADV Profile Reminder] FAILED to log summary: {e}')


if __name__ == '__main__':
    run_fadv_profile_reminder()
