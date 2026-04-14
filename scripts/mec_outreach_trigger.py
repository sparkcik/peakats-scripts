#!/usr/bin/env python3
"""
mec_outreach_trigger.py
Detects candidates needing MEC/DL outreach based on drug/BG status combination,
queues the correct template SMS via sms_send_queue.

Runs on forge-local (Fly.io) every 30 minutes via forge_runner.py scheduler.

Usage:
    python3 scripts/mec_outreach_trigger.py                # process all eligible
    python3 scripts/mec_outreach_trigger.py --dry-run      # preview without sending
    python3 scripts/mec_outreach_trigger.py --client solpac # single client
    python3 scripts/mec_outreach_trigger.py --limit 10     # cap at 10 sends
"""

import os
import sys
import argparse
import requests
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text

# -- Config -------------------------------------------------------------------
DB_URL = os.environ.get('DB_URL', 'postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require')
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')
FROM_NUMBER = '+14704704766'
CREATED_BY = 'mec_outreach_trigger'

# -- Template bodies ----------------------------------------------------------
TEMPLATE_15_BODY = (
    "[FIRST], great news -- your background check has cleared. The last step is "
    "your drug screen and I need your Medical Examiner Certificate (MEC) from your "
    "DOT physical along with a photo of your Driver License to get you placed.\n\n"
    "When you have both ready, upload here -- takes 2 minutes: "
    "https://bit.ly/mec-dl-form\n\n"
    "Please reply YES or NO to confirm if you have already completed the drug screen "
    "and physical. If yes, submit your MEC and DL using that link. If no, let me know "
    "how soon you can go -- I have a position ready for you.\n\n"
    "Kai\nPEAKrecruiting\nQuestions? (470) 470-4766"
)

TEMPLATE_46_BODY = (
    "[FIRST], great news -- your drug screen is complete. I am waiting on your "
    "background check to finalize and want to make sure I am ready to move the "
    "moment it clears.\n\n"
    "I need your Medical Examiner Certificate (MEC) from your DOT physical along "
    "with a photo of your Driver License on file now so there is no delay.\n\n"
    "Upload here -- takes 2 minutes: https://bit.ly/mec-dl-form\n\n"
    "Reply YES if you have your MEC card ready to upload, or NO if you still need "
    "to complete your DOT physical.\n\n"
    "Kai\nPEAKrecruiting\nQuestions? (470) 470-4766"
)

TEMPLATE_37_BODY = (
    "[FIRST], checking in on your file -- things are moving on my end. Your drug "
    "screen is complete and I am almost there.\n\n"
    "The last thing I need is your Medical Examiner Certificate (MEC) from your "
    "DOT physical along with a photo of your Driver License to finalize everything.\n\n"
    "Upload here -- takes 2 minutes: https://bit.ly/mec-dl-form\n\n"
    "Please reply YES or NO to confirm you have your MEC card ready. If yes, submit "
    "using that link. If no, let me know how soon you can get your DOT physical done.\n\n"
    "Kai\nPEAKrecruiting\nQuestions? (470) 470-4766"
)

# -- Supabase helpers ---------------------------------------------------------
SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}


def enforce_blackout(dt):
    """Push any send time outside 7:30AM-7:30PM ET to next 7:30AM ET window."""
    try:
        import pytz
    except ImportError:
        return dt
    ET = pytz.timezone('America/New_York')
    if dt.tzinfo is None:
        import pytz as _tz
        dt = _tz.utc.localize(dt)
    dt_et = dt.astimezone(ET)
    hour, minute = dt_et.hour, dt_et.minute
    # Blackout: before 7:30AM or at/after 7:30PM
    in_blackout = (hour < 7) or (hour == 7 and minute < 30) or (hour > 19) or (hour == 19 and minute >= 30)
    if in_blackout:
        delivery = dt_et.replace(hour=7, minute=30, second=0, microsecond=0)
        if (hour > 19) or (hour == 19 and minute >= 30):
            delivery = delivery + timedelta(days=1)
        return delivery.astimezone(pytz.utc).replace(tzinfo=None)
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


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


# -- Template routing ---------------------------------------------------------
BG_PROACTIVE = ('In Progress', 'Consider', 'Needs Further Review')


def select_template(drug_status, bg_status):
    """Return (template_id, template_name, template_body) based on drug/BG combo."""
    if drug_status == 'Pass' and bg_status == 'Eligible':
        return (15, 'MEC Outreach -- Drug Pass, BG Eligible (Urgency)', TEMPLATE_15_BODY)
    if drug_status == 'Pass' and bg_status in BG_PROACTIVE:
        return (46, 'MEC Outreach -- Drug Pass, BG In Progress (Proactive)', TEMPLATE_46_BODY)
    if drug_status == 'Pass':
        return (46, 'MEC Outreach -- Drug Pass, BG Other (Proactive)', TEMPLATE_46_BODY)
    if drug_status == 'In Progress' and bg_status == 'Eligible':
        return (15, 'MEC Outreach -- Drug In Progress, BG Eligible (Urgency)', TEMPLATE_15_BODY)
    if drug_status == 'In Progress':
        return (37, 'MEC Outreach -- Drug In Progress, BG Active (Warm)', TEMPLATE_37_BODY)
    return None


# -- Main ---------------------------------------------------------------------
def run_mec_outreach(dry_run=False, client_filter=None, limit=None):
    print(f'[MEC Outreach] {"DRY RUN -- " if dry_run else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')
    if client_filter:
        print(f'[MEC Outreach] Client filter: {client_filter}')
    if limit:
        print(f'[MEC Outreach] Limit: {limit}')

    # Query candidates needing MEC outreach via direct SQL
    sql = """
        SELECT id, first_name, last_name, client_id, phone, drug_test_status, background_status
        FROM candidates
        WHERE (
            (drug_test_status = 'Pass' AND background_status IN ('Eligible','In Progress','Consider','Needs Further Review'))
            OR
            (drug_test_status = 'In Progress' AND background_status IN ('In Progress','Consider','Eligible'))
        )
        AND mec_dl_outreach_sent_at IS NULL
        AND (mec_dl_collection_stage IS NULL OR mec_dl_collection_stage NOT IN ('RECEIVED','SUBMITTED'))
        AND status NOT IN ('Rejected','Hired','Transferred')
        AND phone IS NOT NULL AND phone != '0000000000'
    """
    if client_filter:
        sql += f" AND client_id = '{client_filter}'"
    sql += " ORDER BY id ASC"
    if limit:
        sql += f" LIMIT {limit}"

    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    candidates = [dict(r._mapping) for r in rows]

    print(f'[MEC Outreach] {len(candidates)} candidate(s) eligible.')

    if not candidates:
        print('[MEC Outreach] Nothing to send. Exiting.')
        return

    count = 0
    skipped = 0
    now = datetime.now(timezone.utc).isoformat()

    for c in candidates:
        if limit and count >= limit:
            print(f'[MEC Outreach] Limit reached ({limit}). Stopping.')
            break

        cid = c['id']
        first = c.get('first_name', '')
        last = c.get('last_name', '')
        client = c.get('client_id', 'unknown')
        drug = c.get('drug_test_status', '')
        bg = c.get('background_status', '')
        phone = format_phone(c.get('phone', ''))

        if not phone:
            skipped += 1
            continue

        template = select_template(drug, bg)
        if not template:
            print(f'  [{cid}] {first} {last} -- skipping, no template match (drug={drug} bg={bg})')
            skipped += 1
            continue

        tpl_id, tpl_name, tpl_body = template
        body = tpl_body.replace('[FIRST]', first)

        print(f'  [{cid}] {first} {last} ({client}) drug={drug} bg={bg} -> T{tpl_id} -> {phone}')

        if dry_run:
            print(f'         [DRY RUN] Would queue T{tpl_id} -- skipping')
            count += 1
            continue

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
                'scheduled_for': enforce_blackout(now).isoformat() if hasattr(now, 'isoformat') else enforce_blackout(datetime.fromisoformat(now)).isoformat(),
                'created_by': CREATED_BY
            })
        except Exception as e:
            print(f'         FAILED to queue SMS: {e}')
            continue

        # Update candidate
        try:
            sb_patch('candidates', {'id': f'eq.{cid}'}, {
                'mec_dl_outreach_sent_at': now,
                'mec_dl_collection_stage': 'OUTREACH_SENT',
                'updated_at': now
            })
        except Exception as e:
            print(f'         FAILED to update candidate: {e}')

        print(f'         [MEC Outreach] Queued T{tpl_id} for {first} {last} ({client}) drug={drug} bg={bg}')
        count += 1

    print(f'\n[MEC Outreach] Done. {count} queued, {skipped} skipped.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MEC outreach trigger')
    parser.add_argument('--dry-run', action='store_true', help='Preview without sending')
    parser.add_argument('--client', type=str, default=None, help='Filter to single client_id')
    parser.add_argument('--limit', type=int, default=None, help='Cap number of sends')
    args = parser.parse_args()
    run_mec_outreach(dry_run=args.dry_run, client_filter=args.client, limit=args.limit)
