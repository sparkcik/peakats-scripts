"""
PEAK Recruiting — MEC/DL FUP Scheduler
Runs on forge-cloud (Fly.io). Schedule: every 6 hours.

FUP CADENCE — matches GCIC cadence exactly (locked 2026-03-24):
  Day 0: Initial outreach (mec_dl_trigger.py handles this)
  Day 1: Template 16 (FUP 1)   — stamp mec_dl_fup1_sent_at
  Day 2: Template 17 (FUP 2)   — stamp mec_dl_fup2_sent_at
  Day 3: Template 18 (Escalation) — stamp mec_dl_escalated_at + log to forge_memory for Charles

GUARDS:
  - Only fires if mec_dl_collection_stage = OUTREACH_SENT
  - Never fires if stage = COMPLETE or REENGAGEMENT_SENT
  - Each tier checks its own stamp before firing — never double-fires
  - Skips invalid phones

SMS PLATFORM: RC only. Queues to sms_send_queue with migration_status=rc_active.
"""

import os
import requests
from datetime import datetime, timedelta, timezone

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
FROM_NUMBER  = '+14704704766'  # Twilio -- RC_FROM retired

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

FUP_CADENCE = [
    {'days': 1, 'template_id': 16, 'stamp_col': 'mec_dl_fup1_sent_at', 'label': 'FUP 1'},
    {'days': 2, 'template_id': 17, 'stamp_col': 'mec_dl_fup2_sent_at', 'label': 'FUP 2'},
    {'days': 3, 'template_id': 18, 'stamp_col': 'mec_dl_escalated_at', 'label': 'Escalation'},
]


def enforce_blackout(dt):
    """Push any send time outside 7:30AM-9PM ET to next 7:30AM ET window."""
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
    in_blackout = (hour < 7) or (hour == 7 and minute < 30) or (hour >= 21)
    if in_blackout:
        delivery = dt_et.replace(hour=7, minute=30, second=0, microsecond=0)
        if (hour > 19) or (hour == 19 and minute >= 30):
            delivery = delivery + timedelta(days=1)
        return delivery.astimezone(pytz.utc).replace(tzinfo=None)
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def fetch_candidates():
    url = (
        f"{SUPABASE_URL}/rest/v1/candidates"
        f"?select=id,first_name,last_name,phone,client_id,"
        f"mec_dl_collection_stage,mec_dl_outreach_sent_at,"
        f"mec_dl_fup1_sent_at,mec_dl_fup2_sent_at,mec_dl_escalated_at"
        f"&mec_dl_collection_stage=eq.OUTREACH_SENT"
        f"&mec_uploaded=neq.1"
        f"&status=not.in.(Rejected,Hired,Transferred)"
        f"&mec_dl_outreach_sent_at=not.is.null"
        f"&phone=not.is.null"
    )
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def get_template_body(template_id, first_name):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/message_templates?id=eq.{template_id}&select=body",
        headers=HEADERS
    )
    r.raise_for_status()
    results = r.json()
    if not results:
        return None
    body = results[0]['body']
    return body.replace('[FIRST]', first_name).replace('[FIRST_NAME]', first_name)


def queue_sms(candidate_id, phone, template_id, first_name):
    body = get_template_body(template_id, first_name)
    if not body:
        print(f'    ERROR: template {template_id} not found')
        return False
    now = datetime.now(timezone.utc).isoformat()
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue",
        headers=HEADERS,
        json={
            'candidate_id':     candidate_id,
            'to_number':        str(phone),
            'from_number':      FROM_NUMBER,
            'body':             body,
            'template_id':      template_id,
            'template_name':    f'MEC FUP T{template_id}',
            'status':           'pending',
            'channel':          'twilio',
            'scheduled_for':    enforce_blackout(now if hasattr(now, 'hour') else datetime.fromisoformat(now.replace('Z',''))).isoformat(),
            'created_by':       'mec_dl_fup_scheduler',
            'migration_status': 'rc_active'
        }
    )
    return r.status_code in (200, 201)


def stamp_candidate(candidate_id, stamp_col):
    now = datetime.now(timezone.utc).isoformat()
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/candidates?id=eq.{candidate_id}",
        headers=HEADERS,
        json={stamp_col: now}
    )


def escalate_to_charles(candidate_id, first_name, last_name, client_id, phone):
    """Log escalation to forge_memory for Charles to action manually."""
    requests.post(
        f"{SUPABASE_URL}/rest/v1/forge_memory",
        headers=HEADERS,
        json={
            'session_date':  datetime.now(timezone.utc).date().isoformat(),
            'category':      'ops_note',
            'subject':       f'MEC/DL escalation — {first_name} {last_name} ({client_id})',
            'content':       (
                f'{first_name} {last_name} (id {candidate_id}, {client_id}, {phone}) '
                f'has not submitted MEC/DL after 3 days of outreach. '
                f'Final FUP (Template 18) sent today. Charles to manage manually. '
                f'Consider: manual call, status change, or reject.'
            ),
            'target_thread': 'PEAK Ops'
        }
    )


def days_since(ts_str):
    if not ts_str:
        return 0
    ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    return (datetime.now(timezone.utc) - ts).total_seconds() / 86400


def fire_queue():
    try:
        requests.post(
            'https://eyopvsmsvbgfuffscfom.supabase.co/functions/v1/forge-bridge',
            headers={'x-api-key': 'peak-forge-2026', 'Content-Type': 'application/json'},
            json={'command': 'sms_queue', 'args': {}},
            timeout=60
        )
        print('  Queue fired via forge-bridge')
    except Exception as ex:
        print(f'  Queue fire error: {ex}')


def log_run(summary):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/forge_memory",
        headers=HEADERS,
        json={
            'session_date':  datetime.now(timezone.utc).date().isoformat(),
            'category':      'ops_note',
            'subject':       'MEC/DL FUP scheduler run',
            'content':       summary,
            'target_thread': 'PEAK Ops'
        }
    )


def run():
    print(f"[{datetime.now()}] MEC/DL FUP scheduler — Day 1/2/3 cadence")

    candidates = fetch_candidates()
    print(f"Found {len(candidates)} at OUTREACH_SENT")

    sent    = []
    skipped = 0
    failed  = []

    for c in candidates:
        cid       = c['id']
        first     = (c.get('first_name') or '').strip().title()
        last      = (c.get('last_name')  or '').strip()
        phone     = str(c.get('phone') or '').strip()
        client_id = c.get('client_id', '')
        outreach_at = c.get('mec_dl_outreach_sent_at')

        if not phone or len(phone) < 10 or phone == '0000000000':
            skipped += 1
            continue

        days_out = days_since(outreach_at)

        # Find the highest applicable FUP that hasn't been sent yet
        fup_to_send = None
        for fup in reversed(FUP_CADENCE):
            already_sent = c.get(fup['stamp_col'])
            if already_sent:
                continue
            if days_out >= fup['days']:
                fup_to_send = fup
                break

        if not fup_to_send:
            skipped += 1
            continue

        print(f"  {cid} {first} {last} — day {days_out:.1f} → {fup_to_send['label']} (T{fup_to_send['template_id']})")

        ok = queue_sms(cid, phone, fup_to_send['template_id'], first)
        if ok:
            stamp_candidate(cid, fup_to_send['stamp_col'])
            # Escalation — log to Ops for Charles
            if fup_to_send['label'] == 'Escalation':
                escalate_to_charles(cid, first, last, client_id, phone)
            sent.append(f"{first} {last} (id {cid}, {fup_to_send['label']})")
        else:
            failed.append(f"{first} {last} (id {cid})")

    if sent:
        fire_queue()

    summary = (
        f"MEC/DL FUP scheduler {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}. "
        f"Sent: {len(sent)}. Skipped: {skipped}. Failed: {len(failed)}.\n"
        f"Sent: {', '.join(sent) or 'none'}.\n"
        f"Failed: {', '.join(failed) or 'none'}."
    )
    log_run(summary)
    print(f"\n{summary}")


if __name__ == '__main__':
    run()
