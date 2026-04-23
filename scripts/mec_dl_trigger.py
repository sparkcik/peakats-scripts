"""
PEAK Recruiting -- MEC/DL Outreach Trigger v4
Runs on forge-cloud (Fly.io). Schedule: every 30 minutes.

TRIGGER RULES (locked 2026-03-23):

MEC OUTREACH -- fire when:
  drug_test_status IN ('Pass', 'In Progress')
  AND background_status NOT IN ('Not Started', 'Ineligible')
  AND mec_dl_collection_stage IS NULL
  AND status NOT IN ('Rejected', 'Hired', 'Transferred', 'Expired')
  AND compliance_override IS NOT TRUE

RE-ENGAGEMENT -- fire when:
  drug_test_status IN ('Expired', 'No Show')
  AND status NOT IN ('Rejected', 'Hired', 'Transferred', 'Expired')
  AND mec_dl_collection_stage IS NULL
  AND compliance_override IS NOT TRUE

EXCLUDED:
  drug_test_status = 'Not Started' or NULL
  background_status = 'Ineligible'
  BG Not Started + no background_id (ops gap -- flagged to Ops)
  compliance_override = TRUE

TEMPLATE ROUTING (MEC):
  drug=Pass or In Progress, BG=Eligible           -> Template 15
  drug=In Progress, BG=In Progress/NFR/Consider   -> Template 37
  drug=Pass, BG=In Progress                       -> Template 46

TEMPLATE ROUTING (Re-engagement):
  drug=Expired    -> Template 38
  drug=No Show    -> Template 50

DEDUP GUARD (v4):
  Before queuing any SMS, check sms_send_queue for any pending/sent
  message to this candidate_id in the last 24 hours. If found, skip.
  This prevents rapid-fire when stamp_outreach fails.

SMS PLATFORM: Twilio only. migration_status=twilio_active. Locked 2026-04-15.
"""

import os
import requests
from datetime import datetime, timezone, timedelta
import pytz

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
FROM_NUMBER  = '+14704704766'

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

HEADERS_REPR = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

MEC_TRIGGER_STATUSES    = ('Pass', 'In Progress')
REENGAGE_STATUSES       = ('Expired', 'No Show')
EXCLUDE_BG_STATUSES     = ('Not Started', 'Ineligible')
SKIP_CANDIDATE_STATUSES = ('Rejected', 'Hired', 'Transferred')

DEDUP_WINDOW_HOURS = 24


def enforce_blackout(dt):
    """Push any send time outside 7:30AM-9PM ET to next 7:30AM ET window."""
    ET = pytz.timezone('America/New_York')
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
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
        f"?select=id,first_name,last_name,phone,client_id,drug_test_status,background_status,background_id"
        f"&mec_dl_collection_stage=is.null"
        f"&status=not.in.(Rejected,Hired,Transferred)"
        f"&drug_test_status=not.in.(Not Started)"
        f"&drug_test_status=not.is.null"
        f"&phone=not.is.null"
        f"&compliance_override=neq.true"
    )
    r = requests.get(url, headers=HEADERS_REPR)
    r.raise_for_status()
    return r.json()


def get_template(template_id):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/message_templates?id=eq.{template_id}&select=body",
        headers=HEADERS_REPR
    )
    r.raise_for_status()
    results = r.json()
    return results[0]['body'] if results else None


def select_mec_template(drug, bg):
    if drug == 'Pass' and bg == 'In Progress':
        return 46
    if bg == 'Eligible':
        return 15
    if drug == 'In Progress':
        return 37
    return 15


def already_queued(candidate_id):
    """
    DEDUP GUARD: Return True if any MEC-related SMS was already queued
    or sent to this candidate in the last DEDUP_WINDOW_HOURS hours.
    Prevents rapid-fire when stamp_outreach fails silently.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=DEDUP_WINDOW_HOURS)).isoformat()
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue"
        f"?candidate_id=eq.{candidate_id}"
        f"&template_id=in.(15,16,17,18,37,46)"
        f"&status=in.(pending,sent)"
        f"&created_at=gte.{cutoff}"
        f"&limit=1"
        f"&select=id",
        headers=HEADERS_REPR
    )
    try:
        return len(r.json()) > 0
    except Exception:
        return False


def queue_sms(candidate_id, phone, template_id, first_name):
    """Queue SMS via Twilio. Returns True on success."""
    body = get_template(template_id)
    if not body:
        print(f'    ERROR: template {template_id} not found')
        return False

    body = body.replace('[FIRST]', first_name).replace('[FIRST_NAME]', first_name)
    now  = datetime.now(timezone.utc)
    scheduled_for = enforce_blackout(now).isoformat()

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue",
        headers=HEADERS,
        json={
            'candidate_id':     candidate_id,
            'to_number':        str(phone),
            'from_number':      FROM_NUMBER,
            'body':             body,
            'template_id':      template_id,
            'template_name':    f'MEC Outreach T{template_id}',
            'status':           'pending',
            'scheduled_for':    scheduled_for,
            'created_by':       'mec_dl_trigger',
            'migration_status': 'twilio_active'
        }
    )
    if r.status_code not in (200, 201):
        print(f'    ERROR: sms_send_queue insert failed ({r.status_code}): {r.text[:200]}')
        return False
    return True


def stamp_outreach(candidate_id, template_id, stage):
    """
    Stamp mec_dl_collection_stage on candidate.
    CRITICAL: checks response code. If this fails, dedup guard catches next run.
    """
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        'mec_dl_collection_stage': stage,
        'mec_dl_outreach_sent_at': now,
        'mec_dl_template_sent':    template_id
    }
    if stage == 'OUTREACH_SENT':
        payload['mec_form_sent_at'] = now

    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/candidates?id=eq.{candidate_id}",
        headers=HEADERS,
        json=payload
    )
    if r.status_code not in (200, 204):
        print(f'    ERROR: stamp_outreach failed for candidate {candidate_id} ({r.status_code}): {r.text[:200]}')
        return False
    return True


def flag_ops_gap(candidate_id, first, last, client_id):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/forge_memory",
        headers=HEADERS,
        json={
            'session_date':  datetime.now(timezone.utc).date().isoformat(),
            'category':      'ops_note',
            'subject':       f'BG not ordered -- {first} {last}',
            'content':       (
                f'{first} {last} (id {candidate_id}, {client_id}) has drug screen activity '
                f'but background_id is null/empty -- BG never ordered in FADV. '
                f'Action: order BG manually. MEC outreach held.'
            ),
            'target_thread': 'PEAK Ops'
        }
    )


def log_run(summary):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/forge_memory",
        headers=HEADERS,
        json={
            'session_date':  datetime.now(timezone.utc).date().isoformat(),
            'category':      'ops_note',
            'subject':       'MEC/DL outreach trigger run',
            'content':       summary,
            'target_thread': 'PEAK Ops'
        }
    )


def run():
    print(f"[{datetime.now()}] MEC/DL trigger v4 -- Twilio only, dedup guard active")

    candidates = fetch_candidates()
    print(f"Found {len(candidates)} candidates to evaluate")

    mec_queued      = []
    reengage_queued = []
    ops_gaps        = []
    deduped         = []
    failed          = []

    for c in candidates:
        cid    = c['id']
        first  = (c.get('first_name') or '').strip().title()
        last   = (c.get('last_name')  or '').strip()
        phone  = str(c.get('phone') or '').strip()
        drug   = (c.get('drug_test_status')  or '').strip()
        bg     = (c.get('background_status') or '').strip()
        bg_id  = (c.get('background_id')     or '').strip()
        client = c.get('client_id', '')

        if not phone or len(phone) < 10 or phone == '0000000000':
            continue

        # DEDUP GUARD -- check before any send attempt
        if already_queued(cid):
            print(f"  DEDUP {cid} {first} {last} -- already queued in last {DEDUP_WINDOW_HOURS}hrs, skipping")
            deduped.append(f"{first} {last} (id {cid})")
            continue

        # Re-engagement path
        if drug in REENGAGE_STATUSES:
            template_id = 38 if drug == 'Expired' else 50
            print(f"  REENGAGE {cid} {first} {last} drug={drug} -> T{template_id}")
            ok = queue_sms(cid, phone, template_id, first)
            if ok:
                stamp_outreach(cid, template_id, 'REENGAGEMENT_SENT')
                reengage_queued.append(f"{first} {last} (id {cid}, T{template_id})")
            else:
                failed.append(f"{first} {last} (id {cid})")
            continue

        if bg == 'Ineligible':
            continue

        # Ops gap -- drug ran, BG never ordered
        if drug in MEC_TRIGGER_STATUSES and bg == 'Not Started' and not bg_id:
            flag_ops_gap(cid, first, last, client)
            ops_gaps.append(f"{first} {last} (id {cid}, {client})")
            continue

        # MEC outreach path
        if drug in MEC_TRIGGER_STATUSES and bg not in EXCLUDE_BG_STATUSES:
            template_id = select_mec_template(drug, bg)
            print(f"  MEC {cid} {first} {last} drug={drug} bg={bg} -> T{template_id}")
            ok = queue_sms(cid, phone, template_id, first)
            if ok:
                stamp_outreach(cid, template_id, 'OUTREACH_SENT')
                mec_queued.append(f"{first} {last} (id {cid}, T{template_id})")
            else:
                failed.append(f"{first} {last} (id {cid})")

    summary = (
        f"MEC/DL trigger v4 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} via Twilio. "
        f"MEC queued: {len(mec_queued)}. Re-engage: {len(reengage_queued)}. "
        f"Deduped: {len(deduped)}. Ops gaps: {len(ops_gaps)}. Failed: {len(failed)}.\n"
        f"MEC: {', '.join(mec_queued) or 'none'}.\n"
        f"Re-engage: {', '.join(reengage_queued) or 'none'}.\n"
        f"Deduped: {', '.join(deduped) or 'none'}.\n"
        f"Ops gaps: {', '.join(ops_gaps) or 'none'}.\n"
        f"Failed: {', '.join(failed) or 'none'}."
    )
    log_run(summary)
    print(f"\n{summary}")


if __name__ == '__main__':
    run()
