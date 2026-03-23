"""
PEAK Recruiting — MEC/DL Outreach Trigger v3
Runs on forge-cloud (Fly.io). Schedule: every 30 minutes.

TRIGGER RULES (locked 2026-03-23):

MEC OUTREACH — fire when:
  drug_test_status IN ('Pass', 'In Progress')
  AND background_status NOT IN ('Not Started', 'Ineligible')
  AND mec_dl_collection_stage IS NULL
  AND status NOT IN ('Rejected', 'Hired', 'Transferred')

RE-ENGAGEMENT — fire when:
  drug_test_status IN ('Expired', 'No Show')
  AND status NOT IN ('Rejected', 'Hired', 'Transferred')
  AND mec_dl_collection_stage IS NULL

EXCLUDED:
  drug_test_status = 'Not Started' or NULL
  background_status = 'Ineligible'
  BG Not Started + no background_id (ops gap — flagged to Ops)

TEMPLATE ROUTING (MEC):
  drug=Pass or In Progress, BG=Eligible           → Template 15
  drug=In Progress, BG=In Progress/NFR/Consider   → Template 37
  drug=Pass, BG=In Progress                       → Template 46

TEMPLATE ROUTING (Re-engagement):
  drug=Expired    → Template 38
  drug=No Show    → Template 50

SMS PLATFORM: RC only. All sends queue to sms_send_queue with migration_status=rc_active.
Never use twilio_send directly. Locked 2026-03-23.
"""

import os
import requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
RC_FROM      = '4708574325'

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

MEC_TRIGGER_STATUSES    = ('Pass', 'In Progress')
REENGAGE_STATUSES       = ('Expired', 'No Show')
EXCLUDE_BG_STATUSES     = ('Not Started', 'Ineligible')
SKIP_CANDIDATE_STATUSES = ('Rejected', 'Hired', 'Transferred')


def fetch_candidates():
    url = (
        f"{SUPABASE_URL}/rest/v1/candidates"
        f"?select=id,first_name,last_name,phone,client_id,drug_test_status,background_status,background_id"
        f"&mec_dl_collection_stage=is.null"
        f"&status=not.in.(Rejected,Hired,Transferred)"
        f"&drug_test_status=not.in.(Not Started)"
        f"&drug_test_status=not.is.null"
        f"&phone=not.is.null"
    )
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def get_template(template_id):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/message_templates?id=eq.{template_id}&select=body",
        headers=HEADERS
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


def queue_sms(candidate_id, phone, template_id, first_name):
    """Queue SMS to sms_send_queue with rc_active. Never call Twilio directly."""
    body = get_template(template_id)
    if not body:
        print(f'    ERROR: template {template_id} not found')
        return False

    body = body.replace('[FIRST]', first_name).replace('[FIRST_NAME]', first_name)
    now  = datetime.now(timezone.utc).isoformat()

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue",
        headers=HEADERS,
        json={
            'candidate_id':   candidate_id,
            'to_number':      str(phone),
            'from_number':    RC_FROM,
            'body':           body,
            'template_id':    template_id,
            'template_name':  f'template_{template_id}',
            'status':         'pending',
            'scheduled_for':  now,
            'created_by':     'mec_dl_trigger',
            'migration_status': 'rc_active'
        }
    )
    return r.status_code in (200, 201)


def stamp_outreach(candidate_id, template_id, stage):
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        'mec_dl_collection_stage': stage,
        'mec_dl_outreach_sent_at': now,
        'mec_dl_template_sent':    template_id
    }
    if stage == 'OUTREACH_SENT':
        payload['mec_form_sent_at'] = now
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/candidates?id=eq.{candidate_id}",
        headers=HEADERS, json=payload
    )


def flag_ops_gap(candidate_id, first, last, client_id):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/forge_memory",
        headers=HEADERS,
        json={
            'session_date': datetime.now(timezone.utc).date().isoformat(),
            'category':     'ops_note',
            'subject':      f'BG not ordered — {first} {last}',
            'content':      (
                f'{first} {last} (id {candidate_id}, {client_id}) has drug screen activity '
                f'but background_id is null/empty — BG never ordered in FADV. '
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
    print(f"[{datetime.now()}] MEC/DL trigger v3 — RC queue only")

    candidates = fetch_candidates()
    print(f"Found {len(candidates)} candidates to evaluate")

    mec_queued    = []
    reengage_queued = []
    ops_gaps      = []
    failed        = []

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

        # Re-engagement path
        if drug in REENGAGE_STATUSES:
            template_id = 38 if drug == 'Expired' else 50
            print(f"  REENGAGE {cid} {first} {last} drug={drug} → T{template_id}")
            ok = queue_sms(cid, phone, template_id, first)
            if ok:
                stamp_outreach(cid, template_id, 'REENGAGEMENT_SENT')
                reengage_queued.append(f"{first} {last} (id {cid}, T{template_id})")
            else:
                failed.append(f"{first} {last} (id {cid})")
            continue

        if bg == 'Ineligible':
            continue

        # Ops gap — drug ran, BG never ordered
        if drug in MEC_TRIGGER_STATUSES and bg == 'Not Started' and not bg_id:
            flag_ops_gap(cid, first, last, client)
            ops_gaps.append(f"{first} {last} (id {cid}, {client})")
            continue

        # MEC outreach path
        if drug in MEC_TRIGGER_STATUSES and bg not in EXCLUDE_BG_STATUSES:
            template_id = select_mec_template(drug, bg)
            print(f"  MEC {cid} {first} {last} drug={drug} bg={bg} → T{template_id}")
            ok = queue_sms(cid, phone, template_id, first)
            if ok:
                stamp_outreach(cid, template_id, 'OUTREACH_SENT')
                mec_queued.append(f"{first} {last} (id {cid}, T{template_id})")
            else:
                failed.append(f"{first} {last} (id {cid})")

    summary = (
        f"MEC/DL trigger v3 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} via RC. "
        f"MEC queued: {len(mec_queued)}. Re-engage queued: {len(reengage_queued)}. "
        f"Ops gaps: {len(ops_gaps)}. Failed: {len(failed)}.\n"
        f"MEC: {', '.join(mec_queued) or 'none'}.\n"
        f"Re-engage: {', '.join(reengage_queued) or 'none'}.\n"
        f"Ops gaps: {', '.join(ops_gaps) or 'none'}.\n"
        f"Failed: {', '.join(failed) or 'none'}."
    )
    log_run(summary)
    print(f"\n{summary}")


if __name__ == '__main__':
    run()
