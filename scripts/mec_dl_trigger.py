"""
PEAK Recruiting — MEC/DL Outreach Trigger
Runs on forge-cloud (Fly.io). Schedule: every 30 minutes.

TRIGGER RULE (locked 2026-03-23):
  Fire MEC/DL outreach when:
    drug_test_status NOT IN ('Not Started', NULL)
    AND mec_dl_collection_stage IS NULL
    AND status NOT IN ('Rejected', 'Hired', 'Transferred')

TEMPLATE ROUTING:
  drug=In Progress, BG=Eligible           → Template 15
  drug=In Progress, BG=anything else      → Template 37
  drug=Pass,        BG=Eligible           → Template 15
  drug=Pass,        BG=In Progress        → Template 46
  drug=Pass,        BG=anything else      → Template 15

GUARD: Never fire if mec_dl_collection_stage is already set.
"""

import os
import json
import requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
FORGE_BRIDGE  = 'https://eyopvsmsvbgfuffscfom.supabase.co/functions/v1/forge-bridge'
FORGE_KEY     = os.environ.get('FORGE_KEY', 'peak-forge-2026')

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}


def get_candidates_needing_mec_outreach():
    """Fetch all candidates who need MEC/DL outreach."""
    url = (
        f"{SUPABASE_URL}/rest/v1/candidates"
        f"?select=id,first_name,last_name,phone,client_id,drug_test_status,background_status"
        f"&mec_dl_collection_stage=is.null"
        f"&status=not.in.(Rejected,Hired,Transferred)"
        f"&drug_test_status=not.in.(Not Started)"
        f"&drug_test_status=not.is.null"
        f"&phone=not.is.null"
    )
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def select_template(drug_status, bg_status):
    """Return template ID based on locked routing rules."""
    drug = (drug_status or '').strip()
    bg   = (bg_status   or '').strip()

    if drug == 'Pass' and bg == 'In Progress':
        return 46
    if drug in ('Pass', 'In Progress') and bg == 'Eligible':
        return 15
    if drug == 'In Progress':
        return 37
    if drug == 'Pass':
        return 15
    # Expired / Needs Further Review / other active states
    return 37


def get_template_body(template_id):
    """Fetch template body from Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/message_templates?id=eq.{template_id}&select=body"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    results = r.json()
    return results[0]['body'] if results else None


def send_sms(candidate_id, phone, template_id, first_name):
    """Queue SMS via forge-bridge twilio_send command."""
    r = requests.post(
        FORGE_BRIDGE,
        headers={'x-api-key': FORGE_KEY, 'Content-Type': 'application/json'},
        json={
            'command': 'twilio_send',
            'args': {
                'candidate_id': candidate_id,
                'phone': phone,
                'template_id': template_id,
                'replacements': {'[FIRST]': first_name, '[FIRST_NAME]': first_name}
            }
        }
    )
    return r.status_code == 200


def stamp_outreach_sent(candidate_id, template_id):
    """Update candidate record to mark outreach sent."""
    now = datetime.now(timezone.utc).isoformat()
    url = f"{SUPABASE_URL}/rest/v1/candidates?id=eq.{candidate_id}"
    payload = {
        'mec_dl_collection_stage':  'OUTREACH_SENT',
        'mec_dl_outreach_sent_at':  now,
        'mec_form_sent_at':         now,
        'mec_dl_template_sent':     template_id
    }
    r = requests.patch(url, headers=HEADERS, json=payload)
    return r.status_code in (200, 204)


def log_to_forge_memory(subject, content):
    """Log run summary to forge_memory."""
    url = f"{SUPABASE_URL}/rest/v1/forge_memory"
    requests.post(url, headers=HEADERS, json={
        'session_date': datetime.now(timezone.utc).date().isoformat(),
        'category': 'ops_note',
        'subject': subject,
        'content': content,
        'target_thread': 'PEAK Ops'
    })


def run():
    print(f"[{datetime.now()}] MEC/DL trigger running...")

    candidates = get_candidates_needing_mec_outreach()
    print(f"Found {len(candidates)} candidates needing MEC/DL outreach")

    if not candidates:
        print("Nothing to send.")
        return

    sent = []
    failed = []

    for c in candidates:
        cid        = c['id']
        first      = c.get('first_name', '')
        last       = c.get('last_name', '')
        phone      = str(c.get('phone', '') or '')
        drug       = c.get('drug_test_status', '')
        bg         = c.get('background_status', '')
        client     = c.get('client_id', '')

        if not phone or len(phone) < 10:
            print(f"  SKIP {cid} {first} {last} — no valid phone")
            continue

        template_id = select_template(drug, bg)
        print(f"  {cid} {first} {last} ({client}) drug={drug} bg={bg} → Template {template_id}")

        ok = send_sms(cid, phone, template_id, first)
        if ok:
            stamp_outreach_sent(cid, template_id)
            sent.append(f"{first} {last} (id {cid}, {client}, T{template_id})")
            print(f"    SENT ✓")
        else:
            failed.append(f"{first} {last} (id {cid})")
            print(f"    FAILED ✗")

    summary = (
        f"MEC/DL trigger run {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}. "
        f"Sent: {len(sent)}. Failed: {len(failed)}.\n"
        f"Sent: {', '.join(sent) or 'none'}.\n"
        f"Failed: {', '.join(failed) or 'none'}."
    )
    log_to_forge_memory('MEC/DL outreach trigger run', summary)
    print(f"\nDone. {summary}")


if __name__ == '__main__':
    run()
