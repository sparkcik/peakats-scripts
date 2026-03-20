#!/usr/bin/env python3
"""
rc_inbox_cron.py
Runs on forge-local (Fly.io) every 30 minutes.
Polls RC inbox, matches candidates, writes to candidate_comms + sms_triage_queue.
Platform-agnostic — swap read function when Twilio replaces RC.
"""

import sys
import json
import requests
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
RC_CLIENT_ID     = '1QDQiRjk50kfxvIVYTT3IA'
RC_CLIENT_SECRET = 'aTMprgZe1Safik4e4qDBnHaKcnA6o9gb3cafm1xQtJxo'
RC_JWT           = 'eyJraWQiOiI4NzYyZjU5OGQwNTk0NGRiODZiZjVjYTk3ODA0NzYwOCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJhdWQiOiJodHRwczovL3BsYXRmb3JtLnJpbmdjZW50cmFsLmNvbS9yZXN0YXBpL29hdXRoL3Rva2VuIiwic3ViIjoiNTM2NDg0MDMwIiwiaXNzIjoiaHR0cHM6Ly9wbGF0Zm9ybS5yaW5nY2VudHJhbC5jb20iLCJleHAiOjM5MjEyMTIzNTksImlhdCI6MTc3MzcyODcxMiwianRpIjoib3IxQTdEUTFULUdLTU5MY0lPSDRzQSJ9.Jwz40n4cSYp5Ke7j3jGJSeY1g-nPPsUbXS8PMw_gmKRGVTp22O4BzCMxSZIFkAde_FOVEOjtqrHCwYha7fj04WPDPI528z1fyTbavivHTb5pYRlQRDVboeL-3GBftOdS4EFt1cDWhA-qDfUO_9ClpxbnBUbWZbWnSNE4oLoZyf8TeC86GvHvftQTljTFzYlKNNA7wHhNAGCygDVMq6NVDIacXB81XADVpJ2DPtRI58M5CvJphnmqzeoYsIVNaQC8C5n-GyQxSGGXleIO6VVxeQ4LMraUWu_JS52Lhswu-Fb8otWft8ephnWDybhaRcjiCkG1uXQX1yOkOWMYrmTKiA'
RC_SERVER        = 'https://platform.ringcentral.com'

SUPABASE_URL = 'https://eyopvsmsvbgfuffscfom.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4'

SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

DRY_RUN = '--dry-run' in sys.argv

# ── Message categorizer ───────────────────────────────────────────────────────
def categorize(body):
    b = (body or '').lower()
    if any(x in b for x in ['gcic', 'signed', 'form', 'document', 'background form', 'authorization']):
        return 'GCIC_REPLY'
    if any(x in b for x in ['background', 'eligible', 'approved', 'check', 'status', 'application']):
        return 'BG_QUESTION'
    if any(x in b for x in ['start', 'available', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'week', 'schedule']):
        return 'AVAILABILITY'
    if any(x in b for x in ['refer', 'friend', 'know someone', 'driver', 'looking for work']):
        return 'REFERRAL'
    if any(x in b for x in ['mec', 'medical', 'license', 'dl ', 'card']):
        return 'MEC_DL'
    return 'UNKNOWN'

def priority_for(category, candidate):
    if category == 'GCIC_REPLY': return 'urgent'
    if category == 'AVAILABILITY': return 'urgent'
    if candidate and candidate.get('background_status') == 'Eligible': return 'urgent'
    if category == 'REFERRAL': return 'normal'
    return 'normal'

# ── RC Auth ───────────────────────────────────────────────────────────────────
def get_rc_token():
    resp = requests.post(
        f'{RC_SERVER}/restapi/oauth/token',
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer', 'assertion': RC_JWT}
    )
    resp.raise_for_status()
    return resp.json()['access_token']

# ── Read inbound (RC) — swap this function for Twilio when migrated ───────────
def read_inbound_sms(token, since_minutes=35):
    """Pull inbound SMS from last N minutes. Overlap slightly to avoid gaps."""
    date_from = (datetime.now(timezone.utc) - timedelta(minutes=since_minutes)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    headers = {'Authorization': f'Bearer {token}'}
    url = f'{RC_SERVER}/restapi/v1.0/account/~/extension/~/message-store'
    params = {'messageType': 'SMS', 'direction': 'Inbound', 'dateFrom': date_from, 'perPage': 100}
    all_records = []
    page = 1
    while url:
        resp = requests.get(url, headers=headers, params=params if page == 1 else {})
        resp.raise_for_status()
        data = resp.json()
        all_records.extend(data.get('records', []))
        nav = data.get('navigation', {})
        url = nav.get('nextPage', {}).get('uri') if nav.get('nextPage') else None
        page += 1
    return all_records

# ── Candidate match ───────────────────────────────────────────────────────────
def match_candidate(phone):
    clean = phone.replace('+1','').replace('-','').replace('(','').replace(')','').replace(' ','')
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/candidates',
        headers=SB_HEADERS,
        params={'phone': f'eq.{clean}', 'select': 'id,first_name,last_name,client_id,status,background_status,gcic_status,gcic_stage'}
    )
    results = resp.json()
    return results[0] if results else None

# ── Already logged check ──────────────────────────────────────────────────────
def already_logged(rc_message_id):
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/candidate_comms',
        headers=SB_HEADERS,
        params={'external_message_id': f'eq.{rc_message_id}', 'select': 'id', 'limit': '1'}
    )
    return len(resp.json()) > 0

# ── Write to candidate_comms ──────────────────────────────────────────────────
def log_to_comms(msg, candidate, category):
    from_number = msg.get('from', {}).get('phoneNumber', '').replace('+1', '')
    body = msg.get('subject', '')
    received_at = msg.get('creationTime', '')
    rc_id = str(msg.get('id', ''))

    payload = {
        'candidate_id':        candidate['id'] if candidate else None,
        'client_id':           candidate.get('client_id') if candidate else None,
        'channel':             'sms',
        'direction':           'inbound',
        'body':                body,
        'sent_at':             received_at,
        'send_mode':           'automated',
        'sent_by':             'rc_inbox_cron',
        'from_number':         from_number,
        'to_number':           '4708574325',
        'external_message_id': rc_id,
        'response_received':   True,
        'response_body':       body,
        'delivery_status':     'delivered'
    }

    requests.post(
        f'{SUPABASE_URL}/rest/v1/candidate_comms',
        headers={**SB_HEADERS, 'Prefer': 'return=minimal'},
        json=payload
    )

# ── Write to sms_triage_queue ─────────────────────────────────────────────────
def log_to_triage(msg, candidate, category):
    from_number = msg.get('from', {}).get('phoneNumber', '').replace('+1', '')
    body = msg.get('subject', '')
    received_at = msg.get('creationTime', '')
    rc_id = str(msg.get('id', ''))

    payload = {
        'candidate_id': candidate['id'] if candidate else None,
        'from_number':  from_number,
        'body':         body,
        'received_at':  received_at,
        'rc_message_id': rc_id,
        'category':     category,
        'needs_reply':  True,
        'priority':     priority_for(category, candidate)
    }

    requests.post(
        f'{SUPABASE_URL}/rest/v1/sms_triage_queue',
        headers={**SB_HEADERS, 'Prefer': 'return=minimal'},
        json=payload
    )

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f'[RC Cron] {"DRY RUN — " if DRY_RUN else ""}Starting at {now}')

    token = get_rc_token()
    messages = read_inbound_sms(token, since_minutes=35)
    print(f'[RC Cron] {len(messages)} inbound messages in last 35 min')

    new = 0
    skipped = 0

    for msg in messages:
        rc_id = str(msg.get('id', ''))

        # Skip if already logged
        if already_logged(rc_id):
            skipped += 1
            continue

        from_number = msg.get('from', {}).get('phoneNumber', '').replace('+1', '')
        body = msg.get('subject', '')
        candidate = match_candidate(from_number)
        category = categorize(body)

        if DRY_RUN:
            name = f"{candidate['first_name']} {candidate['last_name']}" if candidate else 'NO MATCH'
            print(f'  [DRY] {from_number} | {name} | {category} | {body[:60]}')
            new += 1
            continue

        log_to_comms(msg, candidate, category)
        log_to_triage(msg, candidate, category)
        new += 1

    print(f'[RC Cron] Done. New: {new} | Already logged: {skipped}')

if __name__ == '__main__':
    main()
