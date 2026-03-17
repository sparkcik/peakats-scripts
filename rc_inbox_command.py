#!/usr/bin/env python3
"""
rc_inbox.py
Forge-local command: read RC inbox for 470-857-4325
Returns structured JSON of inbound messages with candidate matches.
Callable via forge-bridge: {"command": "rc_inbox", "args": ["--limit", "50"]}
"""

import sys
import json
import argparse
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
RC_CLIENT_ID     = '1QDQiRjk50kfxvIVYTT3IA'
RC_CLIENT_SECRET = 'aTMprgZe1Safik4e4qDBnHaKcnA6o9gb3cafm1xQtJxo'
RC_JWT           = 'eyJraWQiOiI4NzYyZjU5OGQwNTk0NGRiODZiZjVjYTk3ODA0NzYwOCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJhdWQiOiJodHRwczovL3BsYXRmb3JtLnJpbmdjZW50cmFsLmNvbS9yZXN0YXBpL29hdXRoL3Rva2VuIiwic3ViIjoiNTM2NDg0MDMwIiwiaXNzIjoiaHR0cHM6Ly9wbGF0Zm9ybS5yaW5nY2VudHJhbC5jb20iLCJleHAiOjM5MjEyMTIzNTksImlhdCI6MTc3MzcyODcxMiwianRpIjoib3IxQTdEUTFULUdLTU5MY0lPSDRzQSJ9.Jwz40n4cSYp5Ke7j3jGJSeY1g-nPPsUbXS8PMw_gmKRGVTp22O4BzCMxSZIFkAde_FOVEOjtqrHCwYha7fj04WPDPI528z1fyTbavivHTb5pYRlQRDVboeL-3GBftOdS4EFt1cDWhA-qDfUO_9ClpxbnBUbWZbWnSNE4oLoZyf8TeC86GvHvftQTljTFzYlKNNA7wHhNAGCygDVMq6NVDIacXB81XADVpJ2DPtRI58M5CvJphnmqzeoYsIVNaQC8C5n-GyQxSGGXleIO6VVxeQ4LMraUWu_JS52Lhswu-Fb8otWft8ephnWDybhaRcjiCkG1uXQX1yOkOWMYrmTKiA'
RC_SERVER        = 'https://platform.ringcentral.com'

SUPABASE_URL     = 'https://eyopvsmsvbgfuffscfom.supabase.co'
SUPABASE_KEY     = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4'

SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}'
}

# ── RC Auth ───────────────────────────────────────────────────────────────────
def get_rc_token():
    resp = requests.post(
        f'{RC_SERVER}/restapi/oauth/token',
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': RC_JWT
        }
    )
    resp.raise_for_status()
    return resp.json()['access_token']

# ── Candidate match ───────────────────────────────────────────────────────────
def match_candidate(phone):
    clean = phone.replace('+1','').replace('-','').replace('(','').replace(')','').replace(' ','')
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/candidates',
        headers=SB_HEADERS,
        params={
            'phone': f'eq.{clean}',
            'select': 'id,first_name,last_name,client_id,status,rwp_score,gcic_status,background_status,drug_test_status'
        }
    )
    results = resp.json()
    return results[0] if results else None

# ── Pull inbox ────────────────────────────────────────────────────────────────
def get_inbox(token, limit=50, unread_only=False):
    params = {
        'messageType': 'SMS',
        'direction': 'Inbound',
        'perPage': limit
    }
    if unread_only:
        params['readStatus'] = 'Unread'

    resp = requests.get(
        f'{RC_SERVER}/restapi/v1.0/account/~/extension/~/message-store',
        headers={'Authorization': f'Bearer {token}'},
        params=params
    )
    resp.raise_for_status()
    return resp.json().get('records', [])

# ── Mark read ─────────────────────────────────────────────────────────────────
def mark_read(token, message_id):
    requests.put(
        f'{RC_SERVER}/restapi/v1.0/account/~/extension/~/message-store/{message_id}',
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        json={'readStatus': 'Read'}
    )

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit',       type=int,  default=50,    help='Max messages to return')
    parser.add_argument('--unread-only', action='store_true',      help='Return only unread messages')
    parser.add_argument('--mark-read',   action='store_true',      help='Mark returned messages as read')
    parser.add_argument('--format',      default='json',           help='Output format: json or text')
    args = parser.parse_args()

    token    = get_rc_token()
    messages = get_inbox(token, limit=args.limit, unread_only=args.unread_only)

    results = []
    for msg in messages:
        from_number = msg.get('from', {}).get('phoneNumber', '').replace('+1', '')
        body        = msg.get('subject', '')
        created     = msg.get('creationTime', '')
        msg_id      = msg.get('id')
        read_status = msg.get('readStatus', 'Read')

        candidate = match_candidate(from_number)

        entry = {
            'message_id':   msg_id,
            'from':         from_number,
            'body':         body,
            'received_at':  created,
            'read_status':  read_status,
            'candidate':    candidate if candidate else None
        }
        results.append(entry)

        if args.mark_read and read_status == 'Unread':
            mark_read(token, msg_id)

    output = {
        'source':      'rc_inbox',
        'kai_number':  '4708574325',
        'count':       len(results),
        'unread_only': args.unread_only,
        'timestamp':   datetime.now(timezone.utc).isoformat(),
        'messages':    results
    }

    if args.format == 'text':
        print(f"RC INBOX — {output['count']} messages | {output['timestamp']}\n")
        for m in results:
            c = m['candidate']
            name = f"{c['first_name']} {c['last_name']} | {c['client_id']} | {c['status']} | RWP:{c['rwp_score']}" if c else 'NO MATCH'
            print(f"[{m['read_status']}] {m['received_at'][:16]} | {m['from']}")
            print(f"  {name}")
            print(f"  {m['body'][:120]}")
            print()
    else:
        print(json.dumps(output, indent=2))

if __name__ == '__main__':
    main()
