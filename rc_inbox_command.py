#!/usr/bin/env python3
"""
rc_inbox_command.py v2
Full paginated RC inbox reader for forge-local.
Loops through ALL pages until exhausted.
Accepts --date-from, --unread-only, --limit, --format, --mark-read flags.
"""

import sys
import json
import argparse
import requests
from datetime import datetime, timezone, timedelta

RC_CLIENT_ID     = '1QDQiRjk50kfxvIVYTT3IA'
RC_CLIENT_SECRET = 'aTMprgZe1Safik4e4qDBnHaKcnA6o9gb3cafm1xQtJxo'
RC_JWT           = 'eyJraWQiOiI4NzYyZjU5OGQwNTk0NGRiODZiZjVjYTk3ODA0NzYwOCIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJhdWQiOiJodHRwczovL3BsYXRmb3JtLnJpbmdjZW50cmFsLmNvbS9yZXN0YXBpL29hdXRoL3Rva2VuIiwic3ViIjoiNTM2NDg0MDMwIiwiaXNzIjoiaHR0cHM6Ly9wbGF0Zm9ybS5yaW5nY2VudHJhbC5jb20iLCJleHAiOjM5MjEyMTIzNTksImlhdCI6MTc3MzcyODcxMiwianRpIjoib3IxQTdEUTFULUdLTU5MY0lPSDRzQSJ9.Jwz40n4cSYp5Ke7j3jGJSeY1g-nPPsUbXS8PMw_gmKRGVTp22O4BzCMxSZIFkAde_FOVEOjtqrHCwYha7fj04WPDPI528z1fyTbavivHTb5pYRlQRDVboeL-3GBftOdS4EFt1cDWhA-qDfUO_9ClpxbnBUbWZbWnSNE4oLoZyf8TeC86GvHvftQTljTFzYlKNNA7wHhNAGCygDVMq6NVDIacXB81XADVpJ2DPtRI58M5CvJphnmqzeoYsIVNaQC8C5n-GyQxSGGXleIO6VVxeQ4LMraUWu_JS52Lhswu-Fb8otWft8ephnWDybhaRcjiCkG1uXQX1yOkOWMYrmTKiA'
RC_SERVER        = 'https://platform.ringcentral.com'

SUPABASE_URL = 'https://eyopvsmsvbgfuffscfom.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4'
SB_HEADERS = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}'}

def get_rc_token():
    resp = requests.post(
        f'{RC_SERVER}/restapi/oauth/token',
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer', 'assertion': RC_JWT}
    )
    resp.raise_for_status()
    return resp.json()['access_token']

def match_candidate(phone):
    clean = phone.replace('+1','').replace('-','').replace('(','').replace(')','').replace(' ','')
    r = requests.get(
        f'{SUPABASE_URL}/rest/v1/candidates',
        headers=SB_HEADERS,
        params={'phone': f'eq.{clean}', 'select': 'id,first_name,last_name,client_id,status,rwp_score,gcic_status,background_status,drug_test_status'}
    )
    results = r.json()
    return results[0] if results else None

def get_all_inbound(token, date_from=None, unread_only=False):
    """
    Pull ALL inbound SMS by paginating through every page.
    RC navigation.nextPage.uri is the pagination mechanism.
    """
    headers = {'Authorization': f'Bearer {token}'}

    if date_from is None:
        date_from = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%dT00:00:00.000Z')

    # Initial params
    params = {
        'messageType': 'SMS',
        'direction':   'Inbound',
        'dateFrom':    date_from,
        'perPage':     100
    }
    if unread_only:
        params['readStatus'] = 'Unread'

    all_records = []
    url = f'{RC_SERVER}/restapi/v1.0/account/~/extension/~/message-store'
    page = 1

    while url:
        if page == 1:
            resp = requests.get(url, headers=headers, params=params)
        else:
            # Use the full nextPage URI directly — no params needed
            resp = requests.get(url, headers=headers)

        resp.raise_for_status()
        data = resp.json()

        records = data.get('records', [])
        all_records.extend(records)
        print(f'[RC] Page {page}: {len(records)} records (total so far: {len(all_records)})', file=sys.stderr)

        # Get next page URI from navigation
        nav = data.get('navigation', {})
        next_page = nav.get('nextPage', {})
        url = next_page.get('uri') if next_page else None
        page += 1

        if not records:
            break

    return all_records

def mark_read(token, message_id):
    requests.put(
        f'{RC_SERVER}/restapi/v1.0/account/~/extension/~/message-store/{message_id}',
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        json={'readStatus': 'Read'}
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit',       type=int,  default=0,      help='Max messages (0 = all)')
    parser.add_argument('--unread-only', action='store_true',       help='Return only unread messages')
    parser.add_argument('--mark-read',   action='store_true',       help='Mark returned messages as read')
    parser.add_argument('--date-from',   default=None,              help='ISO date string (default: 30 days ago)')
    parser.add_argument('--format',      default='json',            help='Output format: json or text')
    args = parser.parse_args()

    token   = get_rc_token()
    records = get_all_inbound(token, date_from=args.date_from, unread_only=args.unread_only)

    if args.limit and args.limit > 0:
        records = records[:args.limit]

    results = []
    for msg in records:
        from_number = msg.get('from', {}).get('phoneNumber', '').replace('+1', '')
        body        = msg.get('subject', '')
        created     = msg.get('creationTime', '')
        msg_id      = msg.get('id')
        read_status = msg.get('readStatus', 'Read')
        candidate   = match_candidate(from_number)

        results.append({
            'message_id':  msg_id,
            'from':        from_number,
            'body':        body,
            'received_at': created,
            'read_status': read_status,
            'candidate':   candidate
        })

        if args.mark_read and read_status == 'Unread':
            mark_read(token, msg_id)

    # ── Direct Supabase upsert to rc_sms_archive ────────────────────────────
    upsert_headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates',
    }
    rows = []
    for m in results:
        raw_from = str(m.get('from', '') or '').strip()
        digits = ''.join(c for c in raw_from if c.isdigit())
        if len(digits) == 11 and digits.startswith('1'):
            digits = digits[1:]
        rows.append({
            'message_id': m['message_id'],
            'from_number': digits or raw_from,
            'body': m.get('body', ''),
            'received_at': m.get('received_at'),
            'read_status': m.get('read_status'),
            'direction': 'inbound',
        })

    upserted = 0
    for i in range(0, len(rows), 100):
        chunk = rows[i:i+100]
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/rc_sms_archive',
            headers=upsert_headers,
            json=chunk,
        )
        if r.status_code in (200, 201):
            upserted += len(chunk)
        else:
            print(f'[ARCHIVE] Batch {i//100+1} error {r.status_code}: {r.text[:200]}', file=sys.stderr)
    print(f'[ARCHIVE] Upserted {upserted} messages to rc_sms_archive', file=sys.stderr)

    output = {
        'source':      'rc_inbox',
        'kai_number':  '4708574325',
        'count':       len(results),
        'unread_only': args.unread_only,
        'date_from':   args.date_from or '30 days ago',
        'timestamp':   datetime.now(timezone.utc).isoformat(),
        'messages':    results
    }

    if args.format == 'text':
        print(f"\nRC INBOX — {output['count']} messages | {output['timestamp']}\n")
        for m in results:
            c = m['candidate']
            name = f"{c['first_name']} {c['last_name']} | {c['client_id']} | {c['status']} | RWP:{c['rwp_score']}" if c else 'NO MATCH'
            status = f"[{m['read_status']}]"
            print(f"{status} {m['received_at'][:16]} | {m['from']}")
            print(f"  {name}")
            print(f"  {m['body'][:120]}")
            print()
    else:
        print(json.dumps(output, indent=2))

if __name__ == '__main__':
    main()
