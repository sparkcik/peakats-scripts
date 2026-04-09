#!/usr/bin/env python3
"""
sms_queue_poller.py
Polls sms_send_queue in Supabase, fires pending messages via RC or Twilio.

ROUTING RULE (locked 2026-03-23):
  migration_status = 'rc_active'     → send via RingCentral API (DEFAULT)
  migration_status = 'twilio_active' → send via Twilio (ONLY when A2P approved)

RC is the default platform. Twilio is blocked until A2P campaign approval.
Never change this routing without explicit Charles approval.

Usage:
  python3 sms_queue_poller.py          # process all due messages
  python3 sms_queue_poller.py --dry-run # preview without sending
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone

# ── RC Config (primary) ───────────────────────────────────────────────────────
RC_CLIENT_ID      = os.environ.get('RC_CLIENT_ID', '1QDQiRjk50kfxvIVYTT3IA')
RC_CLIENT_SECRET  = os.environ.get('RC_CLIENT_SECRET', 'aTMprgZe1Safik4e4qDBnHaKcnA6o9gb3cafm1xQtJxo')
RC_JWT            = os.environ.get('RC_JWT', '')
RC_FROM_NUMBER    = os.environ.get('RC_FROM_NUMBER', '4708574325')
RC_SERVER         = 'https://platform.ringcentral.com'

_rc_access_token = None
_rc_token_expiry = 0

# ── Twilio Config (blocked until A2P approved) ────────────────────────────────
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID', 'AC7c95b5dfb1d6bda35b75cc16186e653c')
TWILIO_AUTH_TOKEN  = os.environ.get('TWILIO_AUTH_TOKEN',  '609a3c093480bbe58382ac8ac1afe468')
TWILIO_FROM_NUMBER = os.environ.get('TWILIO_FROM_NUMBER', '+14704704766')
TWILIO_A2P_APPROVED = os.environ.get('TWILIO_A2P_APPROVED', 'false').lower() == 'true'

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4')

DRY_RUN = '--dry-run' in sys.argv

SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

# ── Supabase helpers ──────────────────────────────────────────────────────────

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
    in_blackout = (hour < 7) or (hour == 7 and minute < 30) or (hour > 19) or (hour == 19 and minute >= 30)
    if in_blackout:
        delivery = dt_et.replace(hour=7, minute=30, second=0, microsecond=0)
        if (hour > 19) or (hour == 19 and minute >= 30):
            delivery = delivery + timedelta(days=1)
        return delivery.astimezone(pytz.utc).replace(tzinfo=None)
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def get_due_messages():
    now = datetime.now(timezone.utc).isoformat()
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue',
        headers=SB_HEADERS,
        params={
            'status': 'eq.pending',
            'scheduled_for': f'lte.{now}',
            'order': 'scheduled_for.asc'
        }
    )
    resp.raise_for_status()
    return resp.json()


def mark_sent(msg_id, message_sid, candidate_id=None, template_name='', platform='rc'):
    now = datetime.now(timezone.utc).isoformat()
    update = {
        'status': 'sent',
        'sent_at': now,
        'delivery_status': 'delivered',
        'updated_at': now
    }
    if platform == 'rc':
        update['rc_message_id'] = message_sid
    else:
        update['twilio_sid'] = message_sid
    requests.patch(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{msg_id}',
        headers=SB_HEADERS, json=update
    )
    if candidate_id:
        try:
            tpl = (template_name or '').upper()
            patch_data = None
            if 'MEC' in tpl or 'RE-ENGAGEMENT' in tpl or 'REENGAGEMENT' in tpl:
                patch_data = {
                    'mec_dl_outreach_sent_at': now,
                    'mec_dl_collection_stage': 'OUTREACH_SENT'
                }
            elif 'GCIC' in tpl:
                patch_data = {'gcic_text_sent': 1}
            if patch_data:
                patch_data['updated_at'] = now
                requests.patch(
                    f'{SUPABASE_URL}/rest/v1/candidates?id=eq.{candidate_id}',
                    headers=SB_HEADERS, json=patch_data
                )
                print(f'         [write-back] Updated candidate {candidate_id}: {list(patch_data.keys())}')
        except Exception as e:
            print(f'         [write-back] Failed for candidate {candidate_id}: {e}')


def mark_failed(msg_id, error):
    now = datetime.now(timezone.utc).isoformat()
    requests.patch(
        f'{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{msg_id}',
        headers=SB_HEADERS,
        json={'status': 'failed', 'delivery_error': str(error)[:500], 'updated_at': now}
    )


def update_comms_log(candidate_id, message_sid, body):
    if not candidate_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/candidate_comms',
        headers=SB_HEADERS,
        params={
            'candidate_id': f'eq.{candidate_id}',
            'direction': 'eq.outbound',
            'external_message_id': 'is.null',
            'order': 'created_at.desc',
            'limit': '1'
        }
    )
    records = resp.json()
    if records:
        comms_id = records[0]['id']
        requests.patch(
            f'{SUPABASE_URL}/rest/v1/candidate_comms?id=eq.{comms_id}',
            headers=SB_HEADERS,
            json={'sent_at': now, 'external_message_id': message_sid, 'updated_at': now}
        )

# ── RC Auth ───────────────────────────────────────────────────────────────────

def get_rc_token():
    """Exchange RC JWT for an access token. Caches until expiry."""
    global _rc_access_token, _rc_token_expiry
    import time as _time
    now = _time.time()
    if _rc_access_token and now < _rc_token_expiry - 60:
        return _rc_access_token
    resp = requests.post(
        f'{RC_SERVER}/restapi/oauth/token',
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': RC_JWT,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    _rc_access_token = data['access_token']
    _rc_token_expiry = now + data.get('expires_in', 3600)
    print(f'         [RC] Token exchanged, expires in {data.get("expires_in")}s')
    return _rc_access_token

# ── RC Send ───────────────────────────────────────────────────────────────────

def send_via_rc(to_number, body):
    """Send SMS via RingCentral API. Returns RC message ID."""
    token = get_rc_token()
    clean = to_number.replace('+1','').replace('-','').replace('(','').replace(')','').replace(' ','')
    normalized_body = body.replace('\\n', '\n').replace('\\r\\n', '\n')
    resp = requests.post(
        f'{RC_SERVER}/restapi/v1.0/account/~/extension/~/sms',
        headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        },
        json={
            'from': {'phoneNumber': f'+1{RC_FROM_NUMBER}'},
            'to': [{'phoneNumber': f'+1{clean}'}],
            'text': normalized_body
        }
    )
    resp.raise_for_status()
    print(f'         [RC] Full response: {resp.json()}')
    msg_id = resp.json().get('id', 'unknown')
    print(f'         [RC] Message ID: {msg_id}')
    return str(msg_id)

# ── Twilio Send (blocked until A2P approved) ──────────────────────────────────

def send_via_twilio(to_number, body):
    """Send via Twilio. ONLY call when TWILIO_A2P_APPROVED=true."""
    if not TWILIO_A2P_APPROVED:
        raise RuntimeError(
            'Twilio A2P campaign not approved. Set TWILIO_A2P_APPROVED=true '
            'only after Charles confirms campaign approval. Use RC instead.'
        )
    clean = to_number.replace('+1','').replace('-','').replace('(','').replace(')','').replace(' ','')
    try:
        send_body = body.encode('utf-8').decode('unicode_escape')
    except Exception:
        send_body = body
    resp = requests.post(
        f'https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json',
        auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
        data={
            'From': TWILIO_FROM_NUMBER,
            'To': f'+1{clean}',
            'Body': send_body,
            'StatusCallback': 'https://peak-forge-local.fly.dev/twilio/status'
        }
    )
    resp.raise_for_status()
    sid = resp.json().get('sid', 'unknown')
    print(f'         [Twilio] SID: {sid}')
    return sid

# ── Route send by migration_status ───────────────────────────────────────────

def send_message(msg):
    """Route to RC or Twilio based on migration_status. RC is default."""
    to_number        = msg['to_number']
    body             = msg['body']
    migration_status = (msg.get('migration_status') or 'rc_active').strip()

    if migration_status == 'twilio_active':
        print(f'         [ROUTING] twilio_active')
        return send_via_twilio(to_number, body), 'twilio'
    else:
        # rc_active or anything else — always RC until A2P approved
        print(f'         [ROUTING] rc_active')
        return send_via_rc(to_number, body), 'rc'

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f'[SMS Poller] {"DRY RUN — " if DRY_RUN else ""}Starting at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}')
    print(f'[SMS Poller] Twilio A2P approved: {TWILIO_A2P_APPROVED}')

    messages = get_due_messages()
    print(f'[SMS Poller] {len(messages)} message(s) due')

    if not messages:
        print('[SMS Poller] Nothing to send. Exiting.')
        return

    sent = 0
    failed = 0

    for msg in messages:
        msg_id       = msg['id']
        to_number    = msg['to_number']
        body         = msg['body']
        candidate_id = msg.get('candidate_id')
        template     = msg.get('template_name', 'manual')
        platform     = (msg.get('migration_status') or 'rc_active')

        print(f'\n[{msg_id}] To: {to_number} | Template: {template} | Platform: {platform}')
        print(f'         Preview: {body[:80]}...')

        if DRY_RUN:
            print(f'         [DRY RUN] Would send via {"Twilio" if platform == "twilio_active" else "RC"} -- skipping')
            continue

        # Normalize line breaks
        if body:
            body = body.replace('\\n', '\n').replace('\\r\\n', '\n').replace('\\r', '\n')
            if '\\n' in body:
                try:
                    body = bytes(body, 'utf-8').decode('unicode_escape')
                except Exception:
                    pass

        try:
            message_sid, used_platform = send_message(msg)
            mark_sent(msg_id, message_sid, candidate_id, template, used_platform)
            update_comms_log(candidate_id, message_sid, body)
            print(f'         SENT via {used_platform.upper()} -- ID: {message_sid}')
            sent += 1
            time.sleep(1.2)
        except Exception as e:
            mark_failed(msg_id, e)
            print(f'         FAILED -- {e}')
            failed += 1

    print(f'\n[SMS Poller] Complete. Sent: {sent} | Failed: {failed}')


if __name__ == '__main__':
    main()
