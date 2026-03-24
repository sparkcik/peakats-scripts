"""
PEAK Recruiting — MEC/DL Form Backfill
Reads all existing rows from the MEC/DL Google Form response sheet
and writes the data to Supabase candidates table for any unprocessed rows.

Sheet ID: 1zM8-Bh_flbnX2THnZcKa-2kcSBriPZXLLrQofSzpAq4
Columns: Timestamp | First Name | Last Name | Phone Number | 
         Medical Certificate (MEC) | Driver License (front only)

Matches candidates by phone (primary) then name fallback.
For each match:
  - Sets mec_storage_path if MEC file URL present
  - Sets dl_storage_path if DL file URL present  
  - Sets mec_uploaded=1, dl_verified=1
  - Sets mec_received_at, dl_received_at
  - Sets mec_dl_collection_stage=COMPLETE
  - Renames and moves files to 05_CANDIDATE_DOCS/{client_id}/mec|dl/
    via forge-drive find_or_create_folder + move_file

Skips rows where candidate already has mec_uploaded=1 (already processed).
"""

import os
import re
import json
import requests
from datetime import datetime, timezone

SUPABASE_URL  = os.environ.get('SUPABASE_URL', 'https://eyopvsmsvbgfuffscfom.supabase.co')
SUPABASE_KEY  = os.environ.get('SUPABASE_KEY')
FORGE_DRIVE   = 'https://eyopvsmsvbgfuffscfom.supabase.co/functions/v1/forge-drive'
FORGE_KEY     = 'peak-forge-2026'
SHEET_ID      = '1zM8-Bh_flbnX2THnZcKa-2kcSBriPZXLLrQofSzpAq4'
DOCS_ROOT     = '1UJfJM6ZMQo2RuVbNWrv4hkBiLnWAZkjB'

SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
}

FD_HEADERS = {
    'x-api-key': FORGE_KEY,
    'Content-Type': 'application/json'
}


def read_sheet():
    """Read the MEC/DL response sheet via forge-drive."""
    r = requests.post(FORGE_DRIVE, headers=FD_HEADERS, json={
        'action': 'read_sheet',
        'file_id': SHEET_ID,
        'sheet_id': 'Form Responses 1',
        'range': 'A1:F500'
    })
    data = r.json()
    if 'error' in data:
        # Try tab index 0
        r2 = requests.post(FORGE_DRIVE, headers=FD_HEADERS, json={
            'action': 'read_sheet',
            'file_id': SHEET_ID,
            'sheet_id': '0',
            'range': 'A:F'
        })
        data = r2.json()
    return data.get('values', [])


def normalize_phone(raw):
    digits = re.sub(r'\D', '', str(raw or ''))
    if len(digits) == 11 and digits[0] == '1':
        digits = digits[1:]
    return digits if len(digits) == 10 else ''


def extract_file_id(url):
    if not url:
        return None
    m = re.search(r'[?&]id=([^&]+)', url) or re.search(r'/d/([^/\?&]+)', url)
    return m.group(1) if m else None


def find_candidate(phone, first, last):
    if phone:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/candidates"
            f"?select=id,first_name,last_name,phone,client_id,mec_uploaded,dl_verified"
            f"&phone=eq.{phone}&limit=2",
            headers=SB_HEADERS
        )
        results = r.json()
        if len(results) == 1:
            return results[0]
    # Name fallback
    if first and last:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/candidates"
            f"?select=id,first_name,last_name,phone,client_id,mec_uploaded,dl_verified"
            f"&first_name=ilike.{requests.utils.quote(first)}"
            f"&last_name=ilike.{requests.utils.quote(last)}&limit=2",
            headers=SB_HEADERS
        )
        results = r.json()
        if len(results) == 1:
            return results[0]
    return None


def get_or_create_folder(parent_id, name):
    r = requests.post(FORGE_DRIVE, headers=FD_HEADERS, json={
        'action': 'find_or_create_folder',
        'parent_id': parent_id,
        'name': name
    })
    data = r.json()
    return data.get('id')


def rename_and_move(file_id, new_name, target_folder_id):
    # Rename
    requests.post(FORGE_DRIVE, headers=FD_HEADERS, json={
        'action': 'rename_file',
        'file_id': file_id,
        'new_name': new_name
    })
    # Move
    r = requests.post(FORGE_DRIVE, headers=FD_HEADERS, json={
        'action': 'move_file',
        'file_id': file_id,
        'new_parent_id': target_folder_id
    })
    data = r.json()
    if 'id' in data:
        return f'https://drive.google.com/file/d/{file_id}/view'
    return None


def update_candidate(cid, updates):
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/candidates?id=eq.{cid}",
        headers=SB_HEADERS, json=updates
    )


def log_failure(first, last, phone, reason, mec_url, dl_url):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/form_match_failures",
        headers=SB_HEADERS,
        json={
            'form_type': 'mec_dl',
            'submitted_phone': phone,
            'submitted_at': datetime.now(timezone.utc).isoformat(),
            'raw_form_data': json.dumps({'first': first, 'last': last, 'phone': phone}),
            'mec_drive_url': mec_url,
            'dl_drive_url': dl_url,
            'resolved': False
        }
    )


def run():
    print(f"[{datetime.now()}] MEC/DL backfill starting...")

    rows = read_sheet()
    if not rows:
        print("No data read from sheet. Check forge-drive read_sheet.")
        return

    headers = rows[0]
    data_rows = rows[1:]
    print(f"Sheet headers: {headers}")
    print(f"Found {len(data_rows)} form submissions to process")

    # Map column indices
    col = {h: i for i, h in enumerate(headers)}
    print(f"Column map: {col}")

    processed = 0
    skipped   = 0
    failed    = 0

    for i, row in enumerate(data_rows):
        def get(key):
            idx = col.get(key)
            return row[idx].strip() if idx is not None and idx < len(row) else ''

        timestamp = get('Timestamp')
        first     = get('First Name').title()
        last      = get('Last Name').title()
        raw_phone = get('Phone Number')
        mec_url   = get('Medical Certificate (MEC)')
        dl_url    = get('Driver License (front only)')
        phone     = normalize_phone(raw_phone)

        print(f"\nRow {i+1}: {first} {last} | phone={phone} | mec={'yes' if mec_url else 'no'} | dl={'yes' if dl_url else 'no'}")

        if not phone and not (first and last):
            print(f"  SKIP — no phone or name")
            skipped += 1
            continue

        candidate = find_candidate(phone, first, last)

        if not candidate:
            mec_file_id = extract_file_id(mec_url)
            dl_file_id  = extract_file_id(dl_url)
            mec_drive   = f'https://drive.google.com/file/d/{mec_file_id}/view' if mec_file_id else None
            dl_drive    = f'https://drive.google.com/file/d/{dl_file_id}/view' if dl_file_id else None
            log_failure(first, last, phone, 'No candidate match in PEAKATS', mec_drive, dl_drive)
            print(f"  FAILED — no match, logged to form_match_failures")
            failed += 1
            continue

        cid       = candidate['id']
        client_id = candidate.get('client_id', '')

        # Skip if already fully processed
        if candidate.get('mec_uploaded') == 1 and (not dl_url or candidate.get('dl_verified') == 1):
            print(f"  SKIP — candidate {cid} already processed")
            skipped += 1
            continue

        now       = datetime.now(timezone.utc).isoformat()
        date_str  = datetime.now(timezone.utc).strftime('%Y%m%d')
        updates   = {'mec_dl_collection_stage': 'COMPLETE', 'mec_form_submitted_at': now}

        # Process MEC file
        if mec_url and not candidate.get('mec_uploaded'):
            mec_file_id = extract_file_id(mec_url)
            if mec_file_id:
                mec_folder = get_or_create_folder(
                    get_or_create_folder(DOCS_ROOT, client_id), 'mec'
                )
                new_name = f"{cid}_{last}_{first}_mec_{date_str}"
                mec_drive_url = rename_and_move(mec_file_id, new_name, mec_folder)
                if mec_drive_url:
                    updates['mec_storage_path'] = mec_drive_url
                    updates['mec_uploaded']      = 1
                    updates['mec_uploaded_at']   = now
                    updates['mec_received_at']   = now
                    print(f"  MEC filed: {mec_drive_url}")

        # Process DL file
        if dl_url and not candidate.get('dl_verified'):
            dl_file_id = extract_file_id(dl_url)
            if dl_file_id:
                dl_folder = get_or_create_folder(
                    get_or_create_folder(DOCS_ROOT, client_id), 'dl'
                )
                new_name = f"{cid}_{last}_{first}_dl_{date_str}"
                dl_drive_url = rename_and_move(dl_file_id, new_name, dl_folder)
                if dl_drive_url:
                    updates['dl_storage_path'] = dl_drive_url
                    updates['dl_verified']     = 1
                    updates['dl_uploaded_at']  = now
                    updates['dl_received_at']  = now
                    print(f"  DL filed: {dl_drive_url}")

        update_candidate(cid, updates)
        print(f"  DONE — candidate {cid} {first} {last} ({client_id}) updated")
        processed += 1

    print(f"\n=== BACKFILL COMPLETE ===")
    print(f"Processed: {processed} | Skipped: {skipped} | Failed: {failed}")


if __name__ == '__main__':
    run()
