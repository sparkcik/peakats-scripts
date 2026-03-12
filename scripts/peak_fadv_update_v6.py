#!/usr/bin/env python3
"""
PEAKATS FADV Update Script v6.0
WRITES DIRECTLY TO SUPABASE (PostgreSQL Cloud)

Updates candidate records with FADV background check data
Detects and flags ANY status changes for recruiter review
NOW WITH FUZZY MATCHING for compound surnames and typos
NOW WITH STATUS NORMALIZATION — raw FADV values mapped to clean taxonomy
NOW WITH AUTO-REJECT — Ineligible BG or Failed Drug → status=Rejected, reject_reason=fadv_ineligible

Usage:
    peak-fadv --batch           # Process all clients
    peak-fadv --client star_one # Process single client
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import argparse
from difflib import SequenceMatcher

# Supabase connection - SINGLE SOURCE OF TRUTH
SUPABASE_URL = "postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

# FADV files location
FADV_DIR = Path.home() / "Library/CloudStorage/GoogleDrive-charles@thefoundry.llc/My Drive/PEAK/#PEAKATS/04_FADV_UPDATES"


# ─── STATUS NORMALIZATION ──────────────────────────────────────────────────────

DRUG_STATUS_MAP = {
    None: 'Not Started',
    '': 'Not Started',
    'Order Received': 'In Progress',
    'Collection Complete': 'In Progress',
    'Collection Event Review': 'In Progress',
    'Negative/Pass': 'Pass',
    'Negative Dilute**': 'Pass',
    'Eligible': 'Pass',
    'Complete**': 'Pass',
    'Positive/Fail*': 'Fail',
    'Order Expired/Donor No Show**': 'Expired',
    'Cancel**': 'Expired',
}

BG_STATUS_MAP = {
    None: 'Not Started',
    '': 'Not Started',
    'Not Ordered': 'Not Started',
    'In Progress': 'In Progress',
    'In Progress*': 'In Progress',
    'In Progress**': 'In Progress',
    'Order Received': 'In Progress',
    'Eligible': 'Eligible',
    'Negative/Pass': 'Eligible',
    'Complete**': 'Eligible',
    'Negative Dilute**': 'Eligible',
    'Needs further review**': 'Consider',
    'In-Eligible For Hire*': 'Ineligible',
    'Case Canceled': 'Expired',
    'Order Expired/Donor No Show**': 'Expired',
}

def normalize_drug_status(raw):
    """Map raw FADV drug string to clean taxonomy value"""
    if not raw or raw == 'nan':
        return 'Not Started'
    raw = raw.strip()
    return DRUG_STATUS_MAP.get(raw, raw)  # fallback to raw if unknown value

def normalize_bg_status(raw):
    """Map raw FADV background string to clean taxonomy value"""
    if not raw or raw == 'nan':
        return 'Not Started'
    raw = raw.strip()
    return BG_STATUS_MAP.get(raw, raw)  # fallback to raw if unknown value


# ─── AUTO-REJECT ───────────────────────────────────────────────────────────────

def apply_auto_reject(conn, candidate_id, bg_status, drug_status, text_func):
    """
    Auto-reject candidate if FADV returns Ineligible BG or Failed drug.
    Will not overwrite Hired status.
    """
    if bg_status == 'Ineligible' or drug_status == 'Fail':
        conn.execute(text_func("""
            UPDATE candidates
            SET status = 'Rejected',
                reject_reason = 'fadv_ineligible',
                updated_at = NOW()
            WHERE id = :id
            AND status NOT IN ('Hired', 'Rejected')
        """), {"id": candidate_id})
        conn.commit()
        reason = 'BG Ineligible' if bg_status == 'Ineligible' else 'Drug Fail'
        print(f"  🚫 AUTO-REJECTED: ID {candidate_id} — {reason}")
        return True
    return False


# ─── FUZZY MATCHING ────────────────────────────────────────────────────────────

def fuzzy_match_score(str1: str, str2: str) -> float:
    """Calculate similarity score between two strings"""
    if not str1 or not str2:
        return 0.0
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()


def find_candidate_match(conn, first_name: str, last_name: str, client_id: str, text_func):
    """
    Enhanced candidate matching with fallback strategies
    Returns: (candidate_row, match_type) or (None, None)
    """
    # Strategy 1: Exact match
    result = conn.execute(text_func("""
        SELECT id, profile_status, legacy_order_status, background_status, drug_test_status, first_name, last_name
        FROM candidates
        WHERE LOWER(first_name) = LOWER(:first_name) 
        AND LOWER(last_name) = LOWER(:last_name)
        AND client_id = :client_id
    """), {"first_name": first_name, "last_name": last_name, "client_id": client_id})
    
    existing = result.fetchone()
    if existing:
        return existing, "exact"
    
    # Strategy 2: First word of last name (handles "BURUCA GAVARRETE" -> "Buruca")
    last_name_first_word = last_name.split()[0] if last_name else ""
    if last_name_first_word and last_name_first_word != last_name:
        result = conn.execute(text_func("""
            SELECT id, profile_status, legacy_order_status, background_status, drug_test_status, first_name, last_name
            FROM candidates
            WHERE LOWER(first_name) = LOWER(:first_name) 
            AND LOWER(last_name) = LOWER(:last_name_first)
            AND client_id = :client_id
        """), {"first_name": first_name, "last_name_first": last_name_first_word, "client_id": client_id})
        
        existing = result.fetchone()
        if existing:
            return existing, "last_name_partial"
    
    # Strategy 3: Fuzzy first name match (handles "MEWASIAH" vs "Mwasiah" typos)
    result = conn.execute(text_func("""
        SELECT id, profile_status, legacy_order_status, background_status, drug_test_status, first_name, last_name
        FROM candidates
        WHERE (LOWER(last_name) = LOWER(:last_name) OR LOWER(last_name) = LOWER(:last_name_first))
        AND client_id = :client_id
    """), {"last_name": last_name, "last_name_first": last_name_first_word, "client_id": client_id})
    
    candidates = result.fetchall()
    
    best_match = None
    best_score = 0.0
    
    for candidate in candidates:
        score = fuzzy_match_score(first_name, candidate[5])
        if score >= 0.85 and score > best_score:
            best_score = score
            best_match = candidate
    
    if best_match:
        return best_match, f"fuzzy_first_name({best_score:.0%})"
    
    # Strategy 4: Fuzzy last name match
    result = conn.execute(text_func("""
        SELECT id, profile_status, legacy_order_status, background_status, drug_test_status, first_name, last_name
        FROM candidates
        WHERE LOWER(first_name) = LOWER(:first_name)
        AND client_id = :client_id
    """), {"first_name": first_name, "client_id": client_id})
    
    candidates = result.fetchall()
    
    for candidate in candidates:
        score = fuzzy_match_score(last_name, candidate[6])
        if score >= 0.80 and score > best_score:
            best_score = score
            best_match = candidate
    
    if best_match:
        return best_match, f"fuzzy_last_name({best_score:.0%})"
    
    return None, None


# ─── CLIENT MAPPING ────────────────────────────────────────────────────────────

CLIENT_MAP = {
    'dd_networks': 'dd_networks',
    'dd networks': 'dd_networks',
    'ddnetworks': 'dd_networks',
    'star_one': 'star_one',
    'star one': 'star_one',
    'starone': 'star_one',
    'jcb': 'jcb',
    'rm_iv': 'rm_iv',
    'rm iv': 'rm_iv',
    'rmiv': 'rm_iv',
    'woodstock': 'woodstock',
    'james_elite': 'james_elite',
    'james elite': 'james_elite',
    'jameselite': 'james_elite',
    'cbm': 'cbm',
    'excellus': 'excellus',
    'elevation_bound': 'excellus',
    'elevation bound': 'excellus',
    'elevationbound': 'excellus',
    'excellus_delivery': 'excellus_delivery',
    'excellus delivery': 'excellus_delivery',
    'excellusdelivery': 'excellus_delivery',
    'excel_route': 'excel_route',
    'excel route': 'excel_route',
    'excelroute': 'excel_route',
    'smart_route': 'smart_route',
    'smart route': 'smart_route',
    'smartroute': 'smart_route',
    'solpac': 'solpac',
}

ALL_CLIENTS = ['dd_networks', 'star_one', 'jcb', 'rm_iv', 'woodstock', 'james_elite', 'cbm', 'excellus', 'excellus_delivery', 'excel_route', 'smart_route', 'solpac']


def normalize_client_name(name):
    """Normalize client name to standard format"""
    if not name:
        return None
    return CLIENT_MAP.get(name.lower().strip(), name.lower().strip())


# ─── FILE TYPE DETECTION ───────────────────────────────────────────────────────

def detect_file_type(csv_path):
    """
    Auto-detect if CSV is Background or Drug file based on column headers
    Returns: 'background', 'drug', or None
    """
    try:
        df = pd.read_csv(csv_path, nrows=1)
        columns = [col.lower() for col in df.columns]
        
        if 'report type' in columns or 'drug screen' in ' '.join(columns).lower():
            if 'report type' in columns:
                return 'drug'
        
        if 'profile status' in columns and 'order status' in columns:
            if 'report type' not in columns:
                return 'background'
        
        if any('drug' in col for col in columns):
            return 'drug'
        if any('background' in col for col in columns):
            return 'background'
        if 'profile status' in columns:
            return 'background'
            
        return None
    except Exception as e:
        print(f"Error detecting file type for {csv_path.name}: {e}")
        return None


# ─── CHANGE DETECTION ─────────────────────────────────────────────────────────

def detect_changes(old_values, new_values):
    """
    Detect what changed between old and new FADV values
    Returns: (has_changes: bool, change_details: str)
    """
    changes = []
    
    fields = [
        ('profile_status', 'Profile'),
        ('order_status', 'Order'),
        ('background_status', 'Background'),
        ('drug_test_status', 'Drug')
    ]
    
    for db_field, display_name in fields:
        old_val = old_values.get(db_field, '') or ''
        new_val = new_values.get(db_field, '') or ''
        
        if old_val in ['None', 'nan', '']:
            old_val = ''
        if new_val in ['None', 'nan', '']:
            new_val = ''
        
        if old_val != new_val and new_val:
            changes.append(f"{display_name}: {old_val or 'None'} → {new_val}")
    
    if changes:
        return True, '; '.join(changes)
    return False, ''


# ─── MAIN UPDATE FUNCTION ─────────────────────────────────────────────────────

def update_fadv_data(client_id=None, batch_mode=False):
    """Update FADV data for specified client or all clients - WRITES TO SUPABASE"""
    
    from sqlalchemy import create_engine, text
    engine = create_engine(SUPABASE_URL)
    
    stats = {
        'total_candidates': 0,
        'updates_applied': 0,
        'changes_flagged': 0,
        'created': 0,
        'auto_rejected': 0,
        'clients_processed': 0,
        'clients_skipped': 0
    }
    
    flagged_candidates = []
    
    if batch_mode:
        clients = ALL_CLIENTS
    else:
        if not client_id:
            print("❌ Error: Client ID required for single-client mode")
            return
        clients = [normalize_client_name(client_id)]
    
    for client in clients:
        client_dir = FADV_DIR / client
        
        if not client_dir.exists():
            stats['clients_skipped'] += 1
            continue
        
        all_csvs = sorted(client_dir.glob('*.csv'), key=lambda x: x.stat().st_mtime, reverse=True)
        all_csvs = [f for f in all_csvs if 'archive' not in str(f).lower()]
        
        if not all_csvs:
            stats['clients_skipped'] += 1
            continue
        
        bg_file = None
        drug_file = None
        
        for csv_file in all_csvs:
            file_type = detect_file_type(csv_file)
            if file_type == 'background' and not bg_file:
                bg_file = csv_file
            elif file_type == 'drug' and not drug_file:
                drug_file = csv_file
        
        if not bg_file and not drug_file:
            stats['clients_skipped'] += 1
            continue
        
        print(f"\n{'='*60}")
        print(f"Processing client: {client.upper()}")
        print(f"{'='*60}")
        
        if bg_file:
            print(f"📋 Background file: {bg_file.name}")
        if drug_file:
            print(f"💊 Drug file: {drug_file.name}")
        
        processed_candidates = set()
        
        for file_path, file_type in [(bg_file, 'background'), (drug_file, 'drug')]:
            if not file_path:
                continue
            
            try:
                df = pd.read_csv(file_path)
                print(f"\nProcessing {file_type} file: {len(df)} records")
                
                for _, row in df.iterrows():
                    stats['total_candidates'] += 1
                    
                    first_name = str(row.get('First Name', '')).strip()
                    last_name = str(row.get('Last Name', '')).strip()
                    
                    if not first_name or not last_name or first_name == 'nan' or last_name == 'nan':
                        continue
                    
                    candidate_key = f"{first_name.lower()}_{last_name.lower()}"
                    
                    if file_type == 'background' and candidate_key in processed_candidates:
                        continue
                    
                    with engine.connect() as conn:
                        existing, match_type = find_candidate_match(conn, first_name, last_name, client, text)
                    
                    if existing:
                        if match_type != "exact":
                            db_name = f"{existing[5]} {existing[6]}"
                            print(f"  🔍 FUZZY MATCH: '{first_name} {last_name}' → '{db_name}' ({match_type})")
                    
                    if not existing:
                        if file_type == 'drug':
                            stats['skipped'] = stats.get('skipped', 0) + 1
                            continue
                        
                        # Background file: CREATE new candidate
                        profile_status = str(row.get('Profile Status', '')).strip()
                        bg_status_raw = str(row.get('Order Status', '')).strip()
                        bg_id = str(row.get('Background ID', '')).strip()
                        
                        if profile_status == 'nan': profile_status = ''
                        if bg_status_raw == 'nan': bg_status_raw = ''
                        if bg_id == 'nan': bg_id = ''
                        
                        bg_status_clean = normalize_bg_status(bg_status_raw)
                        
                        with engine.connect() as conn:
                            conn.execute(text("""
                                INSERT INTO candidates (
                                    client_id, first_name, last_name,
                                    profile_status,
                                    background_status, background_id,
                                    status, created_at, updated_at
                                ) VALUES (
                                    :client_id, :first_name, :last_name,
                                    :profile_status,
                                    :bg_status, :bg_id,
                                    'Active', NOW(), NOW()
                                )
                            """), {
                                "client_id": client,
                                "first_name": first_name,
                                "last_name": last_name,
                                "profile_status": profile_status or None,
                                "bg_status": bg_status_clean or None,
                                "bg_id": bg_id or None
                            })
                            conn.commit()
                        
                        stats['created'] += 1
                        print(f"  ➕ CREATED: {first_name} {last_name} (FADV-only)")
                        processed_candidates.add(candidate_key)
                        continue
                    
                    candidate_id = existing[0]
                    
                    old_values = {
                        'profile_status': existing[1] or '',
                        'order_status': existing[2] or '',
                        'background_status': existing[3] or '',
                        'drug_test_status': existing[4] or ''
                    }
                    
                    new_values = {}
                    auto_rejected = False
                    
                    with engine.connect() as conn:
                        if file_type == 'background':
                            profile_status = str(row.get('Profile Status', '')).strip()
                            bg_status_raw = str(row.get('Order Status', '')).strip()
                            bg_id = str(row.get('Background ID', '')).strip()
                            
                            if profile_status == 'nan': profile_status = ''
                            if bg_status_raw == 'nan': bg_status_raw = ''
                            if bg_id == 'nan': bg_id = ''
                            
                            # Normalize to clean taxonomy
                            bg_status_clean = normalize_bg_status(bg_status_raw)
                            
                            new_values['profile_status'] = profile_status
                            new_values['background_status'] = bg_status_clean
                            
                            conn.execute(text("""
                                UPDATE candidates
                                SET profile_status = :profile_status,
                                    background_status = :bg_status,
                                    background_id = :bg_id,
                                    updated_at = NOW()
                                WHERE id = :id
                            """), {
                                "profile_status": profile_status or None,
                                "bg_status": bg_status_clean or None,
                                "bg_id": bg_id or None,
                                "id": candidate_id
                            })
                            conn.commit()
                            
                            # Auto-reject if Ineligible
                            auto_rejected = apply_auto_reject(
                                conn, candidate_id,
                                bg_status_clean,
                                old_values.get('drug_test_status', ''),
                                text
                            )
                        
                        elif file_type == 'drug':
                            report_type = str(row.get('Report Type', '')).strip()
                            report_status_raw = str(row.get('Report Status', '')).strip()
                            order_id = str(row.get('Order ID', '')).strip()
                            
                            if report_type == 'nan': report_type = ''
                            if report_status_raw == 'nan': report_status_raw = ''
                            if order_id == 'nan': order_id = ''
                            
                            if 'medical' in report_type.lower():
                                continue
                            
                            if report_type == 'Drug Screen':
                                drug_status_clean = normalize_drug_status(report_status_raw)
                                new_values['drug_test_status'] = drug_status_clean
                                
                                conn.execute(text("""
                                    UPDATE candidates
                                    SET drug_test_status = :drug_status,
                                        drug_test_id = :drug_id,
                                        updated_at = NOW()
                                    WHERE id = :id
                                """), {
                                    "drug_status": drug_status_clean or None,
                                    "drug_id": order_id or None,
                                    "id": candidate_id
                                })
                                conn.commit()
                                
                                # Auto-reject if Fail
                                auto_rejected = apply_auto_reject(
                                    conn, candidate_id,
                                    old_values.get('background_status', ''),
                                    drug_status_clean,
                                    text
                                )
                                
                            elif report_type == 'Background Screen':
                                new_values['background_status'] = normalize_bg_status(report_status_raw)
                                
                                conn.execute(text("""
                                    UPDATE candidates
                                    SET background_id = :bg_id,
                                        updated_at = NOW()
                                    WHERE id = :id
                                """), {
                                    "bg_id": order_id or None,
                                    "id": candidate_id
                                })
                                conn.commit()
                    
                    if auto_rejected:
                        stats['auto_rejected'] += 1
                    
                    # Detect changes and flag
                    has_changes, change_details = detect_changes(old_values, new_values)
                    
                    if has_changes:
                        with engine.connect() as conn:
                            conn.execute(text("""
                                UPDATE candidates
                                SET fadv_change_flag = 1,
                                    fadv_change_details = :details,
                                    fadv_last_updated = NOW()
                                WHERE id = :id
                            """), {
                                "details": change_details,
                                "id": candidate_id
                            })
                            conn.commit()
                        
                        stats['changes_flagged'] += 1
                        flagged_candidates.append({
                            'name': f"{first_name} {last_name}",
                            'client': client,
                            'changes': change_details,
                            'auto_rejected': auto_rejected
                        })
                        flag_icon = '🚫' if auto_rejected else '🚩'
                        print(f"  {flag_icon} FLAGGED: {first_name} {last_name} - {change_details}")
                    
                    stats['updates_applied'] += 1
                    
                    if file_type == 'background':
                        processed_candidates.add(candidate_key)
                
            except Exception as e:
                print(f"❌ Error processing {file_type} file: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        stats['clients_processed'] += 1
        
        # Archive files after successful processing
        if bg_file or drug_file:
            archive_dir = client_dir / "archive"
            archive_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if bg_file:
                archive_name = f"{bg_file.stem}_{timestamp}{bg_file.suffix}"
                bg_file.rename(archive_dir / archive_name)
                print(f"  📁 Archived: {bg_file.name}")
            
            if drug_file:
                archive_name = f"{drug_file.stem}_{timestamp}{drug_file.suffix}"
                drug_file.rename(archive_dir / archive_name)
                print(f"  📁 Archived: {drug_file.name}")
    
    # ─── SUMMARY ──────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("FADV UPDATE SUMMARY")
    print(f"{'='*60}")
    if batch_mode:
        print(f"Clients processed: {stats['clients_processed']}")
        print(f"Clients skipped:   {stats['clients_skipped']}")
    print(f"Total records in files: {stats['total_candidates']}")
    print(f"Updates applied:        {stats['updates_applied']}")
    print(f"➕ New candidates created:          {stats['created']}")
    print(f"⏭️  Drug records skipped (not in DB): {stats.get('skipped', 0)}")
    print(f"🚩 Changes flagged:                 {stats['changes_flagged']}")
    print(f"🚫 Auto-rejected (FADV ineligible): {stats['auto_rejected']}")
    
    if flagged_candidates:
        print(f"\n{'='*60}")
        print("FLAGGED CANDIDATES BY CHANGE TYPE")
        print(f"{'='*60}")
        
        auto_rejected_list = [c for c in flagged_candidates if c['auto_rejected']]
        eligible_changes   = [c for c in flagged_candidates if 'Eligible' in c['changes'] and not c['auto_rejected']]
        pass_changes       = [c for c in flagged_candidates if ('Pass' in c['changes']) and not c['auto_rejected']]
        review_changes     = [c for c in flagged_candidates if 'Consider' in c['changes'] and not c['auto_rejected']]
        other_changes      = [c for c in flagged_candidates if c not in auto_rejected_list + eligible_changes + pass_changes + review_changes]
        
        if auto_rejected_list:
            print(f"\n🚫 AUTO-REJECTED ({len(auto_rejected_list)}):")
            for c in auto_rejected_list:
                print(f"   • {c['name']} ({c['client']}) - {c['changes']}")
        
        if eligible_changes:
            print(f"\n✅ ELIGIBLE ({len(eligible_changes)}):")
            for c in eligible_changes:
                print(f"   • {c['name']} ({c['client']})")
        
        if pass_changes:
            print(f"\n✅ DRUG PASS ({len(pass_changes)}):")
            for c in pass_changes:
                print(f"   • {c['name']} ({c['client']})")
        
        if review_changes:
            print(f"\n⚠️  NEEDS REVIEW / CONSIDER ({len(review_changes)}):")
            for c in review_changes:
                print(f"   • {c['name']} ({c['client']})")
        
        if other_changes:
            print(f"\n📋 OTHER CHANGES ({len(other_changes)}):")
            for c in other_changes:
                print(f"   • {c['name']} ({c['client']}) - {c['changes']}")
    
    print(f"\n{'='*60}")
    print("✅ FADV update complete! Changes written directly to cloud.")
    print("   Dashboard will reflect updates immediately.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update FADV data - writes directly to Supabase cloud')
    parser.add_argument('--client', help='Client ID for single-client mode')
    parser.add_argument('--batch', action='store_true', help='Process all clients')
    
    args = parser.parse_args()
    
    if args.batch:
        update_fadv_data(batch_mode=True)
    elif args.client:
        update_fadv_data(client_id=args.client)
    else:
        print("PEAKATS FADV Update v6.0 - Direct to Cloud")
        print("=" * 50)
        print("Usage:")
        print("  Single client: python3 peak_fadv_update_v6.py --client star_one")
        print("  All clients:   python3 peak_fadv_update_v6.py --batch")
        print("")
        print("Or use alias:")
        print("  peak-fadv --batch")
