#!/usr/bin/env python3
"""
PEAKATS CSV Import Module v3.0
WRITES DIRECTLY TO SUPABASE (PostgreSQL Cloud)

Changes from v2:
- status at ingest = 'Intake' (not 'No Resume' -- removed entirely)
- background_status = 'Not Started' on insert
- drug_test_status = 'Not Started' on insert
- home_pool set from CLIENT_TO_POOL map (error if client_id not in map)
- Provisional RWP scoring from Indeed screening question columns
- rwp_source = 'provisional' when scored from CSV
- Phone missing -> NULL + CANDIDATE_OPS action item
- Email missing -> NULL + CANDIDATE_OPS action item
- Validation: client_id must be in CLIENT_TO_POOL map before any imports
"""

import csv
import os
import re
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Supabase connection
SUPABASE_URL = os.environ.get(
    'SUPABASE_URL',
    'postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require'
)

# ── CLIENT TO POOL MAP (locked) ───────────────────────────────────────────────
CLIENT_TO_POOL = {
    'cbm':                  'nor_pool',
    'cnf_services':         'nor_pool',
    'dd_networks':          'nor_pool',
    'star_one':             'nor_pool',
    'excel_route':          'east_point_pool',
    'woodstock':            'marietta_pool',
    'gods_vision':          'aus_pool',
    'jcb':                  'aus_pool',
    'rm_iv':                'aus_pool',
    'excellus':             'aus_pool',
    'elevation_bound':      'aus_pool',
    'smart_route':          'eastaboga_pool',
    'legacy_chattanooga':   'chatt_pool',
    'legacy_ooltewah':      'ool_pool',
    'legacy_tuscaloosa':    'tusc_pool',
    'solpac':               'brz_pool',
    'atlas_routes':         'brz_pool',
}

# ── PROVISIONAL RWP SCORING RULES ────────────────────────────────────────────
# Match by column name (case-insensitive, partial match ok)
# Returns (score, classification)

FEDEX_ACTIVE_KEYWORDS   = ['currently working as a fedex', 'current fedex']
FEDEX_YEARS_KEYWORDS    = ['years of fedex', 'fedex driving experience']
DELIVERY_DRIVER_KEYWORDS = ['professional delivery driver', 'delivery driver']
DELIVERY_CO_KEYWORDS    = ['which delivery companies', 'delivery companies have you worked']
VEHICLE_TYPE_KEYWORDS   = ['type of vehicle', 'vehicle do you drive']
START_DATE_KEYWORDS     = ['how soon can you start', 'available to start']

CDL_KEYWORDS   = ['cdl', 'semi', 'box truck', 'tractor', 'flatbed']
GIG_KEYWORDS   = ['doordash', 'uber', 'lyft', 'instacart', 'shipt', 'amazon flex', 'gig']
PRO_KEYWORDS   = ['ups', 'amazon', 'usps', 'dhl', 'fedex ground', 'ontrac', 'lasership']

RWP_CLASSIFICATIONS = {
    11: 'FEDEX_ACTIVE',
    10: 'FEDEX_EXPERIENCED',
    9:  'PRO_DELIVERY',
    6:  'COMMERCIAL_VEHICLE',
    3:  'GIG_ONLY',
    1:  'REVIEWED_NO_EXP',
    0:  'UNSCORED',
}


def find_col(row: Dict, keywords: List[str]) -> Optional[str]:
    """Find a column value by keyword match on column name."""
    for key in row:
        key_lower = key.lower()
        for kw in keywords:
            if kw.lower() in key_lower:
                return str(row[key]).strip() if row[key] else ''
    return None


def score_provisional(row: Dict) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Score candidate from Indeed screening question columns.
    Returns (score, classification, rwp_source) or (None, None, None) if no questions found.
    """
    # Check if any scoring columns are present
    has_fedex_active  = find_col(row, FEDEX_ACTIVE_KEYWORDS)
    has_fedex_years   = find_col(row, FEDEX_YEARS_KEYWORDS)
    has_delivery      = find_col(row, DELIVERY_DRIVER_KEYWORDS)
    has_delivery_co   = find_col(row, DELIVERY_CO_KEYWORDS)
    has_vehicle       = find_col(row, VEHICLE_TYPE_KEYWORDS)

    # If none of the scoring questions are present, return None (no scoring)
    if all(v is None for v in [has_fedex_active, has_fedex_years, has_delivery, has_delivery_co, has_vehicle]):
        return None, None, None

    # Rule 1: Currently a FedEx driver -> score 11
    if has_fedex_active:
        val = has_fedex_active.lower()
        if 'yes' in val or 'current' in val:
            return 11, RWP_CLASSIFICATIONS[11], 'provisional'

    # Rule 2: Has FedEx years experience (not current) -> score 10
    if has_fedex_years:
        val = has_fedex_years.lower()
        if val and val not in ('no', '0', 'none', ''):
            try:
                years = float(re.search(r'[\d.]+', val).group())
                if years > 0:
                    return 10, RWP_CLASSIFICATIONS[10], 'provisional'
            except:
                if 'yes' in val or 'experience' in val:
                    return 10, RWP_CLASSIFICATIONS[10], 'provisional'

    # Rule 3: Professional delivery + UPS/Amazon/USPS/DHL -> score 9
    if has_delivery and has_delivery_co:
        delivery_val = has_delivery.lower()
        co_val = has_delivery_co.lower()
        if ('yes' in delivery_val or 'professional' in delivery_val):
            if any(kw in co_val for kw in PRO_KEYWORDS):
                return 9, RWP_CLASSIFICATIONS[9], 'provisional'

    # Rule 4: Vehicle type scoring
    if has_vehicle:
        veh = has_vehicle.lower()
        if any(kw in veh for kw in CDL_KEYWORDS):
            return 6, RWP_CLASSIFICATIONS[6], 'provisional'
        if any(kw in veh for kw in GIG_KEYWORDS):
            return 3, RWP_CLASSIFICATIONS[3], 'provisional'

    # Has questions but couldn't score -> provisional unscored
    return None, None, None


class CSVImporter:
    def __init__(self, client_id: str):
        # Validate client_id is in pool map
        if client_id not in CLIENT_TO_POOL:
            raise ValueError(
                f"client_id '{client_id}' is not in CLIENT_TO_POOL map. "
                f"Add it before importing. Valid clients: {list(CLIENT_TO_POOL.keys())}"
            )
        self.client_id = client_id
        self.home_pool = CLIENT_TO_POOL[client_id]
        self.engine = None
        self.stats = {
            'total_rows': 0,
            'imported': 0,
            'duplicates': 0,
            'action_items_created': 0,
            'errors': []
        }

    def connect(self):
        from sqlalchemy import create_engine
        self.engine = create_engine(SUPABASE_URL)

    def clean_phone(self, phone: str) -> Optional[str]:
        """Clean phone to 10-digit string. Returns None if invalid."""
        if not phone or str(phone).strip() in ('', '0', 'None', 'nan'):
            return None
        digits = re.sub(r'\D', '', str(phone))
        if len(digits) == 11 and digits.startswith('1'):
            digits = digits[1:]
        if len(digits) == 10:
            return digits
        return None

    def parse_name(self, full_name: str) -> Tuple[str, str]:
        if not full_name or full_name.strip() in ('', 'None', 'nan'):
            return 'Unknown', 'Unknown'
        parts = full_name.strip().split()
        if len(parts) == 0:
            return 'Unknown', 'Unknown'
        elif len(parts) == 1:
            return parts[0].title(), ''
        else:
            return parts[0].title(), ' '.join(parts[1:]).title()

    def candidate_exists(self, email: str, first_name: str, last_name: str) -> Tuple[bool, Optional[int]]:
        from sqlalchemy import text
        with self.engine.connect() as conn:
            if email:
                result = conn.execute(text("""
                    SELECT id FROM candidates
                    WHERE LOWER(email) = LOWER(:email) AND client_id = :client_id
                    LIMIT 1
                """), {"email": email, "client_id": self.client_id})
                row = result.fetchone()
                if row:
                    return True, row[0]
            result = conn.execute(text("""
                SELECT id FROM candidates
                WHERE LOWER(first_name) = LOWER(:first)
                AND LOWER(last_name) = LOWER(:last)
                AND client_id = :client_id
                LIMIT 1
            """), {"first": first_name, "last": last_name, "client_id": self.client_id})
            row = result.fetchone()
            if row:
                return True, row[0]
        return False, None

    def create_action_item(self, task: str, priority: str = '🟡'):
        """Log a CANDIDATE_OPS action item to Supabase."""
        from sqlalchemy import text
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO action_items (task, priority, category, domain, status, created_at, updated_at)
                    VALUES (:task, :priority, 'CANDIDATE_OPS', 'PEAK Ops', 'PENDING', NOW(), NOW())
                """), {"task": task, "priority": priority})
                conn.commit()
            self.stats['action_items_created'] += 1
        except Exception as e:
            print(f"  Warning: could not create action item: {e}")

    def import_candidate(self, row: Dict) -> bool:
        from sqlalchemy import text

        try:
            # Extract fields
            full_name = (
                row.get('name', '') or row.get('Name', '') or
                row.get('Candidate Name', '') or ''
            ).strip()
            email_raw = (
                row.get('email', '') or row.get('Email', '') or
                row.get('Email Address', '') or ''
            ).lower().strip()
            phone_raw = (
                row.get('phone', '') or row.get('Phone', '') or
                row.get('Phone Number', '') or ''
            ).strip()

            first_name, last_name = self.parse_name(full_name)

            if first_name == 'Unknown' and last_name == 'Unknown':
                self.stats['errors'].append({'type': 'missing_name', 'row': dict(row)})
                return False

            # Duplicate check
            email_clean = email_raw if email_raw and '@' in email_raw else None
            exists, existing_id = self.candidate_exists(email_clean, first_name, last_name)
            if exists:
                self.stats['duplicates'] += 1
                # Upgrade Indeed proxy email to real email if available
                if email_clean and '@indeedemail.com' not in email_clean:
                    with self.engine.connect() as conn:
                        result = conn.execute(text(
                            "SELECT email FROM candidates WHERE id = :id"
                        ), {"id": existing_id})
                        existing_email = result.fetchone()
                        if existing_email and '@indeedemail.com' in (existing_email[0] or ''):
                            conn.execute(text(
                                "UPDATE candidates SET email = :email, updated_at = NOW() WHERE id = :id"
                            ), {"email": email_clean, "id": existing_id})
                            conn.commit()
                            print(f"  Updated email: {first_name} {last_name}")
                return False

            # Phone validation
            phone_clean = self.clean_phone(phone_raw)
            if phone_clean is None:
                self.create_action_item(
                    f"Missing/invalid phone on CSV import: {first_name} {last_name} "
                    f"({self.client_id}) -- verify and update manually."
                )

            # Email validation
            if email_clean is None:
                self.create_action_item(
                    f"Missing email on CSV import: {first_name} {last_name} "
                    f"({self.client_id}) -- verify and update manually."
                )

            # Provisional RWP scoring
            rwp_score, rwp_classification, rwp_source = score_provisional(row)

            # Insert
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO candidates (
                        client_id,
                        email,
                        phone,
                        first_name,
                        last_name,
                        status,
                        background_status,
                        drug_test_status,
                        home_pool,
                        rwp_score,
                        rwp_source,
                        rwp_classification,
                        import_source,
                        source_channel,
                        created_at,
                        updated_at
                    ) VALUES (
                        :client_id, :email, :phone, :first_name, :last_name,
                        'Intake',
                        'Not Started',
                        'Not Started',
                        :home_pool,
                        :rwp_score,
                        :rwp_source,
                        :rwp_classification,
                        'indeed', 'indeed',
                        NOW(), NOW()
                    )
                """), {
                    "client_id":        self.client_id,
                    "email":            email_clean,
                    "phone":            phone_clean,
                    "first_name":       first_name,
                    "last_name":        last_name,
                    "home_pool":        self.home_pool,
                    "rwp_score":        rwp_score,
                    "rwp_source":       rwp_source,
                    "rwp_classification": rwp_classification,
                })
                conn.commit()

            self.stats['imported'] += 1
            return True

        except Exception as e:
            self.stats['errors'].append({
                'type': 'import_error',
                'row': dict(row),
                'error': str(e)
            })
            return False

    def process_csv(self, csv_path: Path) -> Dict:
        self.connect()

        print(f"\nProcessing: {csv_path.name}")
        print(f"  Client:    {self.client_id}")
        print(f"  Pool:      {self.home_pool}")

        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except Exception:
                    dialect = 'excel'
                reader = csv.DictReader(f, dialect=dialect)

                # Log column names on first run for debug
                headers = reader.fieldnames or []
                print(f"  Columns:   {headers}")

                for row in reader:
                    self.stats['total_rows'] += 1
                    self.import_candidate(row)

            print(f"\n  Imported:   {self.stats['imported']}")
            print(f"  Duplicates: {self.stats['duplicates']}")
            print(f"  Actions:    {self.stats['action_items_created']}")
            if self.stats['errors']:
                print(f"  Errors:     {len(self.stats['errors'])}")
                for e in self.stats['errors'][:5]:
                    print(f"    - {e.get('type')}: {e.get('error', '')}")

            return self.stats

        except Exception as e:
            print(f"  Error processing CSV: {e}")
            self.stats['errors'].append({'type': 'file_error', 'error': str(e)})
            return self.stats


def import_csv(csv_path: Path, client_id: str) -> Dict:
    """Convenience function."""
    importer = CSVImporter(client_id)
    return importer.process_csv(csv_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Import Indeed CSV to Supabase -- PEAKATS v3.0')
    parser.add_argument('csv_path', help='Path to candidates.csv')
    parser.add_argument('--client', required=True, help='Client ID (e.g., cbm, gods_vision)')
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        exit(1)

    print("=" * 60)
    print("PEAKATS CSV Import v3.0")
    print("=" * 60)

    try:
        stats = import_csv(csv_path, args.client)
    except ValueError as e:
        print(f"\nERROR: {e}")
        exit(1)

    print("\n" + "=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"Total rows:   {stats['total_rows']}")
    print(f"Imported:     {stats['imported']}")
    print(f"Duplicates:   {stats['duplicates']}")
    print(f"Action items: {stats['action_items_created']}")
    print(f"Errors:       {len(stats['errors'])}")
    print("=" * 60)
