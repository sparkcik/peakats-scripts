#!/usr/bin/env python3
"""
PEAKATS CSV Import Module v2.0
WRITES DIRECTLY TO SUPABASE (PostgreSQL Cloud)

Processes Indeed candidates.csv exports and imports into database
Duplicate check: email + client_id OR name + client_id
"""

import csv
import os
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

# Supabase connection - SINGLE SOURCE OF TRUTH
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require')


class CSVImporter:
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.engine = None
        self.stats = {
            'total_rows': 0,
            'imported': 0,
            'duplicates': 0,
            'errors': []
        }
    
    def connect(self):
        """Connect to Supabase"""
        from sqlalchemy import create_engine
        self.engine = create_engine(SUPABASE_URL)
    
    def clean_phone(self, phone: str) -> str:
        """
        Clean phone number to 10-digit format
        Removes all non-numeric characters and country codes
        """
        if not phone:
            return "0000000000"
        
        # Remove all non-numeric characters
        digits = re.sub(r'\D', '', str(phone))
        
        # Remove leading 1 (US country code)
        if len(digits) == 11 and digits.startswith('1'):
            digits = digits[1:]
        
        # Validate length
        if len(digits) == 10:
            return digits
        elif len(digits) == 0:
            return "0000000000"
        else:
            return digits if digits else "0000000000"
    
    def parse_name(self, full_name: str) -> Tuple[str, str]:
        """
        Parse full name into first and last name
        Returns (first_name, last_name) in Title Case
        """
        if not full_name:
            return "Unknown", "Unknown"
        
        # Split on whitespace
        parts = full_name.strip().split()
        
        if len(parts) == 0:
            return "Unknown", "Unknown"
        elif len(parts) == 1:
            return parts[0].title(), ""
        else:
            # First part is first name, rest is last name
            first_name = parts[0].title()
            last_name = " ".join(parts[1:]).title()
            return first_name, last_name
    
    def candidate_exists(self, email: str, first_name: str, last_name: str) -> Tuple[bool, int]:
        """
        Check if candidate already exists in database for this client
        Checks BOTH email match AND name match to catch duplicates with different emails
        Returns (exists: bool, existing_id: int or None)
        """
        from sqlalchemy import text
        
        with self.engine.connect() as conn:
            # First check by email
            if email:
                result = conn.execute(text("""
                    SELECT id FROM candidates 
                    WHERE LOWER(email) = LOWER(:email) AND client_id = :client_id
                """), {"email": email, "client_id": self.client_id})
                row = result.fetchone()
                if row:
                    return True, row[0]
            
            # Also check by name (catches duplicates with different emails)
            result = conn.execute(text("""
                SELECT id FROM candidates 
                WHERE LOWER(first_name) = LOWER(:first_name) 
                AND LOWER(last_name) = LOWER(:last_name)
                AND client_id = :client_id
            """), {
                "first_name": first_name,
                "last_name": last_name,
                "client_id": self.client_id
            })
            row = result.fetchone()
            if row:
                return True, row[0]
        
        return False, None
    
    def import_candidate(self, row: Dict) -> bool:
        """
        Import a single candidate row
        Returns True if imported, False if skipped (duplicate)
        """
        from sqlalchemy import text
        
        try:
            # Extract and clean data
            # Handle Indeed's actual column names (lowercase, single name field)
            full_name = (row.get('name', '') or row.get('Name', '') or 
                        row.get('Candidate Name', '')).strip()
            email = (row.get('email', '') or row.get('Email', '') or 
                    row.get('Email Address', '')).lower().strip()
            phone = (row.get('phone', '') or row.get('Phone', '') or 
                    row.get('Phone Number', '')).strip()
            applied_date = (row.get('date', '') or row.get('Date', '') or 
                          row.get('Applied Date', '') or row.get('Application Date', '')).strip()
            job_title = (row.get('job title', '') or row.get('Job Title', '') or 
                        row.get('Position', '')).strip()
            candidate_id = row.get('Candidate ID', '') or row.get('ID', '')
            
            # Parse name first (needed for duplicate check)
            first_name, last_name = self.parse_name(full_name)
            
            # Validate - need at least name
            if first_name == "Unknown" and last_name == "Unknown":
                self.stats['errors'].append({
                    'type': 'missing_name',
                    'row': row
                })
                return False
            
            # Check for duplicates (by email OR by name)
            exists, existing_id = self.candidate_exists(email, first_name, last_name)
            if exists:
                self.stats['duplicates'] += 1
                # Optionally update email if we have a better one
                if email and '@indeedemail.com' not in email:
                    with self.engine.connect() as conn:
                        # Check if existing record has Indeed proxy email
                        result = conn.execute(text("""
                            SELECT email FROM candidates WHERE id = :id
                        """), {"id": existing_id})
                        existing_email = result.fetchone()
                        if existing_email and '@indeedemail.com' in (existing_email[0] or ''):
                            # Update with real email
                            conn.execute(text("""
                                UPDATE candidates SET email = :email, updated_at = NOW()
                                WHERE id = :id
                            """), {"email": email, "id": existing_id})
                            conn.commit()
                            print(f"  📧 Updated email for {first_name} {last_name}")
                return False
            
            # Clean phone
            clean_phone = self.clean_phone(phone)
            
            # Insert into Supabase
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO candidates (
                        client_id,
                        email,
                        phone,
                        first_name,
                        last_name,
                        status,
                        import_source,
                        source_channel,
                        created_at,
                        updated_at
                    ) VALUES (
                        :client_id, :email, :phone, :first_name, :last_name,
                        'No Resume', 'indeed', 'indeed',
                        NOW(), NOW()
                    )
                """), {
                    "client_id": self.client_id,
                    "email": email if email else None,
                    "phone": clean_phone,
                    "first_name": first_name,
                    "last_name": last_name,
                })
                conn.commit()
            
            self.stats['imported'] += 1
            return True
            
        except Exception as e:
            self.stats['errors'].append({
                'type': 'import_error',
                'row': row,
                'error': str(e)
            })
            return False
    
    def process_csv(self, csv_path: Path) -> Dict:
        """
        Process entire CSV file
        Returns statistics dictionary
        """
        self.connect()
        
        print(f"\n📂 Processing: {csv_path.name}")
        print(f"   Client: {self.client_id}")
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                # Detect dialect
                sample = f.read(1024)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except:
                    dialect = 'excel'
                
                reader = csv.DictReader(f, dialect=dialect)
                
                for row in reader:
                    self.stats['total_rows'] += 1
                    self.import_candidate(row)
            
            # Print summary
            print(f"\n   ✅ Imported: {self.stats['imported']}")
            print(f"   ⏭️  Duplicates skipped: {self.stats['duplicates']}")
            if self.stats['errors']:
                print(f"   ❌ Errors: {len(self.stats['errors'])}")
            
            return self.stats
            
        except Exception as e:
            print(f"   ❌ Error processing CSV: {e}")
            self.stats['errors'].append({'type': 'file_error', 'error': str(e)})
            return self.stats


def import_csv(csv_path: Path, client_id: str) -> Dict:
    """
    Convenience function to import a CSV file to Supabase
    
    Args:
        csv_path: Path to candidates.csv
        client_id: Client identifier (e.g., 'star_one')
    
    Returns:
        Statistics dictionary
    """
    importer = CSVImporter(client_id)
    return importer.process_csv(csv_path)


# Standalone usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Import Indeed CSV to Supabase')
    parser.add_argument('csv_path', help='Path to candidates.csv')
    parser.add_argument('--client', required=True, help='Client ID (e.g., star_one)')
    
    args = parser.parse_args()
    
    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        exit(1)
    
    print("=" * 60)
    print("PEAKATS CSV Import v2.0 - Direct to Cloud")
    print("=" * 60)
    
    stats = import_csv(csv_path, args.client)
    
    print("\n" + "=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"Total rows: {stats['total_rows']}")
    print(f"Imported: {stats['imported']}")
    print(f"Duplicates: {stats['duplicates']}")
    print(f"Errors: {len(stats['errors'])}")
    print("=" * 60)
