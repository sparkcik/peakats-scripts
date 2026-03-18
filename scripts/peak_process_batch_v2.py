#!/usr/bin/env python3
"""
PEAKATS Batch Processor v3
Main orchestrator for post-email era candidate processing
Scans inbox folders, processes CSV + resumes, manages file movement
NOW WRITES DIRECTLY TO SUPABASE (no SQLite)
"""

import os
import json
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Import our modules - use v2 that writes to Supabase
try:
    from peak_csv_import_v2 import import_csv
    from peak_rig_processor_v2 import process_resumes
except ImportError:
    try:
        # Fallback to non-v2 names if renamed
        from peak_csv_import import import_csv
        from peak_rig_processor import process_resumes
    except ImportError:
        print("ERROR: Could not import PEAKATS modules")
        print("Ensure peak_csv_import_v2.py and peak_rig_processor_v2.py are in the same directory")
        raise

class BatchProcessor:
    def __init__(self, base_path: Path):
        self.base_path = base_path
        self.config = self.load_config()
        self.client_registry = self.load_client_registry()
        self.batch_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        self.resume_limit = 0  # 0 = no limit
        self.create_unmatched = True  # Default: create new records for unmatched resumes
        
        # Paths
        self.inbox_path = base_path / "01_INBOX"
        self.processed_path = base_path / "02_PROCESSED"
        self.fadv_queue_path = base_path / "03_FADV_QUEUE"
        self.errors_path = base_path / "99_ERRORS"
        self.logs_path = base_path / "00_SYSTEM" / "logs"
        
        # Ensure paths exist
        for path in [self.processed_path, self.fadv_queue_path, self.errors_path, self.logs_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        self.log_lines = []
        self.total_stats = {
            'clients_processed': 0,
            'total_candidates': 0,
            'total_resumes': 0,
            'total_errors': 0
        }
    
    def load_config(self) -> Dict:
        """Load system configuration"""
        config_path = self.base_path / "00_SYSTEM" / "config.json"
        if config_path.exists():
            with open(config_path, 'r') as f:
                return json.load(f)
        return {}
    
    def load_client_registry(self) -> Dict:
        """Load client registry"""
        registry_path = self.base_path / "00_SYSTEM" / "client_registry.json"
        if registry_path.exists():
            with open(registry_path, 'r') as f:
                return json.load(f)
        return {'clients': {}}
    
    def log(self, message: str, level: str = "INFO"):
        """Add message to log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_line = f"[{timestamp}] [{level}] {message}"
        self.log_lines.append(log_line)
        print(log_line)
    
    def validate_client(self, client_id: str) -> bool:
        """Validate client exists in registry"""
        if client_id not in self.client_registry.get('clients', {}):
            self.log(f"Unknown client: {client_id}", "WARNING")
            return False
        
        if not self.client_registry['clients'][client_id].get('active', True):
            self.log(f"Client {client_id} is inactive", "WARNING")
            return False
        
        return True
    
    def scan_inbox_for_work(self) -> List[str]:
        """
        Scan inbox folders for clients with files to process
        Returns list of client_ids with work pending
        """
        clients_with_work = []
        
        for client_folder in self.inbox_path.iterdir():
            if not client_folder.is_dir():
                continue
            
            client_id = client_folder.name
            
            # Skip template folder
            if client_id.startswith('_'):
                continue
            
            # Check for candidates.csv
            csv_file = client_folder / "candidates.csv"
            resumes_folder = client_folder / "resumes"
            
            if csv_file.exists() or (resumes_folder.exists() and any(resumes_folder.iterdir())):
                clients_with_work.append(client_id)
        
        return clients_with_work
    
    def process_client_batch(self, client_id: str) -> Dict:
        """
        Process a single client's inbox
        Returns processing statistics
        """
        self.log("=" * 80)
        self.log(f"PROCESSING CLIENT: {client_id.upper()}")
        self.log("=" * 80)
        
        client_inbox = self.inbox_path / client_id
        
        # CSV merge pre-pass: combine multiple CSVs into one
        csv_files = list(client_inbox.glob("*.csv"))

        if len(csv_files) > 1:
            self.log(f"\n[PRE-PASS: CSV MERGE]")
            frames = []
            for cf in csv_files:
                try:
                    frames.append(pd.read_csv(cf))
                except Exception as e:
                    self.log(f"⚠ Could not read {cf.name}: {e}", "WARNING")
            if frames:
                merged = pd.concat(frames, ignore_index=True)
                # Dedup on email (case-insensitive), keep first occurrence
                if 'email' in merged.columns:
                    merged['_email_lower'] = merged['email'].astype(str).str.lower()
                    merged = merged.drop_duplicates(subset='_email_lower', keep='first')
                    merged = merged.drop(columns=['_email_lower'])
                merged_name = f"candidates_merged_{self.batch_timestamp}.csv"
                merged_path = client_inbox / merged_name
                merged.to_csv(merged_path, index=False)
                self.log(f"📎 Merged {len(csv_files)} CSV files → {merged_name} ({len(merged)} rows)")
                # Remove originals
                for cf in csv_files:
                    cf.unlink()
                csv_file = merged_path
            else:
                csv_file = None
        elif len(csv_files) == 1:
            csv_file = csv_files[0]
        else:
            csv_file = None
        
        resumes_folder = client_inbox / "resumes"
        
        batch_stats = {
            'client_id': client_id,
            'csv_stats': None,
            'resume_stats': None,
            'errors': [],
            'status': 'pending'
        }
        
        # Phase 1: Import CSV
        if csv_file and csv_file.exists():
            self.log(f"\n[PHASE 1: CSV IMPORT]")
            self.log(f"Processing: {csv_file.name}")  # Show actual filename
            
            try:
                csv_stats = import_csv(csv_file, client_id)  # v2: no db_path needed
                batch_stats['csv_stats'] = csv_stats
                
                self.log(f"✓ Imported: {csv_stats['imported']} candidates")
                self.log(f"✓ Duplicates skipped: {csv_stats['duplicates']}")
                
                if csv_stats['errors']:
                    self.log(f"⚠ Errors: {len(csv_stats['errors'])}", "WARNING")
                    batch_stats['errors'].extend(csv_stats['errors'])
                
            except Exception as e:
                self.log(f"✗ CSV import failed: {e}", "ERROR")
                batch_stats['errors'].append({'type': 'csv_import_failed', 'error': str(e)})
                batch_stats['status'] = 'failed'
                return batch_stats
        else:
            self.log(f"⚠ No candidates.csv found", "WARNING")
        
        # Phase 2: Process Resumes
        if resumes_folder.exists() and any(resumes_folder.iterdir()):
            self.log(f"\n[PHASE 2: RESUME PROCESSING]")
            if self.resume_limit > 0:
                self.log(f"⚠ LIMIT: Processing max {self.resume_limit} resumes", "WARNING")
            
            # Create archive folder for immediate resume archiving
            processed_folder = self.processed_path / client_id / self.batch_timestamp
            archive_resumes_folder = processed_folder / "resumes"
            archive_resumes_folder.mkdir(parents=True, exist_ok=True)
            
            try:
                resume_stats = process_resumes(
                    resumes_folder, 
                    client_id, 
                    limit=self.resume_limit if self.resume_limit > 0 else None,
                    archive_folder=archive_resumes_folder,
                    create_unmatched=self.create_unmatched
                )
                batch_stats['resume_stats'] = resume_stats
                
                self.log(f"✓ Resumes found: {resume_stats['resumes_found']}")
                self.log(f"✓ Matched (exact): {resume_stats['matched_exact']}")
                self.log(f"✓ Matched (fuzzy): {resume_stats['matched_fuzzy']}")
                
                if resume_stats.get('created', 0) > 0:
                    self.log(f"✓ Created (new): {resume_stats['created']}")
                
                if resume_stats['unmatched'] > 0:
                    self.log(f"⚠ Unmatched: {resume_stats['unmatched']}", "WARNING")
                
                if resume_stats['api_errors'] > 0:
                    self.log(f"⚠ API errors: {resume_stats['api_errors']}", "WARNING")
                
                # Score distribution
                self.log(f"\n[RWP SCORE DISTRIBUTION]")
                for score, count in resume_stats['score_distribution'].items():
                    if count > 0:
                        score_name = {
                            '11.0': 'FEDEX_ACTIVE',
                            '10.0': 'FEDEX_FORMER',
                            '9.0': 'DELIVERY_EXP',
                            '6.0': 'GEN_PROF_DRIVING',
                            '3.0': 'LOW_RELEVANCE',
                            '0.0': 'UNWEIGHTED'
                        }.get(score, 'UNKNOWN')
                        self.log(f"  {score_name} ({score}): {count} candidates")
                
            except Exception as e:
                self.log(f"✗ Resume processing failed: {e}", "ERROR")
                batch_stats['errors'].append({'type': 'resume_processing_failed', 'error': str(e)})
                batch_stats['status'] = 'partial'
                return batch_stats
        else:
            self.log(f"⚠ No resumes folder found or empty", "WARNING")
        
        # Phase 3: File Management
        self.log(f"\n[PHASE 3: FILE CLEANUP]")
        
        try:
            # Processed folder was already created for resume archiving
            processed_folder = self.processed_path / client_id / self.batch_timestamp
            processed_folder.mkdir(parents=True, exist_ok=True)
            
            # Resumes are already moved during processing - just log it
            dest_resumes = processed_folder / "resumes"
            if dest_resumes.exists() and any(dest_resumes.iterdir()):
                self.log(f"✓ Resumes archived to: {dest_resumes}")
            
            # Recreate empty resumes folder for next batch if needed
            if not resumes_folder.exists():
                resumes_folder.mkdir(parents=True, exist_ok=True)
            self.log(f"✓ Inbox folder ready: {resumes_folder}")
            
            # Move CSV to FADV queue with prefix
            if csv_file.exists():
                fadv_prefix = self.client_registry['clients'][client_id].get('fadv_prefix', client_id.upper())
                fadv_filename = f"{fadv_prefix}_{self.batch_timestamp}.csv"
                fadv_dest = self.fadv_queue_path / fadv_filename
                
                shutil.copy(str(csv_file), str(fadv_dest))
                self.log(f"✓ Copied CSV to FADV queue: {fadv_filename}")
                
                # Also archive original CSV
                shutil.move(str(csv_file), str(processed_folder / "candidates.csv"))
                self.log(f"✓ Archived CSV to: {processed_folder}")
            
            batch_stats['status'] = 'success'
            
        except Exception as e:
            self.log(f"✗ File management failed: {e}", "ERROR")
            batch_stats['errors'].append({'type': 'file_management_failed', 'error': str(e)})
            batch_stats['status'] = 'partial'
        
        # Phase 4: Check for orphaned candidates
        self.log(f"\n[PHASE 4: ORPHAN CHECK]")
        orphans = self.find_orphaned_candidates(client_id)
        csv_only = self.find_csv_only_candidates(client_id)

        if orphans:
            self.log(f"⚠ Found {len(orphans)} orphaned candidates (no resume file)", "WARNING")
            for orphan in orphans[:5]:
                self.log(f"  - {orphan['first_name']} {orphan['last_name']} ({orphan['email']})")
            batch_stats['orphans'] = orphans
        else:
            self.log(f"✓ No orphaned candidates")

        if csv_only:
            self.log(f"⚠ Found {len(csv_only)} CSV-only candidates (no resume scored)", "WARNING")
            for c in csv_only[:5]:
                self.log(f"  - {c['first_name']} {c['last_name']} ({c['email']})")
            batch_stats['csv_only'] = csv_only
        else:
            self.log(f"✓ No CSV-only candidates")
        
        return batch_stats
    
    def find_orphaned_candidates(self, client_id: str) -> List[Dict]:
        """Find candidates with no resume processed"""
        from sqlalchemy import create_engine, text
        
        DB_URL = "postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"
        engine = create_engine(DB_URL)
        
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, first_name, last_name, email
                FROM candidates
                WHERE client_id = :client_id
                AND (rwp_score IS NULL OR rwp_score = 0)
                AND created_at::timestamp >= NOW() - INTERVAL '1 day'
            """), {"client_id": client_id})
            
            return [{"id": row[0], "first_name": row[1], "last_name": row[2], "email": row[3]} for row in result]

    def find_csv_only_candidates(self, client_id: str) -> List[Dict]:
        """Find candidates imported from CSV but never resume-scored (status still Intake, no RWP)"""
        from sqlalchemy import create_engine, text

        DB_URL = "postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"
        engine = create_engine(DB_URL)

        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, first_name, last_name, email
                FROM candidates
                WHERE client_id = :client_id
                AND status = 'Intake'
                AND (rwp_score IS NULL OR rwp_score = 0)
                AND resume_filename IS NULL
            """), {"client_id": client_id})

            return [{"id": row[0], "first_name": row[1], "last_name": row[2], "email": row[3]} for row in result]

    def generate_processing_report(self, all_stats: List[Dict]):
        """Generate comprehensive processing report"""
        report_lines = []
        
        report_lines.append("=" * 80)
        report_lines.append("PEAKATS BATCH PROCESSING REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Operator: {os.environ.get('USER', 'unknown')}@peakrecruitingco.com")
        report_lines.append("")
        
        # Process each client
        for stats in all_stats:
            client_id = stats['client_id']
            client_name = self.client_registry['clients'][client_id]['display_name']
            
            report_lines.append("=" * 80)
            report_lines.append(f"CLIENT: {client_name} ({client_id})")
            report_lines.append("=" * 80)
            
            # CSV Import Stats
            if stats['csv_stats']:
                csv = stats['csv_stats']
                report_lines.append("\n[PHASE 1: CSV IMPORT]")
                report_lines.append(f"✓ Imported: {csv['imported']} candidates")
                report_lines.append(f"✓ Duplicates skipped: {csv['duplicates']}")
                
                if csv['errors']:
                    report_lines.append(f"⚠ Errors: {len(csv['errors'])}")
            
            # Resume Processing Stats
            if stats['resume_stats']:
                res = stats['resume_stats']
                report_lines.append("\n[PHASE 2: RESUME PROCESSING]")
                report_lines.append(f"✓ Resumes found: {res['resumes_found']}")
                report_lines.append(f"✓ Matched (exact): {res['matched_exact']}")
                report_lines.append(f"✓ Matched (fuzzy): {res['matched_fuzzy']}")
                report_lines.append(f"✗ Unmatched: {res['unmatched']}")
                
                report_lines.append("\n[RWP SCORE DISTRIBUTION]")
                for score, count in res['score_distribution'].items():
                    if count > 0:
                        score_name = {
                            '10.0': 'FEDEX_DIRECT',
                            '8.5': 'PRO_COURIER',
                            '6.0': 'GEN_PROF_DRIVING',
                            '3.0': 'LOW_RELEVANCE',
                            '0.0': 'UNWEIGHTED'
                        }.get(score, 'UNKNOWN')
                        report_lines.append(f"  {score_name} ({score}): {count} candidates")
            
            # Orphans
            if stats.get('orphans'):
                report_lines.append("\n[ORPHANED CANDIDATES - REQUIRES ATTENTION]")
                report_lines.append(f"! {len(stats['orphans'])} candidates imported but no resume found:")
                for orphan in stats['orphans'][:10]:
                    report_lines.append(f"  - {orphan['first_name']} {orphan['last_name']} ({orphan['email']})")
            
            # Status
            report_lines.append(f"\nSTATUS: {stats['status'].upper()}")
            report_lines.append("")
        
        # Summary
        report_lines.append("=" * 80)
        report_lines.append("OVERALL SUMMARY")
        report_lines.append("=" * 80)
        report_lines.append(f"Clients processed: {len(all_stats)}")
        
        total_imported = sum(s['csv_stats']['imported'] for s in all_stats if s['csv_stats'])
        total_scored = sum(s['resume_stats']['matched_exact'] + s['resume_stats']['matched_fuzzy'] 
                          for s in all_stats if s['resume_stats'])
        
        report_lines.append(f"Total candidates imported: {total_imported}")
        report_lines.append(f"Total candidates scored: {total_scored}")
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def save_log(self, content: str):
        """Save processing log to file"""
        log_file = self.logs_path / f"{self.batch_timestamp}_processing.log"
        
        with open(log_file, 'w') as f:
            f.write(content)
        
        self.log(f"\n✓ Log saved to: {log_file}")
    
    def run(self):
        """Main execution routine"""
        print("\n" + "=" * 80)
        print("PEAKATS BATCH PROCESSOR")
        print("=" * 80)
        print(f"Timestamp: {self.batch_timestamp}")
        print(f"Base path: {self.base_path}")
        print("")
        
        # Scan for work
        clients_to_process = self.scan_inbox_for_work()
        
        if not clients_to_process:
            self.log("No clients with pending work found in inbox")
            self.log("Exiting...")
            return
        
        self.log(f"Found {len(clients_to_process)} clients with pending work:")
        for client in clients_to_process:
            self.log(f"  - {client}")
        
        # Process each client
        all_stats = []
        
        for client_id in clients_to_process:
            # Validate client
            if not self.validate_client(client_id):
                continue
            
            # Process
            stats = self.process_client_batch(client_id)
            all_stats.append(stats)
        
        # Generate report
        report = self.generate_processing_report(all_stats)
        
        # Save log
        self.save_log(report)
        
        # Print final summary
        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE")
        print("=" * 80)
        print(f"Clients processed: {len(all_stats)}")
        print(f"Log file: {self.logs_path / f'{self.batch_timestamp}_processing.log'}")
        print("=" * 80)

def main():
    """Entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PEAKATS Batch Processor')
    parser.add_argument('--batch', action='store_true', help='Run batch processing')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of resumes to process (0 = no limit)')
    parser.add_argument('--skip-unmatched', action='store_true', help='Skip creating records for unmatched resumes (old behavior)')
    args = parser.parse_args()
    
    # Auto-detect PEAKATS location from script location
    script_dir = Path(__file__).parent.resolve()
    
    # If script is in 'scripts' folder, go up one level
    if script_dir.name == 'scripts':
        base_path = script_dir.parent
    elif (script_dir / "00_SYSTEM").exists():
        base_path = script_dir
    else:
        # Fallback to hardcoded path
        base_path = Path.home() / "Library" / "CloudStorage" / "GoogleDrive-charles@thefoundry.llc" / "My Drive" / "PEAK" / "#PEAKATS"
        if not base_path.exists():
            base_path = Path.home() / "PEAK" / "PEAKATS"
    
    if not (base_path / "00_SYSTEM").exists():
        print("ERROR: PEAKATS not found")
        print(f"Searched: {script_dir}")
        print(f"Searched: {base_path}")
        print("Run this script from the PEAKATS folder containing 00_SYSTEM/")
        return
    
    processor = BatchProcessor(base_path)
    processor.resume_limit = args.limit
    
    # Default is to create unmatched, flag disables it
    if args.skip_unmatched:
        processor.create_unmatched = False
        print("⏭️  SKIP UNMATCHED MODE: Unmatched resumes will go to review report only")
    
    processor.run()

if __name__ == "__main__":
    main()
