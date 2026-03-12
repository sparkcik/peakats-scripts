#!/usr/bin/env python3
"""
PEAK Resume Recovery — Stage & Score
Collects 72 scattered CBM resumes into a staging folder,
then runs peak_rig_processor_v2.py against them.

Usage:
  python3 score_missing_resumes.py
"""

import os
import csv
import shutil
from pathlib import Path

# === CONFIG ===
PEAKATS_ROOT = Path("/Users/charles/Library/CloudStorage/GoogleDrive-charles@thefoundry.llc/My Drive/PEAK/#PEAKATS")
FOUND_CSV = PEAKATS_ROOT / "CBM_RESUMES_FOUND.csv"
STAGING_DIR = PEAKATS_ROOT / "_staging_rwp"
SCRIPTS_DIR = PEAKATS_ROOT / "scripts"

def main():
    print("=" * 60)
    print("PEAK RESUME RECOVERY — STAGE & SCORE")
    print("=" * 60)
    
    # Step 1: Read found resumes CSV
    candidates = []
    with open(FOUND_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            candidates.append(row)
    
    print(f"\nResumes to recover: {len(candidates)}")
    
    # Step 2: Create staging folder
    if STAGING_DIR.exists():
        shutil.rmtree(STAGING_DIR)
    STAGING_DIR.mkdir(parents=True)
    print(f"Staging folder: {STAGING_DIR}")
    
    # Step 3: Copy resumes to staging
    copied = 0
    failed = 0
    for c in candidates:
        src = Path(c['FOUND_PATH'].strip())
        if src.exists():
            dst = STAGING_DIR / src.name
            # Handle duplicate filenames
            if dst.exists():
                stem = dst.stem
                suffix = dst.suffix
                counter = 2
                while dst.exists():
                    dst = STAGING_DIR / f"{stem}_{counter}{suffix}"
                    counter += 1
            shutil.copy2(str(src), str(dst))
            copied += 1
        else:
            print(f"  ⚠️  Not found: {src}")
            failed += 1
    
    print(f"\n✅ Copied: {copied}")
    if failed:
        print(f"❌ Failed: {failed}")
    
    # Step 4: Run RIG processor
    print(f"\n{'=' * 60}")
    print(f"LAUNCHING RIG PROCESSOR")
    print(f"Folder: {STAGING_DIR}")
    print(f"Client: cbm")
    print(f"{'=' * 60}\n")
    
    os.system(f'cd "{SCRIPTS_DIR}" && python3 peak_rig_processor_v2.py "{STAGING_DIR}" cbm')
    
    # Step 5: Cleanup staging
    print(f"\n{'=' * 60}")
    cleanup = input("Delete staging folder? (y/n): ").strip().lower()
    if cleanup == 'y':
        shutil.rmtree(STAGING_DIR)
        print("✅ Staging folder deleted")
    else:
        print(f"📁 Staging folder kept at: {STAGING_DIR}")
    
    print("\nDone.")

if __name__ == "__main__":
    main()
