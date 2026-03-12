"""
PEAK Resume Finder — Checks local Drive for missing CBM resumes
Run from your Mac where Google Drive is synced.

Usage:
  python3 find_missing_resumes.py
"""

import os
import csv
import glob

# === CONFIG ===
# Adjust this to your Google Drive PEAKATS root
PEAKATS_ROOT = "/Users/charles/Library/CloudStorage/GoogleDrive-charles@thefoundry.llc/My Drive/PEAK/#PEAKATS"

# The 152 candidates missing resumes (from CBM_NO_RESUME_152.csv)
CSV_PATH = "CBM_NO_RESUME_152.csv"  # Place in same directory, or use full path

# === SCAN ===
def find_all_pdfs(root):
    """Recursively find all PDFs under PEAKATS"""
    pdfs = {}
    for path in glob.glob(os.path.join(root, "**", "*.pdf"), recursive=True):
        filename = os.path.basename(path).lower()
        pdfs[filename] = path
    return pdfs

def main():
    print("=" * 60)
    print("PEAK RESUME FINDER")
    print("=" * 60)
    
    # Find all PDFs on disk
    print(f"\nScanning: {PEAKATS_ROOT}")
    all_pdfs = find_all_pdfs(PEAKATS_ROOT)
    print(f"Found {len(all_pdfs)} total PDFs on Drive\n")
    
    # Load missing candidates
    candidates = []
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            candidates.append(row)
    
    print(f"Checking {len(candidates)} candidates...\n")
    
    found = []
    not_found = []
    
    for c in candidates:
        expected = c['EXPECTED_RESUME_FILENAME'].lower()
        
        if expected in all_pdfs:
            found.append({**c, 'FOUND_PATH': all_pdfs[expected]})
        else:
            # Try partial match (first + last name anywhere in filename)
            first = c['FIRST_NAME'].lower().strip()
            last = c['LAST_NAME'].lower().strip()
            partial = [p for fn, p in all_pdfs.items() if first in fn and last in fn]
            
            if partial:
                found.append({**c, 'FOUND_PATH': partial[0]})
            else:
                not_found.append(c)
    
    # Results
    print("=" * 60)
    print(f"FOUND:     {len(found)}")
    print(f"NOT FOUND: {len(not_found)}")
    print("=" * 60)
    
    # Export found
    if found:
        with open('CBM_RESUMES_FOUND.csv', 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=list(found[0].keys()))
            w.writeheader()
            w.writerows(found)
        print(f"\n✅ Saved: CBM_RESUMES_FOUND.csv ({len(found)} records)")
    
    # Export not found
    if not_found:
        with open('CBM_RESUMES_NOT_FOUND.csv', 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=list(not_found[0].keys()))
            w.writeheader()
            w.writerows(not_found)
        print(f"❌ Saved: CBM_RESUMES_NOT_FOUND.csv ({len(not_found)} records)")
    
    print("\nDone.")

if __name__ == "__main__":
    main()
