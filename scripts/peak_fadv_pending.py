#!/usr/bin/env python3
"""
PEAKATS FADV PENDING EXPORT
Finds candidates NOT YET submitted to FADV and exports to CSV

Usage:
    python3 peak_fadv_pending.py                    # All clients, score >= 9
    python3 peak_fadv_pending.py --client star_one  # Specific client
    python3 peak_fadv_pending.py --min-score 6      # Lower score threshold
    python3 peak_fadv_pending.py --show-all         # Include already submitted
"""

import os
import csv
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text

# Database URL (same as peak_rig_processor_v2.py)
DB_URL = "postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

# Output directory
OUTPUT_DIR = Path.home() / "Library/CloudStorage/GoogleDrive-charles@thefoundry.llc/My Drive/PEAK/#PEAKATS/03_FADV_QUEUE"

def get_fadv_pending(client_id=None, min_score=9.0, show_all=False):
    """
    Query Supabase for candidates pending FADV submission
    
    Args:
        client_id: Filter by specific client (None = all clients)
        min_score: Minimum RWP score to include (default 9.0)
        show_all: If True, include candidates already submitted to FADV
    
    Returns:
        List of candidate dicts
    """
    engine = create_engine(DB_URL)
    
    # Build query
    # Check for candidates where FADV fields are empty/null
    query = """
        SELECT 
            id,
            client_id,
            first_name,
            last_name,
            email,
            phone,
            rwp_score,
            rwp_classification,
            rwp_rationale,
            status,
            profile_status,
            background_status,
            drug_test_status,
            created_at
        FROM candidates
        WHERE rwp_score >= :min_score
    """
    
    params = {"min_score": min_score}
    
    # Filter by client if specified
    if client_id:
        query += " AND client_id = :client_id"
        params["client_id"] = client_id
    
    # Filter for pending FADV (not yet submitted)
    if not show_all:
        query += """
            AND (
                profile_status IS NULL 
                OR profile_status = ''
                OR profile_status = 'Not Started'
                OR background_status IS NULL
                OR background_status = ''
            )
        """
    
    # Always exclude terminal statuses (unless --show-all)
    if not show_all:
        query += """
            AND (
                status NOT IN ('Hired', 'Transferred', 'Rejected')
                OR status IS NULL
            )
        """
    
    query += " ORDER BY rwp_score DESC, client_id, last_name"
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        columns = result.keys()
        rows = result.fetchall()
    
    engine.dispose()
    
    # Convert to list of dicts
    candidates = []
    for row in rows:
        candidate = dict(zip(columns, row))
        candidates.append(candidate)
    
    return candidates

def export_to_csv(candidates, client_id=None):
    """Export candidates to CSV file"""
    
    if not candidates:
        print("✅ No candidates pending FADV submission!")
        return None
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    if client_id:
        filename = f"FADV_PENDING_{client_id}_{timestamp}.csv"
    else:
        filename = f"FADV_PENDING_ALL_{timestamp}.csv"
    
    filepath = OUTPUT_DIR / filename
    
    # Define columns for export (FADV-friendly format)
    export_columns = [
        'first_name',
        'last_name', 
        'email',
        'phone',
        'client_id',
        'rwp_score',
        'rwp_classification',
        'rwp_rationale',
        'status'
    ]
    
    # Write CSV
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=export_columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(candidates)
    
    return filepath

def print_summary(candidates, client_id=None, show_all=False):
    """Print summary report"""
    
    print("\n" + "=" * 70)
    print("📋 FADV PENDING SUBMISSION REPORT")
    print("=" * 70)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if client_id:
        print(f"Client Filter: {client_id}")
    
    if not show_all:
        print(f"Excluding: Hired, Transferred, Rejected")
    else:
        print(f"Showing: ALL candidates (including Hired/Transferred/Rejected)")
    
    print(f"\n📊 Total Candidates Pending: {len(candidates)}")
    
    # Group by client
    by_client = {}
    for c in candidates:
        client = c['client_id']
        if client not in by_client:
            by_client[client] = []
        by_client[client].append(c)
    
    print(f"\n📋 By Client:")
    for client, cands in sorted(by_client.items(), key=lambda x: -len(x[1])):
        scores = [c['rwp_score'] for c in cands if c['rwp_score']]
        avg_score = sum(scores) / len(scores) if scores else 0
        print(f"   {client}: {len(cands)} candidates (avg score: {avg_score:.1f})")
    
    # Score distribution
    print(f"\n🎯 By RWP Score:")
    score_dist = {}
    for c in candidates:
        score = c['rwp_score']
        if score not in score_dist:
            score_dist[score] = 0
        score_dist[score] += 1
    
    for score in sorted(score_dist.keys(), reverse=True):
        classification = {
            11.0: "FEDEX_ACTIVE",
            10.0: "FEDEX_FORMER", 
            9.0: "DELIVERY_EXP",
            6.0: "GEN_PROF_DRIVING",
            3.0: "LOW_RELEVANCE",
            0.0: "UNWEIGHTED"
        }.get(score, "UNKNOWN")
        print(f"   {score} ({classification}): {score_dist[score]}")
    
    # Show top candidates
    print(f"\n🔝 Top 10 Priority Candidates:")
    for i, c in enumerate(candidates[:10], 1):
        name = f"{c['first_name']} {c['last_name']}"
        print(f"   {i}. {name:<25} | {c['rwp_score']} | {c['client_id']}")
    
    print("=" * 70)

def main():
    import sys
    
    # Parse arguments
    client_id = None
    min_score = 9.0
    show_all = False
    
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--client' and i + 1 < len(args):
            client_id = args[i + 1]
            i += 2
        elif args[i] == '--min-score' and i + 1 < len(args):
            min_score = float(args[i + 1])
            i += 2
        elif args[i] == '--show-all':
            show_all = True
            i += 1
        elif args[i] == '--help':
            print(__doc__)
            return
        else:
            i += 1
    
    print("\n🔍 Querying Supabase for candidates pending FADV...")
    
    # Query database
    candidates = get_fadv_pending(client_id, min_score, show_all)
    
    # Print summary
    print_summary(candidates, client_id, show_all)
    
    # Export to CSV
    if candidates:
        filepath = export_to_csv(candidates, client_id)
        print(f"\n💾 Exported to: {filepath}")
        
        # Open file
        os.system(f'open "{filepath}"')
    
    print("\n✅ Done!")

if __name__ == "__main__":
    main()
