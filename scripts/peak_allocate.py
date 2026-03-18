#!/usr/bin/env python3
"""
PEAKATS Batch Allocator
Splits a batch of Indeed CSVs and PDFs across multiple clients at a station.

Usage:
  python3 peak_allocate.py --station norcross --inbox ~/path/to/batch/
  python3 peak_allocate.py --station norcross --clients cbm,fitzpatrick --inbox ~/path/to/batch/
  python3 peak_allocate.py --station norcross --inbox ~/path/to/batch/ --dry-run
"""

import argparse
import json
import os
import re
import shutil
import sys
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd


def get_base_path() -> Path:
    """Resolve PEAKATS base path from env or standard fallback."""
    env = os.environ.get("PEAKATS_DIR")
    if env:
        p = Path(env)
        if (p / "00_SYSTEM").exists():
            return p

    fallback = (
        Path.home()
        / "Library"
        / "CloudStorage"
        / "GoogleDrive-charles@thefoundry.llc"
        / "My Drive"
        / "PEAK"
        / "#PEAKATS"
    )
    if (fallback / "00_SYSTEM").exists():
        return fallback

    print("ERROR: Cannot locate PEAKATS base directory.")
    print("Set PEAKATS_DIR or ensure the standard Drive path exists.")
    sys.exit(1)


def load_registry(base: Path) -> dict:
    path = base / "00_SYSTEM" / "client_registry.json"
    with open(path) as f:
        return json.load(f)


def extract_name_from_filename(filename: str) -> str:
    """Extract candidate name from a resume filename."""
    name = filename
    # Remove extension
    name = re.sub(r'\.(pdf|PDF)$', '', name)
    # Remove common suffixes
    name = re.sub(r'[_\s-]*(resume|cv|Resume|CV|RESUME)$', '', name)
    # CamelCase → spaces
    name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
    # Underscores/dashes → spaces
    name = re.sub(r'[_-]', ' ', name)
    # Collapse whitespace
    name = ' '.join(name.split()).strip()
    return name.lower()


def fuzzy_score(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def main():
    parser = argparse.ArgumentParser(description="PEAKATS Batch Allocator")
    parser.add_argument("--station", required=True, help="Station name (e.g. norcross)")
    parser.add_argument("--inbox", required=True, help="Path to batch folder with CSVs and PDFs")
    parser.add_argument("--clients", help="Comma-separated client subset (must belong to station)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without moving files")
    args = parser.parse_args()

    base = get_base_path()
    registry = load_registry(base)
    inbox = Path(args.inbox).expanduser().resolve()

    if not inbox.exists() or not inbox.is_dir():
        print(f"ERROR: Inbox folder not found: {inbox}")
        sys.exit(1)

    # --- Resolve target clients ---
    station_clients = [
        cid for cid, entry in registry.get("clients", {}).items()
        if entry.get("station") == args.station and entry.get("active", True)
    ]

    if not station_clients:
        print(f"ERROR: No active clients found for station '{args.station}'")
        sys.exit(1)

    if args.clients:
        requested = [c.strip() for c in args.clients.split(",")]
        for cid in requested:
            if cid not in registry.get("clients", {}):
                print(f"ERROR: Client '{cid}' not in registry")
                sys.exit(1)
            if registry["clients"][cid].get("station") != args.station:
                print(f"ERROR: Client '{cid}' does not belong to station '{args.station}'")
                sys.exit(1)
        target_clients = requested
    else:
        target_clients = sorted(station_clients)

    print(f"\nStation: {args.station}")
    print(f"Clients: {', '.join(target_clients)}")
    if args.dry_run:
        print("MODE: DRY RUN (no files will be moved)\n")
    print()

    # --- Merge CSVs ---
    csv_files = list(inbox.glob("*.csv"))
    if not csv_files:
        print("ERROR: No CSV files found in inbox")
        sys.exit(1)

    frames = []
    for cf in csv_files:
        try:
            frames.append(pd.read_csv(cf))
        except Exception as e:
            print(f"⚠ Could not read {cf.name}: {e}")

    if not frames:
        print("ERROR: No readable CSV files")
        sys.exit(1)

    merged = pd.concat(frames, ignore_index=True)
    if "email" in merged.columns:
        merged["_email_lower"] = merged["email"].astype(str).str.lower()
        merged = merged.drop_duplicates(subset="_email_lower", keep="first")
        merged = merged.drop(columns=["_email_lower"])

    total_candidates = len(merged)
    print(f"📊 Merged {len(csv_files)} CSV(s) → {total_candidates} candidates (deduped)")

    # --- Round-robin split ---
    client_count = len(target_clients)
    allocations = {cid: [] for cid in target_clients}

    for idx, (_, row) in enumerate(merged.iterrows()):
        cid = target_clients[idx % client_count]
        allocations[cid].append(row)

    # Build name lookup per client for PDF matching
    client_names = {}
    for cid, rows in allocations.items():
        names = []
        for row in rows:
            full_name = str(row.get("name", "") or row.get("Name", "") or
                           row.get("Candidate Name", "")).strip()
            if full_name:
                names.append(full_name)
        client_names[cid] = names

    # --- Route PDFs ---
    pdf_files = list(inbox.glob("*.pdf")) + list(inbox.glob("*.PDF"))
    pdf_routed = {cid: 0 for cid in target_clients}
    unmatched_pdfs = []
    fallback_client = target_clients[0]

    print(f"\n📄 Routing {len(pdf_files)} PDFs...\n")

    for pdf in sorted(pdf_files, key=lambda p: p.name.lower()):
        pdf_name = extract_name_from_filename(pdf.name)

        best_client = None
        best_score = 0.0
        best_match_name = None

        for cid, names in client_names.items():
            for candidate_name in names:
                score = fuzzy_score(pdf_name, candidate_name)
                if score > best_score:
                    best_score = score
                    best_client = cid
                    best_match_name = candidate_name

        if best_score >= 0.75 and best_client:
            dest_dir = base / "01_INBOX" / best_client / "resumes"
            dest_dir.mkdir(parents=True, exist_ok=True)
            print(f"  ✅ {pdf.name} → {best_client} (matched: {best_match_name}, {best_score:.0%})")
            if not args.dry_run:
                shutil.move(str(pdf), str(dest_dir / pdf.name))
            pdf_routed[best_client] += 1
        else:
            dest_dir = base / "01_INBOX" / fallback_client / "resumes"
            dest_dir.mkdir(parents=True, exist_ok=True)
            print(f"  ⚠️  {pdf.name} → {fallback_client} (unmatched, fallback)")
            if not args.dry_run:
                shutil.move(str(pdf), str(dest_dir / pdf.name))
            pdf_routed[fallback_client] += 1
            unmatched_pdfs.append(pdf.name)

    # --- Write CSVs ---
    print(f"\n📝 Writing candidate CSVs...\n")

    for cid, rows in allocations.items():
        if not rows:
            continue

        client_inbox = base / "01_INBOX" / cid
        client_inbox.mkdir(parents=True, exist_ok=True)
        (client_inbox / "resumes").mkdir(exist_ok=True)

        csv_path = client_inbox / "candidates.csv"
        new_df = pd.DataFrame(rows)

        # Append to existing CSV if present, dedup
        if csv_path.exists():
            try:
                existing = pd.read_csv(csv_path)
                combined = pd.concat([existing, new_df], ignore_index=True)
                if "email" in combined.columns:
                    combined["_email_lower"] = combined["email"].astype(str).str.lower()
                    combined = combined.drop_duplicates(subset="_email_lower", keep="first")
                    combined = combined.drop(columns=["_email_lower"])
                new_df = combined
                print(f"  📎 {cid}: appended to existing CSV ({len(new_df)} total rows)")
            except Exception as e:
                print(f"  ⚠ {cid}: could not read existing CSV, overwriting: {e}")
        else:
            print(f"  📄 {cid}: writing {len(new_df)} rows")

        if not args.dry_run:
            new_df.to_csv(csv_path, index=False)

    # --- Summary ---
    print(f"\n{'=' * 50}")
    print("ALLOCATION SUMMARY")
    print(f"{'=' * 50}")
    print(f"Station: {args.station}")
    print(f"Total candidates: {total_candidates}")
    print(f"Total PDFs: {len(pdf_files)}")
    print()

    name_width = max(len(cid) for cid in target_clients)
    for cid in target_clients:
        count = len(allocations[cid])
        pdfs = pdf_routed[cid]
        print(f"  {cid:<{name_width}}  {count} candidates, {pdfs} PDFs routed")

    if unmatched_pdfs:
        print(f"\n  ⚠ Unmatched PDFs: {len(unmatched_pdfs)} (moved to {fallback_client}/resumes/ as fallback)")
    else:
        print(f"\n  ✅ All PDFs matched")

    if args.dry_run:
        print(f"\n  🔒 DRY RUN — no files were moved")
    else:
        print(f"\n  ✅ Ready to run: peak-process --batch")


if __name__ == "__main__":
    main()
