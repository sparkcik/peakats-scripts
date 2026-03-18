#!/usr/bin/env python3
"""
PEAKATS Client Setup Script
Creates folder structure and registry entry for a new client.

Usage: python3 peak_setup_client.py <client_id> <display_name> <fadv_prefix>
"""

import sys
import os
import re
import json
from pathlib import Path
from datetime import date


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


def validate_client_id(client_id: str) -> bool:
    """client_id must be lowercase letters, digits, and underscores only."""
    return bool(re.fullmatch(r"[a-z0-9_]+", client_id))


def main():
    if len(sys.argv) != 4:
        print("Usage: python3 peak_setup_client.py <client_id> <display_name> <fadv_prefix>")
        sys.exit(1)

    client_id = sys.argv[1]
    display_name = sys.argv[2]
    fadv_prefix = sys.argv[3]

    # --- Validate ---
    if not validate_client_id(client_id):
        print(f"ERROR: Invalid client_id '{client_id}'")
        print("Must be lowercase letters, digits, and underscores only (no spaces).")
        sys.exit(1)

    base = get_base_path()
    registry_path = base / "00_SYSTEM" / "client_registry.json"

    # --- Check registry for duplicates ---
    with open(registry_path, "r") as f:
        registry = json.load(f)

    if client_id in registry.get("clients", {}):
        print(f"ERROR: Client '{client_id}' already exists in registry.")
        print(f"  display_name: {registry['clients'][client_id].get('display_name')}")
        print("Aborting — no changes made.")
        sys.exit(1)

    # --- Create folders ---
    folders = [
        base / "01_INBOX" / client_id,
        base / "01_INBOX" / client_id / "resumes",
        base / "02_PROCESSED" / client_id,
        base / "03_FADV_QUEUE" / client_id,
        base / "04_FADV_UPDATES" / client_id,
        base / "04_FADV_UPDATES" / client_id / "archive",
        base / "05_CANDIDATE_DOCS" / client_id,
    ]

    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)

    # --- Update registry ---
    registry["clients"][client_id] = {
        "display_name": display_name,
        "active": True,
        "fadv_prefix": fadv_prefix,
        "notes": f"New client added {date.today().isoformat()}",
    }
    registry["last_updated"] = f"{date.today().isoformat()}T00:00:00.000000Z"

    with open(registry_path, "w") as f:
        json.dump(registry, f, indent=2)
        f.write("\n")

    # --- Checklist ---
    print(f"\nClient '{client_id}' setup complete.\n")
    print(f"  display_name : {display_name}")
    print(f"  fadv_prefix  : {fadv_prefix}")
    print(f"  base_path    : {base}\n")
    print("Checklist:")
    print("  ✅ Folders created")
    print("  ✅ Registry updated")
    print("  ⬜ Add to peakats_context.active_clients in Supabase")
    print("  ⬜ Configure FADV account (CSP ID, package, company, facility, position)")
    print("  ⬜ Get access to client Indeed employer account")
    print("  ⬜ Run: peak-process --batch to verify client processes without errors")


if __name__ == "__main__":
    main()
