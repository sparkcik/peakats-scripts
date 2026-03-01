#!/usr/bin/env python3
"""
FADV Entry Bot — Multi-Client
Submits New Subject forms on enterprise.fadv.com for any configured client.
Reads live from Supabase. Logs results to fadv_entry_results/.

Setup (one-time):
    pip install playwright sqlalchemy psycopg2-binary --break-system-packages
    python3 -m playwright install chromium

Run:
    export FADV_USER=KSMITH
    export FADV_PASS=your_password_here

    python3 fadv_entry_bot.py --client solpac
    python3 fadv_entry_bot.py --client solpac --test      # First 5 only
    python3 fadv_entry_bot.py --client cbm
    python3 fadv_entry_bot.py --list                      # Show available clients
"""

import os
import sys
import csv
import time
import re
import argparse
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, text
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─── DATABASE ─────────────────────────────────────────────────────────────────

DB_URL = "postgresql://postgres.eyopvsmsvbgfuffscfom:peakats2026@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

FADV_URL = "https://enterprise.fadv.com/pub/l/shell/shell.jsp"

# ─── CLIENT CONFIGS ───────────────────────────────────────────────────────────
# Add a new block here when onboarding each client.
# Values come from inspecting the FADV portal dropdowns for that client's account.
# All values are strings matching the <option value="..."> in the portal HTML.

CLIENT_CONFIGS = {

    "solpac": {
        "display_name":      "Solpac (Braselton, GA)",
        "client_id":         "solpac",           # PEAKATS client_id
        "csp_id":            "V9030188",
        "package_value":     "2426",             # A - NON CDL DRIVER PKG + PHYSICAL AND DRUG (M7)
        "company_id":        "300 - ISP Pickup & Delivery",
        "facility_id":       "00310 - BRASELTON, GA",
        "position_type":     "A - P&D Non-CDL Driver",
        "driver_type":       None,               # Leave as Select One
    },

    "cbm": {
        "display_name":      "CBM Logistics",
        "client_id":         "cbm",
        "csp_id":            None,               # TODO: inspect FADV portal for CBM
        "package_value":     None,               # TODO
        "company_id":        None,               # TODO
        "facility_id":       None,               # TODO
        "position_type":     None,               # TODO
        "driver_type":       None,
    },

    "dd_networks": {
        "display_name":      "DD Networks",
        "client_id":         "dd_networks",
        "csp_id":            None,               # TODO
        "package_value":     None,               # TODO
        "company_id":        None,               # TODO
        "facility_id":       None,               # TODO
        "position_type":     None,               # TODO
        "driver_type":       None,
    },

    # ── Template for new clients ───────────────────────────────────────────
    # "new_client": {
    #     "display_name":  "Client Name",
    #     "client_id":     "new_client",
    #     "csp_id":        "VXXXXXXX",
    #     "package_value": "XXXX",
    #     "company_id":    "XXX - Description",
    #     "facility_id":   "XXXXX - CITY, ST",
    #     "position_type": "A - P&D Non-CDL Driver",
    #     "driver_type":   None,
    # },
}


# ─── DATABASE ─────────────────────────────────────────────────────────────────

def load_candidates(client_id: str) -> list:
    """Pull eligible candidates for a given client from Supabase."""
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, first_name, last_name, email, phone, rwp_score
            FROM candidates
            WHERE client_id = :client_id
              AND status NOT IN ('Rejected', 'Hired')
              AND rwp_score >= 6
              AND NOT (first_name = 'None' AND last_name = 'Provided')
              AND phone != '0000000000'
            ORDER BY rwp_score DESC, last_name ASC
        """), {"client_id": client_id}).fetchall()
    engine.dispose()

    return [
        {
            "peakats_id": r[0],
            "first_name":  r[1],
            "last_name":   r[2],
            "email":       r[3] or "",
            "phone":       r[4] or "",
            "rwp_score":   r[5],
        }
        for r in rows
    ]


# ─── RESULTS LOG ──────────────────────────────────────────────────────────────

FIELDS = ["peakats_id", "first_name", "last_name", "rwp_score", "status", "error", "timestamp"]

def init_log(results_file: Path):
    results_file.parent.mkdir(parents=True, exist_ok=True)
    with open(results_file, "w", newline="") as f:
        csv.DictWriter(f, fieldnames=FIELDS).writeheader()
    print(f"📋 Log: {results_file}")

def log_result(results_file: Path, row: dict):
    with open(results_file, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=FIELDS).writerow(row)


# ─── BROWSER AUTOMATION ───────────────────────────────────────────────────────

def login(page, username: str, password: str):
    print("🔐 Logging in...")
    page.goto(FADV_URL, wait_until="networkidle", timeout=30000)
    page.fill('input[name="USER"]', username)
    page.fill('input[name="PASSWORD"]', password)
    page.click('input[type="submit"]')
    page.wait_for_load_state("networkidle", timeout=30000)

    if "login" in page.url.lower() or "Login" in page.title():
        raise Exception("Login failed — check FADV_USER / FADV_PASS")
    print("✅ Logged in")


def navigate_new_subject(page):
    """Click Profile Advantage → New Subject in sidebar."""
    try:
        page.click('text=Profile Advantage', timeout=5000)
        time.sleep(0.4)
    except PWTimeout:
        pass  # Already expanded
    page.click('text=New Subject', timeout=10000)
    page.wait_for_load_state("networkidle", timeout=20000)
    page.wait_for_selector('#CDC_NEW_SUBJECT_FIRST_NAME', timeout=15000)


def fill_and_submit(page, candidate: dict, config: dict) -> dict:
    """Fill one New Subject form and submit. Returns result dict."""
    result = {
        "peakats_id": candidate["peakats_id"],
        "first_name":  candidate["first_name"],
        "last_name":   candidate["last_name"],
        "rwp_score":   candidate["rwp_score"],
        "status":      "error",
        "error":       "",
        "timestamp":   datetime.now().isoformat(),
    }

    try:
        navigate_new_subject(page)

        # ── Email Information ──────────────────────────────────────────────
        page.fill('#CDC_NEW_SUBJECT_FIRST_NAME', candidate["first_name"])
        page.fill('#CDC_NEW_SUBJECT_LAST_NAME',  candidate["last_name"])

        if candidate["email"]:
            page.fill('#CDC_NEW_SUBJECT_EMAIL_ADDRESS', candidate["email"])

        # Phone — class GGO0QWGJ3C is unique to the phone field
        # Avoids the dynamic ext-gen ID which changes every session
        if candidate["phone"]:
            digits = re.sub(r'\D', '', candidate["phone"])[:10]
            page.fill('input.GGO0QWGJ3C', digits)

        # ── Order Information — CSP ID ─────────────────────────────────────
        # Dot in ID requires attribute selector (CSS # shorthand breaks on dots)
        page.select_option(
            'select[id="Order.Info.RefID3"]',
            value=config["csp_id"]
        )
        time.sleep(0.5)

        # ── Package ───────────────────────────────────────────────────────
        page.select_option(
            '#CDC_NEW_SUBJECT_PACKAGE_LABEL',
            value=config["package_value"]
        )
        time.sleep(1.5)  # Package triggers dynamic UI reload — wait for it

        # ── Select From Drop Down section ─────────────────────────────────
        page.select_option('select[id="Company ID"]',   value=config["company_id"])
        time.sleep(0.3)
        page.select_option('select[id="Facility ID"]',  value=config["facility_id"])
        time.sleep(0.3)
        page.select_option('select[id="Position Type"]', value=config["position_type"])
        time.sleep(0.3)

        # Driver Type — leave as Select One if None
        if config.get("driver_type"):
            page.select_option('select[id="Driver Type"]', value=config["driver_type"])
            time.sleep(0.3)

        # ── Submit ────────────────────────────────────────────────────────
        # ext-gen214 is dynamic — click by stable inner text instead
        page.click('td.html-face:has-text("Send")', timeout=10000)
        page.wait_for_load_state("networkidle", timeout=20000)

        # Detect success
        body = page.inner_text('body')
        if any(kw in body.lower() for kw in ["confirmation", "success", "subject id", "order placed", "has been submitted"]):
            result["status"] = "success"
        else:
            result["status"] = "submitted"  # Sent — confirm keyword not detected, spot check portal

        print(f"  ✅ {candidate['first_name']} {candidate['last_name']}")

    except Exception as e:
        result["error"] = str(e)
        print(f"  ❌ {candidate['first_name']} {candidate['last_name']} — {e}")

    return result


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FADV Entry Bot — Multi-Client")
    parser.add_argument("--client", help="Client key (e.g. solpac, cbm, dd_networks)")
    parser.add_argument("--test",   action="store_true", help="Test mode — first 5 candidates only")
    parser.add_argument("--list",   action="store_true", help="List configured clients")
    args = parser.parse_args()

    # List clients
    if args.list:
        print("\nConfigured clients:")
        for key, cfg in CLIENT_CONFIGS.items():
            ready = "✅" if cfg["csp_id"] else "⚠️  TODO"
            print(f"  {ready}  {key:20s} — {cfg['display_name']}")
        print()
        return

    if not args.client:
        parser.print_help()
        sys.exit(1)

    client_key = args.client.lower()
    if client_key not in CLIENT_CONFIGS:
        print(f"❌ Unknown client: '{client_key}'")
        print(f"   Run with --list to see available clients")
        sys.exit(1)

    config = CLIENT_CONFIGS[client_key]

    # Check config is complete
    missing = [k for k in ["csp_id", "package_value", "company_id", "facility_id", "position_type"]
               if not config.get(k)]
    if missing:
        print(f"❌ Client '{client_key}' config incomplete. Missing: {', '.join(missing)}")
        print(f"   Edit CLIENT_CONFIGS in this script to add the values.")
        sys.exit(1)

    # Credentials
    fadv_user = os.environ.get("FADV_USER")
    fadv_pass = os.environ.get("FADV_PASS")
    if not fadv_user or not fadv_pass:
        print("❌ Set credentials first:")
        print("   export FADV_USER=KSMITH")
        print("   export FADV_PASS=your_password_here")
        sys.exit(1)

    # Load candidates
    print(f"\n📥 Loading candidates — {config['display_name']}...")
    candidates = load_candidates(config["client_id"])

    if args.test:
        candidates = candidates[:5]
        print(f"🧪 TEST MODE — {len(candidates)} candidates")
    else:
        print(f"   {len(candidates)} candidates queued")

    if not candidates:
        print("No eligible candidates found.")
        return

    from collections import Counter
    for score, count in sorted(Counter(c["rwp_score"] for c in candidates).items(), reverse=True):
        print(f"   RWP {score}: {count}")

    # Results log
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    results_file = Path(f"fadv_entry_results/{client_key}_{timestamp}.csv")
    init_log(results_file)

    # Run
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,  # Watch it run; flip True for fully unattended
            slow_mo=150
        )
        page = browser.new_page()
        success = failed = 0

        try:
            login(page, fadv_user, fadv_pass)
            total = len(candidates)

            for idx, candidate in enumerate(candidates, 1):
                print(f"\n[{idx}/{total}] RWP {candidate['rwp_score']} — "
                      f"{candidate['first_name']} {candidate['last_name']}")

                result = fill_and_submit(page, candidate, config)
                log_result(results_file, result)

                if result["status"] in ("success", "submitted"):
                    success += 1
                else:
                    failed += 1

                time.sleep(6)  # Pause between submissions

        except Exception as e:
            print(f"\n💥 Fatal: {e}")
        finally:
            browser.close()

    print(f"\n{'='*50}")
    print(f"FADV ENTRY BOT — {client_key.upper()} COMPLETE")
    print(f"  ✅ Submitted: {success}")
    print(f"  ❌ Failed:    {failed}")
    print(f"  📋 Log:       {results_file}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
