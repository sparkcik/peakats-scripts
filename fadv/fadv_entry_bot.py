#!/usr/bin/env python3
import os, sys, csv, time, re, argparse
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

DB_URL = os.environ["SUPABASE_DB_URL"]
FADV_LOGIN_URL = "https://enterprise.fadv.com/pub/l/login/userLogin.do?redirect_url=https://enterprise.fadv.com&referer=/"
FADV_CLIENT_ID = "042443sdp"

CLIENT_CONFIGS = {
    "solpac": {
        "display_name": "Solpac (Braselton, GA)",
        "client_id": "solpac",
        "csp_id": "V9030188",
        "package_value": "2426",
        "company_id": "300 - ISP Pickup & Delivery",
        "facility_id": "00310 - BRASELTON, GA",
        "position_type": "A - P&D Non-CDL Driver",
        "driver_type": None,
    },
    "cbm": {
        "display_name": "CBM Logistics",
        "client_id": "cbm",
        "csp_id": None, "package_value": None,
        "company_id": None, "facility_id": None,
        "position_type": None, "driver_type": None,
    },
    "dd_networks": {
        "display_name": "DD Networks",
        "client_id": "dd_networks",
        "csp_id": None, "package_value": None,
        "company_id": None, "facility_id": None,
        "position_type": None, "driver_type": None,
    },
}

def load_candidates(client_id):
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
    return [{"peakats_id":r[0],"first_name":r[1],"last_name":r[2],"email":r[3] or "","phone":r[4] or "","rwp_score":r[5]} for r in rows]

FIELDS = ["peakats_id","first_name","last_name","rwp_score","status","error","timestamp"]

def init_log(results_file):
    results_file.parent.mkdir(parents=True, exist_ok=True)
    with open(results_file, "w", newline="") as f:
        csv.DictWriter(f, fieldnames=FIELDS).writeheader()
    print(f"Log: {results_file}")

def log_result(results_file, row):
    with open(results_file, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=FIELDS).writerow(row)

def login(page, username, password):
    print("Opening FADV login page...")
    page.goto(FADV_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
    print("")
    print("=" * 50)
    print("ACTION REQUIRED: Log in manually in the browser")
    print(f"  Client ID: {FADV_CLIENT_ID}")
    print(f"  User ID:   {username}")
    print(f"  Password:  {password}")
    print("")
    print("1. Log in to FADV")
    print("2. Navigate to Profile Advantage > New Subject")
    print("3. When the New Subject form is visible, press ENTER here")
    print("=" * 50)
    input()
    print(f"Continuing — URL: {page.url}")

def navigate_new_subject(page):
    # Click Profile Advantage then New Subject by exact text
    page.locator(".GGO0QWGPLD:has-text('Profile Advantage')").first.click(timeout=10000)
    time.sleep(1.5)
    page.locator("span.GGO0QWGCMD:has-text('New Subject')").first.click(timeout=10000)
    time.sleep(3)
    # Wait for Ext JS to render the form into the main frame
    page.wait_for_selector("#CDC_NEW_SUBJECT_FIRST_NAME", timeout=20000)
    print(f"  Form ready")

def get_form_context(page):
    """Form renders in main page via Ext JS — return page directly."""
    return page

def fill_and_submit(page, candidate, config):
    result = {"peakats_id":candidate["peakats_id"],"first_name":candidate["first_name"],"last_name":candidate["last_name"],"rwp_score":candidate["rwp_score"],"status":"error","error":"","timestamp":datetime.now().isoformat()}
    try:
        navigate_new_subject(page)
        ctx = get_form_context(page)

        # Email Information section
        ctx.fill("#CDC_NEW_SUBJECT_FIRST_NAME", candidate["first_name"])
        ctx.fill("#CDC_NEW_SUBJECT_LAST_NAME", candidate["last_name"])
        if candidate["email"]:
            ctx.fill("#CDC_NEW_SUBJECT_EMAIL_ADDRESS", candidate["email"])

        # Phone — two separate fields: Country Code + Area Code/Phone Number
        if candidate["phone"]:
            digits = re.sub(r"\D", "", candidate["phone"])[:10]
            try:
                ctx.fill("#CDC_NEW_SUBJECT_PHONE_NUMBER", digits)
            except:
                # Fallback: fill area code field directly
                try:
                    ctx.fill("input[name*='phone']", digits)
                except:
                    pass

        # Order Information — CSP ID
        ctx.select_option("select[id=\"Order.Info.RefID3\"]", value=config["csp_id"])
        time.sleep(0.5)

        # Package Selections
        ctx.select_option("#CDC_NEW_SUBJECT_PACKAGE_LABEL", value=config["package_value"])
        time.sleep(2)

        # Select From Drop Down section
        ctx.select_option("select[id=\"Company ID\"]", value=config["company_id"])
        time.sleep(0.3)
        ctx.select_option("select[id=\"Facility ID\"]", value=config["facility_id"])
        time.sleep(0.3)
        ctx.select_option("select[id=\"Position Type\"]", value=config["position_type"])
        time.sleep(0.3)

        # Send button — visible at bottom of form
        ctx.click("input[value='Send'], button:has-text('Send')", timeout=10000)
        page.wait_for_load_state("networkidle", timeout=20000)
        result["status"] = "submitted"
        print(f"  OK {candidate['first_name']} {candidate['last_name']}")
    except Exception as e:
        result["error"] = str(e)
        print(f"  FAIL {candidate['first_name']} {candidate['last_name']} -- {e}")
    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client")
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        for k,v in CLIENT_CONFIGS.items():
            print(f"  {k} -- {v['display_name']}")
        return

    if not args.client:
        parser.print_help(); sys.exit(1)

    config = CLIENT_CONFIGS.get(args.client.lower())
    if not config:
        print(f"Unknown client: {args.client}"); sys.exit(1)

    missing = [k for k in ["csp_id","package_value","company_id","facility_id","position_type"] if not config.get(k)]
    if missing:
        print(f"Config incomplete: {missing}"); sys.exit(1)

    fadv_user = os.environ.get("FADV_USER")
    fadv_pass = os.environ.get("FADV_PASS")
    if not fadv_user or not fadv_pass:
        print("Set FADV_USER and FADV_PASS env vars"); sys.exit(1)

    candidates = load_candidates(config["client_id"])
    if args.test:
        candidates = candidates[:5]
        print(f"TEST MODE -- {len(candidates)} candidates")
    else:
        print(f"{len(candidates)} candidates queued")

    if not candidates:
        print("No candidates."); return

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    results_file = Path(f"fadv_entry_results/{args.client}_{ts}.csv")
    init_log(results_file)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=150)
        page = browser.new_page()
        success = failed = 0
        try:
            login(page, fadv_user, fadv_pass)
            for idx, candidate in enumerate(candidates, 1):
                print(f"[{idx}/{len(candidates)}] {candidate['first_name']} {candidate['last_name']}")
                result = fill_and_submit(page, candidate, config)
                log_result(results_file, result)
                if result["status"] == "submitted":
                    success += 1
                else:
                    failed += 1
                time.sleep(6)
        except Exception as e:
            print(f"Fatal: {e}")
        finally:
            browser.close()

    print(f"Done. Submitted: {success}  Failed: {failed}  Log: {results_file}")

if __name__ == "__main__":
    main()
