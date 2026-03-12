#!/usr/bin/env python3
"""
PEAK Recruiting — GCIC Batch Form Filler v2.1
March 2026

Reads Google Sheet (Form responses) → fills GCIC PDF per candidate.

Workflow:
  Candidate fills Google Form
  → Sheet auto-populates
  → this script reads Sheet, generates filled PDFs
  → Print → candidate signs → scan → email to FADV

VALIDITY: Hard-locked at 90 days. Not configurable.

Usage:
  python3 gcic_batch_filler.py                   # all pending rows
  python3 gcic_batch_filler.py --client cbm      # filter by client
  python3 gcic_batch_filler.py --name Smith      # filter by last name
  python3 gcic_batch_filler.py --row 3           # single row (1-based)
  python3 gcic_batch_filler.py --dry-run         # preview, no PDFs
  python3 gcic_batch_filler.py --calibrate       # coordinate check PDF
  python3 gcic_batch_filler.py --inspect         # print Sheet column headers
"""

import os, sys, json, argparse, io
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request, urllib.parse

# ══════════════════════════════════════════════════════════════════════════════
# LOCKED CONFIG
# ══════════════════════════════════════════════════════════════════════════════

VALIDITY_DAYS = 90   # LOCKED. GCIC authorization valid 90 days from signature.

SHEET_ID  = "1Mi6Pr_cRq2MaMzFtXVVHn5VR419Ehryih4nLB3vxl88"
TAB_GID   = "1734212484"

SCRIPT_DIR   = Path(__file__).parent
TEMPLATE_PDF = SCRIPT_DIR / "gcic_template.pdf"
OUTPUT_DIR   = SCRIPT_DIR / "gcic_output"
OUTPUT_DIR.mkdir(exist_ok=True)

OAUTH = {
    "client_id":     "298611701884-lmd1ooh1cg6dj0v0f69f0ol7qperb63c.apps.googleusercontent.com",
    "client_secret": "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",
    "refresh_token": "1//05iJ0UxQTMwSgCgYIARAAGAUSNwF-L9IrpidbRlcST4GYNDsqM6gDFr4NoU7kvdaKjxEo5s41agJIH7YeOEx4U2u5Y9de3sSMdhg",
}

# ══════════════════════════════════════════════════════════════════════════════
# COLUMN MAP
# These must match your Google Form question text exactly.
# Run --inspect to verify against live Sheet headers.
# ══════════════════════════════════════════════════════════════════════════════

COL = {
    "timestamp":   "Timestamp",
    "client":      "Client",
    "first_name":  "First Name",
    "last_name":   "Last Name",
    "middle_name": "Middle Name",
    "address":     "Street Address",
    "city":        "City",
    "state":       "State",
    "zip":         "ZIP Code",
    "dob":         "Date of Birth",
    "ssn":         "Social Security Number",
    "sex":         "Sex",
    "race":        "Race",
    "processed":   "Processed",
}

# ══════════════════════════════════════════════════════════════════════════════
# GCIC FIELD COORDINATE MAP
#
# PDF coordinates: origin bottom-left, units = points (72pt = 1 inch).
# Standard US Letter = 612 x 792 pts.
#
# Calibrated to the standard Georgia GCIC consent form.
# Run --calibrate to verify visually. Adjust x/y if text lands in wrong place.
# ══════════════════════════════════════════════════════════════════════════════

FIELDS = {
    # Precision-calibrated from live PDF text extraction (March 11, 2026).
    # Bottom-left origin (reportlab). Font size 12pt throughout.
    # All values derived from actual rendered output vs form label positions.
    "entity":       {"x": 176, "y": 729, "size": 12},   # "First Advantage" on authorize line
    "last_name":    {"x": 98,  "y": 624, "size": 12},   # aligned to LAST column (label x=93)
    "first_name":   {"x": 260, "y": 624, "size": 12},   # aligned to FIRST column (label x=281)
    "middle_name":  {"x": 435, "y": 624, "size": 12},   # aligned to MIDDLE column
    "address":      {"x": 156, "y": 563, "size": 12},   # right of STREET label
    "city":         {"x": 156, "y": 527, "size": 12},   # right of CITY label
    "state":        {"x": 350, "y": 527, "size": 12},   # after city text area
    "zip":          {"x": 430, "y": 527, "size": 12},   # after state
    "dob":          {"x": 270, "y": 462, "size": 12},   # DOB column, row between header and validity
    "ssn":          {"x": 396, "y": 462, "size": 12},   # SSN column, same row as dob
    "validity":     {"x": 250, "y": 365, "size": 12},   # LOCKED 90 — on "valid for" line
    "sig_date":     {"x": 434, "y": 267, "size": 12},   # on Date line
    "sex": {
        "type": "checkbox",
        "options": {
            "Male":    (62, 478),   # MALE label y_tl=314 → y_bl=478
            "Female":  (62, 454),   # FEMALE label y_tl=338 → y_bl=454
            "Unknown": (62, 429),   # UNKNOWN label y_tl=363 → y_bl=429
        },
    },
    "race": {
        "type": "checkbox",
        "options": {
            "White":                  (161, 478),   # y_tl=314
            "Black":                  (161, 465),   # y_tl=327
            "Black/African American": (161, 465),
            "Asian":                  (161, 453),   # y_tl=339
            "Asian/Pacific Islander": (161, 453),
            "Hispanic":               (161, 440),   # y_tl=352
            "Hispanic/Latino":        (161, 440),
            "Unknown":                (161, 427),   # y_tl=365
        },
    },
    "auth_box": {
        "type": "checkbox",
        "options": {"check": (76, 365)},   # authorization consent box
    },
    "purpose_e": {
        "type": "checkbox",
        "options": {"E": (75, 198)},       # E – Employment, y_tl=594
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════

def get_token():
    """Get OAuth access token. Tries forge-drive creds, falls back to gcloud ADC."""
    try:
        data = urllib.parse.urlencode({
            "client_id":     OAUTH["client_id"],
            "client_secret": OAUTH["client_secret"],
            "refresh_token": OAUTH["refresh_token"],
            "grant_type":    "refresh_token",
        }).encode()
        req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data)
        with urllib.request.urlopen(req) as r:
            resp = json.loads(r.read())
            if "access_token" in resp:
                return resp["access_token"]
    except Exception:
        pass

    # Fallback: gcloud ADC (works when running locally as charles@thefoundry.llc)
    try:
        import subprocess
        result = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            capture_output=True, text=True, timeout=5
        )
        token = result.stdout.strip()
        if token:
            return token
    except Exception:
        pass

    raise RuntimeError(
        "Cannot get Google auth token.\n"
        "Run: gcloud auth login && gcloud auth application-default login"
    )


# ══════════════════════════════════════════════════════════════════════════════
# SHEETS
# ══════════════════════════════════════════════════════════════════════════════

def get_tab_name(token):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}?fields=sheets.properties"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as r:
        meta = json.loads(r.read())
    for s in meta.get("sheets", []):
        if str(s["properties"]["sheetId"]) == TAB_GID:
            return s["properties"]["title"]
    return "Form Responses 1"


def read_sheet():
    token    = get_token()
    tab      = get_tab_name(token)
    url      = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{urllib.parse.quote(tab)}"
    req      = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as r:
        data = json.loads(r.read())
    rows = data.get("values", [])
    if not rows:
        return []
    headers = rows[0]
    return [
        dict(zip(headers, row + [""] * max(0, len(headers) - len(row))))
        for row in rows[1:]
    ]


def mark_processed(row_number: int, token: str = None):
    """Write Y to Processed column. row_number is 1-based data index."""
    if token is None:
        token = get_token()
    tab      = get_tab_name(token)
    hdr_url  = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{urllib.parse.quote(tab)}!1:1"
    req      = urllib.request.Request(hdr_url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as r:
        headers = json.loads(r.read()).get("values", [[]])[0]

    try:
        col_idx    = headers.index(COL["processed"])
        col_letter = chr(ord("A") + col_idx)
    except ValueError:
        print(f"  ⚠️  No '{COL['processed']}' column in Sheet — skipping mark")
        return

    sheet_row  = row_number + 1   # +1 for header
    range_str  = f"{tab}!{col_letter}{sheet_row}"
    url        = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{urllib.parse.quote(range_str)}?valueInputOption=RAW"
    payload    = json.dumps({"values": [["Y"]]}).encode()
    req2       = urllib.request.Request(
        url, data=payload, method="PUT",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req2) as r:
        r.read()


# ══════════════════════════════════════════════════════════════════════════════
# PDF GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def fill_pdf(candidate: dict, output_path: Path, today: datetime) -> bool:
    """Fill GCIC form using AcroForm widget API (PyMuPDF).
    Template is a native AcroForm PDF — must set widget values directly.
    Overlay/reportlab approach does not work due to field layer masking.
    """
    if not TEMPLATE_PDF.exists():
        print(f"\n  ❌ Template PDF not found: {TEMPLATE_PDF}")
        print(f"     Download the blank GCIC form from Drive and save it there.")
        return False

    try:
        import fitz  # PyMuPDF
    except ImportError:
        print("  ❌ Missing package: pymupdf")
        print("  Run: pip install pymupdf --break-system-packages")
        return False

    # AcroForm field name → sex value mapping
    SEX_FIELD_MAP = {
        "male": "MALE", "female": "FEMALE", "unknown": "UNKNOWN"
    }
    # AcroForm field name → race value mapping
    RACE_FIELD_MAP = {
        "white":                  "WHITE",
        "black":                  "BLACK",
        "black/african american": "BLACK",
        "asian":                  "ASIAN",
        "asian/pacific islander": "ASIAN",
        "hispanic":               "HISPANIC",
        "hispanic/latino":        "HISPANIC",
        "unknown":                "UNKNOWN RACE",
    }

    try:
        doc  = fitz.open(str(TEMPLATE_PDF))
        page = doc[0]

        last  = (candidate.get("last_name")   or "").strip()
        first = (candidate.get("first_name")  or "").strip()
        mid   = (candidate.get("middle_name") or "").strip()

        # NAME field: clear it — draw names directly at exact column x-positions
        # Column positions measured from template label geometry (PDF coords, top-left origin):
        #   LAST   x=93.4   FIRST x=284.8   MIDDLE x=479.2   y=172
        NAME_Y   = 172
        LAST_X   = 93.4
        FIRST_X  = 284.8
        MIDDLE_X = 440.0

        text_values = {
            "AGENCY NAME":                    "First Advantage",
            "NAME":                           "",   # cleared — drawn directly below
            "STREET":                         (candidate.get("address") or "").strip(),
            "CITY STATE ZIP":                 f"{candidate.get('city','')}, {candidate.get('state','')}  {candidate.get('zip','')}",
            "DATE OF BIRTH":                  (candidate.get("dob") or "").strip(),
            "SOCIAL SECURITY NUMBER":         (candidate.get("ssn") or "").strip(),
            "This authorization is valid for": str(VALIDITY_DAYS),
            "Date":                           today.strftime("%m/%d/%Y"),
        }

        sex_field  = SEX_FIELD_MAP.get((candidate.get("sex") or "").lower(), "")
        race_field = RACE_FIELD_MAP.get((candidate.get("race") or "").lower(), "")
        check_on   = {sex_field, race_field, "OPT 1", "E"}  # OPT 1 = auth box, E = purpose

        for widget in page.widgets():
            wname = widget.field_name

            if widget.field_type_string == "Text" and wname in text_values:
                widget.field_value   = text_values[wname]
                widget.text_fontsize = 11
                widget.update()

            elif widget.field_type_string == "CheckBox" and wname in check_on:
                widget.field_value = True
                widget.update()

        # Draw names directly at exact column positions (bypasses AcroForm auto-centering)
        if last:
            page.insert_text((LAST_X,   NAME_Y), last,  fontsize=11, color=(0, 0, 0))
        if first:
            page.insert_text((FIRST_X,  NAME_Y), first, fontsize=11, color=(0, 0, 0))
        if mid:
            page.insert_text((MIDDLE_X, NAME_Y), mid,   fontsize=11, color=(0, 0, 0))

        doc.save(str(output_path))
        return True

    except Exception as e:
        print(f"  ❌ PDF error: {e}")
        import traceback; traceback.print_exc()
        return False


def calibration_pdf():
    if not TEMPLATE_PDF.exists():
        print(f"❌ Template not found: {TEMPLATE_PDF}"); return

    from pypdf import PdfReader, PdfWriter
    from reportlab.pdfgen import canvas as rl_canvas

    reader = PdfReader(str(TEMPLATE_PDF))
    page   = reader.pages[0]
    pw, ph = float(page.mediabox.width), float(page.mediabox.height)
    packet = io.BytesIO()
    c      = rl_canvas.Canvas(packet, pagesize=(pw, ph))

    c.setStrokeColorRGB(0.85, 0.85, 0.85); c.setLineWidth(0.3)
    for x in range(0, int(pw), 50):
        c.line(x, 0, x, ph)
        c.setFont("Helvetica", 5); c.setFillColorRGB(0.6, 0.6, 0.6)
        c.drawString(x + 1, 4, str(x))
    for y in range(0, int(ph), 50):
        c.line(0, y, pw, y)
        c.drawString(2, y + 2, str(y))

    c.setFillColorRGB(0.8, 0, 0)
    for key, cfg in FIELDS.items():
        if cfg.get("type") == "checkbox":
            for opt, (ox, oy) in cfg["options"].items():
                c.setFont("Helvetica-Bold", 6)
                c.drawString(ox, oy, f"[{key}:{opt}]")
        else:
            c.setFont("Helvetica-Bold", 7)
            c.drawString(cfg["x"], cfg["y"], f"[{key}]")

    c.save(); packet.seek(0)
    overlay = PdfReader(packet).pages[0]
    page.merge_page(overlay)
    writer = PdfWriter()
    writer.add_page(page)
    out = OUTPUT_DIR / f"GCIC_CALIBRATION_{datetime.today().strftime('%Y%m%d')}.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    print(f"✅ Calibration PDF: {out}")
    print("   Red labels show field placement. Adjust x/y in FIELDS if off.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run(client_filter=None, name_filter=None, row_filter=None, dry_run=False):
    today = datetime.today()
    print("\n" + "=" * 60)
    print("PEAK — GCIC BATCH FILLER")
    print(f"Date:     {today.strftime('%B %d, %Y')}")
    print(f"Validity: {VALIDITY_DAYS} days from signature  [LOCKED]")
    print(f"Output:   {OUTPUT_DIR}")
    print("=" * 60)

    print(f"\n📋 Reading Sheet...")
    rows = read_sheet()
    print(f"   {len(rows)} total rows")

    stats = {"generated": 0, "skipped": 0, "error": 0}
    token = get_token()

    for i, row in enumerate(rows):
        row_num = i + 1

        if row_filter and row_num != row_filter:
            continue
        if row.get(COL["processed"], "").upper() == "Y":
            stats["skipped"] += 1
            continue
        if client_filter:
            if client_filter.lower() not in row.get(COL.get("client", ""), "").lower():
                continue
        if name_filter:
            if name_filter.lower() not in row.get(COL["last_name"], "").lower():
                continue

        c = {k: row.get(v, "").strip() for k, v in COL.items()}
        name = f"{c['last_name']}, {c['first_name']}"
        print(f"\n[{row_num}] {name}")

        if dry_run:
            print("  DRY RUN")
            for k, v in c.items():
                if v and k not in ("timestamp", "processed"):
                    print(f"    {k}: {v}")
            continue

        fname    = f"GCIC_{c['last_name']}_{c['first_name']}_{today.strftime('%Y%m%d')}.pdf"
        fname    = fname.replace(" ", "_").replace("/", "-")
        out_path = OUTPUT_DIR / fname

        ok = fill_pdf(c, out_path, today)
        if ok:
            print(f"  ✅ {fname}")
            stats["generated"] += 1
            try:
                mark_processed(row_num, token)
                print(f"  ✔  Row {row_num} marked processed in Sheet")
            except Exception as e:
                print(f"  ⚠️  Could not mark row {row_num}: {e}")
        else:
            stats["error"] += 1

    print("\n" + "=" * 60)
    print(f"Generated:  {stats['generated']}")
    print(f"Skipped:    {stats['skipped']}  (already processed)")
    print(f"Errors:     {stats['error']}")
    if stats["generated"]:
        print(f"\n📁 {OUTPUT_DIR}")
        print(f"   Validity: {VALIDITY_DAYS} days from signature  [LOCKED]")
        print("   → Print → Sign → Scan → Email to FADV")
    print("=" * 60)


def main():
    p = argparse.ArgumentParser(description="PEAK GCIC Batch Form Filler")
    p.add_argument("--client",    help="Filter by client (cbm, solpac, etc.)")
    p.add_argument("--name",      help="Filter by last name")
    p.add_argument("--row",       type=int, help="Single row number (1-based)")
    p.add_argument("--dry-run",   action="store_true")
    p.add_argument("--calibrate", action="store_true")
    p.add_argument("--inspect",   action="store_true")
    args = p.parse_args()

    if args.calibrate:
        calibration_pdf(); return

    if args.inspect:
        rows = read_sheet()
        if rows:
            print("Sheet columns:")
            for i, col in enumerate(rows[0].keys()):
                print(f"  {i+1}. '{col}'")
        return

    run(
        client_filter=args.client,
        name_filter=args.name,
        row_filter=args.row,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
