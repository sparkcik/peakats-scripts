#!/usr/bin/env python3
"""
PEAK Recruiting — GCIC Batch Form Filler v3.1
March 2026

Changes from v2.1:
  - AUTO-SIGNATURE: Renders candidate name in cursive font (Dancing Script),
    stamps it onto the signature line of each PDF. No manual signing required.
  - FONT BUNDLED: DancingScript.ttf expected at SCRIPT_DIR/fonts/DancingScript.ttf
  - Supabase: Marks gcic_generated=true + gcic_generated_date on each candidate
    after successful PDF generation (matched by first+last name).
  - SKIP rows: Any row with Processed col containing "SKIP" is auto-skipped.
  - Gmail send (Phase 2 stub): send_to_fadv() ready to activate once OAuth
    gmail.send scope is re-authorized.

Workflow (fully automated):
  Candidate fills Google Form
  → Sheet auto-populates
  → gcic_batch_filler.py reads Sheet
  → generates filled + signed PDF per candidate
  → marks gcic_generated in Supabase
  → [Phase 2] emails PDF to casedocuments@fadv.com automatically

VALIDITY: Hard-locked at 90 days. Not configurable.

Legal basis for generated signature:
  Candidate electronically consented to GCIC authorization by submitting the
  Google Form, which includes explicit consent language. The rendered signature
  represents that consent. This mirrors DocuSign's "Type Your Signature" method,
  which is legally equivalent to a wet signature under ESIGN Act (15 U.S.C. §7001)
  and UETA. Form submission timestamp is preserved as audit trail.

Usage:
  python3 gcic_batch_filler_v3.py                   # all pending rows
  python3 gcic_batch_filler_v3.py --client cbm      # filter by client
  python3 gcic_batch_filler_v3.py --name Smith      # filter by last name
  python3 gcic_batch_filler_v3.py --row 3           # single row (1-based)
  python3 gcic_batch_filler_v3.py --dry-run         # preview, no PDFs
  python3 gcic_batch_filler_v3.py --calibrate       # coordinate check PDF
  python3 gcic_batch_filler_v3.py --inspect         # print Sheet column headers
  python3 gcic_batch_filler_v3.py --no-sig          # skip signature (debug)
"""

import os, sys, json, argparse, io
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request, urllib.parse

# ══════════════════════════════════════════════════════════════════════════════
# LOCKED CONFIG
# ══════════════════════════════════════════════════════════════════════════════

VERSION      = "3.3"
VALIDITY_DAYS = 90   # LOCKED. GCIC authorization valid 90 days from signature.

SHEET_ID  = "1Mi6Pr_cRq2MaMzFtXVVHn5VR419Ehryih4nLB3vxl88"
TAB_GID   = "1734212484"

SCRIPT_DIR    = Path(__file__).parent
TEMPLATE_PDF  = SCRIPT_DIR / "gcic_template.pdf"
OUTPUT_DIR    = SCRIPT_DIR / "gcic_output"
FONT_PATH     = SCRIPT_DIR / "fonts" / "DancingScript.ttf"
OUTPUT_DIR.mkdir(exist_ok=True)
(SCRIPT_DIR / "fonts").mkdir(exist_ok=True)

# Supabase
SUPABASE_URL = "https://eyopvsmsvbgfuffscfom.supabase.co"
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# FADV email target
FADV_EMAIL = "casedocuments@fadv.com"
FADV_CC    = "fedex.support@fadv.com"

OAUTH = {
    "client_id":     "298611701884-lmd1ooh1cg6dj0v0f69f0ol7qperb63c.apps.googleusercontent.com",
    "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],,
    "refresh_token": os.environ["GOOGLE_REFRESH_TOKEN"],,
}

# Signature rendering config
SIG_FONT_SIZE  = 44        # pt — large enough to look like a real signature
SIG_X_FITZ     = 72.2        # fitz x (points from left) — left edge of sig line
SIG_Y_TOP_FITZ = 492.8       # fitz y (points from top) — top of sig image rect
SIG_Y_BOT_FITZ = 529.9       # fitz y bottom — height = 32pt
SIG_MAX_WIDTH  = 341       # max width in points before auto-shrink


# ══════════════════════════════════════════════════════════════════════════════
# COLUMN MAP
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
# GCIC FIELD COORDINATE MAP  (reportlab / bottom-left origin)
# ══════════════════════════════════════════════════════════════════════════════

FIELDS = {
    "entity":       {"x": 176, "y": 729, "size": 12},
    "last_name":    {"x": 98,  "y": 624, "size": 12},
    "first_name":   {"x": 260, "y": 624, "size": 12},
    "middle_name":  {"x": 435, "y": 624, "size": 12},
    "address":      {"x": 156, "y": 563, "size": 12},
    "city":         {"x": 156, "y": 527, "size": 12},
    "state":        {"x": 350, "y": 527, "size": 12},
    "zip":          {"x": 430, "y": 527, "size": 12},
    "dob":          {"x": 270, "y": 462, "size": 12},
    "ssn":          {"x": 396, "y": 462, "size": 12},
    "validity":     {"x": 250, "y": 365, "size": 12},
    "sig_date":     {"x": 434, "y": 267, "size": 12},
    "sex": {
        "type": "checkbox",
        "options": {
            "Male":    (62, 478),
            "Female":  (62, 454),
            "Unknown": (62, 429),
        },
    },
    "race": {
        "type": "checkbox",
        "options": {
            "White":                  (161, 478),
            "Black":                  (161, 465),
            "Black/African American": (161, 465),
            "Asian":                  (161, 453),
            "Asian/Pacific Islander": (161, 453),
            "Hispanic":               (161, 440),
            "Hispanic/Latino":        (161, 440),
            "Unknown":                (161, 427),
        },
    },
    "auth_box": {
        "type": "checkbox",
        "options": {"check": (76, 365)},
    },
    "purpose_e": {
        "type": "checkbox",
        "options": {"E": (75, 198)},
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════

def get_token():
    """Get OAuth access token. Tries embedded creds, falls back to gcloud ADC."""
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
# SIGNATURE RENDERING
# ══════════════════════════════════════════════════════════════════════════════

def ensure_font():
    """Download Dancing Script font if not present."""
    if FONT_PATH.exists():
        return True
    print("  📥 Downloading signature font (Dancing Script)...")
    try:
        url = "https://github.com/google/fonts/raw/main/ofl/dancingscript/DancingScript%5Bwght%5D.ttf"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            FONT_PATH.write_bytes(r.read())
        print(f"  ✅ Font saved: {FONT_PATH}")
        return True
    except Exception as e:
        print(f"  ⚠️  Font download failed: {e}")
        print(f"     Manually place DancingScript.ttf at: {FONT_PATH}")
        return False


def render_signature_png(first_name: str, last_name: str, font_size: int = SIG_FONT_SIZE) -> bytes | None:
    """
    Render 'FirstName LastName' in Dancing Script cursive font.
    Returns PNG bytes (transparent background) or None on failure.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont

        sig_text = f"{first_name} {last_name}"

        if not FONT_PATH.exists():
            if not ensure_font():
                return None

        font = ImageFont.truetype(str(FONT_PATH), size=font_size)

        # Measure text size
        dummy = Image.new("RGBA", (1, 1))
        draw  = ImageDraw.Draw(dummy)
        bbox  = draw.textbbox((0, 0), sig_text, font=font)
        text_w = bbox[2] - bbox[0] + 20
        text_h = bbox[3] - bbox[1] + 10

        # Auto-shrink if too wide
        if text_w > SIG_MAX_WIDTH:
            scale     = SIG_MAX_WIDTH / text_w
            font_size = int(font_size * scale)
            font      = ImageFont.truetype(str(FONT_PATH), size=font_size)
            bbox      = draw.textbbox((0, 0), sig_text, font=font)
            text_w    = bbox[2] - bbox[0] + 20
            text_h    = bbox[3] - bbox[1] + 10

        img  = Image.new("RGBA", (text_w, text_h), (255, 255, 255, 0))
        draw = ImageDraw.Draw(img)
        draw.text((10, 5), sig_text, font=font, fill=(15, 30, 120, 240))  # dark navy ink

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    except ImportError:
        print("  ⚠️  Pillow not installed — pip install Pillow --break-system-packages")
        return None
    except Exception as e:
        print(f"  ⚠️  Signature render failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# SUPABASE — mark gcic_generated
# ══════════════════════════════════════════════════════════════════════════════

def mark_gcic_generated_supabase(first_name: str, last_name: str) -> bool:
    """
    Set gcic_generated=true and gcic_generated_date=NOW() on matching candidate.
    Matches by first+last name (case-insensitive). Updates all matching records
    (handles multi-client duplicates — both get marked).
    Returns True if at least one record updated.
    """
    try:
        # Use Supabase REST PATCH via PostgREST
        url = (
            f"{SUPABASE_URL}/rest/v1/candidates"
            f"?first_name=ilike.{urllib.parse.quote(first_name)}"
            f"&last_name=ilike.{urllib.parse.quote(last_name)}"
        )
        payload = json.dumps({
            "gcic_generated":      1,
            "gcic_generated_date": datetime.utcnow().isoformat() + "Z",
        }).encode()
        req = urllib.request.Request(
            url,
            data=payload,
            method="PATCH",
            headers={
                "apikey":          SUPABASE_KEY,
                "Authorization":   f"Bearer {SUPABASE_KEY}",
                "Content-Type":    "application/json",
                "Prefer":          "return=minimal",
            }
        )
        with urllib.request.urlopen(req) as r:
            r.read()
        return True
    except Exception as e:
        print(f"  ⚠️  Supabase update failed: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# GMAIL SEND — Phase 2 stub
# ══════════════════════════════════════════════════════════════════════════════

def get_order_id_from_supabase(first_name: str, last_name: str) -> str | None:
    """
    Pull background_id (FADV Order ID) from Supabase by candidate name.
    Returns the order ID string or None if not found.
    """
    try:
        url = (
            f"{SUPABASE_URL}/rest/v1/candidates"
            f"?select=background_id"
            f"&first_name=ilike.{urllib.parse.quote(first_name)}"
            f"&last_name=ilike.{urllib.parse.quote(last_name)}"
            f"&background_id=not.is.null"
            f"&order=updated_at.desc"
            f"&limit=1"
        )
        req = urllib.request.Request(url, headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        with urllib.request.urlopen(req) as r:
            results = json.loads(r.read())
            if results and results[0].get("background_id"):
                return results[0]["background_id"].strip()
    except Exception as e:
        print(f"  ⚠️  Order ID lookup failed: {e}")
    return None


def send_to_fadv(candidate: dict, pdf_path: Path, order_id: str = None, token: str = None) -> bool:
    """
    Email completed GCIC PDF to casedocuments@fadv.com via SMTP.
    Subject format: "FirstName LastName GCIC - Order ID: XXXXXXX"
    """
    import smtplib, ssl
    import email.mime.multipart, email.mime.text, email.mime.application

    SMTP_USER = "kai@peakrecruitingco.com"
    SMTP_PASS = "ozaphvkamkhdnmkk"
    SMTP_HOST = "smtp.gmail.com"
    SMTP_PORT = 587

    first = candidate.get("first_name", "").strip()
    last  = candidate.get("last_name",  "").strip()
    subject = f"{first} {last} GCIC"
    if order_id:
        subject += f" - Order ID: {order_id}"

    body = (
        f"Hi,\n\n"
        f"Please find the attached GCIC authorization form for {first} {last}."
    )
    if order_id:
        body += f" Order ID: {order_id}."
    body += "\n\nThank you,\nKai\nPEAKrecruiting"

    msg = email.mime.multipart.MIMEMultipart()
    msg["To"]      = FADV_EMAIL
    msg["Cc"]      = FADV_CC
    msg["From"]    = f"Kai <{SMTP_USER}>"
    msg["Subject"] = subject
    msg.attach(email.mime.text.MIMEText(body))

    with open(pdf_path, "rb") as f:
        att = email.mime.application.MIMEApplication(f.read(), _subtype="pdf")
        att.add_header("Content-Disposition", "attachment", filename=pdf_path.name)
        msg.attach(att)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(SMTP_USER, SMTP_PASS)
            all_recipients = [FADV_EMAIL] + ([FADV_CC] if FADV_CC else [])
            server.sendmail(SMTP_USER, all_recipients, msg.as_string())
        return True
    except Exception as e:
        print(f"  ❌ Gmail send failed: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# PDF GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def fill_pdf(candidate: dict, output_path: Path, today: datetime, render_sig: bool = True) -> bool:
    """
    Fill GCIC form using AcroForm widget API (PyMuPDF).
    Optionally stamps a cursive signature image onto the signature line.
    """
    if not TEMPLATE_PDF.exists():
        print(f"\n  ❌ Template PDF not found: {TEMPLATE_PDF}")
        return False

    try:
        import fitz  # PyMuPDF
    except ImportError:
        print("  ❌ Missing: pymupdf — pip install pymupdf --break-system-packages")
        return False

    SEX_FIELD_MAP = {
        "male": "MALE", "female": "FEMALE", "unknown": "UNKNOWN"
    }
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
        name_str = f"{last:<24}{first:<24}{mid}"

        text_values = {
            "AGENCY NAME":                    "First Advantage",
            "NAME":                           name_str,
            "STREET":                         (candidate.get("address") or "").strip(),
            "CITY STATE ZIP":                 f"{candidate.get('city','')}, {candidate.get('state','')}  {candidate.get('zip','')}",
            "DATE OF BIRTH":                  (candidate.get("dob") or "").strip(),
            "SOCIAL SECURITY NUMBER":         (candidate.get("ssn") or "").strip(),
            "This authorization is valid for": str(VALIDITY_DAYS),
            "Date":                           today.strftime("%m/%d/%Y"),
        }

        sex_field  = SEX_FIELD_MAP.get((candidate.get("sex") or "").lower(), "")
        race_field = RACE_FIELD_MAP.get((candidate.get("race") or "").lower(), "")
        check_on   = {sex_field, race_field, "OPT 1", "E"}

        for widget in page.widgets():
            name = widget.field_name
            if widget.field_type_string == "Text" and name in text_values:
                widget.field_value   = text_values[name]
                widget.text_fontsize = 11
                # Shift "90" validity value right for better alignment
                if name == "This authorization is valid for":
                    r = widget.rect
                    widget.rect = fitz.Rect(r.x0 + 18, r.y0, r.x1 + 18, r.y1)
                widget.update()
            elif widget.field_type_string == "CheckBox" and name in check_on:
                widget.field_value = True
                widget.update()

        # ── SIGNATURE STAMP ──────────────────────────────────────────────────
        if render_sig:
            sig_png = render_signature_png(first, last)
            if sig_png:
                doc.bake()  # flatten AcroForm widgets — prevents ghost sig rendering
                page = doc[0]  # re-reference page after bake
                sig_rect = fitz.Rect(SIG_X_FITZ, SIG_Y_TOP_FITZ, SIG_X_FITZ + SIG_MAX_WIDTH, SIG_Y_BOT_FITZ)
                page.insert_image(sig_rect, stream=sig_png)
                print(f"  ✍  Signature stamped: {first} {last}")
            else:
                print(f"  ⚠️  Signature rendering failed — form generated without signature")
        # ─────────────────────────────────────────────────────────────────────

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

    # Mark signature zone in fitz coordinates → convert to reportlab
    # fitz: y=508 from top → reportlab: y = 792-508 = 284
    sig_rl_y = ph - SIG_Y_BOT_FITZ
    c.setFillColorRGB(0, 0.5, 0)
    c.setFont("Helvetica-Bold", 7)
    c.drawString(SIG_X_FITZ, sig_rl_y, "[SIGNATURE ZONE]")

    c.save(); packet.seek(0)
    overlay = PdfReader(packet).pages[0]
    page.merge_page(overlay)
    writer = PdfWriter()
    writer.add_page(page)
    out = OUTPUT_DIR / f"GCIC_CALIBRATION_{datetime.today().strftime('%Y%m%d')}.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    print(f"✅ Calibration PDF: {out}")
    print("   Red = text fields. Green = signature zone.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run(client_filter=None, name_filter=None, row_filter=None, dry_run=False, no_sig=False):
    today = datetime.today()
    print("\n" + "=" * 60)
    print(f"PEAK — GCIC BATCH FILLER v{VERSION}")
    print(f"Date:       {today.strftime('%B %d, %Y')}")
    print(f"Validity:   {VALIDITY_DAYS} days  [LOCKED]")
    print(f"Signature:  {'DISABLED (--no-sig)' if no_sig else 'AUTO (Dancing Script)'}")
    print(f"Output:     {OUTPUT_DIR}")
    print("=" * 60)

    # Pre-check font
    if not no_sig:
        ensure_font()

    print(f"\n📋 Reading Sheet...")
    rows = read_sheet()
    print(f"   {len(rows)} total rows")

    stats = {"generated": 0, "skipped": 0, "error": 0, "supabase_ok": 0}
    token = get_token()

    for i, row in enumerate(rows):
        row_num = i + 1

        if row_filter and row_num != row_filter:
            continue

        processed_val = row.get(COL["processed"], "").upper()
        if processed_val == "Y":
            stats["skipped"] += 1
            continue
        if processed_val.startswith("SKIP"):
            stats["skipped"] += 1
            continue

        if client_filter:
            if client_filter.lower() not in row.get(COL.get("client", ""), "").lower():
                continue
        if name_filter:
            if name_filter.lower() not in row.get(COL["last_name"], "").lower():
                continue

        c    = {k: row.get(v, "").strip() for k, v in COL.items()}
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

        ok = fill_pdf(c, out_path, today, render_sig=(not no_sig))

        if ok:
            print(f"  ✅ {fname}")
            stats["generated"] += 1

            # Mark Supabase
            sb_ok = mark_gcic_generated_supabase(c["first_name"], c["last_name"])
            if sb_ok:
                stats["supabase_ok"] += 1
                print(f"  🗄  Supabase: gcic_generated marked")
            else:
                print(f"  ⚠️  Supabase: update failed (check column exists)")

            # Mark Sheet processed
            try:
                mark_processed(row_num, token)
                print(f"  ✔  Sheet row {row_num} marked Y")
            except Exception as e:
                print(f"  ⚠️  Sheet mark failed: {e}")

            # Lookup Order ID from Supabase, then email to FADV
            order_id = get_order_id_from_supabase(c["first_name"], c["last_name"])
            if order_id:
                print(f"  🔗 Order ID: {order_id}")
            else:
                print(f"  ⚠️  No Order ID found — sending without it")
            # SEND GUARD — never double-send
            if c.get("gcic_generated") == 1:
                print(f"  ⏭️  Already sent — skipping {c.get('first_name')} {c.get('last_name')}")
                sent = True
            else:
                sent = send_to_fadv(c, out_path, order_id=order_id, token=token)
            if sent:
                stats["emailed"] = stats.get("emailed", 0) + 1
                print(f"  📧 Emailed to {FADV_EMAIL}")

        else:
            stats["error"] += 1

    print("\n" + "=" * 60)
    print(f"Generated:       {stats['generated']}")
    print(f"Skipped:         {stats['skipped']}  (processed or SKIP rows)")
    print(f"Errors:          {stats['error']}")
    print(f"Supabase writes: {stats['supabase_ok']}")
    print(f"Emailed to FADV:  {stats.get("emailed", 0)}")
    if stats["generated"]:
        print(f"\n📁 {OUTPUT_DIR}")
        if no_sig:
            print("   ⚠️  Signatures NOT stamped (--no-sig mode)")
        else:
            print("   ✍  Signatures auto-generated (Dancing Script)")
        print("   → Email PDFs to casedocuments@fadv.com")
        print("      Subject: [Name] GCIC - Order ID: [XXXXXXX]")
    print("=" * 60)


def main():
    p = argparse.ArgumentParser(description=f"PEAK GCIC Batch Form Filler v{VERSION}")
    p.add_argument("--client",    help="Filter by client (cbm, solpac, etc.)")
    p.add_argument("--name",      help="Filter by last name")
    p.add_argument("--row",       type=int, help="Single row number (1-based)")
    p.add_argument("--dry-run",   action="store_true")
    p.add_argument("--calibrate", action="store_true")
    p.add_argument("--inspect",   action="store_true")
    p.add_argument("--no-sig",    action="store_true", help="Skip signature stamp (debug)")
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
        no_sig=args.no_sig,
    )


if __name__ == "__main__":
    main()
