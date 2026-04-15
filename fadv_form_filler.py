"""
FADV Add User Form Auto-Filler
==============================
Triggered by Google Form submission via Apps Script webhook.
Fills the FADV Add User PDF with AO company info and 
pre-populated PEAK user credentials, then emails the 
completed PDF to the AO for forwarding to FADV.

Google Form fields needed from AO:
  - Full name (for signature)
  - Company name
  - FADV Account Number suffix (after 042443)
  - Company address (city, state, zip)
  - Company phone
  - Email address (to receive the completed form)

PEAK fields pre-filled (never changes):
  - User 1: Kai Michael Smith / kai@peakrecruitingco.com / 470-857-4325
  - User 2: Camille Shearouse / camille@peakrecruitingco.com / 470-419-4212
  - Relationship: PEAK Recruiting, recruiting services
  - Employee? No
  - Both users: Add Access

Usage:
  python3 fadv_form_filler.py \
    --name "Andrew Papp" \
    --company "Papp Logistics LLC" \
    --account_suffix "SDP" \
    --address "123 Main St, Braselton, GA 30517" \
    --phone "470-555-1234" \
    --email "papplogistics@gmail.com" \
    --output "FADV_Add_User_Papp_Logistics.pdf"
"""

import argparse
import smtplib
import ssl
import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from pypdf import PdfReader, PdfWriter

# ── PEAK Constants (never changes) ───────────────────────────────────────────
PEAK_USER1_NAME  = "Kai Michael Smith"
PEAK_USER1_EMAIL = "kai@peakrecruitingco.com"
PEAK_USER1_PHONE = "470-857-4325"

PEAK_USER2_NAME  = "Camille Shearouse"
PEAK_USER2_EMAIL = "camille@peakrecruitingco.com"
PEAK_USER2_PHONE = "470-419-4212"

RELATIONSHIP_TEXT = "PEAK Recruiting, recruiting services"

TEMPLATE_PDF = "/home/claude/FADV_Add_User_Form_template.pdf"

# ── Field IDs (from AcroForm inspection) ─────────────────────────────────────
FIELDS = {
    "account_suffix" : "Text Field 1",
    "company_name"   : "Text Field 2",
    "address"        : "Text Field 3",
    "phone"          : "Text Field 4",
    "user1_access"   : "Radio Button 1",   # /0 = Add
    "user1_name"     : "Text Field 5",
    "user1_email"    : "Text Field 6",
    "user1_phone"    : "Text Field 7",
    "user2_access"   : "Radio Button 2",   # /0 = Add
    "user2_name"     : "Text Field 8",
    "user2_email"    : "Text Field 9",
    "user2_phone"    : "Text Field 10",
    "employee_cert"  : "Radio Button 3",   # /1 = No
    "relationship"   : "Text Field 11",
    "auth_name"      : "Text Field 12",
    "date"           : "Text Field 13",
}


def fill_pdf(ao_name, company_name, account_suffix, address, phone, output_path, template_path=TEMPLATE_PDF):
    """Fill FADV Add User form with AO info + pre-populated PEAK credentials."""
    reader = PdfReader(template_path)
    writer = PdfWriter()
    writer.append(reader)

    today = datetime.today().strftime("%B %d, %Y")

    field_values = {
        FIELDS["account_suffix"] : account_suffix,
        FIELDS["company_name"]   : company_name,
        FIELDS["address"]        : address,
        FIELDS["phone"]          : phone,
        FIELDS["user1_access"]   : "/0",   # Add Access
        FIELDS["user1_name"]     : PEAK_USER1_NAME,
        FIELDS["user1_email"]    : PEAK_USER1_EMAIL,
        FIELDS["user1_phone"]    : PEAK_USER1_PHONE,
        FIELDS["user2_access"]   : "/0",   # Add Access
        FIELDS["user2_name"]     : PEAK_USER2_NAME,
        FIELDS["user2_email"]    : PEAK_USER2_EMAIL,
        FIELDS["user2_phone"]    : PEAK_USER2_PHONE,
        FIELDS["employee_cert"]  : "/1",   # No — not employees
        FIELDS["relationship"]   : RELATIONSHIP_TEXT,
        FIELDS["auth_name"]      : ao_name,
        FIELDS["date"]           : today,
    }

    writer.update_page_form_field_values(writer.pages[0], field_values, auto_regenerate=False)

    with open(output_path, "wb") as f:
        writer.write(f)

    print(f"PDF filled: {output_path}")
    return output_path


def send_email_with_pdf(ao_email, ao_name, company_name, pdf_path,
                        gmail_user, gmail_app_password):
    """Send filled PDF to AO via Gmail."""
    msg = MIMEMultipart()
    msg["From"]    = f"Kai Clarke <{gmail_user}>"
    msg["To"]      = ao_email
    msg["Subject"] = f"FADV Add User Form — {company_name}"

    body = f"""Hi {ao_name.split()[0]},

Please find the completed FADV Add User Request form attached.

To get us added to your account, simply forward this email (with the attachment) to:

    FedEx.Support@FADV.com

Once FADV processes the request we will have access and can begin managing your pipeline directly.

Let me know if you have any questions.

Kai
PEAKrecruiting
Questions? (470) 857-4325"""

    msg.attach(MIMEText(body, "plain"))

    with open(pdf_path, "rb") as f:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    filename = os.path.basename(pdf_path)
    part.add_header("Content-Disposition", f"attachment; filename={filename}")
    msg.attach(part)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, ao_email, msg.as_string())

    print(f"Email sent to {ao_email}")


def main():
    parser = argparse.ArgumentParser(description="Fill and send FADV Add User form")
    parser.add_argument("--name",           required=True, help="AO full name (for signature)")
    parser.add_argument("--company",        required=True, help="Company name")
    parser.add_argument("--account_suffix", required=True, help="FADV account suffix after 042443")
    parser.add_argument("--address",        required=True, help="Company address")
    parser.add_argument("--phone",          required=True, help="Company phone")
    parser.add_argument("--email",          required=True, help="AO email to receive form")
    parser.add_argument("--output",         default=None,  help="Output PDF path (optional)")
    parser.add_argument("--send",           action="store_true", help="Send email after filling")
    parser.add_argument("--gmail_user",     default="kai@peakrecruitingco.com")
    parser.add_argument("--gmail_password", default=os.environ.get("GMAIL_APP_PASSWORD", ""))
    args = parser.parse_args()

    # Build output filename
    if not args.output:
        safe_name = args.company.replace(" ", "_").replace("/", "_")
        args.output = f"/home/claude/FADV_Add_User_{safe_name}.pdf"

    fill_pdf(
        ao_name        = args.name,
        company_name   = args.company,
        account_suffix = args.account_suffix,
        address        = args.address,
        phone          = args.phone,
        output_path    = args.output,
    )

    if args.send:
        send_email_with_pdf(
            ao_email        = args.email,
            ao_name         = args.name,
            company_name    = args.company,
            pdf_path        = args.output,
            gmail_user      = args.gmail_user,
            gmail_app_password = args.gmail_password,
        )


if __name__ == "__main__":
    main()
