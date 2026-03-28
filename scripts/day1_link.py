import os, sys, requests, argparse
from datetime import datetime, timezone

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
RC_CLIENT_ID = os.environ.get("RC_CLIENT_ID", "")
RC_CLIENT_SECRET = os.environ.get("RC_CLIENT_SECRET", "")
RC_JWT = os.environ.get("RC_JWT", "")
RC_FROM = os.environ.get("RC_FROM_NUMBER", "+14708574325")

SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

def get_rc_token():
    r = requests.post(
        "https://platform.ringcentral.com/restapi/oauth/token",
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": RC_JWT},
        timeout=10
    )
    r.raise_for_status()
    return r.json()["access_token"]

def normalize_phone(phone):
    digits = "".join(c for c in str(phone) if c.isdigit())
    if len(digits) == 10:
        return "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return "+1" + digits

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-id", type=int, required=True)
    parser.add_argument("--poc-contact-id", type=int, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--start-time", required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--client-name", required=True)
    parser.add_argument("--poc-pronoun", default="them")
    parser.add_argument("--poc-pronoun-obj", default="them")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/candidates?select=id,first_name,last_name,phone&id=eq.{args.candidate_id}",
        headers=SB_HEADERS
    )
    candidate = r.json()[0]

    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/contacts?select=id,first_name,last_name,phone&id=eq.{args.poc_contact_id}",
        headers=SB_HEADERS
    )
    poc = r.json()[0]

    body = f"""{candidate["first_name"]}, great connecting you with {poc["first_name"]} at {args.client_name}.

You are confirmed to start {args.start_date}.
Please report to {args.address}, arriving {args.start_time}. {poc["first_name"]} will walk you through everything when you arrive. Call {args.poc_pronoun_obj} directly on arrival so {args.poc_pronoun} can bring you through.

{poc["first_name"]}, {candidate["first_name"]} is all set and ready to go.
Looking forward to a great start - K

Kai
PEAKrecruiting
Questions? (470) 857-4325"""

    to_numbers = [normalize_phone(candidate["phone"]), normalize_phone(poc["phone"])]
    print(f"[day1_link] Candidate: {candidate['first_name']} {candidate['last_name']} -> {to_numbers[0]}")
    print(f"[day1_link] POC: {poc['first_name']} {poc['last_name']} -> {to_numbers[1]}")
    print(f"[day1_link] Preview:\n{body}")

    if args.dry_run:
        print("[day1_link] DRY RUN -- no send")
        return

    token = get_rc_token()
    resp = requests.post(
        "https://platform.ringcentral.com/restapi/v1.0/account/~/extension/~/sms",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "from": {"phoneNumber": RC_FROM},
            "to": [{"phoneNumber": n} for n in to_numbers],
            "text": body
        },
        timeout=15
    )
    print(f"[day1_link] RC status={resp.status_code}")

    if resp.status_code in (200, 201):
        msg_id = resp.json().get("id", "")
        requests.post(
            f"{SUPABASE_URL}/rest/v1/sms_send_queue",
            headers=SB_HEADERS,
            json={
                "candidate_id": args.candidate_id,
                "to_number": to_numbers[0],
                "cc_number": to_numbers[1],
                "cc_contact_id": args.poc_contact_id,
                "from_number": RC_FROM,
                "body": body,
                "template_id": 55,
                "template_name": "Day 1 Placement Link -- Candidate + Client POC",
                "status": "sent",
                "channel": "rc_group",
                "rc_message_id": str(msg_id),
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "created_by": "day1_link"
            }
        )
        print(f"[day1_link] SUCCESS -- group MMS sent to {to_numbers}")
    else:
        print(f"[day1_link] FAILED: {resp.text[:300]}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
