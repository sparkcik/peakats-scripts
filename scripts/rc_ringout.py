import os, sys, argparse, requests

RC_CLIENT_ID     = os.environ.get("RC_CLIENT_ID", "")
RC_CLIENT_SECRET = os.environ.get("RC_CLIENT_SECRET", "")
RC_JWT           = os.environ.get("RC_JWT", "")
RC_SERVER        = "https://platform.ringcentral.com"
RC_FROM          = os.environ.get("RC_FROM_NUMBER", "+14043862799")

def get_token():
    r = requests.post(
        f"{RC_SERVER}/restapi/oauth/token",
        auth=(RC_CLIENT_ID, RC_CLIENT_SECRET),
        data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": RC_JWT},
        timeout=10
    )
    r.raise_for_status()
    return r.json()["access_token"]

def normalize(phone):
    digits = "".join(c for c in str(phone) if c.isdigit())
    if len(digits) == 10:
        return "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return "+1" + digits

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", required=True)
    args = parser.parse_args()
    to = normalize(args.to)
    print(f"[rc_ringout] from={RC_FROM} to={to}")
    token = get_token()
    resp = requests.post(
        f"{RC_SERVER}/restapi/v1.0/account/~/extension/~/ring-out",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"from": {"phoneNumber": RC_FROM}, "to": {"phoneNumber": to}, "playPrompt": False, "country": {"id": "1"}},
        timeout=15
    )
    print(f"[rc_ringout] status={resp.status_code}")
    if resp.status_code in (200, 201):
        print(f"[rc_ringout] SUCCESS")
    else:
        print(f"[rc_ringout] FAILED: {resp.text[:300]}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
