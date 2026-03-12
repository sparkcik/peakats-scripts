#!/usr/bin/env python3
"""
PEAK — OAuth Re-Authorization for Gmail Send
Run this once on your Mac to get a new refresh token that includes gmail.send scope.

This opens a browser window. After authorizing, it prints the new refresh_token.
Paste that token into gcic_batch_filler_v3.py → OAUTH["refresh_token"].

Usage:
    python3 reauth_gmail_send.py
"""

import json
import urllib.request
import urllib.parse
import http.server
import threading
import webbrowser
import secrets

CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
REDIRECT_URI  = "http://localhost:8765/callback"
PORT          = 8765

SCOPES = " ".join([
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
])

auth_code = None
state = secrets.token_urlsafe(16)


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h2>Authorization complete. You can close this tab.</h2></body></html>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<html><body><h2>Error: no code received.</h2></body></html>")

    def log_message(self, format, *args):
        pass  # suppress server logs


def main():
    # Build auth URL
    params = urllib.parse.urlencode({
        "client_id":     CLIENT_ID,
        "redirect_uri":  REDIRECT_URI,
        "response_type": "code",
        "scope":         SCOPES,
        "access_type":   "offline",
        "prompt":        "consent",   # force refresh_token to be returned
        "state":         state,
    })
    auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{params}"

    # Start local callback server
    server = http.server.HTTPServer(("localhost", PORT), CallbackHandler)
    thread = threading.Thread(target=server.handle_request)
    thread.start()

    print("=" * 60)
    print("PEAK OAuth Re-Authorization")
    print("=" * 60)
    print(f"\nScopes requested:")
    for s in SCOPES.split():
        print(f"  • {s}")
    print(f"\nOpening browser for authorization...")
    print(f"(If browser doesn't open, visit this URL manually:)\n{auth_url}\n")

    webbrowser.open(auth_url)
    thread.join(timeout=120)

    if not auth_code:
        print("❌ No authorization code received. Did you approve in the browser?")
        return

    # Exchange code for tokens
    token_data = urllib.parse.urlencode({
        "code":          auth_code,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri":  REDIRECT_URI,
        "grant_type":    "authorization_code",
    }).encode()

    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=token_data,
        method="POST"
    )
    with urllib.request.urlopen(req) as r:
        tokens = json.loads(r.read())

    if "refresh_token" not in tokens:
        print("❌ No refresh_token in response. Try revoking access at:")
        print("   https://myaccount.google.com/permissions")
        print("   Then re-run this script.")
        print(f"\nFull response: {json.dumps(tokens, indent=2)}")
        return

    print("=" * 60)
    print("✅ SUCCESS — New refresh token with gmail.send scope:")
    print("=" * 60)
    print(f'\nrefresh_token: "{tokens["refresh_token"]}"')
    print("\nPaste this into gcic_batch_filler_v3.py:")
    print('  OAUTH = {')
    print(f'    "refresh_token": "{tokens["refresh_token"]}",')
    print('    ... (keep client_id and client_secret as-is)')
    print('  }')
    print("=" * 60)


if __name__ == "__main__":
    main()
