#!/usr/bin/env python3
"""
forge_runner.py -- forge-local executor daemon
Part of Crucible OS / forge-local

Listens for authenticated webhook commands from forge-cloud/bridge
and executes whitelisted scripts on the local Mac.

Port:    5678
Auth:    X-Forge-Key header (set in FORGE_RUNNER_KEY env var)
Logging: ~/peakats-scripts/logs/forge_runner.log

Usage:
    python3 infra/forge_runner.py

Managed by launchd (com.crucible.forge-runner.plist)

v1.4.0 -- 2026-04-06
  - Fixed _poll_sms_queue: now executes sms_queue_poller.py via subprocess
  - Fixed _run_daily_reminders: now executes reminder scripts via subprocess
  - Removed twilio_call_sync from WHITELIST (script does not exist)
  - pg_cron is the primary scheduler; internal scheduler is backup
"""

import os
import base64
import subprocess
import logging
import json
import threading
import time
import schedule
from datetime import datetime, timezone
from pathlib import Path
from flask import Flask, request, jsonify, Response, make_response
import requests as http_requests

# -- Config -------------------------------------------------------------------

PORT = 5678
SCRIPTS_DIR = Path(os.environ.get("SCRIPTS_DIR", str(Path.home() / "peakats-scripts")))
LOG_DIR = Path(os.environ.get("LOG_DIR", str(SCRIPTS_DIR / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)

AUTH_KEY = os.environ.get("FORGE_RUNNER_KEY", "forge-local-2026")

# -- Command Whitelist --------------------------------------------------------
# Maps command alias -> actual script path + default args
# Only these commands can be executed. Nothing else.

WHITELIST = {
    "fadv_update": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_fadv_update_v6.2.py"),
        "description": "FADV reconciliation -- sync BG/drug status from CSV exports to Supabase",
        "allowed_args": ["--batch", "--client"],
    },
    "fadv_update_batch": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_fadv_update_v6.2.py"),
        "description": "FADV reconciliation -- all clients batch mode",
        "fixed_args": ["--batch"],
        "allowed_args": [],
    },
    "rig_process": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_rig_processor_v2.py"),
        "description": "Resume scoring via Gemini -- process new resumes for a client",
        "allowed_args": ["--client", "--limit", "--create-unmatched"],
    },
    "batch_process": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_process_batch_v2.py"),
        "description": "Batch resume processing -- full pipeline for a client folder",
        "allowed_args": ["--client"],
    },
    "find_missing": {
        "script": str(SCRIPTS_DIR / "scripts" / "find_missing_resumes.py"),
        "description": "Find candidates with no RWP score",
        "allowed_args": ["--client"],
    },
    "score_missing": {
        "script": str(SCRIPTS_DIR / "scripts" / "score_missing_resumes.py"),
        "description": "Score candidates that have resumes but no RWP score",
        "allowed_args": ["--client"],
    },
    "fadv_bot": {
        "script": str(SCRIPTS_DIR / "fadv" / "fadv_entry_bot.py"),
        "description": "FADV entry bot -- submit candidates to FADV portal",
        "allowed_args": ["--client", "--test", "--list"],
    },
    "sms_queue": {
        "script": str(SCRIPTS_DIR / "scripts" / "twilio_sms_send.py"),
        "description": "SMS queue poller -- send pending scheduled messages via Twilio",
        "allowed_args": ["--dry-run", "--limit"],
    },
    "shell": {
        "script": "__shell__",
        "description": "Read-only shell commands for Forge file audits (find, ls, cat, git)",
        "allowed_args": ["cmd"],
    },
    "ping": {
        "script": None,
        "description": "Health check -- returns daemon status",
        "allowed_args": [],
    },
    "rc_inbox": {
        "script": str(SCRIPTS_DIR / "rc_inbox_command.py"),
        "description": "Read RC SMS inbox -- returns inbound messages with candidate matches",
        "allowed_args": ["--limit", "--unread-only", "--mark-read", "--format"],
    },
    "rc_inbox_cron": {
        "script": str(SCRIPTS_DIR / "rc_inbox_cron.py"),
        "description": "Poll RC inbox, match candidates, write to candidate_comms + sms_triage_queue",
        "allowed_args": ["--dry-run"],
    },
    "rc_data_capture": {
        "script": str(SCRIPTS_DIR / "scripts" / "rc_data_capture_cloud.py"),
        "description": "Capture RC SMS + call history to Supabase archive tables",
        "allowed_args": ["--days"],
    },
    "twilio_send": {
        "script": str(SCRIPTS_DIR / "scripts" / "twilio_sms_send.py"),
        "description": "Send pending SMS via Twilio from sms_send_queue",
        "allowed_args": ["--dry-run", "--limit"],
    },
    "twilio_blast": {
        "script": str(SCRIPTS_DIR / "scripts" / "twilio_blast.py"),
        "description": "Blast Template 39 to unsent rc_contact_export numbers via Twilio",
        "allowed_args": ["--dry-run", "--limit"],
    },
    "gcic_outreach": {
        "script": str(SCRIPTS_DIR / "scripts" / "gcic_outreach_trigger.py"),
        "description": "GCIC outreach -- queue Template 2 SMS for BG In Progress candidates",
        "allowed_args": ["--dry-run"],
    },
    "mec_outreach": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_outreach_trigger.py"),
        "description": "MEC/DL outreach -- send Template 15/46/37 SMS based on drug/BG status",
        "allowed_args": [],
    },
    "mec_dl_backfill": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_dl_backfill.py"),
        "description": "Backfill existing MEC/DL form responses into Supabase",
        "allowed_args": [],
    },
    "fadv_profile_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "fadv_profile_reminder.py"),
        "description": "FADV profile completion cadence -- T67/T68 daily escalation + Day 3 flag",
        "allowed_args": ["--dry-run"],
    },
    "mec_dl_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_dl_reminder.py"),
        "description": "MEC/DL reminder cadence -- 3-day escalating reminders (T16/17/18)",
        "allowed_args": ["--dry-run"],
    },
    "drug_screen_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "drug_screen_reminder.py"),
        "description": "Drug screen reminder cadence -- 3-day escalating reminders (T48/49/50)",
        "allowed_args": ["--dry-run"],
    },
    "fadv_action_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "fadv_action_reminder.py"),
        "description": "FADV action reminder cadence -- 3-day escalating reminders (T42/43/44)",
        "allowed_args": ["--dry-run"],
    },
    "gcic_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "gcic_reminder.py"),
        "description": "GCIC reminder cadence -- 3-day escalating reminders (T8/9/10)",
        "allowed_args": ["--dry-run"],
    },
    "mec_dl_fup": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_dl_fup_scheduler.py"),
        "description": "MEC/DL follow-up scheduler",
        "allowed_args": [],
    },
    "day1_link": {
        "script": "scripts/day1_link.py",
        "allowed_args": [
            "--candidate-id", "--poc-contact-id", "--start-date",
            "--start-time", "--address", "--client-name",
            "--poc-pronoun", "--poc-pronoun-obj", "--dry-run"
        ],
    },
    "rc_ringout": {
        "script": "scripts/rc_ringout.py",
        "allowed_args": ["--to"],
    },
    "indeed_intake": {
        "script": str(SCRIPTS_DIR / "scripts" / "indeed_intake_processor.py"),
        "description": "Indeed intake -- fetch portal pages via Chrome cookies, parse resumes, score + write to Supabase",
        "allowed_args": ["--dry_run", "--batch_size", "--station", "--skip_score"],
    },
}

# -- Safe shell commands (read-only whitelist) ---------------------------------
SAFE_SHELL_PREFIXES = ("find ", "ls ", "cat ", "head ", "tail ", "wc ", "git ", "echo ")

# -- Logging ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "forge_runner.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("forge-runner")

# -- Flask App ----------------------------------------------------------------

app = Flask(__name__)


@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, PUT, DELETE"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Forge-Key"
    return response

@app.route("/twilio/preflight", methods=["OPTIONS", "GET"])
def cors_preflight():
    return "", 200

import traceback as _tb

@app.errorhandler(Exception)
def _handle_exception(e):
    log.error(f"Unhandled: {_tb.format_exc()}")
    return jsonify({"error": str(e), "trace": _tb.format_exc()[-800:]}), 500

def authenticate(req):
    """Validate X-Forge-Key header."""
    return req.headers.get("X-Forge-Key") == AUTH_KEY


def build_command(command: str, args: dict) -> list | None:
    spec = WHITELIST.get(command)
    if not spec:
        return None, f"Unknown command: {command}"

    if command == "ping":
        return ["ping"], None

    if command == "shell":
        cmd_str = args.get("cmd", "").strip()
        if not cmd_str:
            return None, "cmd arg required for shell command"
        if not any(cmd_str.startswith(p) for p in SAFE_SHELL_PREFIXES):
            return None, f"Shell command not allowed: {cmd_str}. Allowed prefixes: {SAFE_SHELL_PREFIXES}"
        return ["bash", "-c", cmd_str], None

    script = spec["script"]
    if not Path(script).exists():
        return None, f"Script not found: {script}"

    cmd = ["python3", script]

    if "fixed_args" in spec:
        cmd.extend(spec["fixed_args"])

    allowed = {a.lstrip("-") for a in spec.get("allowed_args", [])}
    if isinstance(args, list):
        for arg in args:
            arg_str = str(arg)
            if arg_str.startswith("--"):
                if arg_str.lstrip("-") not in allowed:
                    return None, f"Arg not allowed for {command}: {arg_str}"
            cmd.append(arg_str)
    else:
        for key, val in args.items():
            flag = f"--{key}" if not key.startswith("--") else key
            flag_bare = flag.lstrip("-")
            if flag_bare not in allowed:
                return None, f"Arg not allowed for {command}: {flag}"
            cmd.append(flag)
            if val is not None and val != "":
                cmd.append(str(val))

    return cmd, None


# -- Routes -------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "running",
        "daemon": "forge-runner",
        "version": "1.5.0",
        "timestamp": datetime.now().isoformat(),
    })


@app.route("/run", methods=["POST"])
def run_command():
    if not authenticate(request):
        log.warning(f"Unauthorized request from {request.remote_addr}")
        return jsonify({"error": "Unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    command = body.get("command", "").strip()
    args = body.get("args", {})
    run_async = body.get("async", False)

    if not command:
        return jsonify({"error": "command required"}), 400

    log.info(f"Received command: {command} | args: {args} | async: {run_async}")

    if command == "ping":
        return jsonify({
            "status": "ok",
            "command": "ping",
            "whitelist": list(WHITELIST.keys()),
            "timestamp": datetime.now().isoformat(),
        })

    cmd, err = build_command(command, args)
    if err:
        log.error(f"Command build failed: {err}")
        return jsonify({"error": err}), 400

    log.info(f"Executing: {' '.join(cmd)}")

    try:
        if run_async:
            subprocess.Popen(
                cmd,
                stdout=open(LOG_DIR / f"{command}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", "w"),
                stderr=subprocess.STDOUT,
                cwd=str(SCRIPTS_DIR),
            )
            return jsonify({
                "status": "started",
                "command": command,
                "args": args,
                "message": f"Running async. Check logs at {LOG_DIR}",
            })
        else:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
                cwd=str(SCRIPTS_DIR),
            )
            return jsonify({
                "status": "complete",
                "command": command,
                "args": args,
                "returncode": result.returncode,
                "stdout": result.stdout[-3000:] if result.stdout else "",
                "stderr": result.stderr[-1000:] if result.stderr else "",
                "success": result.returncode == 0,
            })

    except subprocess.TimeoutExpired:
        log.error(f"Command timed out: {command}")
        return jsonify({"error": "Command timed out (300s limit)"}), 504

    except Exception as e:
        log.error(f"Execution error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/whitelist", methods=["GET"])
def list_whitelist():
    if not authenticate(request):
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify({
        "commands": {
            name: {
                "description": spec.get("description", ""),
                "allowed_args": spec.get("allowed_args", []),
            }
            for name, spec in WHITELIST.items()
        }
    })


# -- Twilio Webhook Routes (public) -------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
TWILIO_FROM_NUMBER = os.environ.get("TWILIO_FROM_NUMBER", "+14704704766")

_SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


def _clean_phone(phone):
    if not phone:
        return ""
    return phone.replace("+1", "").replace("-", "").replace("(", "").replace(")", "").replace(" ", "")


def _match_candidate(phone):
    clean = _clean_phone(phone)
    if not SUPABASE_URL:
        return None
    resp = http_requests.get(
        f"{SUPABASE_URL}/rest/v1/candidates",
        headers=_SB_HEADERS,
        params={"phone": f"eq.{clean}", "select": "id,first_name,last_name,client_id,status"},
    )
    rows = resp.json() if resp.status_code == 200 else []
    return rows[0] if rows else None


TWIML_EMPTY = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'
TWIML_GREETING = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Play>https://eyopvsmsvbgfuffscfom.supabase.co/storage/v1/object/public/voicemail/kai_voicemail.mp3</Play>
    <Record maxLength="120" action="/twilio/voice/recording" transcribe="false" />
    <Say voice="alice">We did not receive a recording. Goodbye.</Say>
</Response>"""
TWIML_RECORDING_ACK = '<?xml version="1.0" encoding="UTF-8"?><Response><Say voice="alice">Thank you. Goodbye.</Say><Hangup/></Response>'


@app.route("/twilio/call", methods=["POST", "OPTIONS"])
def twilio_outbound_call():
    if request.method == "OPTIONS":
        return "", 200
    data = request.json or {}
    to = data.get("to", "")
    if not to:
        return jsonify({"error": "to is required"}), 400
    digits = "".join(c for c in to if c.isdigit())
    if len(digits) == 10:
        digits = "1" + digits
    to_e164 = "+" + digits
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    try:
        r = http_requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Calls.json",
            auth=(account_sid, auth_token),
            data={
                "To": "+14043862799",
                "From": TWILIO_FROM_NUMBER,
                "Twiml": "<Response><Dial timeout='30'><Number>" + to_e164 + "</Number></Dial></Response>"
            }
        )
        body = r.json()
        log.info(f"[twilio/call] {to_e164} -> {body.get('sid','')}")
        if r.status_code >= 400:
            return jsonify({"error": body.get("message", "call failed")}), 500
        now = datetime.now(timezone.utc).isoformat()
        candidate = _match_candidate(to_e164)
        http_requests.post(
            f"{SUPABASE_URL}/rest/v1/candidate_comms",
            headers={**_SB_HEADERS, "Prefer": "return=minimal"},
            json={
                "candidate_id": candidate["id"] if candidate else None,
                "client_id": candidate["client_id"] if candidate else None,
                "channel": "voice",
                "direction": "outbound",
                "body": f"Outbound call to {to_e164}",
                "sent_at": now,
                "sent_by": "pwa_dialpad",
                "send_mode": "manual",
                "from_number": _clean_phone(TWILIO_FROM_NUMBER),
                "to_number": _clean_phone(to_e164),
                "delivery_status": body.get("status", "queued"),
                "external_message_id": body.get("sid", ""),
            },
        )
        return jsonify({"status": body.get("status"), "sid": body.get("sid")})
    except Exception as e:
        log.error(f"[twilio/call] Exception: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/twilio/send", methods=["POST", "OPTIONS"])
def twilio_send_sms():
    if request.method == "OPTIONS":
        return "", 200
    data = request.json or {}
    to = data.get("to", "")
    body_text = data.get("body", "")
    candidate_id = data.get("candidate_id")
    template_name = data.get("template_name", "direct_send")
    if not to:
        return jsonify({"error": "to is required"}), 400
    digits = "".join(c for c in to if c.isdigit())
    if len(digits) == 10:
        digits = "1" + digits
    to_e164 = "+" + digits
    to_clean = digits[-10:] if len(digits) >= 10 else digits
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    now = datetime.now(timezone.utc).isoformat()

    # Write queue row before sending -- P2 requirement: all sends must log to sms_send_queue
    queue_row = {
        "to_number": to_clean,
        "body": body_text,
        "status": "pending",
        "migration_status": "twilio_active",
        "scheduled_for": now,
        "created_at": now,
        "updated_at": now,
        "template_name": template_name,
    }
    if candidate_id:
        queue_row["candidate_id"] = candidate_id
    queue_resp = http_requests.post(
        f"{SUPABASE_URL}/rest/v1/sms_send_queue",
        headers={**_SB_HEADERS, "Prefer": "return=representation"},
        json=queue_row,
    )
    queue_id = None
    if queue_resp.status_code in (200, 201):
        rows = queue_resp.json()
        queue_id = rows[0].get("id") if rows else None
        log.info(f"[twilio/send] Queue row created id={queue_id}")
    else:
        log.warning(f"[twilio/send] Queue row failed: {queue_resp.status_code} {queue_resp.text[:200]}")

    try:
        r = http_requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            auth=(account_sid, auth_token),
            data={"To": to_e164, "From": TWILIO_FROM_NUMBER, "Body": body_text or " "}
        )
        body_resp = r.json()
        if r.status_code >= 400:
            if queue_id:
                http_requests.patch(
                    f"{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{queue_id}",
                    headers={**_SB_HEADERS, "Prefer": "return=minimal"},
                    json={"status": "failed", "delivery_status": "failed", "updated_at": datetime.now(timezone.utc).isoformat()},
                )
            return jsonify({"error": body_resp.get("message", "send failed")}), 500
        sid = body_resp.get("sid")
        status = body_resp.get("status")
        # Update queue row with SID and delivery status
        if queue_id:
            http_requests.patch(
                f"{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{queue_id}",
                headers={**_SB_HEADERS, "Prefer": "return=minimal"},
                json={"status": "sent", "twilio_sid": sid, "delivery_status": status, "sent_at": datetime.now(timezone.utc).isoformat(), "updated_at": datetime.now(timezone.utc).isoformat()},
            )
        log.info(f"[twilio/send] Sent to {to_e164} sid={sid} queue_id={queue_id}")
        return jsonify({"status": status, "sid": sid, "queue_id": queue_id})
    except Exception as e:
        log.error(f"[twilio/send] Exception: {e}")
        if queue_id:
            http_requests.patch(
                f"{SUPABASE_URL}/rest/v1/sms_send_queue?id=eq.{queue_id}",
                headers={**_SB_HEADERS, "Prefer": "return=minimal"},
                json={"status": "failed", "delivery_status": "failed", "updated_at": datetime.now(timezone.utc).isoformat()},
            )
        return jsonify({"error": str(e)}), 500

@app.route("/twilio/sms", methods=["POST"])
def twilio_inbound_sms():
    from_number = request.form.get("From", "")
    body = request.form.get("Body", "")
    media_url = request.form.get("MediaUrl0", "")
    message_sid = request.form.get("MessageSid", "")
    log.info(f"[twilio] Inbound SMS from {from_number}: {body[:80]}")
    candidate = _match_candidate(from_number)
    now = datetime.now(timezone.utc).isoformat()
    clean_from = _clean_phone(from_number)

    http_requests.post(
        f"{SUPABASE_URL}/rest/v1/sms_triage_queue",
        headers={**_SB_HEADERS, "Prefer": "return=minimal"},
        json={
            "from_number": clean_from,
            "body": body,
            "rc_message_id": f"twilio-{message_sid}",
            "candidate_id": candidate["id"] if candidate else None,
            "received_at": now,
            "needs_reply": True,
            "priority": "normal" if candidate else "unknown",
            "category": "candidate" if candidate else "unmatched",
            "media_url": media_url or None,
        },
    )

    if candidate:
        http_requests.post(
            f"{SUPABASE_URL}/rest/v1/candidate_comms",
            headers={**_SB_HEADERS, "Prefer": "return=minimal"},
            json={
                "candidate_id": candidate["id"],
                "client_id": candidate["client_id"],
                "channel": "sms",
                "direction": "inbound",
                "body": body,
                "sent_at": now,
                "sent_by": "twilio_webhook",
                "send_mode": "automated",
                "from_number": clean_from,
                "to_number": _clean_phone(TWILIO_FROM_NUMBER),
                "delivery_status": "delivered",
                "external_message_id": message_sid,
            },
        )
        log.info(f"[twilio] Logged inbound SMS -> candidate {candidate['id']}")
    else:
        log.warning(f"[twilio] No candidate match for {from_number} -- logged to triage as unmatched")
    return Response(TWIML_EMPTY, mimetype="application/xml")


@app.route("/twilio/voice", methods=["POST"])
def twilio_inbound_voice():
    from_number = request.form.get("From", "")
    call_sid = request.form.get("CallSid", "")
    log.info(f"[twilio] Inbound call from {from_number}")
    clean = "".join(c for c in from_number if c.isdigit())
    if clean.startswith("1") and len(clean) == 11:
        clean = clean[1:]
    whisper_url = f"https://eyopvsmsvbgfuffscfom.supabase.co/functions/v1/twilio-whisper?caller={clean}"
    caller_id = from_number if from_number else TWILIO_FROM_NUMBER
    candidate = _match_candidate(from_number)
    now = datetime.now(timezone.utc).isoformat()
    http_requests.post(
        f"{SUPABASE_URL}/rest/v1/candidate_comms",
        headers={**_SB_HEADERS, "Prefer": "return=minimal"},
        json={
            "candidate_id": candidate["id"] if candidate else None,
            "client_id": candidate["client_id"] if candidate else None,
            "channel": "voice",
            "direction": "inbound",
            "body": f"Inbound call from {from_number}",
            "sent_at": now,
            "sent_by": "twilio_webhook",
            "send_mode": "automated",
            "from_number": _clean_phone(from_number),
            "to_number": _clean_phone(TWILIO_FROM_NUMBER),
            "delivery_status": "delivered",
            "external_message_id": call_sid,
        },
    )
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Dial callerId="{caller_id}" timeout="12" action="/twilio/voice/missed">
        <Number url="{whisper_url}">+14043862799</Number>
    </Dial>
</Response>"""
    return Response(twiml, mimetype="application/xml")


@app.route("/twilio/voice/recording", methods=["POST"])
def twilio_voice_recording():
    from_number = request.form.get("From", "")
    call_sid = request.form.get("CallSid", "")
    recording_url = request.form.get("RecordingUrl", "")
    recording_sid = request.form.get("RecordingSid", "")
    recording_duration = int(request.form.get("RecordingDuration", "0"))
    log.info(f"[twilio] Recording from {from_number}: {recording_url} ({recording_duration}s)")
    candidate = _match_candidate(from_number)
    now = datetime.now(timezone.utc).isoformat()
    clean_from = _clean_phone(from_number)
    candidate_name = None
    if candidate:
        candidate_name = f"{candidate.get('first_name','')} {candidate.get('last_name','')}".strip()

    http_requests.post(
        f"{SUPABASE_URL}/rest/v1/twilio_voicemail",
        headers={**_SB_HEADERS, "Prefer": "return=minimal"},
        json={
            "call_sid": call_sid,
            "recording_sid": recording_sid,
            "recording_url": recording_url + ".mp3",
            "from_number": clean_from,
            "to_number": _clean_phone(TWILIO_FROM_NUMBER),
            "duration_seconds": recording_duration,
            "candidate_id": candidate["id"] if candidate else None,
            "candidate_name": candidate_name,
            "listened": False,
            "created_at": now,
        },
    )
    log.info(f"[twilio] Voicemail saved to twilio_voicemail from {from_number}")
    return Response(TWIML_RECORDING_ACK, mimetype="application/xml")


@app.route("/twilio/voice/missed", methods=["POST"])
def twilio_voice_missed():
    """Fires when the forward to Charles's cell times out -- play Kai voicemail."""
    log.info("[twilio] Call missed -- playing Kai voicemail")
    return Response(TWIML_GREETING, mimetype="application/xml")


@app.route("/legal", methods=["GET"])
def legal_tracker():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Legal Resource Tracker</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#ffffff;--bg2:#f8f8f6;--bg3:#f0efe9;
  --text:#1a1a1a;--text2:#555;--text3:#888;
  --border:#e5e3da;--border2:#d0cec5;
  --card:#ffffff;--card2:#f8f8f6;
  --green-bg:#E1F5EE;--green:#0F6E56;--green-dark:#085041;
  --amber-bg:#FAEEDA;--amber:#854F0B;--amber-dark:#633806;
  --blue-bg:#E6F1FB;--blue:#185FA5;
  --accent:#c8a84b;
  --script-bg:#f0efe9;--script-border:#d0cec5;
}
@media(prefers-color-scheme:dark){:root{
  --bg:#0f1117;--bg2:#141c26;--bg3:#0d1520;
  --text:#e2e8f0;--text2:#94a3b8;--text3:#475569;
  --border:#1e2d3d;--border2:#2d3748;
  --card:#141c26;--card2:#0f1117;
  --green-bg:#0d1f16;--green:#34d399;--green-dark:#a7f3d0;
  --amber-bg:#1a1500;--amber:#fbbf24;--amber-dark:#fde68a;
  --blue-bg:#1e3a5f;--blue:#60a5fa;
  --script-bg:#0d1f16;--script-border:rgba(200,168,75,0.3);
}}
body{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding:24px 16px}
.wrap{max-width:900px;margin:0 auto}
h1{font-size:20px;font-weight:600;color:#fff;margin-bottom:4px}
.sub{font-size:13px;color:var(--text2);margin-bottom:20px}
.script-box{background:var(--script-bg);border:1px solid var(--script-border);border-radius:10px;padding:16px 20px;margin-bottom:24px}
.script-label{font-size:11px;font-weight:600;color:var(--accent);letter-spacing:0.08em;text-transform:uppercase;margin-bottom:8px}
.script-text{font-size:14px;color:var(--text);line-height:1.7;font-style:italic}
.copy-btn{margin-top:10px;font-size:12px;padding:6px 14px;border-radius:6px;border:0.5px solid var(--script-border);background:var(--bg3);color:var(--accent);cursor:pointer;font-family:inherit}
.copy-btn:hover{opacity:0.85}
.stats{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap}
.stat{background:var(--bg2);border-radius:8px;padding:10px 16px;min-width:110px}
.stat-n{font-size:22px;font-weight:600;color:var(--text)}
.stat-n.called{color:#34d399}
.stat-n.msg{color:#fbbf24}
.stat-l{font-size:11px;color:var(--text3);margin-top:2px}
.filters{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.fb{font-size:12px;padding:5px 12px;border-radius:6px;border:0.5px solid var(--border);background:var(--bg2);color:var(--text2);cursor:pointer;font-family:inherit}
.fb.active{background:var(--bg);color:var(--text);border-color:var(--border2)}
.card{background:var(--card);border:0.5px solid var(--border);border-radius:10px;margin-bottom:10px;overflow:hidden;transition:border-color 0.15s}
.card:hover{border-color:var(--border2)}
.card.st-called{border-left:3px solid var(--green)}
.card.st-left-msg{border-left:3px solid var(--amber)}
.card.st-no-answer{border-left:3px solid var(--text3)}
.card.st-appt-set{border-left:3px solid var(--blue)}
.card-top{display:grid;grid-template-columns:36px 80px 1fr auto;gap:12px;align-items:start;padding:12px 14px}
.pri{width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:600;flex-shrink:0;margin-top:2px}
.pri.free{background:var(--green-bg);color:var(--green);border:1px solid var(--green)}
.pri.paid{background:var(--amber-bg);color:var(--amber);border:1px solid var(--amber)}
.badge{font-size:10px;font-weight:600;padding:3px 8px;border-radius:6px;display:inline-block;margin-top:3px}
.badge.free{background:var(--green-bg);color:var(--green);border:0.5px solid var(--green)}
.badge.paid{background:var(--amber-bg);color:var(--amber);border:0.5px solid var(--amber)}
.org-name{font-size:14px;font-weight:500;color:var(--text);margin-bottom:3px}
.org-meta{font-size:12px;color:var(--text2);margin-bottom:2px}
.org-what{font-size:12px;color:var(--text2)}
.org-note{font-size:11px;color:var(--text3);font-style:italic;margin-top:2px}
.actions{display:flex;flex-direction:column;align-items:flex-end;gap:8px;min-width:130px}
select{font-size:12px;padding:5px 8px;border-radius:6px;border:0.5px solid var(--border2);background:var(--bg2);color:var(--text);cursor:pointer;width:130px;font-family:inherit}
.call-btn{font-size:12px;color:var(--blue);background:var(--blue-bg);padding:5px 10px;border-radius:6px;white-space:nowrap;border:none;cursor:pointer;width:130px;text-align:center;font-family:inherit}
.call-btn:hover{opacity:0.85}
.notes-wrap{padding:0 14px 12px}
textarea{width:100%;font-size:13px;padding:8px 10px;border-radius:6px;border:0.5px solid var(--border);background:var(--bg2);color:var(--text);resize:vertical;min-height:80px;font-family:'DM Sans',sans-serif;line-height:1.5}
textarea:focus{outline:none;border-color:#4a5568}
textarea::placeholder{color:var(--text3)}
</style>
</head>
<body>
<div class="wrap">
  <h1>Legal Resource Tracker</h1>
  <p class="sub">Foreclosure defense and Chapter 13 contacts</p>

  <div class="script-box">
    <div class="script-label">Hardship Script -- Copy and say this on every call</div>
    <div class="script-text" id="script-text">"86-year-old mother on low income, hospitalized and in rehab for 2 months, Rocket Mortgage foreclosure sale only weeks away. Need emergency Chapter 13 automatic stay filing to stop the sale and cure arrears. Can you help file or see her right away?"</div>
    <button class="copy-btn" onclick="copyScript()">Copy script</button>
  </div>

  <div class="stats" id="stats"></div>
  <div class="filters">
    <button class="fb active" onclick="setFilter('all',this)">All</button>
    <button class="fb" onclick="setFilter('free',this)">Free only</button>
    <button class="fb" onclick="setFilter('paid',this)">Paid only</button>
    <button class="fb" onclick="setFilter('called',this)">Called</button>
    <button class="fb" onclick="setFilter('pending',this)">Not yet called</button>
  </div>
  <div id="list"></div>
</div>

<script>
const DATA=[
  {id:1,type:'free',org:'Georgia Legal Services Program',loc:'Savannah / Chatham',phone:'9126512180',phoneDisplay:'(912) 651-2180',what:'Free Ch13 filing, foreclosure defense, low-income representation',note:'Local Savannah office -- call first for Chatham'},
  {id:2,type:'free',org:'Elderly Legal Assistance Program (ELAP)',loc:'Chatham / Savannah area',phone:'8882208399',phoneDisplay:'1-888-220-8399',what:'Free legal help for 60+, Ch13 referral & housing issues',note:'Seniors 60+ -- perfect for Mom, fast intake'},
  {id:3,type:'free',org:'Georgia Senior Legal Aid Hotline',loc:'Statewide (Atlanta)',phone:'4043899992',phoneDisplay:'(404) 389-9992',what:'Free advice & referral for seniors facing foreclosure',note:'Mon-Thu mornings; 60+ priority'},
  {id:4,type:'free',org:'Atlanta Legal Aid Society',loc:'Atlanta Metro',phone:'4045245811',phoneDisplay:'(404) 524-5811',what:'Free foreclosure help, Ch13 coordination',note:'Strong on Rocket cases; low-income'},
  {id:5,type:'free',org:'Georgia Legal Services Program Statewide',loc:'Statewide',phone:'18334577529',phoneDisplay:'1-833-457-7529',what:'Routes to local free attorney for Ch13/foreclosure',note:'Backup if Savannah line busy'},
  {id:6,type:'paid',org:'Gastin & Hill, Attorneys at Law',loc:'Savannah',phone:'9122320203',phoneDisplay:'(912) 232-0203',what:'Emergency Chapter 13, foreclosure stop',note:'Free consult; experienced in Savannah sales'},
  {id:7,type:'paid',org:'Barbara B. Braziel Law',loc:'Savannah / Chatham',phone:'8335221069',phoneDisplay:'(833) 522-1069',what:'Chapter 13 filings, debt relief',note:'Free initial consult; senior-friendly'},
  {id:8,type:'paid',org:'Law Office of Jeffrey S. Hanna',loc:'Savannah',phone:'9122336515',phoneDisplay:'(912) 233-6515',what:'Chapter 13 & foreclosure defense',note:'Free consult; stops imminent sales'},
  {id:9,type:'paid',org:'Craig Black Law Firm',loc:'Atlanta (statewide)',phone:'6788881778',phoneDisplay:'(678) 888-1778',what:'Senior Ch13 emergency filings',note:'Free consult; $0-down options common'},
  {id:10,type:'paid',org:'The Kent Law Firm',loc:'Atlanta',phone:'4045047090',phoneDisplay:'(404) 504-7090',what:'40+ years stopping foreclosures via Ch13',note:'Free consult; high success rate'},
];

let state={};
let filter='all';

function load(){
  DATA.forEach(r=>{
    try{const s=localStorage.getItem('legal_'+r.id);state[r.id]=s?JSON.parse(s):{status:'',notes:''};}
    catch{state[r.id]={status:'',notes:''};}
  });
  render();
}

function save(id){try{localStorage.setItem('legal_'+id,JSON.stringify(state[id]));}catch(e){}}

function setFilter(f,btn){
  filter=f;
  document.querySelectorAll('.fb').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  render();
}

function copyScript(){
  const t=document.getElementById('script-text').innerText;
  navigator.clipboard.writeText(t).then(()=>{
    const b=event.target;b.textContent='Copied!';
    setTimeout(()=>b.textContent='Copy script',2000);
  });
}

function render(){
  const called=Object.values(state).filter(s=>s.status==='called').length;
  const leftMsg=Object.values(state).filter(s=>s.status==='left-msg').length;
  const noAnswer=Object.values(state).filter(s=>s.status==='no-answer').length;
  const appt=Object.values(state).filter(s=>s.status==='appt-set').length;
  document.getElementById('stats').innerHTML=
    `<div class="stat"><div class="stat-n called">${called}</div><div class="stat-l">Called</div></div>`+
    `<div class="stat"><div class="stat-n" style="color:var(--amber)">${leftMsg}</div><div class="stat-l">Left message</div></div>`+
    `<div class="stat"><div class="stat-n" style="color:var(--blue)">${appt}</div><div class="stat-l">Appt set</div></div>`+
    `<div class="stat"><div class="stat-n">${noAnswer}</div><div class="stat-l">No answer</div></div>`;

  const rows=DATA.filter(r=>{
    if(filter==='free') return r.type==='free';
    if(filter==='paid') return r.type==='paid';
    if(filter==='called') return state[r.id].status==='called';
    if(filter==='pending') return !state[r.id].status;
    return true;
  });

  document.getElementById('list').innerHTML=rows.map(r=>{
    const s=state[r.id];
    const cls=s.status?'st-'+s.status:'';
    return `<div class="card ${cls}">
      <div class="card-top">
        <div class="pri ${r.type}">${r.id}</div>
        <div><span class="badge ${r.type}">${r.type==='free'?'FREE':'PAID'}</span></div>
        <div>
          <div class="org-name">${r.org}</div>
          <div class="org-meta">${r.loc} &bull; ${r.phoneDisplay}</div>
          <div class="org-what">${r.what}</div>
          <div class="org-note">${r.note}</div>
        </div>
        <div class="actions">
          <select onchange="upStatus(${r.id},this.value)">
            <option value="" ${!s.status?'selected':''}>-- status --</option>
            <option value="called" ${s.status==='called'?'selected':''}>Called</option>
            <option value="left-msg" ${s.status==='left-msg'?'selected':''}>Left message</option>
            <option value="no-answer" ${s.status==='no-answer'?'selected':''}>No answer</option>
            <option value="not-available" ${s.status==='not-available'?'selected':''}>Not available</option>
            <option value="appt-set" ${s.status==='appt-set'?'selected':''}>Appt set</option>
          </select>
          <button class="call-btn" onclick="window.location.href='tel:+1${r.phone}'">Call ${r.phoneDisplay}</button>
        </div>
      </div>
      <div class="notes-wrap">
        <textarea placeholder="Notes -- what they said, attorney name, next steps, appointment time..." onblur="upNotes(${r.id},this.value)">${s.notes}</textarea>
      </div>
    </div>`;
  }).join('');
}

function upStatus(id,val){state[id].status=val;save(id);render();}
function upNotes(id,val){state[id].notes=val;save(id);}
load();
</script>
</body>
</html>"""
    return Response(html, content_type='text/html')


# -- Scheduler ----------------------------------------------------------------
# NOTE: pg_cron is the primary scheduler (hits forge-bridge every 15/30 min).
# The internal scheduler below is a backup layer only.
# Both run independently -- pg_cron does not require forge-runner to be the initiator.

def _run_script(command_name):
    """Execute a whitelisted script via subprocess. Used by internal scheduler."""
    spec = WHITELIST.get(command_name)
    if not spec or not spec.get("script"):
        log.error(f"[scheduler] Unknown or invalid command: {command_name}")
        return
    script = spec["script"]
    if not Path(script).exists():
        log.error(f"[scheduler] Script not found: {script}")
        return
    log.info(f"[scheduler] Running: {command_name}")
    try:
        result = subprocess.run(
            ["python3", script],
            capture_output=True,
            text=True,
            timeout=270,
            cwd=str(SCRIPTS_DIR),
        )
        if result.stdout:
            log.info(f"[scheduler][{command_name}] stdout: {result.stdout[-500:]}")
        if result.stderr:
            log.warning(f"[scheduler][{command_name}] stderr: {result.stderr[-300:]}")
        log.info(f"[scheduler][{command_name}] returncode: {result.returncode}")
    except subprocess.TimeoutExpired:
        log.error(f"[scheduler][{command_name}] timed out after 270s")
    except Exception as e:
        log.error(f"[scheduler][{command_name}] exception: {e}")


def _poll_sms_queue():
    """Execute sms_queue_poller.py every 15 min."""
    _run_script("sms_queue")


def _run_gcic_outreach():
    """Execute gcic_outreach_trigger.py every 30 min."""
    _run_script("gcic_outreach")


def _run_daily_reminders():
    """Execute all daily reminder scripts at 08:00 UTC."""
    for job in ["gcic_reminder", "mec_dl_reminder", "drug_screen_reminder", "fadv_action_reminder"]:
        _run_script(job)


def _sms_scheduler():
    """Backup SMS queue poller -- every 15 minutes."""
    import schedule as _schedule
    _schedule.every(15).minutes.do(_poll_sms_queue)
    log.info("[scheduler] sms_queue_poller started -- every 15 min (backup to pg_cron)")
    while True:
        _schedule.run_pending()
        time.sleep(60)


def _run_mec_outreach():
    """Execute mec_outreach_trigger.py every 30 min."""
    _run_script("mec_outreach")


def _mec_outreach_scheduler():
    """Backup MEC outreach -- every 30 minutes."""
    import schedule as _schedule
    _schedule.every(30).minutes.do(_run_mec_outreach)
    log.info("[scheduler] mec_outreach started -- every 30 min (backup to pg_cron)")
    while True:
        _schedule.run_pending()
        import time as _time
        _time.sleep(1)


def _fadv_profile_reminder_scheduler():
    """Daily FADV profile completion reminder -- T67/T68 escalation + Day 3 flag."""
    import schedule as _schedule
    import subprocess as _subprocess

    def _run():
        script = str(SCRIPTS_DIR / "scripts" / "fadv_profile_reminder.py")
        try:
            result = _subprocess.run(["python3", script], capture_output=True, text=True, timeout=120)
            if result.stdout:
                log.info(f"[fadv_profile_reminder] {result.stdout[-500:]}")
            if result.returncode != 0:
                log.warning(f"[fadv_profile_reminder] exit {result.returncode}: {result.stderr[-300:]}")
        except Exception as e:
            log.error(f"[fadv_profile_reminder] failed: {e}")

    _schedule.every().day.at("08:30").do(_run)
    log.info("[scheduler] fadv_profile_reminder started -- daily at 08:30 (backup to pg_cron)")
    while True:
        _schedule.run_pending()
        import time
        time.sleep(60)


def _gcic_outreach_scheduler():
    """Backup GCIC outreach -- every 30 minutes."""
    import schedule as _schedule
    _schedule.every(30).minutes.do(_run_gcic_outreach)
    log.info("[scheduler] gcic_outreach started -- every 30 min (backup to pg_cron)")
    while True:
        _schedule.run_pending()
        time.sleep(60)


def _daily_scheduler():
    """Backup daily reminders -- 08:00 UTC."""
    import schedule as _schedule
    _schedule.every().day.at("08:00").do(_run_daily_reminders)
    log.info("[scheduler] daily_reminders started -- 08:00 UTC (backup to pg_cron)")
    while True:
        _schedule.run_pending()
        time.sleep(60)



# ---------------------------------------------------------------------------
# Client Dashboard  GET /d/<token>
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://eyopvsmsvbgfuffscfom.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

def _supa_get(path):
    r = http_requests.get(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers={"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"},
        timeout=10,
    )
    return r.json()

RWP_META = {
    "FEDEX_ACTIVE":     {"bg": "#185FA5", "label": "FedEx Active"},
    "FEDEX_FORMER":     {"bg": "#185FA5", "label": "FedEx Former"},
    "DELIVERY_EXP":     {"bg": "#0F6E56", "label": "Delivery Exp"},
    "WAREHOUSE_EXP":    {"bg": "#5DCAA5", "label": "Warehouse Exp"},
    "COMMERCIAL_DRIVER":{"bg": "#BA7517", "label": "Commercial Driver"},
    "LOW_RELEVANCE":    {"bg": "#888780", "label": "Low Relevance"},
}

def _pill(score, cls):
    if not cls:
        return ""
    m = RWP_META.get(cls, {"bg": "#888", "label": cls})
    return f'<span style="background:{m["bg"]};color:#fff;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600">{m["label"]} {score or ""}</span>'

def _bc(v):
    if not v:
        return '<span style="color:#ccc">--</span>'
    l = v.lower()
    if l in ("eligible", "pass", "negative/pass"):
        return f'<span style="color:#0F6E56;font-weight:600">{v}</span>'
    if l in ("ineligible", "fail"):
        return f'<span style="color:#A32D2D;font-weight:600">{v}</span>'
    if "progress" in l or "review" in l or "event" in l:
        return f'<span style="color:#BA7517;font-weight:600">{v}</span>'
    return f'<span style="color:#555">{v}</span>'

def _ck(v):
    return '<span style="color:#0F6E56;font-weight:700">&#10003;</span>' if v == 1 else '<span style="color:#e0e0e0">--</span>'

def _tr(c, am, cls="", hide_contacts=False):
    cid = c["id"]
    sv = am.get(cid, {})
    a = sv.get("action") or "none"
    nt = (sv.get("notes") or "").replace('"', "&quot;")
    sc = f" s-{a}" if a != "none" else ""
    rshow = "show" if a == "not_a_fit" else ""
    ron = "on" if sv.get("reroute_requested") else ""
    qc = '<span style="color:#0F6E56;font-weight:600">Done</span>' if c.get("qcert_completed_at") else '<span style="color:#e0e0e0">--</span>'
    rd = '<span style="color:#0F6E56;font-weight:600">Done</span>' if c.get("road_test_date") else '<span style="color:#e0e0e0">--</span>'
    station_map = {
        'legacy_chattanooga': 'CHA', 'legacy_ooltewah': 'OOL', 'legacy_tuscaloosa': 'TUS',
        'cbm': 'NOR', 'cnf_services': 'CNF', 'gods_vision': 'AUS',
        'rade_logistics': 'BRZ',
        'deera_express': 'BRZ',
        'a_to_z_route_services': 'MAR',
    }
    cid_val = c.get("client_id","")
    station_label = station_map.get(cid_val, cid_val.upper()[:3])
    station_colors = {
        'legacy_chattanooga': '#185FA5', 'legacy_ooltewah': '#0F6E56', 'legacy_tuscaloosa': '#c8a84b',
        'cbm': '#1a3a2a', 'cnf_services': '#1a3a2a', 'gods_vision': '#BA7517', 'rade_logistics': '#BA7517',
        'deera_express': '#BA7517',
        'a_to_z_route_services': '#1a3a2a',
    }
    station_color = station_colors.get(cid_val, '#888')
    station_pill = f'<span style="background:{station_color};color:#fff;padding:1px 7px;border-radius:8px;font-size:10px;font-weight:600">{station_label}</span>'
    return f"""<tr class="{cls}" id="row-{cid}">
      <td>{"<button class='name-btn' onclick='showCard("+str(cid)+")'>"+c.get("first_name","")+" "+c.get("last_name","")+"</button>" if not hide_contacts else c.get("first_name","")+" "+c.get("last_name","")}</td>
      <td>{station_pill}</td>
      <td>{_pill(c.get("rwp_score"), c.get("rwp_classification"))}</td>
      <td>{_bc(c.get("background_status"))}</td>
      <td>{_bc(c.get("drug_test_status"))}</td>
      <td>{"&mdash;" if c.get("background_status") == "Eligible" and not c.get("gcic_uploaded") else _ck(c.get("gcic_uploaded"))}</td>
      <td>{_ck(c.get("mec_uploaded"))}</td>
      <td>{_ck(c.get("dl_verified"))}</td>
      <td style="font-size:11px;color:var(--text2)">{c.get("fedex_id") or "&mdash;"}</td>
      <td>{qc}</td><td>{rd}</td>
      <td><select class="hire-sel{sc}" onchange="onAction({cid},this)">
        <option value="none"{"selected" if a=="none" else ""}>-- Select --</option>
        <option value="on_deck"{"selected" if a=="on_deck" else ""}>On Deck</option>
        <option value="hired"{"selected" if a=="hired" else ""}>Hired</option>
        <option value="not_a_fit"{"selected" if a=="not_a_fit" else ""}>Not a fit</option>
      </select>
      <div class="reject-reason {rshow}" id="rr-{cid}"><textarea class="notes-input" id="rr-txt-{cid}" placeholder="Required: reason for rejection..." style="min-height:48px;width:100%;margin-top:4px"></textarea><button class="reject-submit" onclick="submitReject({cid})">Submit rejection</button></div></td>
      <td><textarea class="notes-input" placeholder="Notes..." onblur="onNotes({cid},this)">{nt}</textarea>
      <span class="sv-flash" id="sv-{cid}">Saved</span></td>
    </tr>"""

def _sec(title, dot, inner, note=""):
    note_html = f'<p class="sec-note">{note}</p>' if note else ""
    return f'''<div class="section"><div class="sec-hdr"><span class="dot" style="background:{dot}"></span><span class="sec-title">{title}</span></div>{note_html}{inner}</div>'''

def _tbl(rows, extra=True):
    extra_cols = "<th>QCert</th><th>Road</th>" if extra else ""
    return f'''<div class="tbl-wrap"><table><thead><tr>
      <th style="text-align:left">Candidate</th><th>Station</th><th>Profile</th><th>Background</th><th>Drug</th>
      <th>GCIC</th><th>MEC</th><th>DL</th><th>FedEx#</th>{extra_cols}<th>Hiring Status</th><th>Notes</th>
    </tr></thead><tbody>{"".join(rows)}</tbody></table></div>'''

@app.route("/d/<token>", methods=["GET"])
def client_dashboard(token):
    # Validate token
    toks = _supa_get(f"client_tokens?token=eq.{token}&active=eq.true&select=client_id,label,hide_contacts")
    if not isinstance(toks, list) or not toks:
        return Response("<html><body><h2>Invalid or expired link.</h2></body></html>", status=403, content_type="text/html")
    client_id = toks[0]["client_id"]
    label = toks[0]["label"]
    hide_contacts = bool(toks[0].get("hide_contacts", False))

    # Candidates: show only where BG has been submitted
    # Generic combined-client map: add any new multi-location clients here
    COMBINED_CLIENTS = {
        "legacy_combined": {
            "locs": ["legacy_chattanooga", "legacy_ooltewah", "legacy_tuscaloosa"],
            "colors": {"legacy_chattanooga": "#185FA5", "legacy_ooltewah": "#0F6E56", "legacy_tuscaloosa": "#c8a84b"},
            "labels": {"legacy_chattanooga": "Chattanooga", "legacy_ooltewah": "Ooltewah", "legacy_tuscaloosa": "Tuscaloosa"},
        },
        "rade_combined": {
            "locs": ["gods_vision", "deera_express"],
            "colors": {"gods_vision": "#BA7517", "deera_express": "#185FA5"},
            "labels": {"gods_vision": "Austell (Rade)", "deera_express": "Braselton (Deera)"},
        },
    }
    IS_COMBINED = client_id in COMBINED_CLIENTS
    COMBINED_CFG = COMBINED_CLIENTS.get(client_id, {})
    COMBINED_LOCS = COMBINED_CFG.get("locs", [])
    COMBINED_COLORS = COMBINED_CFG.get("colors", {})
    COMBINED_LABELS = COMBINED_CFG.get("labels", {})
    cid_filter = f"client_id=in.({','.join(COMBINED_LOCS)})" if IS_COMBINED else f"client_id=eq.{client_id}"
    cands = _supa_get(
        f"candidates?{cid_filter}"
        "&status=not.in.(Rejected,Hired,Transferred,Expired)"
        "&background_status=in.(Eligible,In Progress,Needs Further Review,Collection Event Review)"
        "&or=(compliance_override.is.null,compliance_override.eq.false)"
        "&select=id,first_name,last_name,email,phone,rwp_score,rwp_classification,"
        "background_status,drug_test_status,gcic_uploaded,mec_uploaded,dl_verified,"
        "qcert_completed_at,road_test_date,client_id,fedex_id"
        "&order=client_id.asc,rwp_score.desc.nullslast"
    )
    if not isinstance(cands, list):
        cands = []

    # Pre-submission count
    pre = _supa_get(
        f"candidates?{cid_filter}"
        "&status=not.in.(Rejected,Hired,Transferred,Expired)"
        "&background_status=not.in.(Eligible,In Progress,Needs Further Review,Collection Event Review)"
        "&or=(compliance_override.is.null,compliance_override.eq.false)"
        "&select=id"
    )
    pre_count = len(pre) if isinstance(pre, list) else 0

    # RWP 11 (FEDEX_ACTIVE) candidates -- Badge Ready regardless of BG/drug
    # These may have bg=Not Started so they get excluded from the main query above
    rwp11 = _supa_get(
        f"candidates?{cid_filter}"
        "&status=not.in.(Rejected,Hired,Transferred,Expired)"
        "&rwp_score=eq.11"
        "&background_status=not.in.(Eligible,In Progress,Needs Further Review,Collection Event Review)"
        "&or=(compliance_override.is.null,compliance_override.eq.false)"
        "&select=id,first_name,last_name,email,phone,rwp_score,rwp_classification,"
        "background_status,drug_test_status,gcic_uploaded,mec_uploaded,dl_verified,"
        "qcert_completed_at,road_test_date,client_id,fedex_id"
    )
    if isinstance(rwp11, list):
        existing_ids = {c["id"] for c in cands}
        cands = cands + [c for c in rwp11 if c["id"] not in existing_ids]

    # Client actions
    acts = _supa_get(f"client_actions?token=eq.{token}&select=candidate_id,action,notes,reroute_requested")
    am = {}
    if isinstance(acts, list):
        for a in acts:
            am[a["candidate_id"]] = a

    badge, prog, rev, hired, naf = [], [], [], [], []
    cdata = {}
    for c in cands:
        cdata[c["id"]] = c
        bg = (c.get("background_status") or "").lower()
        dr = (c.get("drug_test_status") or "").lower()
        ca = am.get(c["id"], {}).get("action")
        if ca == "not_a_fit":
            naf.append(c)
            continue
        if ca == "hired":
            hired.append(_tr(c, am, "badge-bg", hide_contacts))
            continue
        rwp = c.get("rwp_score") or 0
        # RWP 1 or below -- exclude from client view entirely
        if rwp <= 1:
            continue
        # RWP 11 (FEDEX_ACTIVE) -- always Badge Ready regardless of BG/drug
        if rwp == 11:
            badge.append(_tr(c, am, "badge-bg", hide_contacts))
        elif bg == "eligible" and dr in ("pass", "negative/pass"):
            badge.append(_tr(c, am, "badge-bg", hide_contacts))
        elif bg == "eligible":
            # BG cleared but drug not yet started -- show in prog awaiting drug
            prog.append(_tr(c, am, hide_contacts=hide_contacts))
        elif bg == "in progress" or (bg == "needs further review" and dr in ("in progress", "pass", "negative/pass")):
            prog.append(_tr(c, am, hide_contacts=hide_contacts))
        else:
            rev.append(c)

    total = len(badge) + len(prog) + len(rev)
    now = datetime.now().strftime("%B %-d, %Y")
    label = label.replace(' — ', ' | ').replace(' -- ', ' | ')

    cdata_js = json.dumps({str(k): {
        "first_name": v.get("first_name",""),
        "last_name": v.get("last_name",""),
        "phone": v.get("phone","") if not hide_contacts else "",
        "email": v.get("email","") if not hide_contacts else "",
        "rwp_classification": v.get("rwp_classification",""),
        "rwp_score": v.get("rwp_score"),
    } for k,v in cdata.items()})

    rev_block = f'''<div class="rev-box"><div class="rev-num">{len(rev)}</div>
      <div class="rev-txt"><strong>{len(rev)} candidate{"s are" if len(rev)!=1 else " is"} under review</strong><br>
      Background verification requires additional documentation. PEAK is actively working these cases.</div></div>''' if rev else ""

    pre_block = f'''<div class="rev-box" style="border-color:#ddd"><div class="rev-num" style="color:#aaa">{pre_count}</div>
      <div class="rev-txt"><strong>{pre_count} candidate{"s are" if pre_count!=1 else " is"} in pre-submission</strong><br>
      In pipeline but background screen not yet submitted. PEAK is working to vet and advance these candidates.</div></div>''' if pre_count else ""

    hired_block = _tbl(hired, True) if hired else ""

    legend_html = ""
    if IS_COMBINED:
        legend_html = '<div style="display:flex;gap:20px;padding:10px 32px;background:#0d1a10;border-bottom:1px solid rgba(200,168,75,0.2);font-size:12px;color:#94a3b8;">' +             ''.join([f'<span style="display:flex;align-items:center;gap:6px;"><span style="width:10px;height:10px;border-radius:50%;background:{COMBINED_COLORS[loc]};display:inline-block"></span>{COMBINED_LABELS[loc]}</span>'
                     for loc in COMBINED_LOCS]) + '</div>'

    def _naf_row(c):
        name = (c.get("first_name","") + " " + c.get("last_name","")).strip()
        action_data = am.get(c["id"], {})
        reason = (action_data.get("reject_reason") or action_data.get("notes") or "No reason provided")
        return (
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:10px 14px;background:var(--tbl-row);border:0.5px solid var(--tbl-border);'
            f'border-left:3px solid #A32D2D;border-radius:6px;margin-bottom:8px;font-size:13px">'
            f'<span style="font-weight:500;color:var(--text)">{name}</span>'
            f'<span style="color:var(--text2);font-size:12px">Not a fit</span>'
            f'<span style="font-size:11px;color:#A32D2D;font-style:italic;max-width:200px;text-align:right">{reason}</span>'
            f'</div>'
        )

    naf_block = ""
    if naf:
        naf_rows = "".join(_naf_row(c) for c in naf)
        naf_block = (
            '<div id="naf-section" style="margin-top:32px;border-top:1px solid var(--tbl-border);padding-top:20px">'
            '<div style=\'display:flex;align-items:center;gap:10px;margin-bottom:10px\'>'
            '<span style=\'width:10px;height:10px;border-radius:50%;background:#A32D2D;display:inline-block\'></span>'
            '<span style=\'font-size:15px;font-weight:500;color:var(--text)\'>Not a Fit</span></div>'
            '<p style=\'font-size:13px;color:var(--text2);margin-bottom:12px\'>Flagged by your team &mdash; returned to PEAK for review and redeployment.</p>'
            f'{naf_rows}'
            '</div>'
        )

    body = (
        (_sec("Badge Ready", "#0F6E56", _tbl(badge, True), "Background cleared, drug test passed." + ("" if hide_contacts else " Click a name to view contact details.")) if badge else "") +
        (_sec("In Progress", "#185FA5", _tbl(prog, True), "Background screening or drug test currently underway.") if prog else "") +
        (_sec("Under Review", "#BA7517", rev_block) if rev else "") +
        (_sec("Pre-Submission", "#aaa", pre_block) if pre_count else "") +
        (_sec("Hired", "#0F6E56", hired_block, "Candidates marked as hired. Shown for record-keeping.") if hired else "") +
        ('<p style="color:#aaa;text-align:center;padding:60px">No active candidates at this time.</p>' if not total and not hired and not naf else "") +
        naf_block
    )

    SUPA_URL = SUPABASE_URL
    ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4"
    ACTION_URL = f"{SUPA_URL}/functions/v1/client-action"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PEAK Pipeline</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:#f5f4f0;--hdr:#1a3a2a;--hdr-border:rgba(200,168,75,0.2);
  --text:#1a1a1a;--text2:#777;--text3:#aaa;
  --tbl-bg:#ffffff;--tbl-border:#e5e7eb;--tbl-row:#f9f9f7;
  --tbl-hover:#f5f5f2;--cell-border:#eee;--cell-text:#333;
  --th-text:#1a3a2a;--stat-bg:rgba(255,255,255,0.08);
  --sel-bg:#fff;--sel-border:#e0e0e0;--sel-text:#333;
  --notes-bg:#fff;--notes-border:#e0e0e0;--notes-text:#333;
  --card-bg:#fff;--card-border:#e5e7eb;
  --rev-bg:#fffbf0;--rev-border:#e5e7eb;--rev-text:#666;
  --name-color:#1a3a2a;--link-color:#185FA5;
  --elig:#0F6E56;--inelig:#A32D2D;--prog:#BA7517;
  --flash:#0F6E56;--footer:#bbb;
}}
@media(prefers-color-scheme:dark){{:root{{
  --bg:#0f1117;--hdr:#0d1f16;--hdr-border:rgba(200,168,75,0.2);
  --text:#e2e8f0;--text2:#64748b;--text3:#475569;
  --tbl-bg:#141c26;--tbl-border:#1e2d3d;--tbl-row:#141c26;
  --tbl-hover:#141c26;--cell-border:#1a2332;--cell-text:#cbd5e1;
  --th-text:#94a3b8;--stat-bg:rgba(255,255,255,0.05);
  --sel-bg:#1a2332;--sel-border:#2d3748;--sel-text:#cbd5e1;
  --notes-bg:#1a2332;--notes-border:#2d3748;--notes-text:#cbd5e1;
  --card-bg:#1a2332;--card-border:#2d3748;
  --rev-bg:#1a2000;--rev-border:#2d3a00;--rev-text:#94a3b8;
  --name-color:#93c5fd;--link-color:#60a5fa;
  --elig:#34d399;--inelig:#f87171;--prog:#fbbf24;
  --flash:#34d399;--footer:#475569;
}}}}
body{{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh}}
.hdr{{background:var(--hdr);border-bottom:1px solid var(--hdr-border);padding:22px 32px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
.hdr h1{{font-family:'DM Sans',sans-serif;font-weight:600;color:#fff;font-size:22px;letter-spacing:-0.3px}}
.hdr p{{color:#c8a84b;font-size:13px;margin-top:3px}}
.hdr-r{{text-align:right}}
.hdr-r .hr-count{{color:#c8a84b;font-size:22px;font-weight:700;display:block;line-height:1.2;margin-bottom:4px}}
.hdr-r .hr-date{{color:#8aab96;font-size:12px;display:block;margin-bottom:2px}}
.hdr-r .hr-brand{{color:#5a7a66;font-size:12px;display:block}}
.stats{{background:var(--hdr);border-bottom:1px solid var(--hdr-border);padding:0 32px 20px;display:flex;gap:14px;flex-wrap:wrap}}
.stat{{background:var(--stat-bg);border:0.5px solid rgba(200,168,75,0.25);border-radius:8px;padding:12px 20px}}
.sv{{font-size:26px;font-weight:600;color:#fff}}
.sl{{font-size:11px;color:#8aab96;margin-top:2px}}
.body{{padding:28px 32px;max-width:1200px;margin:0 auto}}
.section{{margin-bottom:36px}}
.sec-hdr{{display:flex;align-items:center;gap:10px;margin-bottom:12px}}
.dot{{width:10px;height:10px;border-radius:50%;display:inline-block;flex-shrink:0}}
.sec-title{{font-size:16px;font-weight:600;color:var(--text)}}
.sec-note{{font-size:13px;color:var(--text2);margin:0 0 12px}}
.tbl-wrap{{overflow-x:auto;border-radius:8px;border:0.5px solid var(--tbl-border)}}
table{{width:100%;border-collapse:collapse;font-size:12px;min-width:860px}}
th{{padding:8px 10px;text-align:center;font-weight:600;color:var(--th-text);background:var(--tbl-row);border-bottom:1.5px solid #c8a84b;white-space:nowrap}}
th:first-child{{text-align:left}}
td{{padding:8px 10px;text-align:center;border-bottom:0.5px solid var(--cell-border);white-space:nowrap;vertical-align:middle;color:var(--cell-text)}}
td:first-child{{text-align:left}}
tr:hover td{{background:var(--tbl-hover)}}
.pill{{padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;color:#fff;display:inline-block}}
.elig{{color:var(--elig);font-weight:600}}.inelig{{color:var(--inelig);font-weight:600}}.prog{{color:var(--prog);font-weight:600}}
.hire-sel{{padding:4px 6px;border-radius:6px;border:1px solid var(--sel-border);font-size:11px;font-family:'DM Sans',sans-serif;background:var(--sel-bg);color:var(--sel-text);cursor:pointer;width:118px}}
.hire-sel.s-on_deck{{background:#e8f4ff;border-color:#185FA5;color:#185FA5;font-weight:600}}
.hire-sel.s-hired{{background:#e8faf2;border-color:#0F6E56;color:#0F6E56;font-weight:600}}
.hire-sel.s-not_a_fit{{background:#fef2f2;border-color:#A32D2D;color:#A32D2D;font-weight:600}}
.notes-input{{width:150px;padding:4px 6px;border-radius:6px;border:1px solid var(--notes-border);font-size:11px;font-family:'DM Sans',sans-serif;resize:none;height:32px;background:var(--notes-bg);color:var(--notes-text)}}
.notes-input:focus{{outline:none;border-color:#c8a84b}}
.sv-flash{{font-size:10px;color:var(--flash);margin-left:3px;opacity:0;transition:opacity 0.3s}}
.sv-flash.show{{opacity:1}}
.reject-reason{{display:none;margin-top:6px;width:100%}}
.reject-reason.show{{display:block}}
.reject-submit{{margin-top:4px;font-size:11px;padding:4px 10px;border-radius:5px;border:1px solid #A32D2D;background:#fef2f2;color:#A32D2D;cursor:pointer;font-family:inherit}}
.reject-submit:hover{{background:#A32D2D;color:#fff}}
.name-btn{{background:none;border:none;cursor:pointer;font-family:'DM Sans',sans-serif;font-size:12px;font-weight:600;color:var(--name-color);text-decoration:underline;text-decoration-style:dotted;text-decoration-color:#c8a84b;padding:0}}
.name-btn:hover{{color:#0F6E56}}
.overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.4);z-index:999}}
.overlay.show{{display:block}}
.ccard{{display:none;position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:var(--card-bg);border:1px solid var(--card-border);border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,0.15);padding:24px 28px;z-index:1000;min-width:280px}}
.ccard.show{{display:block}}
.cc-name{{font-family:'DM Serif Display',serif;font-size:18px;color:var(--text);margin-bottom:16px;padding-right:24px}}
.cc-row{{display:flex;align-items:center;gap:10px;margin-bottom:10px;font-size:13px}}
.cc-lbl{{color:var(--text2);width:64px;flex-shrink:0;font-size:12px}}
.cc-val a{{color:var(--link-color);text-decoration:none}}
.cc-close{{position:absolute;top:12px;right:14px;cursor:pointer;color:var(--text3);font-size:20px;line-height:1;background:none;border:none}}
.rev-box{{background:var(--rev-bg);border:0.5px solid var(--rev-border);border-radius:8px;padding:16px 20px;display:flex;align-items:center;gap:16px}}
.rev-num{{font-size:32px;font-weight:600;color:#BA7517;flex-shrink:0}}
.rev-txt{{font-size:13px;color:var(--rev-text);line-height:1.6}}
.footer{{text-align:center;padding:28px;font-size:12px;color:var(--footer)}}
.footer strong{{color:#c8a84b}}
@media(max-width:700px){{.hdr,.stats,.body{{padding-left:16px;padding-right:16px}}}}
</style>
</head>
<body>
<div class="overlay" id="ov" onclick="closeCard()"></div>
<div class="ccard" id="cc">
  <button class="cc-close" onclick="closeCard()">&times;</button>
  <div class="cc-name" id="cc-name"></div>
  <div class="cc-row"><span class="cc-lbl">Phone</span><span class="cc-val" id="cc-phone"></span></div>
  <div class="cc-row"><span class="cc-lbl">Email</span><span class="cc-val" id="cc-email"></span></div>
  <div class="cc-row"><span class="cc-lbl">Profile</span><span class="cc-val" id="cc-rwp"></span></div>
</div>
<div class="hdr">
  <div><h1>Candidate Pipeline</h1><p>{label}</p></div>
  <div class="hdr-r"><span class="hr-count">{total} active candidates</span><span class="hr-date">Updated {now}</span><span class="hr-brand">Powered by PEAKrecruiting</span></div>
</div>
<div class="stats">
  <div class="stat"><div class="sv" style="color:#c8a84b">{len(badge)}</div><div class="sl">Badge Ready</div></div>
  <div class="stat"><div class="sv">{len(prog)}</div><div class="sl">In Progress</div></div>
  <div class="stat"><div class="sv">{len(rev)}</div><div class="sl">Under Review</div></div>
  <div class="stat"><div class="sv" style="color:#0F6E56">{len(hired)}</div><div class="sl">Hired</div></div>
  <div class="stat"><div class="sv" style="color:#aaa">{pre_count}</div><div class="sl">Pre-Submission</div></div>
</div>
{legend_html}<div class="body">{body}</div>
<div class="footer">Powered by PEAKrecruiting &nbsp;&middot;&nbsp; Kai &nbsp;&middot;&nbsp; 470-470-4766</div>
<script>
const TOKEN="{token}";
const ACTION_URL="{ACTION_URL}?token="+TOKEN;
const RWP={json.dumps({k:v["label"] for k,v in RWP_META.items()})};
const CDATA={cdata_js};
function showCard(id){{const c=CDATA[id];if(!c)return;document.getElementById("cc-name").textContent=c.first_name+" "+c.last_name;document.getElementById("cc-phone").innerHTML=c.phone?'<a href="tel:'+c.phone+'">'+c.phone+"</a>":"--";document.getElementById("cc-email").innerHTML=c.email?'<a href="mailto:'+c.email+'">'+c.email+"</a>":"--";const r=RWP[c.rwp_classification];document.getElementById("cc-rwp").textContent=r?(r+(c.rwp_score?" "+c.rwp_score:"")):"--";document.getElementById("cc").classList.add("show");document.getElementById("ov").classList.add("show");}}
function closeCard(){{document.getElementById("cc").classList.remove("show");document.getElementById("ov").classList.remove("show");}}
async function post(cid,fields){{return fetch(ACTION_URL,{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{candidate_id:cid,...fields}})}});}}
function flash(id){{const el=document.getElementById("sv-"+id);if(el){{el.classList.add("show");setTimeout(()=>el.classList.remove("show"),2000);}}}}
function onAction(id,sel){{const a=sel.value;sel.className="hire-sel"+(a!=="none"?" s-"+a:"");const rr=document.getElementById("rr-"+id);if(rr){{a==="not_a_fit"?rr.classList.add("show"):rr.classList.remove("show");}}if(a!=="not_a_fit"){{post(id,{{action:a}}).then(()=>flash(id));}}}}
function submitReject(id){{
  const ta=document.getElementById("rr-txt-"+id);
  const reason=(ta&&ta.value.trim())||"";
  if(!reason){{ta&&(ta.style.border="1px solid #A32D2D");return;}}
  // Disable button immediately to prevent double-fire on rapid taps
  const btn=ta&&ta.parentNode&&ta.parentNode.querySelector(".reject-submit");
  if(btn){{btn.disabled=true;btn.textContent="Submitting...";}}
  post(id,{{action:"not_a_fit",reject_reason:reason,notes:reason}}).then(function(){{
    const row=document.getElementById("row-"+id);
    if(!row)return;
    // Get candidate name from the row
    const nameCell=row.querySelector("td:first-child");
    const name=nameCell?nameCell.innerText.trim():"Candidate";
    const ts=new Date().toLocaleTimeString([],{{hour:"2-digit",minute:"2-digit"}});
    // Fade out the row from its current section
    row.style.transition="opacity 0.35s";
    row.style.opacity="0";
    setTimeout(function(){{
      row.remove();
      // Ensure Not a Fit section exists at bottom of page
      var naf=document.getElementById("naf-section");
      if(!naf){{
        naf=document.createElement("div");
        naf.id="naf-section";
        naf.style.cssText="margin-top:32px;border-top:1px solid var(--tbl-border);padding-top:20px";
        naf.innerHTML="<div style='display:flex;align-items:center;gap:10px;margin-bottom:10px'>"
          +"<span style='width:10px;height:10px;border-radius:50%;background:#A32D2D;display:inline-block'></span>"
          +"<span style='font-size:15px;font-weight:500;color:var(--text)'>Not a Fit</span></div>"
          +"<p style='font-size:13px;color:var(--text2);margin-bottom:12px'>Flagged by your team -- returned to PEAK for review and redeployment.</p>"
          +"<div id='naf-list'></div>";
        document.querySelector(".body")&&document.querySelector(".body").appendChild(naf);
      }}
      // Append a compact record card to the Not a Fit list
      var list=document.getElementById("naf-list");
      var card=document.createElement("div");
      card.style.cssText="display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--tbl-row);border:0.5px solid var(--tbl-border);border-left:3px solid #A32D2D;border-radius:6px;margin-bottom:8px;font-size:13px";
      card.innerHTML="<span style='font-weight:500;color:var(--text)'>"+name+"</span>"
        +"<span style='color:var(--text2);font-size:12px'>Not a fit &bull; "+ts+"</span>"
        +"<span style='font-size:11px;color:#A32D2D;font-style:italic;max-width:200px;text-align:right'>"+reason+"</span>";
      list&&list.appendChild(card);
    }},350);
  }});
}}
function onNotes(id,ta){{const n=ta.value;post(id,{{notes:n}}).then(()=>flash(id));}}
</script>
</body>
</html>"""

    return Response(html, status=200, content_type="text/html; charset=utf-8")


if __name__ == "__main__":
    log.info("=" * 60)
    log.info("forge-runner v1.5.0 starting")
    log.info(f"Port:        {PORT}")
    log.info(f"Scripts dir: {SCRIPTS_DIR}")
    log.info(f"Log dir:     {LOG_DIR}")
    log.info(f"Whitelist:   {list(WHITELIST.keys())}")
    log.info("Scheduler:   pg_cron is primary. Internal scheduler is backup.")
    log.info("=" * 60)

    threading.Thread(target=_sms_scheduler, daemon=True).start()
    threading.Thread(target=_gcic_outreach_scheduler, daemon=True).start()
    threading.Thread(target=_mec_outreach_scheduler, daemon=True).start()
    threading.Thread(target=_fadv_profile_reminder_scheduler, daemon=True).start()
    threading.Thread(target=_daily_scheduler, daemon=True).start()

    port = int(os.environ.get("PORT", 5678))
    app.run(host="0.0.0.0", port=port, debug=False)
# cache bust Sat Apr 18 13:30:16 EDT 2026
