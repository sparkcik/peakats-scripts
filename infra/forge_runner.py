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
from flask import Flask, request, jsonify, Response
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
        "script": str(SCRIPTS_DIR / "sms_queue_poller.py"),
        "description": "SMS queue poller -- send pending scheduled messages via Twilio",
        "allowed_args": ["--dry-run"],
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

def _tr(c, am, cls=""):
    cid = c["id"]
    sv = am.get(cid, {})
    a = sv.get("action") or "none"
    nt = (sv.get("notes") or "").replace('"', "&quot;")
    sc = f" s-{a}" if a != "none" else ""
    rshow = "show" if a == "rejected" else ""
    ron = "on" if sv.get("reroute_requested") else ""
    qc = '<span style="color:#0F6E56;font-weight:600">Done</span>' if c.get("qcert_completed_at") else '<span style="color:#e0e0e0">--</span>'
    rd = '<span style="color:#0F6E56;font-weight:600">Done</span>' if c.get("road_test_date") else '<span style="color:#e0e0e0">--</span>'
    return f"""<tr class="{cls}" id="row-{cid}">
      <td><button class="name-btn" onclick="showCard({cid})">{c.get("first_name","")} {c.get("last_name","")}</button></td>
      <td>{_pill(c.get("rwp_score"), c.get("rwp_classification"))}</td>
      <td>{_bc(c.get("background_status"))}</td>
      <td>{_bc(c.get("drug_test_status"))}</td>
      <td>{_ck(c.get("gcic_uploaded"))}</td>
      <td>{_ck(c.get("mec_uploaded"))}</td>
      <td>{_ck(c.get("dl_verified"))}</td>
      <td>{qc}</td><td>{rd}</td>
      <td><select class="hire-sel{sc}" onchange="onAction({cid},this)">
        <option value="none"{"selected" if a=="none" else ""}>-- Select --</option>
        <option value="contacting"{"selected" if a=="contacting" else ""}>Contacting</option>
        <option value="hired"{"selected" if a=="hired" else ""}>Hired</option>
        <option value="rejected"{"selected" if a=="rejected" else ""}>Rejected</option>
      </select>
      <button class="reroute-btn {rshow} {ron}" onclick="onReroute({cid},this)">Re-route</button></td>
      <td><textarea class="notes-input" placeholder="Notes..." onblur="onNotes({cid},this)">{nt}</textarea>
      <span class="sv-flash" id="sv-{cid}">Saved</span></td>
    </tr>"""

def _sec(title, dot, inner, note=""):
    note_html = f'<p class="sec-note">{note}</p>' if note else ""
    return f'''<div class="section"><div class="sec-hdr"><span class="dot" style="background:{dot}"></span><span class="sec-title">{title}</span></div>{note_html}{inner}</div>'''

def _tbl(rows, extra=True):
    extra_cols = "<th>QCert</th><th>Road</th>" if extra else ""
    return f'''<div class="tbl-wrap"><table><thead><tr>
      <th style="text-align:left">Candidate</th><th>Profile</th><th>Background</th><th>Drug</th>
      <th>GCIC</th><th>MEC</th><th>DL</th>{extra_cols}<th>Hiring Status</th><th>Notes</th>
    </tr></thead><tbody>{"".join(rows)}</tbody></table></div>'''

@app.route("/d/<token>", methods=["GET"])
def client_dashboard(token):
    # Validate token
    toks = _supa_get(f"client_tokens?token=eq.{token}&active=eq.true&select=client_id,label")
    if not isinstance(toks, list) or not toks:
        return Response("<html><body><h2>Invalid or expired link.</h2></body></html>", status=403, content_type="text/html")
    client_id = toks[0]["client_id"]
    label = toks[0]["label"]

    # Candidates: show only where BG has been submitted
    cands = _supa_get(
        f"candidates?client_id=eq.{client_id}"
        "&status=not.in.(Rejected,Hired,Transferred)"
        "&background_status=in.(Eligible,In Progress,Needs Further Review,Collection Event Review)"
        "&or=(compliance_override.is.null,compliance_override.eq.false)"
        "&select=id,first_name,last_name,email,phone,rwp_score,rwp_classification,"
        "background_status,drug_test_status,gcic_uploaded,mec_uploaded,dl_verified,"
        "qcert_completed_at,road_test_date"
        "&order=rwp_score.desc.nullslast"
    )
    if not isinstance(cands, list):
        cands = []

    # Pre-submission count
    pre = _supa_get(
        f"candidates?client_id=eq.{client_id}"
        "&status=not.in.(Rejected,Hired,Transferred)"
        "&background_status=not.in.(Eligible,In Progress,Needs Further Review,Collection Event Review)"
        "&or=(compliance_override.is.null,compliance_override.eq.false)"
        "&select=id"
    )
    pre_count = len(pre) if isinstance(pre, list) else 0

    # Client actions
    acts = _supa_get(f"client_actions?token=eq.{token}&select=candidate_id,action,notes,reroute_requested")
    am = {}
    if isinstance(acts, list):
        for a in acts:
            am[a["candidate_id"]] = a

    badge, prog, rev, hired = [], [], [], []
    cdata = {}
    for c in cands:
        cdata[c["id"]] = c
        bg = (c.get("background_status") or "").lower()
        dr = (c.get("drug_test_status") or "").lower()
        ca = am.get(c["id"], {}).get("action")
        if ca == "hired":
            hired.append(_tr(c, am, "badge-bg"))
            continue
        if bg == "eligible" and dr in ("pass", "negative/pass"):
            badge.append(_tr(c, am, "badge-bg"))
        elif bg == "in progress" or (bg == "needs further review" and dr in ("in progress", "pass", "negative/pass")):
            prog.append(_tr(c, am))
        else:
            rev.append(c)

    total = len(badge) + len(prog) + len(rev)
    now = datetime.now().strftime("%B %-d, %Y")

    cdata_js = json.dumps({str(k): {
        "first_name": v.get("first_name",""),
        "last_name": v.get("last_name",""),
        "phone": v.get("phone",""),
        "email": v.get("email",""),
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

    body = (
        (_sec("Badge Ready", "#0F6E56", _tbl(badge, True), "Background cleared, drug test passed. Click a name to view contact details.") if badge else "") +
        (_sec("In Progress", "#185FA5", _tbl(prog, False), "Background screening or drug test currently underway.") if prog else "") +
        (_sec("Under Review", "#BA7517", rev_block) if rev else "") +
        (_sec("Pre-Submission", "#aaa", pre_block) if pre_count else "") +
        (_sec("Hired", "#0F6E56", hired_block, "Candidates marked as hired. Shown for record-keeping.") if hired else "") +
        ('<p style="color:#aaa;text-align:center;padding:60px">No active candidates at this time.</p>' if not total and not hired else "")
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
body{{font-family:'DM Sans',sans-serif;background:#f5f4f0;color:#1a1a1a;min-height:100vh}}
.hdr{{background:#1a3a2a;padding:22px 32px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
.hdr h1{{font-family:'DM Sans',sans-serif;font-weight:600;color:#fff;font-size:22px;letter-spacing:-0.3px}}
.hdr p{{color:#c8a84b;font-size:13px;margin-top:3px}}
.hdr-r{{text-align:right;color:#8aab96;font-size:12px}}
.hdr-r strong{{color:#c8a84b;font-size:15px;display:block;margin-bottom:3px}}
.stats{{background:#1a3a2a;padding:0 32px 20px;display:flex;gap:14px;flex-wrap:wrap}}
.stat{{background:rgba(255,255,255,0.08);border:0.5px solid rgba(200,168,75,0.3);border-radius:8px;padding:12px 20px}}
.sv{{font-size:26px;font-weight:600;color:#fff}}
.sl{{font-size:11px;color:#8aab96;margin-top:2px}}
.body{{padding:28px 32px;max-width:1200px;margin:0 auto}}
.section{{margin-bottom:36px}}
.sec-hdr{{display:flex;align-items:center;gap:10px;margin-bottom:12px}}
.dot{{width:10px;height:10px;border-radius:50%;display:inline-block;flex-shrink:0}}
.sec-title{{font-size:16px;font-weight:600;color:#1a3a2a}}
.sec-note{{font-size:13px;color:#777;margin:0 0 12px}}
.tbl-wrap{{overflow-x:auto;border-radius:8px;border:0.5px solid #e5e7eb}}
table{{width:100%;border-collapse:collapse;font-size:12px;min-width:860px}}
th{{padding:8px 10px;text-align:center;font-weight:600;color:#1a3a2a;background:#f9f9f7;border-bottom:1.5px solid #c8a84b;white-space:nowrap}}
th:first-child{{text-align:left}}
td{{padding:8px 10px;text-align:center;border-bottom:0.5px solid #eee;white-space:nowrap;vertical-align:middle}}
td:first-child{{text-align:left}}
.pill{{padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;color:#fff;display:inline-block}}
.elig{{color:#0F6E56;font-weight:600}}.inelig{{color:#A32D2D;font-weight:600}}.prog{{color:#BA7517;font-weight:600}}
.hire-sel{{padding:4px 6px;border-radius:6px;border:1px solid #e0e0e0;font-size:11px;font-family:'DM Sans',sans-serif;background:#fff;cursor:pointer;width:118px}}
.hire-sel.s-contacting{{background:#e8f4ff;border-color:#185FA5;color:#185FA5;font-weight:600}}
.hire-sel.s-hired{{background:#e8faf2;border-color:#0F6E56;color:#0F6E56;font-weight:600}}
.hire-sel.s-rejected{{background:#fef2f2;border-color:#A32D2D;color:#A32D2D;font-weight:600}}
.notes-input{{width:150px;padding:4px 6px;border-radius:6px;border:1px solid #e0e0e0;font-size:11px;font-family:'DM Sans',sans-serif;resize:none;height:32px}}
.notes-input:focus{{outline:none;border-color:#c8a84b}}
.sv-flash{{font-size:10px;color:#0F6E56;margin-left:3px;opacity:0;transition:opacity 0.3s}}
.sv-flash.show{{opacity:1}}
.reroute-btn{{padding:3px 7px;border-radius:5px;border:1px solid #BA7517;background:#fff8f0;color:#BA7517;font-size:10px;cursor:pointer;margin-top:3px;display:none}}
.reroute-btn.show{{display:inline-block}}.reroute-btn.on{{background:#BA7517;color:#fff}}
.name-btn{{background:none;border:none;cursor:pointer;font-family:'DM Sans',sans-serif;font-size:12px;font-weight:600;color:#1a3a2a;text-decoration:underline;text-decoration-style:dotted;text-decoration-color:#c8a84b;padding:0}}
.name-btn:hover{{color:#0F6E56}}
.overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.3);z-index:999}}
.overlay.show{{display:block}}
.ccard{{display:none;position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:#fff;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,0.18);padding:24px 28px;z-index:1000;min-width:280px}}
.ccard.show{{display:block}}
.cc-name{{font-family:'DM Serif Display',serif;font-size:18px;color:#1a3a2a;margin-bottom:16px;padding-right:24px}}
.cc-row{{display:flex;align-items:center;gap:10px;margin-bottom:10px;font-size:13px}}
.cc-lbl{{color:#888;width:64px;flex-shrink:0;font-size:12px}}
.cc-val a{{color:#185FA5;text-decoration:none}}
.cc-close{{position:absolute;top:12px;right:14px;cursor:pointer;color:#aaa;font-size:20px;line-height:1;background:none;border:none}}
.rev-box{{background:#fffbf0;border:0.5px solid #e5e7eb;border-radius:8px;padding:16px 20px;display:flex;align-items:center;gap:16px}}
.rev-num{{font-size:32px;font-weight:600;color:#BA7517;flex-shrink:0}}
.rev-txt{{font-size:13px;color:#666;line-height:1.6}}
.footer{{text-align:center;padding:28px;font-size:12px;color:#bbb}}
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
  <div class="hdr-r"><strong>{total} active candidates</strong>Updated {now}</div>
</div>
<div class="stats">
  <div class="stat"><div class="sv" style="color:#c8a84b">{len(badge)}</div><div class="sl">Badge Ready</div></div>
  <div class="stat"><div class="sv">{len(prog)}</div><div class="sl">In Progress</div></div>
  <div class="stat"><div class="sv">{len(rev)}</div><div class="sl">Under Review</div></div>
  <div class="stat"><div class="sv" style="color:#0F6E56">{len(hired)}</div><div class="sl">Hired</div></div>
  <div class="stat"><div class="sv" style="color:#aaa">{pre_count}</div><div class="sl">Pre-Submission</div></div>
</div>
<div class="body">{body}</div>
<div class="footer">Powered by <strong>PEAKrecruiting</strong> &bull; Questions? (470) 470-4766</div>
<script>
const TOKEN="{token}";
const ACTION_URL="{ACTION_URL}";
const RWP={json.dumps({k:v["label"] for k,v in RWP_META.items()})};
const CDATA={cdata_js};
function showCard(id){{const c=CDATA[id];if(!c)return;document.getElementById("cc-name").textContent=c.first_name+" "+c.last_name;document.getElementById("cc-phone").innerHTML=c.phone?'<a href="tel:'+c.phone+'">'+c.phone+"</a>":"--";document.getElementById("cc-email").innerHTML=c.email?'<a href="mailto:'+c.email+'">'+c.email+"</a>":"--";const r=RWP[c.rwp_classification];document.getElementById("cc-rwp").textContent=r?(r+(c.rwp_score?" "+c.rwp_score:"")):"--";document.getElementById("cc").classList.add("show");document.getElementById("ov").classList.add("show");}}
function closeCard(){{document.getElementById("cc").classList.remove("show");document.getElementById("ov").classList.remove("show");}}
async function post(cid,fields){{await fetch(ACTION_URL,{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{token:TOKEN,candidate_id:cid,...fields}})}});}}
function flash(id){{const el=document.getElementById("sv-"+id);if(el){{el.classList.add("show");setTimeout(()=>el.classList.remove("show"),2000);}}}}
function onAction(id,sel){{const a=sel.value;sel.className="hire-sel"+(a!=="none"?" s-"+a:"");const row=document.getElementById("row-"+id);const btn=row&&row.querySelector(".reroute-btn");if(btn)a==="rejected"?btn.classList.add("show"):btn.classList.remove("show","on");post(id,{{action:a,reroute_requested:false}}).then(()=>flash(id));}}
function onReroute(id,btn){{const was=btn.classList.contains("on");btn.classList.toggle("on");const reason=!was?(prompt("Brief reason for re-route (optional):")||""):"";post(id,{{action:"rejected",reroute_requested:!was,reject_reason:reason}}).then(()=>flash(id));}}
function onNotes(id,ta){{const n=ta.value;post(id,{{notes:n}}).then(()=>flash(id));}}
</script>
</body>
</html>"""

    return Response(html, status=200, content_type="text/html; charset=utf-8")



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

def _tr(c, am, cls=""):
    cid = c["id"]
    sv = am.get(cid, {})
    a = sv.get("action") or "none"
    nt = (sv.get("notes") or "").replace('"', "&quot;")
    sc = f" s-{a}" if a != "none" else ""
    rshow = "show" if a == "rejected" else ""
    ron = "on" if sv.get("reroute_requested") else ""
    qc = '<span style="color:#0F6E56;font-weight:600">Done</span>' if c.get("qcert_completed_at") else '<span style="color:#e0e0e0">--</span>'
    rd = '<span style="color:#0F6E56;font-weight:600">Done</span>' if c.get("road_test_date") else '<span style="color:#e0e0e0">--</span>'
    return f"""<tr class="{cls}" id="row-{cid}">
      <td><button class="name-btn" onclick="showCard({cid})">{c.get("first_name","")} {c.get("last_name","")}</button></td>
      <td>{_pill(c.get("rwp_score"), c.get("rwp_classification"))}</td>
      <td>{_bc(c.get("background_status"))}</td>
      <td>{_bc(c.get("drug_test_status"))}</td>
      <td>{_ck(c.get("gcic_uploaded"))}</td>
      <td>{_ck(c.get("mec_uploaded"))}</td>
      <td>{_ck(c.get("dl_verified"))}</td>
      <td>{qc}</td><td>{rd}</td>
      <td><select class="hire-sel{sc}" onchange="onAction({cid},this)">
        <option value="none"{"selected" if a=="none" else ""}>-- Select --</option>
        <option value="contacting"{"selected" if a=="contacting" else ""}>Contacting</option>
        <option value="hired"{"selected" if a=="hired" else ""}>Hired</option>
        <option value="rejected"{"selected" if a=="rejected" else ""}>Rejected</option>
      </select>
      <button class="reroute-btn {rshow} {ron}" onclick="onReroute({cid},this)">Re-route</button></td>
      <td><textarea class="notes-input" placeholder="Notes..." onblur="onNotes({cid},this)">{nt}</textarea>
      <span class="sv-flash" id="sv-{cid}">Saved</span></td>
    </tr>"""

def _sec(title, dot, inner, note=""):
    note_html = f'<p class="sec-note">{note}</p>' if note else ""
    return f'''<div class="section"><div class="sec-hdr"><span class="dot" style="background:{dot}"></span><span class="sec-title">{title}</span></div>{note_html}{inner}</div>'''

def _tbl(rows, extra=True):
    extra_cols = "<th>QCert</th><th>Road</th>" if extra else ""
    return f'''<div class="tbl-wrap"><table><thead><tr>
      <th style="text-align:left">Candidate</th><th>Profile</th><th>Background</th><th>Drug</th>
      <th>GCIC</th><th>MEC</th><th>DL</th>{extra_cols}<th>Hiring Status</th><th>Notes</th>
    </tr></thead><tbody>{"".join(rows)}</tbody></table></div>'''

@app.route("/d/<token>", methods=["GET"])
def client_dashboard(token):
    # Validate token
    toks = _supa_get(f"client_tokens?token=eq.{token}&active=eq.true&select=client_id,label")
    if not isinstance(toks, list) or not toks:
        return Response("<html><body><h2>Invalid or expired link.</h2></body></html>", status=403, content_type="text/html")
    client_id = toks[0]["client_id"]
    label = toks[0]["label"]

    # Candidates: show only where BG has been submitted
    cands = _supa_get(
        f"candidates?client_id=eq.{client_id}"
        "&status=not.in.(Rejected,Hired,Transferred)"
        "&background_status=in.(Eligible,In Progress,Needs Further Review,Collection Event Review)"
        "&or=(compliance_override.is.null,compliance_override.eq.false)"
        "&select=id,first_name,last_name,email,phone,rwp_score,rwp_classification,"
        "background_status,drug_test_status,gcic_uploaded,mec_uploaded,dl_verified,"
        "qcert_completed_at,road_test_date"
        "&order=rwp_score.desc.nullslast"
    )
    if not isinstance(cands, list):
        cands = []

    # Pre-submission count
    pre = _supa_get(
        f"candidates?client_id=eq.{client_id}"
        "&status=not.in.(Rejected,Hired,Transferred)"
        "&background_status=not.in.(Eligible,In Progress,Needs Further Review,Collection Event Review)"
        "&or=(compliance_override.is.null,compliance_override.eq.false)"
        "&select=id"
    )
    pre_count = len(pre) if isinstance(pre, list) else 0

    # Client actions
    acts = _supa_get(f"client_actions?token=eq.{token}&select=candidate_id,action,notes,reroute_requested")
    am = {}
    if isinstance(acts, list):
        for a in acts:
            am[a["candidate_id"]] = a

    badge, prog, rev, hired = [], [], [], []
    cdata = {}
    for c in cands:
        cdata[c["id"]] = c
        bg = (c.get("background_status") or "").lower()
        dr = (c.get("drug_test_status") or "").lower()
        ca = am.get(c["id"], {}).get("action")
        if ca == "hired":
            hired.append(_tr(c, am, "badge-bg"))
            continue
        if bg == "eligible" and dr in ("pass", "negative/pass"):
            badge.append(_tr(c, am, "badge-bg"))
        elif bg == "in progress" or (bg == "needs further review" and dr in ("in progress", "pass", "negative/pass")):
            prog.append(_tr(c, am))
        else:
            rev.append(c)

    total = len(badge) + len(prog) + len(rev)
    now = datetime.now().strftime("%B %-d, %Y")

    cdata_js = json.dumps({str(k): {
        "first_name": v.get("first_name",""),
        "last_name": v.get("last_name",""),
        "phone": v.get("phone",""),
        "email": v.get("email",""),
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

    body = (
        (_sec("Badge Ready", "#0F6E56", _tbl(badge, True), "Background cleared, drug test passed. Click a name to view contact details.") if badge else "") +
        (_sec("In Progress", "#185FA5", _tbl(prog, False), "Background screening or drug test currently underway.") if prog else "") +
        (_sec("Under Review", "#BA7517", rev_block) if rev else "") +
        (_sec("Pre-Submission", "#aaa", pre_block) if pre_count else "") +
        (_sec("Hired", "#0F6E56", hired_block, "Candidates marked as hired. Shown for record-keeping.") if hired else "") +
        ('<p style="color:#aaa;text-align:center;padding:60px">No active candidates at this time.</p>' if not total and not hired else "")
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
body{{font-family:'DM Sans',sans-serif;background:#f5f4f0;color:#1a1a1a;min-height:100vh}}
.hdr{{background:#1a3a2a;padding:22px 32px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
.hdr h1{{font-family:'DM Sans',sans-serif;font-weight:600;color:#fff;font-size:22px;letter-spacing:-0.3px}}
.hdr p{{color:#c8a84b;font-size:13px;margin-top:3px}}
.hdr-r{{text-align:right;color:#8aab96;font-size:12px}}
.hdr-r strong{{color:#c8a84b;font-size:15px;display:block;margin-bottom:3px}}
.stats{{background:#1a3a2a;padding:0 32px 20px;display:flex;gap:14px;flex-wrap:wrap}}
.stat{{background:rgba(255,255,255,0.08);border:0.5px solid rgba(200,168,75,0.3);border-radius:8px;padding:12px 20px}}
.sv{{font-size:26px;font-weight:600;color:#fff}}
.sl{{font-size:11px;color:#8aab96;margin-top:2px}}
.body{{padding:28px 32px;max-width:1200px;margin:0 auto}}
.section{{margin-bottom:36px}}
.sec-hdr{{display:flex;align-items:center;gap:10px;margin-bottom:12px}}
.dot{{width:10px;height:10px;border-radius:50%;display:inline-block;flex-shrink:0}}
.sec-title{{font-size:16px;font-weight:600;color:#1a3a2a}}
.sec-note{{font-size:13px;color:#777;margin:0 0 12px}}
.tbl-wrap{{overflow-x:auto;border-radius:8px;border:0.5px solid #e5e7eb}}
table{{width:100%;border-collapse:collapse;font-size:12px;min-width:860px}}
th{{padding:8px 10px;text-align:center;font-weight:600;color:#1a3a2a;background:#f9f9f7;border-bottom:1.5px solid #c8a84b;white-space:nowrap}}
th:first-child{{text-align:left}}
td{{padding:8px 10px;text-align:center;border-bottom:0.5px solid #eee;white-space:nowrap;vertical-align:middle}}
td:first-child{{text-align:left}}
.pill{{padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;color:#fff;display:inline-block}}
.elig{{color:#0F6E56;font-weight:600}}.inelig{{color:#A32D2D;font-weight:600}}.prog{{color:#BA7517;font-weight:600}}
.hire-sel{{padding:4px 6px;border-radius:6px;border:1px solid #e0e0e0;font-size:11px;font-family:'DM Sans',sans-serif;background:#fff;cursor:pointer;width:118px}}
.hire-sel.s-contacting{{background:#e8f4ff;border-color:#185FA5;color:#185FA5;font-weight:600}}
.hire-sel.s-hired{{background:#e8faf2;border-color:#0F6E56;color:#0F6E56;font-weight:600}}
.hire-sel.s-rejected{{background:#fef2f2;border-color:#A32D2D;color:#A32D2D;font-weight:600}}
.notes-input{{width:150px;padding:4px 6px;border-radius:6px;border:1px solid #e0e0e0;font-size:11px;font-family:'DM Sans',sans-serif;resize:none;height:32px}}
.notes-input:focus{{outline:none;border-color:#c8a84b}}
.sv-flash{{font-size:10px;color:#0F6E56;margin-left:3px;opacity:0;transition:opacity 0.3s}}
.sv-flash.show{{opacity:1}}
.reroute-btn{{padding:3px 7px;border-radius:5px;border:1px solid #BA7517;background:#fff8f0;color:#BA7517;font-size:10px;cursor:pointer;margin-top:3px;display:none}}
.reroute-btn.show{{display:inline-block}}.reroute-btn.on{{background:#BA7517;color:#fff}}
.name-btn{{background:none;border:none;cursor:pointer;font-family:'DM Sans',sans-serif;font-size:12px;font-weight:600;color:#1a3a2a;text-decoration:underline;text-decoration-style:dotted;text-decoration-color:#c8a84b;padding:0}}
.name-btn:hover{{color:#0F6E56}}
.overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.3);z-index:999}}
.overlay.show{{display:block}}
.ccard{{display:none;position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:#fff;border-radius:12px;box-shadow:0 20px 60px rgba(0,0,0,0.18);padding:24px 28px;z-index:1000;min-width:280px}}
.ccard.show{{display:block}}
.cc-name{{font-family:'DM Serif Display',serif;font-size:18px;color:#1a3a2a;margin-bottom:16px;padding-right:24px}}
.cc-row{{display:flex;align-items:center;gap:10px;margin-bottom:10px;font-size:13px}}
.cc-lbl{{color:#888;width:64px;flex-shrink:0;font-size:12px}}
.cc-val a{{color:#185FA5;text-decoration:none}}
.cc-close{{position:absolute;top:12px;right:14px;cursor:pointer;color:#aaa;font-size:20px;line-height:1;background:none;border:none}}
.rev-box{{background:#fffbf0;border:0.5px solid #e5e7eb;border-radius:8px;padding:16px 20px;display:flex;align-items:center;gap:16px}}
.rev-num{{font-size:32px;font-weight:600;color:#BA7517;flex-shrink:0}}
.rev-txt{{font-size:13px;color:#666;line-height:1.6}}
.footer{{text-align:center;padding:28px;font-size:12px;color:#bbb}}
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
  <div class="hdr-r"><strong>{total} active candidates</strong>Updated {now}</div>
</div>
<div class="stats">
  <div class="stat"><div class="sv" style="color:#c8a84b">{len(badge)}</div><div class="sl">Badge Ready</div></div>
  <div class="stat"><div class="sv">{len(prog)}</div><div class="sl">In Progress</div></div>
  <div class="stat"><div class="sv">{len(rev)}</div><div class="sl">Under Review</div></div>
  <div class="stat"><div class="sv" style="color:#0F6E56">{len(hired)}</div><div class="sl">Hired</div></div>
  <div class="stat"><div class="sv" style="color:#aaa">{pre_count}</div><div class="sl">Pre-Submission</div></div>
</div>
<div class="body">{body}</div>
<div class="footer">Powered by <strong>PEAKrecruiting</strong> &bull; Questions? (470) 470-4766</div>
<script>
const TOKEN="{token}";
const ACTION_URL="{ACTION_URL}";
const RWP={json.dumps({k:v["label"] for k,v in RWP_META.items()})};
const CDATA={cdata_js};
function showCard(id){{const c=CDATA[id];if(!c)return;document.getElementById("cc-name").textContent=c.first_name+" "+c.last_name;document.getElementById("cc-phone").innerHTML=c.phone?'<a href="tel:'+c.phone+'">'+c.phone+"</a>":"--";document.getElementById("cc-email").innerHTML=c.email?'<a href="mailto:'+c.email+'">'+c.email+"</a>":"--";const r=RWP[c.rwp_classification];document.getElementById("cc-rwp").textContent=r?(r+(c.rwp_score?" "+c.rwp_score:"")):"--";document.getElementById("cc").classList.add("show");document.getElementById("ov").classList.add("show");}}
function closeCard(){{document.getElementById("cc").classList.remove("show");document.getElementById("ov").classList.remove("show");}}
async function post(cid,fields){{await fetch(ACTION_URL,{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{token:TOKEN,candidate_id:cid,...fields}})}});}}
function flash(id){{const el=document.getElementById("sv-"+id);if(el){{el.classList.add("show");setTimeout(()=>el.classList.remove("show"),2000);}}}}
function onAction(id,sel){{const a=sel.value;sel.className="hire-sel"+(a!=="none"?" s-"+a:"");const row=document.getElementById("row-"+id);const btn=row&&row.querySelector(".reroute-btn");if(btn)a==="rejected"?btn.classList.add("show"):btn.classList.remove("show","on");post(id,{{action:a,reroute_requested:false}}).then(()=>flash(id));}}
function onReroute(id,btn){{const was=btn.classList.contains("on");btn.classList.toggle("on");const reason=!was?(prompt("Brief reason for re-route (optional):")||""):"";post(id,{{action:"rejected",reroute_requested:!was,reject_reason:reason}}).then(()=>flash(id));}}
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
    threading.Thread(target=_daily_scheduler, daemon=True).start()

    port = int(os.environ.get("PORT", 5678))
    app.run(host="0.0.0.0", port=port, debug=False)
