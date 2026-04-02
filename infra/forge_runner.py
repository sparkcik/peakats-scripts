#!/usr/bin/env python3
"""
forge_runner.py — forge-local executor daemon
Part of Crucible OS / forge-local

Listens for authenticated webhook commands from forge-cloud/bridge
and executes whitelisted scripts on the local Mac.

Port:    5678
Auth:    X-Forge-Key header (set in FORGE_RUNNER_KEY env var)
Logging: ~/peakats-scripts/logs/forge_runner.log

Usage:
    python3 infra/forge_runner.py

Managed by launchd (com.crucible.forge-runner.plist)
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

# ── Config ─────────────────────────────────────────────────────────────────────

PORT = 5678
SCRIPTS_DIR = Path(os.environ.get("SCRIPTS_DIR", str(Path.home() / "peakats-scripts")))
LOG_DIR = Path(os.environ.get("LOG_DIR", str(SCRIPTS_DIR / "logs")))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Auth key — set FORGE_RUNNER_KEY in environment or launchd plist
AUTH_KEY = os.environ.get("FORGE_RUNNER_KEY", "forge-local-2026")

# ── Command Whitelist ───────────────────────────────────────────────────────────
# Maps command alias → actual script path + default args
# Only these commands can be executed. Nothing else.

WHITELIST = {
    "fadv_update": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_fadv_update_v6.2.py"),
        "description": "FADV reconciliation — sync BG/drug status from CSV exports to Supabase",
        "allowed_args": ["--batch", "--client"],
    },
    "fadv_update_batch": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_fadv_update_v6.2.py"),
        "description": "FADV reconciliation — all clients batch mode",
        "fixed_args": ["--batch"],
        "allowed_args": [],
    },
    "rig_process": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_rig_processor_v2.py"),
        "description": "Resume scoring via Gemini — process new resumes for a client",
        "allowed_args": ["--client", "--limit", "--create-unmatched"],
    },
    "batch_process": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_process_batch_v2.py"),
        "description": "Batch resume processing — full pipeline for a client folder",
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
        "description": "FADV entry bot — submit candidates to FADV portal",
        "allowed_args": ["--client", "--test", "--list"],
    },
    "sms_queue": {
        "script": str(SCRIPTS_DIR / "sms_queue_poller.py"),
        "description": "SMS queue poller — send pending scheduled messages via RingCentral",
        "allowed_args": ["--dry-run"],
    },
    "shell": {
        "script": "__shell__",
        "description": "Read-only shell commands for Forge file audits (find, ls, cat, git)",
        "allowed_args": ["cmd"],
    },
    "ping": {
        "script": None,
        "description": "Health check — returns daemon status",
        "allowed_args": [],
    },
    "rc_inbox": {
        "script": str(SCRIPTS_DIR / "rc_inbox_command.py"),
        "description": "Read RC SMS inbox for 470-857-4325 — returns inbound messages with candidate matches",
        "allowed_args": ["--limit", "--unread-only", "--mark-read", "--format"],
    },
    "rc_inbox_cron": {
        "script": str(SCRIPTS_DIR / "rc_inbox_cron.py"),
        "description": "Poll RC inbox every 30 min, match candidates, write to candidate_comms + sms_triage_queue",
        "allowed_args": ["--dry-run"],
    },
    "rc_data_capture": {
        "script": str(SCRIPTS_DIR / "scripts" / "rc_data_capture_cloud.py"),
        "description": "Capture RC SMS + call history to Supabase archive tables and rebuild contact export",
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
        "description": "GCIC outreach — queue Template 2 SMS for candidates with BG In Progress and no GCIC sent",
        "allowed_args": ["--dry-run"],
    },
    "mec_outreach": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_dl_trigger.py"),
        "description": "MEC/DL outreach — send Template 15/46/37 SMS based on drug/BG status for MEC/DL collection",
        "allowed_args": [],
    },
    "mec_dl_backfill": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_dl_backfill.py"),
        "description": "Backfill existing MEC/DL form responses into Supabase",
        "allowed_args": [],
    },
    "mec_dl_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_dl_reminder.py"),
        "description": "MEC/DL reminder cadence -- 3-day escalating reminders (T16/17/18) for candidates with outreach sent but docs not uploaded",
        "allowed_args": ["--dry-run"],
    },
    "drug_screen_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "drug_screen_reminder.py"),
        "description": "Drug screen reminder cadence -- 3-day escalating reminders (T48/49/50) for candidates with drug outreach sent but test not started",
        "allowed_args": ["--dry-run"],
    },
    "fadv_action_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "fadv_action_reminder.py"),
        "description": "FADV action reminder cadence -- 3-day escalating reminders (T42/43/44) for candidates needing further review action",
        "allowed_args": ["--dry-run"],
    },
    "gcic_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "gcic_reminder.py"),
        "description": "GCIC reminder cadence -- 3-day escalating reminders (T8/9/10) for candidates with GCIC outreach sent but form not completed",
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
    "twilio_call_sync": {
        "script": "scripts/twilio_call_sync.py",
        "allowed_args": [],
    },
}

# ── Safe shell commands (read-only whitelist) ───────────────────────────────────
SAFE_SHELL_PREFIXES = ("find ", "ls ", "cat ", "head ", "tail ", "wc ", "git ", "echo ")

# ── Logging ─────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "forge_runner.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("forge-runner")

# ── Flask App ───────────────────────────────────────────────────────────────────

app = Flask(__name__)





@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, PUT, DELETE"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Forge-Key"
    return response

@app.route("/twilio/call", methods=["OPTIONS"])
@app.route("/twilio/send", methods=["OPTIONS"])
@app.route("/voicemail/audio", methods=["OPTIONS"])
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

    # Shell passthrough — read-only commands only
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
        # List-style args: validate each flag against the whitelist
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


# ── Routes ──────────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "running",
        "daemon": "forge-runner",
        "version": "1.2.0",
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
                "description": spec["description"],
                "allowed_args": spec.get("allowed_args", []),
            }
            for name, spec in WHITELIST.items()
        }
    })


# ── Twilio Webhook Routes (public — no X-Forge-Key required) ──────────────────
# These handle inbound Twilio SMS and voice POSTs on the existing port 8080.

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
    <Say voice="alice">You have reached Kai at PEAK recruiting. Please leave a message after the tone.</Say>
    <Record maxLength="120" action="/twilio/voice/recording" transcribe="false" />
    <Say voice="alice">We did not receive a recording. Goodbye.</Say>
</Response>"""
TWIML_RECORDING_ACK = '<?xml version="1.0" encoding="UTF-8"?><Response><Say voice="alice">Thank you. Goodbye.</Say><Hangup/></Response>'


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

    # Always write to sms_triage_queue -- this is what the PWA reads
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

    # Also log to candidate_comms if matched
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
    <Dial callerId="{caller_id}" timeout="20" action="/twilio/voice/missed">
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

    # Write to twilio_voicemail -- this is what the PWA reads
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


# Start schedulers regardless of how app is launched (gunicorn or direct)
_scheduler_thread = threading.Thread(target=_sms_scheduler, daemon=True)
_scheduler_thread.start()
_daily_thread = threading.Thread(target=_daily_scheduler, daemon=True)
_daily_thread.start()
log.info("Schedulers started")

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("forge-runner v1.3.0 starting")
    log.info(f"Port:        {PORT}")
    log.info(f"Scripts dir: {SCRIPTS_DIR}")
    log.info(f"Log dir:     {LOG_DIR}")
    log.info(f"Whitelist:   {list(WHITELIST.keys())}")
    log.info("Scheduler:   sms_queue_poller every 15 min")
    log.info("Scheduler:   daily reminders at 08:00 UTC, gcic_outreach every 30 min")
    log.info("=" * 60)

    port = int(os.environ.get("PORT", 5678))
    app.run(host="0.0.0.0", port=port, debug=False)
