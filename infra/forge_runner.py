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
        "description": "MEC/DL reminder cadence — 3-day escalating reminders (T16/17/18) for candidates with outreach sent but docs not uploaded",
        "allowed_args": ["--dry-run"],
    },
    "drug_screen_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "drug_screen_reminder.py"),
        "description": "Drug screen reminder cadence — 3-day escalating reminders (T48/49/50) for candidates with drug outreach sent but test not started",
        "allowed_args": ["--dry-run"],
    },
    "fadv_action_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "fadv_action_reminder.py"),
        "description": "FADV action reminder cadence — 3-day escalating reminders (T42/43/44) for candidates needing further review action",
        "allowed_args": ["--dry-run"],
    },
    "gcic_reminder": {
        "script": str(SCRIPTS_DIR / "scripts" / "gcic_reminder.py"),
        "description": "GCIC reminder cadence — 3-day escalating reminders (T8/9/10) for candidates with GCIC outreach sent but form not completed",
        "allowed_args": ["--dry-run"],
    },
    "mec_dl_fup": {
        "script": str(SCRIPTS_DIR / "scripts" / "mec_dl_fup_scheduler.py"),
        "description": "MEC/DL follow-up scheduler",
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
    log.info(f"[twilio] Inbound SMS from {from_number}: {body[:80]}")
    candidate = _match_candidate(from_number)
    if candidate:
        now = datetime.now(timezone.utc).isoformat()
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
                "from_number": _clean_phone(from_number),
                "to_number": _clean_phone(TWILIO_FROM_NUMBER),
                "delivery_status": "delivered",
            },
        )
        log.info(f"[twilio] Logged inbound SMS -> candidate {candidate['id']}")
    else:
        log.warning(f"[twilio] No candidate match for {from_number}")
    return Response(TWIML_EMPTY, mimetype="application/xml")


@app.route("/twilio/voice", methods=["POST"])
def twilio_inbound_voice():
    from_number = request.form.get("From", "")
    log.info(f"[twilio] Inbound call from {from_number}")
    return Response(TWIML_GREETING, mimetype="application/xml")


@app.route("/twilio/voice/recording", methods=["POST"])
def twilio_voice_recording():
    from_number = request.form.get("From", "")
    recording_url = request.form.get("RecordingUrl", "")
    recording_duration = request.form.get("RecordingDuration", "0")
    log.info(f"[twilio] Recording from {from_number}: {recording_url} ({recording_duration}s)")
    candidate = _match_candidate(from_number)
    if candidate:
        now = datetime.now(timezone.utc).isoformat()
        http_requests.post(
            f"{SUPABASE_URL}/rest/v1/candidate_comms",
            headers={**_SB_HEADERS, "Prefer": "return=minimal"},
            json={
                "candidate_id": candidate["id"],
                "client_id": candidate["client_id"],
                "channel": "voice",
                "direction": "inbound",
                "body": f"Voicemail ({recording_duration}s): {recording_url}",
                "sent_at": now,
                "sent_by": "twilio_voice",
                "send_mode": "automated",
                "from_number": _clean_phone(from_number),
                "to_number": _clean_phone(TWILIO_FROM_NUMBER),
                "delivery_status": "delivered",
            },
        )
        log.info(f"[twilio] Logged voicemail -> candidate {candidate['id']}")
    else:
        log.warning(f"[twilio] No candidate match for voicemail from {from_number}")
    return Response(TWIML_RECORDING_ACK, mimetype="application/xml")


@app.route("/twilio/status", methods=["POST"])
def twilio_status_callback():
    message_sid = request.form.get("MessageSid", "")
    message_status = request.form.get("MessageStatus", "")
    log.info(f"[twilio] Status callback: SID={message_sid} status={message_status}")
    if message_sid and SUPABASE_URL:
        now = datetime.now(timezone.utc).isoformat()
        http_requests.patch(
            f"{SUPABASE_URL}/rest/v1/sms_send_queue?twilio_sid=eq.{message_sid}",
            headers=_SB_HEADERS,
            json={
                "delivery_status": message_status,
                "updated_at": now,
            },
        )
    return Response(status=204)


# ── Scheduler — SMS Queue Poller (every 15 minutes) ───────────────────────────

def _sms_scheduler():
    """Background thread: fires sms_queue_poller.py every 15 minutes."""
    script = str(SCRIPTS_DIR / "sms_queue_poller.py")
    while True:
        try:
            log.info("[scheduler] Triggering sms_queue_poller.py (15-min interval)")
            result = subprocess.run(
                ["python3", script],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=str(SCRIPTS_DIR),
            )
            log.info(f"[scheduler] sms_queue_poller exit={result.returncode}")
            if result.stdout:
                for line in result.stdout.strip().splitlines():
                    log.info(f"[scheduler] {line}")
            if result.stderr:
                for line in result.stderr.strip().splitlines():
                    log.warning(f"[scheduler] {line}")
        except Exception as e:
            log.error(f"[scheduler] sms_queue_poller error: {e}")
        time.sleep(900)


# ── Scheduler — Daily Reminders + GCIC Outreach ──────────────────────────────

def _run_script(command):
    """Execute a whitelisted script by command name."""
    script_path = WHITELIST.get(command, {}).get("script")
    if not script_path:
        log.warning(f"[scheduler] Unknown command: {command}")
        return
    try:
        log.info(f"[scheduler] Running {command}")
        result = subprocess.run(
            ["python3", script_path],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(SCRIPTS_DIR),
        )
        log.info(f"[scheduler] {command} exit={result.returncode}")
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                log.info(f"[scheduler] {line}")
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                log.warning(f"[scheduler] {line}")
    except Exception as e:
        log.error(f"[scheduler] {command} error: {e}")


def _daily_scheduler():
    """Background thread: fires daily reminder scripts at 8am UTC."""
    schedule.every().day.at("08:00").do(_run_script, "gcic_reminder")
    schedule.every().day.at("08:05").do(_run_script, "mec_dl_reminder")
    schedule.every().day.at("08:10").do(_run_script, "drug_screen_reminder")
    schedule.every().day.at("08:15").do(_run_script, "fadv_action_reminder")
    schedule.every(30).minutes.do(_run_script, "gcic_outreach")
    while True:
        schedule.run_pending()
        time.sleep(60)


# ── Main ────────────────────────────────────────────────────────────────────────

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

    scheduler = threading.Thread(target=_sms_scheduler, daemon=True)
    scheduler.start()

    daily = threading.Thread(target=_daily_scheduler, daemon=True)
    daily.start()

    port = int(os.environ.get("PORT", 5678))
    app.run(host="0.0.0.0", port=port, debug=False)
