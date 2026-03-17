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
from datetime import datetime, timezone
from pathlib import Path
from flask import Flask, request, jsonify

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
        "script": str(SCRIPTS_DIR / "scripts" / "peak_fadv_update_v6.py"),
        "description": "FADV reconciliation — sync BG/drug status from CSV exports to Supabase",
        "allowed_args": ["--batch", "--client"],
    },
    "fadv_update_batch": {
        "script": str(SCRIPTS_DIR / "scripts" / "peak_fadv_update_v6.py"),
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

    allowed = spec.get("allowed_args", [])
    if isinstance(args, list):
        # List-style args: validate each flag against the whitelist
        for arg in args:
            arg_str = str(arg)
            if arg_str.startswith("--"):
                flag_bare = arg_str.lstrip("-")
                if f"--{flag_bare}" not in [f"--{a}" for a in allowed]:
                    return None, f"Arg not allowed for {command}: {arg_str}"
            cmd.append(arg_str)
    else:
        for key, val in args.items():
            flag = f"--{key}" if not key.startswith("--") else key
            flag_bare = flag.lstrip("-")
            if f"--{flag_bare}" not in [f"--{a}" for a in allowed]:
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
        "version": "1.1.0",
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


# ── Scheduler — SMS Queue Poller (daily 11:30 UTC / 7:30 AM ET) ───────────────

def _sms_scheduler():
    """Background thread: fires sms_queue_poller.py once daily at 11:30 UTC."""
    last_run_date = None
    script = str(SCRIPTS_DIR / "sms_queue_poller.py")
    while True:
        try:
            now = datetime.now(timezone.utc)
            today = now.date()
            if now.hour == 11 and now.minute == 30 and last_run_date != today:
                last_run_date = today
                log.info("[scheduler] Triggering sms_queue_poller.py (11:30 UTC)")
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
        time.sleep(60)


# ── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("forge-runner v1.2.0 starting")
    log.info(f"Port:        {PORT}")
    log.info(f"Scripts dir: {SCRIPTS_DIR}")
    log.info(f"Log dir:     {LOG_DIR}")
    log.info(f"Whitelist:   {list(WHITELIST.keys())}")
    log.info("Scheduler:   sms_queue_poller @ 11:30 UTC daily")
    log.info("=" * 60)

    scheduler = threading.Thread(target=_sms_scheduler, daemon=True)
    scheduler.start()

    port = int(os.environ.get("PORT", 5678))
    app.run(host="0.0.0.0", port=port, debug=False)
