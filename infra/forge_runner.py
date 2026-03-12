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
    python3 forge_runner.py

Managed by launchd (com.crucible.forge-runner.plist)
"""

import os
import subprocess
import logging
import json
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify

# ── Config ─────────────────────────────────────────────────────────────────────

PORT = 5678
SCRIPTS_DIR = Path.home() / "peakats-scripts"
LOG_DIR = SCRIPTS_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Auth key — set FO_RGERUNNER_KEY in environment or launchd plist
AUTH_KEY = os.environ.get("FORGE_RUNNER_KEY", "forge-local-2026")

# ── Command Whitelist ───────────────────────────────────────────────────────────
# Maps command alias → actual script path + default args
# Only these commands can be executed. Nothing else.

WHITELIST = {
    "fadv_update": {
        "script": str(SCRIPTS_DIR / "peak_fadv_update_v6.py"),
        "description": "FADV reconciliation — sync BG/drug status from CSV exports to Supabase",
        "allowed_args": ["--batch", "--client"],
    },
    "fadv_update_batch": {
        "script": str(SCRIPTS_DIR / "peak_fadv_update_v6.py"),
        "description": "FADV reconciliation — all clients batch mode",
        "fixed_args": ["--batch"],
        "allowed_args": [],
    },
    "rig_process": {
        "script": str(SCRIPTS_DIR / "peak_rig_processor_v2.py"),
        "description": "Resume scoring via Gemini — process new resumes for a client",
        "allowed_args": ["--client", "--limit", "--create-unmatched"],
    },
    "batch_process": {
        "script": str(SCRIPTS_DIR / "peak_process_batch_v2.py"),
        "description": "Batch resume processing — full pipeline for a client folder",
        "allowed_args": ["--client"],
    },
    "find_missing": {
        "script": str(SCRIPTS_DIR / "find_missing_resumes.py"),
        "description": "Find candidates with no RWP score",
        "allowed_args": ["--client"],
    },
    "score_missing": {
        "script": str(SCRIPTS_DIR / "score_missing_resumes.py"),
        "description": "Score candidates that have resumes but no RWP score",
        "allowed_args": ["--client"],
    },
    "gcic_batch": {
        "script": str(SCRIPTS_DIR / "gcic_batch_filler.py"),
        "description": "Generate GCIC PDFs from Google Sheet responses",
        "allowed_args": ["--row", "--limit", "--dry-run"],
    },
    "ping": {
        "script": None,
        "description": "Health check — returns daemon status",
        "allowed_args": [],
    },
}

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
    """
    Build the subprocess command from alias + args.
    Validates all args against whitelist.
    Returns None if validation fails.
    """
    spec = WHITELIST.get(command)
    if not spec:
        return None, f"Unknown command: {command}"

    if command == "ping":
        return ["ping"], None

    script = spec["script"]
    if not Path(script).exists():
        return None, f"Script not found: {script}"

    cmd = ["python3", script]

    # Add fixed args if any
    if "fixed_args" in spec:
        cmd.extend(spec["fixed_args"])

    # Validate and add caller-supplied args
    allowed = spec.get("allowed_args", [])
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
    """Public health check — no auth required."""
    return jsonify({
        "status": "running",
        "daemon": "forge-runner",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
    })


@app.route("/run", methods=["POST"])
def run_command():
    """
    Execute a whitelisted command.

    Body:
        {
            "command": "fadv_update",
            "args": {"client": "cbm"},
            "async": false
        }
    """
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

    # Handle ping
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
            # Fire and forget — return immediately
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
            # Synchronous — wait for completion, return output
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 min max for sync calls
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
    """Return available commands. Requires auth."""
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


# ── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("forge-runner starting")
    log.info(f"Port:        {PORT}")
    log.info(f"Scripts dir: {SCRIPTS_DIR}")
    log.info(f"Log dir:     {LOG_DIR}")
    log.info(f"Whitelist:   {list(WHITELIST.keys())}")
    log.info("=" * 60)

    app.run(host="127.0.0.1", port=PORT, debug=False)
