#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# PEAK — GCIC Pipeline Runner
# Runs gcic_batch_filler_v3.py on a schedule.
# New Sheet rows → PDFs → signed → emailed to FADV → Supabase flagged.
#
# INSTALL (run once):
#   chmod +x ~/peakats-scripts/gcic_pipeline_runner.sh
#   crontab -e
#   Add: */15 * * * * ~/peakats-scripts/gcic_pipeline_runner.sh
#
# That runs every 15 minutes. Adjust interval as needed.
# ═══════════════════════════════════════════════════════════════

SCRIPT_DIR="$HOME/peakats-scripts"
SCRIPT="$SCRIPT_DIR/gcic_batch_filler_v3.py"
LOG="$SCRIPT_DIR/logs/gcic_pipeline.log"
PYTHON="/usr/local/bin/python3"

mkdir -p "$SCRIPT_DIR/logs"

# Rotate log if over 5MB
if [ -f "$LOG" ] && [ $(wc -c < "$LOG") -gt 5242880 ]; then
    mv "$LOG" "${LOG}.bak"
fi

echo "" >> "$LOG"
echo "══════════════════════════════════════" >> "$LOG"
echo "RUN: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
echo "══════════════════════════════════════" >> "$LOG"

# Check script exists
if [ ! -f "$SCRIPT" ]; then
    echo "ERROR: Script not found at $SCRIPT" >> "$LOG"
    exit 1
fi

# Run the filler — captures stdout+stderr to log
$PYTHON "$SCRIPT" >> "$LOG" 2>&1
EXIT_CODE=$?

echo "Exit code: $EXIT_CODE" >> "$LOG"
echo "Done: $(date '+%H:%M:%S')" >> "$LOG"

exit $EXIT_CODE
