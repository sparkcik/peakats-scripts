#!/bin/bash
# ── GCIC BATCH FILLER — SETUP & FIRST RUN ─────────────────────────────────────
# Copy and paste this entire block. No edits required.
# Only prerequisite: gcic_template.pdf saved in ~/peakats_fresh/scripts/

set -e
SCRIPTS_DIR="$HOME/peakats_fresh/peakats_fresh/scripts"
mkdir -p "$SCRIPTS_DIR/gcic_output"

# 1. Install dependencies
pip3 install pypdf reportlab --break-system-packages -q

# 2. Copy script into place (script file comes from Claude output)
# If running from the directory where gcic_batch_filler.py was saved:
cp "$(dirname "$0")/gcic_batch_filler.py" "$SCRIPTS_DIR/gcic_batch_filler.py" 2>/dev/null \
  || echo "  → Copy manually if needed: cp gcic_batch_filler.py $SCRIPTS_DIR/"

cd "$SCRIPTS_DIR"

# 3. Inspect Sheet columns (verify COL map matches actual headers)
echo ""
echo "── STEP 1: Inspecting Sheet headers ─────────────────────────"
python3 gcic_batch_filler.py --inspect

# 4. Generate calibration PDF (verify field placement on template)
echo ""
echo "── STEP 2: Generating calibration PDF ───────────────────────"
python3 gcic_batch_filler.py --calibrate

# 5. Dry run — preview first row
echo ""
echo "── STEP 3: Dry run row 1 ─────────────────────────────────────"
python3 gcic_batch_filler.py --row 1 --dry-run

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Setup complete. Review calibration PDF in gcic_output/ folder."
echo "If field placement looks correct, run the batch:"
echo ""
echo "  cd $SCRIPTS_DIR"
echo "  python3 gcic_batch_filler.py                  # all pending"
echo "  python3 gcic_batch_filler.py --client cbm     # CBM only"
echo "  python3 gcic_batch_filler.py --client solpac  # Solpac only"
echo "═══════════════════════════════════════════════════════════════"
