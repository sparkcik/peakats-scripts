# PEAK Recruiting -- Claude Code Context

## Project overview
PEAKATS is an applicant tracking system for FedEx ISP driver recruiting, built on Reflex with a Supabase (PostgreSQL) backend. ~2,300 candidate records.

## The stack
- **PEAKATS**: Reflex web app (dashboard + candidate management)
- **Supabase**: PostgreSQL backend for all candidate data, templates, ops state
- **forge-local**: Internal API server on Fly.io for scheduled jobs and pipeline operations
- **RingCentral (RC)**: Business SMS/voice platform (migrating away)
- **Twilio**: Replacing RC for outbound SMS

## Key paths
```
~/peakats_fresh/                          # Reflex app (PEAKATS dashboard)
~/peakats-scripts/scripts/               # All pipeline scripts
~/Library/CloudStorage/GoogleDrive-.../CRUCIBLE OS/PEAK/PEAKATS/   # Drive-synced data folders
```

## Active scripts (scripts/)
| Script | Purpose |
|---|---|
| `peak_fadv_update_v6.2.py` | FADV background check CSV sync. Alias: peak-fadv |
| `peak_rig_processor_v2.py` | Resume scoring via Gemini. Alias: peak-process |
| `peak_csv_import_v2.py` | Indeed CSV intake |
| `peak_process_batch_v2.py` | Full pipeline batch orchestrator |
| `peak_setup_client.py` | New client folder + registry setup |
| `peak_fadv_pending.py` | Export candidates not yet submitted to FADV |
| `peak_allocate.py` | Split batches across clients at a station |
| `rc_data_capture.py` | Extract RC SMS + call history to Supabase |
| `rc_inbox_cron.py` | 30-min RC inbox polling (deployed to forge-local) |
| `find_missing_resumes.py` | Find unscored candidates |
| `score_missing_resumes.py` | Score unscored candidates |

## Deploy rule
PEAKATS deploys via `reflex deploy` only. Never `git push` to trigger a deploy.

## External identity
All client/candidate-facing communications use the name **Kai**. The operator is Charles. Never expose Charles externally.

## Hard rules
- Never expose `ssn_encrypted` in any output or log
- No em dashes or en dashes in any output
- `peak_fadv_update_v6.py` is retired -- use v6.2
- RWP scoring logic (v2.1) is locked -- no changes without explicit approval
- Credentials belong in environment variables or `.env` (gitignored), never in committed files

## GitHub
`sparkcik/peakats-scripts` -- push all script changes here after editing.
<!-- redeploy trigger: 2026-04-05T19:54:22.241874 -->
