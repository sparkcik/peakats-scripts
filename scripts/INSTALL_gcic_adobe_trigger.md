# GCIC Adobe Sign Trigger -- Installation Guide

## 1. Create the Apps Script project

1. Go to [script.google.com](https://script.google.com)
2. Click **New project**
3. Rename to **PEAKATS GCIC Adobe Trigger**
4. Delete the default `myFunction` code

## 2. Paste the code

1. Open `gcic_adobe_trigger.gs` from this repo
2. Copy the entire contents
3. Paste into the Apps Script editor (replacing everything in `Code.gs`)
4. Click **Save** (Ctrl+S)

## 3. Create Gmail labels

In Gmail, create this label if it doesn't already exist:

- `GCIC/Processed` -- applied to processed signed GCIC emails

To create: Gmail sidebar > scroll down > **Create new label** > type `GCIC/Processed`.

## 4. Verify Drive folder

The script saves signed PDFs to this Drive folder:

- **GCIC_DOCS_ROOT**: `1UJfJM6ZMQo2RuVbNWrv4hkBiLnWAZkjB`

Confirm this folder exists and is accessible by the script's Google account. The script auto-creates `{client_id}/gcic/` subfolders as needed.

## 5. Set the time-based trigger

1. In Apps Script editor, click the **clock icon** (Triggers) in the left sidebar
2. Click **+ Add Trigger** (bottom right)
3. Configure:
   - **Function**: `processSignedGCICEmails`
   - **Deployment**: Head
   - **Event source**: Time-driven
   - **Type of time-based trigger**: Minutes timer
   - **Interval**: Every 15 minutes
4. Click **Save**

## 6. Grant permissions

On the first trigger run (or manual run via the Play button):

1. Google will prompt: "This app wants to access your Google Account"
2. Click **Review permissions**
3. Select your `charles@thefoundry.llc` account
4. Click **Advanced** > **Go to PEAKATS GCIC Adobe Trigger (unsafe)**
5. Click **Allow**

Required scopes:
- `https://www.googleapis.com/auth/gmail.modify` (read emails, apply labels)
- `https://www.googleapis.com/auth/gmail.send` (send GCIC to casedocuments@fadv.com)
- `https://www.googleapis.com/auth/drive` (save PDF to Drive)
- `https://www.googleapis.com/auth/script.external_request` (call Supabase API)

## 7. Test manually

1. Click the **Play** button next to `processSignedGCICEmails`
2. Check **Execution log** for output
3. Verify:
   - PDF saved to Drive under `GCIC_DOCS_ROOT/{client_id}/gcic/`
   - Email sent to `casedocuments@fadv.com` (check Sent folder)
   - Supabase `candidates` table shows `gcic_stage='SUBMITTED_TO_FADV'` and `gcic_status='COMPLETE'`

## Flow summary

```
Adobe Sign "Signed and Filed" email arrives
  -> Gmail trigger fires (every 15 min)
  -> Skip test emails (Kai + Kai)
  -> Parse: candidate name from subject, Order ID if present
  -> Extract PDF attachment
  -> Match candidate in Supabase (exact name only)
  -> If matched:
       Save PDF to Drive: {client_id}/gcic/YYYY-MM-DD_LastFirst_GCIC.pdf
       Email PDF to casedocuments@fadv.com
       UPDATE candidates SET gcic_stage='SUBMITTED_TO_FADV', gcic_status='COMPLETE'
  -> If unmatched:
       INSERT forge_memory (ops_note for manual review)
  -> Label email as GCIC/Processed
```
