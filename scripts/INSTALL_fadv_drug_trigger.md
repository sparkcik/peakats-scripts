# FADV Drug Screen Trigger -- Installation Guide

## 1. Create the Apps Script project

1. Go to [script.google.com](https://script.google.com)
2. Click **New project**
3. Rename the project to **PEAKATS FADV Drug Trigger**
4. Delete the default `myFunction` code

## 2. Paste the code

1. Open `fadv_drug_trigger.gs` from this repo
2. Copy the entire contents
3. Paste into the Apps Script editor (replacing everything in `Code.gs`)
4. Click **Save** (Ctrl+S)

## 3. Create Gmail labels

In Gmail, create these labels if they don't already exist:

- `FADV/Drug-Orders` -- applied to processed drug screen emails
- `FADV/Processed` -- shared label used by all FADV parsers to prevent reprocessing

To create: Gmail sidebar > scroll down > **Create new label** > type `FADV/Drug-Orders` (Gmail auto-nests under FADV).

## 4. Set the time-based trigger

1. In Apps Script editor, click the **clock icon** (Triggers) in the left sidebar
2. Click **+ Add Trigger** (bottom right)
3. Configure:
   - **Function**: `processNewDrugScreenEmails`
   - **Deployment**: Head
   - **Event source**: Time-driven
   - **Type of time-based trigger**: Minutes timer
   - **Interval**: Every 15 minutes
4. Click **Save**

## 5. Grant Gmail permissions

On the first trigger run (or manual run via the Play button):

1. Google will prompt: "This app wants to access your Google Account"
2. Click **Review permissions**
3. Select your `charles@thefoundry.llc` account
4. Click **Advanced** > **Go to PEAKATS FADV Drug Trigger (unsafe)**
5. Click **Allow** to grant Gmail read/write and external URL access

Required scopes:
- `https://www.googleapis.com/auth/gmail.modify` (read emails, apply labels)
- `https://www.googleapis.com/auth/script.external_request` (call Supabase API)

## 6. Test manually

1. Click the **Play** button next to `processNewDrugScreenEmails`
2. Check **Execution log** for output
3. Verify in Supabase that `sms_send_queue` has new pending rows
4. Verify `candidates` table shows `drug_test_status = 'In Progress'` for matched candidates

## Flow summary

```
FADV email arrives
  -> Gmail trigger fires (every 15 min)
  -> Parse: name, barcode, client hint
  -> Match candidate in Supabase
  -> If matched:
       UPDATE candidates SET drug_test_status = 'In Progress'
       INSERT sms_send_queue (Template 47 with barcode)
  -> If unmatched:
       INSERT forge_memory (ops_note for manual review)
  -> Label email as FADV/Drug-Orders + FADV/Processed
```
