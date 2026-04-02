/**
 * PEAKATS GCIC Gmail Parser -- HARDENED v4 (standalone project)
 * v4 fixes: accent normalization, name suffix stripping, double RE: prefix,
 *           reversed Order ID format, writeSrNumber_ helper extracted.
 */

// ── CONFIG ────────────────────────────────────────────────────────────────────
const SUPABASE_URL         = 'https://eyopvsmsvbgfuffscfom.supabase.co';
const SUPABASE_KEY         = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4';
const FADV_EMAIL           = 'casedocuments@fadv.com';
const ALERT_EMAIL          = 'kai@peakrecruitingco.com';
const CANDIDATE_DOCS_ROOT  = '1UJfJM6ZMQo2RuVbNWrv4hkBiLnWAZkjB';
const GCIC_LABEL_PENDING   = 'GCIC/Pending';
const GCIC_LABEL_PROCESSED = 'GCIC/Processed';
const GCIC_LABEL_FAILED    = 'GCIC/Failed';

// ── MAIN ENTRY POINT ─────────────────────────────────────────────────────────
function processGcicEmails() {
  const pendingLabel = GmailApp.getUserLabelByName(GCIC_LABEL_PENDING);
  if (!pendingLabel) { Logger.log('Label not found: ' + GCIC_LABEL_PENDING); return; }

  const processedLabel = getOrCreateLabel_(GCIC_LABEL_PROCESSED);
  const failedLabel    = getOrCreateLabel_(GCIC_LABEL_FAILED);
  const threads        = pendingLabel.getThreads(0, 50);

  Logger.log('Found ' + threads.length + ' GCIC threads');
  let adobeOk = 0, fadvOk = 0, skipped = 0, errors = 0;

  threads.forEach(function(thread) {
    const messages = thread.getMessages();
    let threadOk   = true;

    messages.forEach(function(message) {
      try {
        const from = message.getFrom();
        const subj = message.getSubject();
        Logger.log('MSG from: [' + from + '] subject: [' + subj + ']');

        if (from.indexOf('adobesign@adobesign.com') !== -1) {
          // Skip non-actionable Adobe Sign email types
          if (subj.indexOf('has been sent out for signature') !== -1 ||
             (subj.indexOf('web form') !== -1 && subj.indexOf('has been created') !== -1) ||
              subj.indexOf('You signed:') !== -1 ||
              subj.indexOf('Please confirm your signature') !== -1 ||
              subj.indexOf('Agreement Exchange Canceled') !== -1 ||
             (subj.indexOf('is Complete') !== -1 && subj.indexOf('Signed and Filed') === -1)) {
            skipped++; return;
          }
          const ok = handleAdobeSign_(message);
          if (ok === true)        { adobeOk++; }
          else if (ok === 'skip') { skipped++; }
          else                    { errors++; threadOk = false; }

        } else if (from.indexOf('support-donotreply@fadv.com') !== -1) {
          const ok = handleFadvSr_(message);
          if (ok === true)        { fadvOk++; }
          else if (ok === 'skip') { skipped++; }
          else                    { errors++; threadOk = false; }

        } else {
          skipped++;
        }
      } catch(e) {
        Logger.log('EXCEPTION processing GCIC message: ' + e.message);
        errors++;
        threadOk = false;
      }
    });

    thread.removeLabel(pendingLabel);
    if (threadOk) {
      thread.addLabel(processedLabel);
      const kaiInbox = GmailApp.getUserLabelByName('Kai/Inbox');
      if (kaiInbox) thread.removeLabel(kaiInbox);
      thread.markRead();
      thread.moveToArchive();
    } else {
      thread.addLabel(failedLabel);
      Logger.log('Thread moved to GCIC/Failed: ' + thread.getFirstMessageSubject());
      alertForge_('GCIC Parser failure', 'Thread moved to GCIC/Failed: ' + thread.getFirstMessageSubject() +
        '\nCheck execution log and re-label to GCIC/Pending to retry.');
    }
  });

  Logger.log('GCIC run complete -- Adobe: ' + adobeOk + ' | FADV SR: ' + fadvOk +
             ' | Skipped: ' + skipped + ' | Errors: ' + errors);
}

// ── HANDLER: Adobe Sign "Signed and Filed" ────────────────────────────────────
// Returns: true = success | 'skip' = intentionally skipped | false = error
function handleAdobeSign_(message) {
  const subject = message.getSubject();

  const nameMatch = subject.match(/\(?\s*between\s+Kai\s+M\s+Clarke\s+and\s+(.+?)\s*\)?\s+is Signed/i);
  if (!nameMatch) {
    Logger.log('Could not parse name from Adobe Sign subject: ' + subject);
    return 'skip';
  }
  const fullName = nameMatch[1].trim().replace(/\)$/, '').trim();

  if (fullName.toLowerCase().indexOf('kai m clarke') !== -1 ||
      fullName.toLowerCase().indexOf('kai m  clarke') !== -1) {
    Logger.log('Skipping test document: ' + subject);
    return 'skip';
  }

  const candidate = findCandidateByName_(fullName);
  if (!candidate) {
    Logger.log('No candidate match for Adobe Sign: ' + fullName + ' -- skipping, manual review needed');
    alertForge_('GCIC no candidate match: ' + fullName,
      'Adobe Sign GCIC signed but no candidate found in DB.\n' +
      'Subject: ' + subject + '\nManual: find candidate, update gcic fields in Supabase.');
    return 'skip';
  }

  if (candidate.gcic_email_sent === 1) {
    Logger.log('Already sent to FADV, skipping: ' + fullName);
    return 'skip';
  }

  const attachments = message.getAttachments();
  const pdf = findPdfAttachment_(attachments);
  if (!pdf) {
    Logger.log('No PDF attachment for: ' + fullName);
    alertForge_('GCIC missing PDF: ' + fullName,
      'Adobe Sign email received for ' + fullName + ' but no PDF attached.\n' +
      'Subject: ' + subject + '\nCandidate ID: ' + candidate.id +
      '\nCheck Adobe Sign portal and reprocess manually.');
    return false;
  }

  const dateStr      = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const lastName     = candidate.last_name  || '';
  const firstName    = candidate.first_name || '';
  const pdfName      = dateStr + '_' + lastName + firstName + '_GCIC.pdf';
  const clientFolder = getOrCreateClientFolder_(candidate.client_id);

  try {
    clientFolder.createFile(pdf.copyBlob().setName(pdfName));
  } catch(e) {
    Logger.log('Drive upload failed for ' + fullName + ': ' + e.message);
    alertForge_('GCIC Drive upload failed: ' + fullName, 'Error: ' + e.message);
    return false;
  }

  const clientDisplay = formatClientName_(candidate.client_id);
  const drugTestId    = candidate.drug_test_id || 'N/A';
  const emailSubject  = firstName + ' ' + lastName + ' GCIC Form - ' + clientDisplay + ' - Order ID: ' + drugTestId;
  const emailBody     =
    'Please find attached the executed GCIC authorization form for the above-referenced candidate. ' +
    'This document was electronically signed via Adobe Acrobat Sign and includes IP capture per GCIC requirements.\n\n' +
    'Candidate: ' + firstName + ' ' + lastName + '\n' +
    'Client: ' + clientDisplay + '\n' +
    'Drug Test Order ID: ' + drugTestId + '\n\n' +
    'Please confirm receipt.\n\n' +
    'Kai Clarke\nPEAKrecruiting';

  try {
    GmailApp.sendEmail(FADV_EMAIL, emailSubject, emailBody, {
      attachments: [pdf.copyBlob().setName(pdfName)]
    });
  } catch(e) {
    Logger.log('FADV email send failed for ' + fullName + ': ' + e.message);
    alertForge_('GCIC FADV send failed: ' + fullName,
      'Error: ' + e.message + '\nPDF uploaded to Drive. Send manually to ' + FADV_EMAIL);
    return false;
  }

  var orderIdMatch = subject.match(/Order ID:\s*([0-9]+)/i);
  var orderId = orderIdMatch ? orderIdMatch[1].trim() : null;

  var finalPatch = {
    gcic_form_completed:       1,
    gcic_form_completed_at:    new Date().toISOString(),
    gcic_uploaded:             1,
    gcic_email_sent:           1,
    gcic_stage:                'SUBMITTED_TO_FADV',
    gcic_status:               'COMPLETE',
    gcic_submitted_to_fadv_at: new Date().toISOString(),
    updated_at:                new Date().toISOString()
  };
  if (orderId) finalPatch.gcic_fadv_order_id = orderId;

  const code = supabasePatch_('candidates', 'id=eq.' + candidate.id, finalPatch);
  if (code !== 200 && code !== 204) {
    Logger.log('DB patch failed (' + code + ') for ' + fullName + ' after FADV send');
    alertForge_('GCIC DB write failed after FADV send: ' + fullName,
      'FADV email WAS sent. Drive upload complete. DB patch failed (' + code + ').\n' +
      'Manual fix: update candidate ID ' + candidate.id + ' in Supabase:\n' +
      'gcic_form_completed=1, gcic_uploaded=1, gcic_email_sent=1, gcic_stage=SUBMITTED_TO_FADV');
    return false;
  }

  Logger.log('Adobe Sign fully processed: ' + fullName + ' -- Drive + FADV + DB confirmed');
  return true;
}

// ── HANDLER: FADV SR# acknowledgment ─────────────────────────────────────────
// Returns: true = success | 'skip' = intentionally skipped | false = error
function handleFadvSr_(message) {
  const subject = message.getSubject();

  // Strip double RE: Re: prefixes before parsing
  const cleanSubject = subject.replace(/^(RE:\s*)+(Re:\s*)*/i, 'RE: ');

  // Standard format: RE: [NAME] GCIC - Order ID: [ID] SR#:[SR]
  const srMatch = cleanSubject.match(/RE:\s*(.+?)\s+GCIC(?:[^S]*?Order ID:\s*(\S+))?\s+SR#:(\S+)/i);

  if (!srMatch) {
    // Reversed format: RE: Order ID: [ID] - GCIC form SR#:[SR]
    const altMatch = cleanSubject.match(/RE:\s*Order ID:\s*(\S+)\s*-\s*GCIC[^S]*SR#:(\S+)/i);
    if (altMatch) {
      const orderId  = altMatch[1].replace(/\.$/, '');
      const srNumber = altMatch[2].replace(/\.$/, '');
      const candidate = findCandidateByDrugTestId_(orderId);
      if (!candidate) {
        Logger.log('No candidate match for reversed-format SR: Order ' + orderId);
        return false;
      }
      return writeSrNumber_(candidate, srNumber, orderId);
    }
    Logger.log('Could not parse FADV SR subject: ' + subject);
    return false;
  }

  const fullName = srMatch[1].trim();
  const orderId  = (srMatch[2] || '').replace(/\.$/, '').trim();
  const srNumber = srMatch[3].replace(/\.$/, '').trim();

  // Normalize: strip name suffixes and accents before lookup
  const normalizedName = fullName
    .replace(/\b(jr\.?|sr\.?|ii|iii|iv)\b/gi, '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim();

  var candidate = orderId ? findCandidateByDrugTestId_(orderId) : null;
  if (!candidate) candidate = findCandidateByName_(normalizedName);
  if (!candidate && normalizedName !== fullName) candidate = findCandidateByName_(fullName);
  if (!candidate) {
    Logger.log('No candidate match for FADV SR: ' + fullName + ' / Order: ' + orderId);
    return false;
  }

  return writeSrNumber_(candidate, srNumber, orderId);
}

// ── WRITE SR# TO CANDIDATE ────────────────────────────────────────────────────
function writeSrNumber_(candidate, srNumber, orderId) {
  if (candidate.gcic_sr_number === srNumber) {
    Logger.log('SR# already recorded, skipping: ' + candidate.first_name + ' ' + candidate.last_name);
    return 'skip';
  }

  const patch = {
    gcic_fadv_outcome:   'Received',
    gcic_sr_number:      srNumber,
    gcic_fadv_sr_number: srNumber,
    gcic_stage:          'SUBMITTED_TO_FADV',
    gcic_status:         'COMPLETE',
    updated_at:          new Date().toISOString()
  };

  if (!candidate.gcic_submitted_to_fadv_at) patch.gcic_submitted_to_fadv_at = new Date().toISOString();
  if (orderId) patch.gcic_fadv_order_id = orderId;

  const existingNotes = candidate.fadv_notes || '';
  if (existingNotes.indexOf('GCIC SR#') === -1) {
    patch.fadv_notes = existingNotes ? existingNotes + '\nGCIC SR#: ' + srNumber : 'GCIC SR#: ' + srNumber;
  }

  const code = supabasePatch_('candidates', 'id=eq.' + candidate.id, patch);
  if (code !== 200 && code !== 204) {
    Logger.log('SR# patch failed (' + code + ') for candidate ' + candidate.id);
    alertForge_('GCIC SR# write failed: ' + candidate.first_name + ' ' + candidate.last_name,
      'SR# ' + srNumber + ' not written to DB. Manual fix needed for candidate ID ' + candidate.id);
    return false;
  }

  Logger.log('FADV SR processed: ' + candidate.first_name + ' ' + candidate.last_name + ' SR#: ' + srNumber);
  return true;
}

// ── CANDIDATE LOOKUPS ─────────────────────────────────────────────────────────
function findCandidateByName_(fullName) {
  const parts = fullName.trim().split(/\s+/);
  if (parts.length < 2) return null;
  const res = supabaseGet_('candidates', {
    select: 'id,first_name,last_name,drug_test_id,client_id,gcic_form_completed,gcic_uploaded,gcic_email_sent,gcic_submitted_to_fadv_at,gcic_sr_number,fadv_notes',
    'first_name': 'ilike.' + parts[0],
    'last_name':  'ilike.' + parts.slice(1).join(' '),
    limit: 1
  });
  return (res && res.length > 0) ? res[0] : null;
}

function findCandidateByDrugTestId_(drugTestId) {
  if (!drugTestId) return null;
  const res = supabaseGet_('candidates', {
    select: 'id,first_name,last_name,drug_test_id,client_id,gcic_form_completed,gcic_uploaded,gcic_email_sent,gcic_submitted_to_fadv_at,gcic_sr_number,fadv_notes',
    'drug_test_id': 'eq.' + drugTestId,
    limit: 1
  });
  return (res && res.length > 0) ? res[0] : null;
}

// ── DRIVE HELPERS ─────────────────────────────────────────────────────────────
function getOrCreateClientFolder_(clientId) {
  const root    = DriveApp.getFolderById(CANDIDATE_DOCS_ROOT);
  const folders = root.getFoldersByName(clientId);
  return folders.hasNext() ? folders.next() : root.createFolder(clientId);
}

function findPdfAttachment_(attachments) {
  for (var i = 0; i < attachments.length; i++) {
    if (attachments[i].getContentType() === 'application/pdf') return attachments[i];
  }
  return null;
}

function formatClientName_(clientId) {
  if (!clientId) return 'Unknown';
  return clientId.split('_').map(function(w) {
    return w.charAt(0).toUpperCase() + w.slice(1);
  }).join(' ');
}

// ── ALERT HELPER ──────────────────────────────────────────────────────────────
function alertForge_(subject, body) {
  try {
    GmailApp.sendEmail(ALERT_EMAIL, '[PEAK ALERT] ' + subject, body);
    Logger.log('Alert sent: ' + subject);
  } catch(e) {
    Logger.log('Alert send failed: ' + e.message);
  }
}

// ── SUPABASE HELPERS ──────────────────────────────────────────────────────────
function supabaseGet_(table, params) {
  const qs = Object.entries(params).map(function(e) {
    return e[0] + '=' + encodeURIComponent(e[1]);
  }).join('&');
  const res = UrlFetchApp.fetch(SUPABASE_URL + '/rest/v1/' + table + '?' + qs, {
    method: 'GET',
    headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY, 'Content-Type': 'application/json' },
    muteHttpExceptions: true
  });
  if (res.getResponseCode() !== 200) {
    Logger.log('supabaseGet_ failed (' + res.getResponseCode() + '): ' + res.getContentText().substring(0, 200));
    return null;
  }
  return JSON.parse(res.getContentText());
}

function supabasePatch_(table, filter, payload) {
  const res = UrlFetchApp.fetch(SUPABASE_URL + '/rest/v1/' + table + '?' + filter, {
    method: 'PATCH',
    headers: {
      'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type': 'application/json', 'Prefer': 'return=minimal'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const code = res.getResponseCode();
  if (code !== 200 && code !== 204) {
    Logger.log('supabasePatch_ error (' + code + '): ' + res.getContentText().substring(0, 300));
  }
  return code;
}

function supabasePost_(table, payload) {
  const res = UrlFetchApp.fetch(SUPABASE_URL + '/rest/v1/' + table, {
    method: 'POST',
    headers: {
      'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type': 'application/json', 'Prefer': 'return=minimal'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  return res.getResponseCode();
}

// ── LABEL HELPER ──────────────────────────────────────────────────────────────
function getOrCreateLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

// ── RETRY FAILED THREADS ──────────────────────────────────────────────────────
function retryFailed() {
  const failedLabel  = getOrCreateLabel_(GCIC_LABEL_FAILED);
  const pendingLabel = getOrCreateLabel_(GCIC_LABEL_PENDING);
  const threads = failedLabel.getThreads(0, 50);
  Logger.log('Retrying ' + threads.length + ' failed GCIC threads');
  threads.forEach(function(t) {
    t.removeLabel(failedLabel);
    t.addLabel(pendingLabel);
  });
  if (threads.length > 0) processGcicEmails();
}

// ── BACKLOG UTILS ─────────────────────────────────────────────────────────────
function labelGcicBacklog() {
  const pendingLabel = getOrCreateLabel_(GCIC_LABEL_PENDING);
  [
    'from:adobesign@adobesign.com subject:"GCIC" subject:"Signed and Filed" -label:GCIC/Pending -label:GCIC/Processed',
    'from:support-donotreply@fadv.com subject:"GCIC" subject:"SR#" -label:GCIC/Pending -label:GCIC/Processed'
  ].forEach(function(query) {
    const threads = GmailApp.search(query);
    Logger.log('Backlog [' + query.substring(0, 60) + ']: ' + threads.length + ' threads');
    threads.forEach(function(t) { t.addLabel(pendingLabel); });
  });
  Logger.log('GCIC backlog labeling complete');
}

function processSpecificThreads() {
  const threadIds = [
    '19d07946cf7cd597',
    '19cfce83984ab366',
    '19cfb6894864d8b4',
    '19cf360dd155a5e2',
    '19cf3553f7187f54'
  ];
  const processedLabel = getOrCreateLabel_(GCIC_LABEL_PROCESSED);
  const failedLabel    = getOrCreateLabel_(GCIC_LABEL_FAILED);
  let adobeOk = 0, skipped = 0, errors = 0;

  threadIds.forEach(function(threadId) {
    try {
      const thread = GmailApp.getThreadById(threadId);
      if (!thread) { Logger.log('Thread not found: ' + threadId); return; }
      let threadOk = true;

      thread.getMessages().forEach(function(message) {
        const from = message.getFrom();
        const subj = message.getSubject();
        Logger.log('Processing: [' + from + '] ' + subj);
        if (from.indexOf('adobesign@adobesign.com') !== -1 && subj.indexOf('is Signed and Filed') !== -1) {
          const ok = handleAdobeSign_(message);
          if (ok === true)        { adobeOk++; }
          else if (ok === 'skip') { skipped++; }
          else                    { errors++; threadOk = false; }
        } else {
          skipped++;
        }
      });

      const gcicPending = GmailApp.getUserLabelByName(GCIC_LABEL_PENDING);
      if (gcicPending) { try { thread.removeLabel(gcicPending); } catch(e) {} }
      thread.addLabel(threadOk ? processedLabel : failedLabel);
      thread.markRead();
    } catch(e) {
      Logger.log('ERROR on thread ' + threadId + ': ' + e.message);
      errors++;
    }
  });

  Logger.log('Specific threads -- Adobe: ' + adobeOk + ' | Skipped: ' + skipped + ' | Errors: ' + errors);
}
