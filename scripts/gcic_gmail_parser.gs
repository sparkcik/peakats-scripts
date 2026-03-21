/**
 * PEAKATS GCIC Gmail Parser
 * Processes two email types from the GCIC/Pending label:
 *   1. Adobe Sign "Signed and Filed" -- updates Supabase, uploads PDF to Drive,
 *      auto-sends to FADV, marks complete
 *   2. FADV SR# acknowledgment -- stores SR number, updates gcic_fadv_outcome
 *
 * Deploy: paste into Google Apps Script editor at script.google.com
 * Trigger: set time-driven trigger -> every 30 minutes
 */

// -- CONFIG (SUPABASE_URL, SUPABASE_KEY, LABEL_PENDING, LABEL_PROCESSED declared in Code.gs) --
const FADV_EMAIL      = 'casedocuments@fadv.com';
const CANDIDATE_DOCS_ROOT = '1UJfJM6ZMQo2RuVbNWrv4hkBiLnWAZkjB';

// -- MAIN ENTRY POINT ---------------------------------------------------------
function processGcicEmails() {
  const pendingLabel = GmailApp.getUserLabelByName(LABEL_PENDING);
  if (!pendingLabel) {
    Logger.log('Label not found: ' + LABEL_PENDING + ' -- create it in Gmail first');
    return;
  }

  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const threads = pendingLabel.getThreads(0, 50);

  Logger.log('Found ' + threads.length + ' GCIC threads to process');

  let adobeOk = 0, fadvOk = 0, skipped = 0, errors = 0;

  threads.forEach(function(thread) {
    const messages = thread.getMessages();
    let threadOk = true;

    messages.forEach(function(message) {
      try {
        Logger.log('MSG from: [' + message.getFrom() + '] subject: [' + message.getSubject() + ']');
        const from = message.getFrom();
        const subj = message.getSubject();

        // Skip non-actionable Adobe Sign emails
        if (from.indexOf('adobesign@adobesign.com') !== -1) {
          if (subj.indexOf('has been sent out for signature') !== -1 ||
              (subj.indexOf('web form') !== -1 && subj.indexOf('has been created') !== -1) ||
              subj.indexOf('You signed:') !== -1) {
            skipped++;
            return;
          }
          if (handleAdobeSign_(message)) adobeOk++;
          else skipped++;
        } else if (from.indexOf('support-donotreply@fadv.com') !== -1) {
          if (handleFadvSr_(message)) fadvOk++;
          else skipped++;
        } else {
          skipped++;
        }
      } catch(e) {
        Logger.log('ERROR processing GCIC message: ' + e.message);
        errors++;
        threadOk = false;
      }
    });

    if (threadOk) {
      thread.removeLabel(pendingLabel);
      thread.addLabel(processedLabel);
      const kaiInbox = GmailApp.getUserLabelByName('Kai/Inbox');
      if (kaiInbox) thread.removeLabel(kaiInbox);
      const peakInbox = GmailApp.getUserLabelByName('PEAK/Inbox');
      if (peakInbox) thread.removeLabel(peakInbox);
      thread.markRead();
      thread.moveToArchive();
    }
  });

  Logger.log('GCIC run complete -- Adobe: ' + adobeOk + ' | FADV SR: ' + fadvOk +
             ' | Skipped: ' + skipped + ' | Errors: ' + errors);
}

// -- HANDLER: Adobe Sign "Signed and Filed" -----------------------------------
function handleAdobeSign_(message) {
  const subject = message.getSubject();

  // Extract candidate name: "GCIC [Form] Acrobat eSig - SIGN ASAP between Kai M Clarke and {NAME} is Signed and Filed!"
  const nameMatch = subject.match(/\(?\s*between\s+Kai\s+M\s+Clarke\s+and\s+(.+?)\s+is Signed/i);
  if (!nameMatch) {
    Logger.log('Could not parse name from Adobe Sign subject: ' + subject);
    return false;
  }
  const fullName = nameMatch[1].trim();

  // Find candidate by name
  const candidate = findCandidateByName_(fullName);
  if (!candidate) {
    Logger.log('No candidate match for Adobe Sign: ' + fullName);
    return false;
  }

  // 1. Mark form completed
  supabasePatch_('candidates', 'id=eq.' + candidate.id, {
    gcic_form_completed: 1,
    gcic_form_completed_at: new Date().toISOString()
  });

  // 2. Get PDF attachment and upload to Drive
  const attachments = message.getAttachments();
  const pdf = findPdfAttachment_(attachments);
  if (!pdf) {
    Logger.log('No PDF attachment found for: ' + fullName);
    return false;
  }

  const dateStr = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const lastName = candidate.last_name || '';
  const firstName = candidate.first_name || '';
  const pdfName = dateStr + '_' + lastName + firstName + '_GCIC.pdf';

  const clientFolder = getOrCreateClientFolder_(candidate.client_id);
  clientFolder.createFile(pdf.copyBlob().setName(pdfName));

  supabasePatch_('candidates', 'id=eq.' + candidate.id, {
    gcic_uploaded: 1
  });

  // 3. Auto-send to FADV
  const clientDisplay = formatClientName_(candidate.client_id);
  const drugTestId = candidate.drug_test_id || 'N/A';

  const emailSubject = firstName + ' ' + lastName + ' GCIC Form - ' + clientDisplay + ' - Order ID: ' + drugTestId;
  const emailBody = 'Please find attached the executed GCIC authorization form for the above-referenced candidate. ' +
    'This document was electronically signed via Adobe Acrobat Sign and includes IP capture per GCIC requirements.\n\n' +
    'Candidate: ' + firstName + ' ' + lastName + '\n' +
    'Client: ' + clientDisplay + '\n' +
    'Drug Test Order ID: ' + drugTestId + '\n\n' +
    'Please confirm receipt.\n\n' +
    'Kai Clarke\n' +
    'PEAKrecruiting\n' +
    'Questions? (470) 857-4325';

  GmailApp.sendEmail(FADV_EMAIL, emailSubject, emailBody, {
    attachments: [pdf.copyBlob().setName(pdfName)]
  });

  supabasePatch_('candidates', 'id=eq.' + candidate.id, {
    gcic_email_sent: 1
  });

  Logger.log('Adobe Sign processed: ' + fullName + ' -> uploaded + emailed FADV');
  return true;
}

// -- HANDLER: FADV SR# acknowledgment -----------------------------------------
function handleFadvSr_(message) {
  const subject = message.getSubject();

  // Pattern: "RE: {FIRST LAST} GCIC Form - {CLIENT} - Order ID: {ORDER_ID} SR#:{SR_NUM}."
  const srMatch = subject.match(/RE:\s*(.+?)\s+GCIC Form\s*-\s*.+?-\s*Order ID:\s*(\S+)\s+SR#:(\S+)/i);
  if (!srMatch) {
    Logger.log('Could not parse FADV SR subject: ' + subject);
    return false;
  }

  const fullName  = srMatch[1].trim();
  const orderId   = srMatch[2].trim().replace(/\.$/, '');
  const srNumber  = srMatch[3].trim().replace(/\.$/, '');

  // Try drug_test_id match first, then name fallback
  var candidate = findCandidateByDrugTestId_(orderId);
  if (!candidate) {
    candidate = findCandidateByName_(fullName);
  }
  if (!candidate) {
    Logger.log('No candidate match for FADV SR: ' + fullName + ' / Order ID: ' + orderId);
    return false;
  }

  // Update outcome + SR number
  const patch = {
    gcic_fadv_outcome: 'Received',
    gcic_sr_number: srNumber
  };

  // Only set submitted timestamp if not already set
  if (!candidate.gcic_submitted_to_fadv_at) {
    patch.gcic_submitted_to_fadv_at = new Date().toISOString();
  }

  // Append SR# to fadv_notes
  const existingNotes = candidate.fadv_notes || '';
  if (existingNotes.indexOf('GCIC SR#') === -1) {
    patch.fadv_notes = existingNotes
      ? existingNotes + '\nGCIC SR#: ' + srNumber
      : 'GCIC SR#: ' + srNumber;
  }

  supabasePatch_('candidates', 'id=eq.' + candidate.id, patch);

  Logger.log('FADV SR processed: ' + fullName + ' -> SR#: ' + srNumber);
  return true;
}

// -- CANDIDATE LOOKUP ---------------------------------------------------------
function findCandidateByName_(fullName) {
  const parts = fullName.trim().split(/\s+/);
  if (parts.length < 2) return null;

  const firstName = parts[0];
  const lastName  = parts.slice(1).join(' ');

  const res = supabaseGet_('candidates', {
    select: 'id,first_name,last_name,drug_test_id,client_id,gcic_form_completed,gcic_uploaded,gcic_email_sent,gcic_submitted_to_fadv_at,gcic_sr_number,fadv_notes',
    'first_name': 'ilike.' + firstName,
    'last_name':  'ilike.' + lastName,
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

// -- DRIVE HELPERS ------------------------------------------------------------
function getOrCreateClientFolder_(clientId) {
  const root = DriveApp.getFolderById(CANDIDATE_DOCS_ROOT);
  const folders = root.getFoldersByName(clientId);
  if (folders.hasNext()) return folders.next();
  return root.createFolder(clientId);
}

function findPdfAttachment_(attachments) {
  for (var i = 0; i < attachments.length; i++) {
    if (attachments[i].getContentType() === 'application/pdf') {
      return attachments[i];
    }
  }
  return null;
}

// -- DISPLAY HELPERS ----------------------------------------------------------
function formatClientName_(clientId) {
  if (!clientId) return 'Unknown';
  return clientId.split('_').map(function(word) {
    return word.charAt(0).toUpperCase() + word.slice(1);
  }).join(' ');
}

// -- SUPABASE HELPERS ---------------------------------------------------------
function supabaseGet_(table, params) {
  const qs = Object.entries(params).map(function(entry) {
    return entry[0] + '=' + encodeURIComponent(entry[1]);
  }).join('&');
  const url = SUPABASE_URL + '/rest/v1/' + table + '?' + qs;
  const res = UrlFetchApp.fetch(url, {
    method: 'GET',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type': 'application/json'
    },
    muteHttpExceptions: true
  });
  if (res.getResponseCode() !== 200) return null;
  return JSON.parse(res.getContentText());
}

function supabasePatch_(table, filter, payload) {
  const url = SUPABASE_URL + '/rest/v1/' + table + '?' + filter;
  UrlFetchApp.fetch(url, {
    method: 'PATCH',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
}

function supabasePost_(table, payload) {
  const url = SUPABASE_URL + '/rest/v1/' + table;
  UrlFetchApp.fetch(url, {
    method: 'POST',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
}

// -- LABEL HELPER -------------------------------------------------------------
function getOrCreateLabel_(labelName) {
  var label = GmailApp.getUserLabelByName(labelName);
  if (!label) label = GmailApp.createLabel(labelName);
  return label;
}

// -- PROCESS SPECIFIC THREADS (one-time manual run) ---------------------------
function processSpecificThreads() {
  const threadIds = [
    '19d07946cf7cd597',  // Derek Taplet
    '19d07656f54a9bb5',  // Kai M Clarke (test - skip)
    '19cfce83984ab366',  // Wayne Rucker
    '19cfb6894864d8b4',  // Marvin Bagwell
    '19cf360dd155a5e2',  // Michael Cobb
    '19cf3553f7187f54'   // Jakireya Norman
  ];

  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const pendingLabel = GmailApp.getUserLabelByName(LABEL_PENDING);

  var adobeOk = 0, skipped = 0, errors = 0;

  threadIds.forEach(function(threadId) {
    try {
      var thread = GmailApp.getThreadById(threadId);
      if (!thread) { Logger.log('Thread not found: ' + threadId); return; }

      var messages = thread.getMessages();

      messages.forEach(function(message) {
        Logger.log('Processing: [' + message.getFrom() + '] ' + message.getSubject());
        var from = message.getFrom();
        var subj = message.getSubject();

        // Skip "Kai M Clarke and Kai M Clarke" (test document)
        if (subj.indexOf('Kai M  Clarke and Kai M  Clarke') !== -1 ||
            subj.indexOf('Kai M Clarke and Kai M Clarke') !== -1) {
          Logger.log('Skipping test document: ' + subj);
          skipped++;
          return;
        }

        if (from.indexOf('adobesign@adobesign.com') !== -1 &&
            subj.indexOf('is Signed and Filed') !== -1) {
          if (handleAdobeSign_(message)) adobeOk++;
          else { Logger.log('handleAdobeSign_ returned false'); skipped++; }
        } else {
          skipped++;
        }
      });

      // Move thread regardless
      if (pendingLabel) thread.removeLabel(pendingLabel);
      thread.addLabel(processedLabel);
      var kaiInbox = GmailApp.getUserLabelByName('Kai/Inbox');
      if (kaiInbox) thread.removeLabel(kaiInbox);
      var peakInbox = GmailApp.getUserLabelByName('PEAK/Inbox');
      if (peakInbox) thread.removeLabel(peakInbox);
      thread.markRead();
      thread.moveToArchive();

    } catch(e) {
      Logger.log('ERROR on thread ' + threadId + ': ' + e.message);
      errors++;
    }
  });

  Logger.log('Specific threads complete -- Adobe: ' + adobeOk + ' | Skipped: ' + skipped + ' | Errors: ' + errors);
}

// -- ONE-TIME BACKLOG LABELER -------------------------------------------------
// Run ONCE manually to catch existing Adobe Sign + FADV SR emails
function labelGcicBacklog() {
  const pendingLabel = getOrCreateLabel_(LABEL_PENDING);

  // Adobe Sign signed-and-filed emails
  const adobeThreads = GmailApp.search(
    'from:adobesign@adobesign.com subject:"GCIC Form" subject:"Signed and Filed" -label:GCIC/Pending -label:GCIC/Processed'
  );
  Logger.log('Adobe Sign backlog threads: ' + adobeThreads.length);
  adobeThreads.forEach(function(thread) {
    thread.addLabel(pendingLabel);
  });

  // FADV SR# acknowledgment emails
  const fadvThreads = GmailApp.search(
    'from:support-donotreply@fadv.com subject:"GCIC Form" subject:"SR#" -label:GCIC/Pending -label:GCIC/Processed'
  );
  Logger.log('FADV SR backlog threads: ' + fadvThreads.length);
  fadvThreads.forEach(function(thread) {
    thread.addLabel(pendingLabel);
  });

  Logger.log('GCIC backlog labeling complete');
}
