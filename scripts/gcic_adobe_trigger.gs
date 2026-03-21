/**
 * PEAKATS GCIC Adobe Sign Trigger
 * Runs on a 15-minute trigger. Detects Adobe Sign "Signed and Filed" emails
 * for GCIC forms, extracts signed PDF, saves to Drive, emails casedocuments@fadv.com,
 * updates Supabase with stage/status/SR fields.
 *
 * Gmail source: adobesign@adobesign.com subject:"Signed and Filed"
 * Deploy: paste into Google Apps Script editor at script.google.com
 * Trigger: set time-driven trigger -> every 15 minutes on processSignedGCICEmails
 */

// -- CONFIG ------------------------------------------------------------------
var SUPABASE_URL = 'https://eyopvsmsvbgfuffscfom.supabase.co';
var SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4';
var FADV_EMAIL = 'casedocuments@fadv.com';
var GCIC_PROCESSED_LABEL = 'GCIC/Processed';
var GCIC_DOCS_ROOT = '1UJfJM6ZMQo2RuVbNWrv4hkBiLnWAZkjB';
var FROM_NUMBER = '+14704704766';

// -- SUPABASE HELPERS --------------------------------------------------------
var SB_HEADERS = {
  'apikey': SUPABASE_KEY,
  'Authorization': 'Bearer ' + SUPABASE_KEY,
  'Content-Type': 'application/json',
  'Prefer': 'return=minimal'
};

function sbGet_(path, params) {
  var qs = Object.keys(params || {}).map(function(k) {
    return encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
  }).join('&');
  var url = SUPABASE_URL + '/rest/v1/' + path + (qs ? '?' + qs : '');
  var resp = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: SB_HEADERS,
    muteHttpExceptions: true
  });
  return JSON.parse(resp.getContentText());
}

function sbPatch_(path, params, body) {
  var qs = Object.keys(params || {}).map(function(k) {
    return encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
  }).join('&');
  var url = SUPABASE_URL + '/rest/v1/' + path + (qs ? '?' + qs : '');
  UrlFetchApp.fetch(url, {
    method: 'patch',
    headers: SB_HEADERS,
    payload: JSON.stringify(body),
    muteHttpExceptions: true
  });
}

function sbInsert_(path, body) {
  var url = SUPABASE_URL + '/rest/v1/' + path;
  UrlFetchApp.fetch(url, {
    method: 'post',
    headers: SB_HEADERS,
    payload: JSON.stringify(body),
    muteHttpExceptions: true
  });
}

// -- GMAIL HELPERS -----------------------------------------------------------
function getOrCreateLabel_(name) {
  var label = GmailApp.getUserLabelByName(name);
  if (!label) {
    label = GmailApp.createLabel(name);
  }
  return label;
}

// -- MAIN ENTRY POINT --------------------------------------------------------
function processSignedGCICEmails() {
  var query = 'from:adobesign@adobesign.com subject:"Signed and Filed" -label:GCIC/Processed';
  var threads = GmailApp.search(query, 0, 50);

  if (threads.length === 0) {
    Logger.log('[GCIC Trigger] No new signed GCIC emails found.');
    return;
  }

  var processedLabel = getOrCreateLabel_(GCIC_PROCESSED_LABEL);

  Logger.log('[GCIC Trigger] Found ' + threads.length + ' thread(s) to process.');

  for (var t = 0; t < threads.length; t++) {
    var messages = threads[t].getMessages();
    for (var m = 0; m < messages.length; m++) {
      var msg = messages[m];
      var subject = msg.getSubject();

      // Skip test emails (Kai signing with himself)
      if (subject.indexOf('Kai M Clarke and Kai M Clarke') !== -1) {
        Logger.log('[GCIC Trigger] Skipping test email: ' + subject);
        continue;
      }

      try {
        processOneGCICEmail_(msg, subject);
      } catch (e) {
        Logger.log('[GCIC Trigger] Error processing "' + subject + '": ' + e.message);
      }
    }
    threads[t].addLabel(processedLabel);
  }

  Logger.log('[GCIC Trigger] Done. Processed ' + threads.length + ' thread(s).');
}

// -- PROCESS SINGLE EMAIL ----------------------------------------------------
function processOneGCICEmail_(msg, subject) {
  // Extract candidate name from subject
  // Patterns: "... between Kai M Clarke and [NAME] is Signed and Filed!"
  var nameMatch = subject.match(/and\s+(.+?)\s+is\s+Signed/i);
  if (!nameMatch) {
    Logger.log('[GCIC Trigger] Could not extract name from subject: ' + subject);
    return;
  }
  var fullName = nameMatch[1].trim();

  // Extract order ID if present
  var orderMatch = subject.match(/Order ID:\s*(\d+)/i);
  var orderId = orderMatch ? orderMatch[1] : null;

  // Get PDF attachment
  var attachments = msg.getAttachments();
  var pdfBlob = null;
  for (var i = 0; i < attachments.length; i++) {
    if (attachments[i].getContentType() === 'application/pdf') {
      pdfBlob = attachments[i];
      break;
    }
  }

  if (!pdfBlob) {
    Logger.log('[GCIC Trigger] No PDF attachment found for: ' + fullName);
    return;
  }

  // Split name
  var nameParts = fullName.split(/\s+/);
  var firstName = nameParts[0] || '';
  var lastName = nameParts.length > 1 ? nameParts.slice(1).join(' ') : '';

  Logger.log('[GCIC Trigger] Candidate: ' + fullName + ' | Order ID: ' + (orderId || 'N/A'));

  // Match candidate
  var candidate = matchCandidate_(firstName, lastName);

  if (candidate) {
    Logger.log('[GCIC Trigger] Matched candidate ID ' + candidate.id + ' (' + candidate.first_name + ' ' + candidate.last_name + ')');
    saveGCICToDrive_(pdfBlob, candidate.client_id, candidate.first_name, candidate.last_name);
    sendToFADV_(pdfBlob, candidate.first_name, candidate.last_name, candidate.background_id, orderId);
    updateSupabase_(candidate.id, orderId);
  } else {
    Logger.log('[GCIC Trigger] No match for: ' + fullName);
    logUnmatched_(fullName, orderId, subject);
  }
}

// -- MATCH CANDIDATE ---------------------------------------------------------
function matchCandidate_(firstName, lastName) {
  // Exact first + last match only -- no fallback
  var results = sbGet_('candidates', {
    'select': 'id,phone,first_name,last_name,client_id,background_id',
    'first_name': 'ilike.' + firstName,
    'last_name': 'ilike.' + lastName,
    'status': 'not.in.(Rejected,Hired,Transferred)',
    'limit': '5'
  });

  if (results && results.length === 1) {
    return results[0];
  }

  if (results && results.length > 1) {
    Logger.log('[GCIC Trigger] WARNING: Multiple matches for ' + firstName + ' ' + lastName + ' -- using first result');
    return results[0];
  }

  return null;
}

// -- SAVE TO DRIVE -----------------------------------------------------------
function saveGCICToDrive_(pdfBlob, clientId, firstName, lastName) {
  try {
    var root = DriveApp.getFolderById(GCIC_DOCS_ROOT);

    // Get or create client subfolder
    var clientFolder;
    var clientFolders = root.getFoldersByName(clientId || 'unknown');
    if (clientFolders.hasNext()) {
      clientFolder = clientFolders.next();
    } else {
      clientFolder = root.createFolder(clientId || 'unknown');
    }

    // Get or create gcic subfolder
    var gcicFolder;
    var gcicFolders = clientFolder.getFoldersByName('gcic');
    if (gcicFolders.hasNext()) {
      gcicFolder = gcicFolders.next();
    } else {
      gcicFolder = clientFolder.createFolder('gcic');
    }

    // Build filename: YYYY-MM-DD_LastFirst_GCIC.pdf
    var today = Utilities.formatDate(new Date(), 'America/New_York', 'yyyy-MM-dd');
    var cleanFirst = (firstName || '').replace(/[^a-zA-Z]/g, '');
    var cleanLast = (lastName || '').replace(/[^a-zA-Z]/g, '');
    var filename = today + '_' + cleanLast + cleanFirst + '_GCIC.pdf';

    pdfBlob.setName(filename);
    gcicFolder.createFile(pdfBlob);
    Logger.log('[GCIC Trigger] Saved to Drive: ' + clientId + '/gcic/' + filename);
  } catch (e) {
    Logger.log('[GCIC Trigger] Drive save failed: ' + e.message);
  }
}

// -- SEND TO FADV ------------------------------------------------------------
function sendToFADV_(pdfBlob, firstName, lastName, backgroundId, orderId) {
  try {
    var refId = orderId || backgroundId || 'Pending';
    var emailSubject = firstName + ' ' + lastName + ' GCIC Form - Order ID: ' + refId;
    var emailBody = 'Please find attached the executed GCIC authorization form for the above-referenced candidate. '
      + 'This document was electronically signed via Adobe Acrobat Sign and includes IP capture per GCIC requirements.\n\n'
      + 'Kai\nPEAKrecruiting\nQuestions? (470) 857-4325';

    GmailApp.sendEmail(FADV_EMAIL, emailSubject, emailBody, {
      attachments: [pdfBlob],
      name: 'Kai - PEAK Recruiting'
    });
    Logger.log('[GCIC Trigger] Emailed FADV: ' + emailSubject);
  } catch (e) {
    Logger.log('[GCIC Trigger] FADV email failed: ' + e.message);
  }
}

// -- UPDATE SUPABASE ---------------------------------------------------------
function updateSupabase_(candidateId, orderId) {
  var now = new Date().toISOString();
  var patch = {
    'gcic_stage': 'SUBMITTED_TO_FADV',
    'gcic_status': 'COMPLETE',
    'gcic_submitted_to_fadv_at': now,
    'updated_at': now
  };
  if (orderId) {
    patch['gcic_fadv_order_id'] = orderId;
  }

  sbPatch_('candidates', { 'id': 'eq.' + candidateId }, patch);
  Logger.log('[GCIC Trigger] Updated candidate ' + candidateId + ' -> gcic_stage=SUBMITTED_TO_FADV, gcic_status=COMPLETE');
}

// -- LOG UNMATCHED -----------------------------------------------------------
function logUnmatched_(fullName, orderId, subject) {
  var today = Utilities.formatDate(new Date(), 'America/New_York', 'yyyy-MM-dd');

  sbInsert_('forge_memory', {
    'session_date': today,
    'category': 'ops_note',
    'subject': 'GCIC Signed -- Unmatched: ' + fullName,
    'content': 'Adobe Sign GCIC signed but no candidate match found. '
      + 'Order ID: ' + (orderId || 'N/A') + '. '
      + 'Email subject: ' + subject + '. '
      + 'Manual match required.',
    'target_thread': 'PEAK Ops',
    'tags': ['gcic', 'unmatched', 'manual_review']
  });

  Logger.log('[GCIC Trigger] Logged unmatched: ' + fullName);
}
