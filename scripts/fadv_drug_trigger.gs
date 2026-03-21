/**
 * PEAKATS FADV Drug Screen Trigger
 * Runs on a 15-minute trigger. Detects incoming FADV drug screen emails,
 * extracts candidate info, updates Supabase drug_test_status, queues SMS
 * via sms_send_queue for immediate outreach with barcode.
 *
 * Gmail source: FADVReports-NoReply@fadv.com subject:"Screening Notification"
 * Deploy: paste into Google Apps Script editor at script.google.com
 * Trigger: set time-driven trigger -> every 15 minutes on processNewDrugScreenEmails
 */

// -- CONFIG ------------------------------------------------------------------
const SUPABASE_URL = 'https://eyopvsmsvbgfuffscfom.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4';
const DRUG_ORDERS_LABEL = 'FADV/Drug-Orders';
const PROCESSED_LABEL   = 'FADV/Processed';
const TEMPLATE_ID       = 47;
const TEMPLATE_NAME     = 'Drug Screen Ordered -- Immediate Outreach';
const FROM_NUMBER       = '+14704704766';

// Template 47 body -- [FIRST] and [BARCODE] replaced at send time
const TEMPLATE_BODY = 'Hi [FIRST], this is Kai from PEAK Recruiting. '
  + 'Your drug screen has been ordered. Please visit any Quest Diagnostics location '
  + 'within 24 hours. Your barcode is: [BARCODE]. '
  + 'Show this barcode at the front desk -- no appointment needed. '
  + 'Reply DONE when complete or call us with any questions.';

// -- SUPABASE HELPERS --------------------------------------------------------
const SB_HEADERS = {
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
function processNewDrugScreenEmails() {
  var query = 'from:FADVReports-NoReply@fadv.com subject:"Screening Notification" -label:FADV/Processed';
  var threads = GmailApp.search(query, 0, 50);

  if (threads.length === 0) {
    Logger.log('[Drug Trigger] No new drug screen emails found.');
    return;
  }

  var drugOrdersLabel = getOrCreateLabel_(DRUG_ORDERS_LABEL);
  var processedLabel  = getOrCreateLabel_(PROCESSED_LABEL);

  Logger.log('[Drug Trigger] Found ' + threads.length + ' thread(s) to process.');

  for (var t = 0; t < threads.length; t++) {
    var messages = threads[t].getMessages();
    for (var m = 0; m < messages.length; m++) {
      var msg = messages[m];
      var body = msg.getPlainBody() || msg.getBody();
      var subject = msg.getSubject();

      try {
        processOneEmail_(body, subject);
      } catch (e) {
        Logger.log('[Drug Trigger] Error processing email "' + subject + '": ' + e.message);
      }
    }
    // Label the thread
    threads[t].addLabel(drugOrdersLabel);
    threads[t].addLabel(processedLabel);
  }

  Logger.log('[Drug Trigger] Done. Processed ' + threads.length + ' thread(s).');
}

// -- EXTRACT BARCODE ---------------------------------------------------------
function extractBarcode_(body) {
  // Authorization # appears in ALL FADV drug screen emails -- primary pattern
  var authMatch = body.match(/Authorization\s*#\s*[\r\n\s]*(\d+)/i);
  if (authMatch) return authMatch[1].trim();

  // FormFox fallback (same number, belt-and-suspenders)
  var foxMatch = body.match(/FormFox Web COC Order Registration Number:\s*(\d+)/i);
  if (foxMatch) return foxMatch[1].trim();

  // Quest QPassport fallback (legacy format, rare)
  var questMatch = body.match(/Quest QPassport\/Barcode #:\s*(\S+)/i);
  if (questMatch) return questMatch[1].trim();

  return 'N/A';
}

// -- EXTRACT CANDIDATE NAME --------------------------------------------------
function extractCandidateName_(body) {
  // Primary: Applicant Name: line (always present)
  var nameMatch = body.match(/Applicant Name:\s*([A-Z\s\-'\.]+?)(?:\r|\n)/i);
  if (nameMatch) return nameMatch[1].trim();

  // Fallback: extract from subject line passed as parameter
  return null;
}

// -- PROCESS SINGLE EMAIL ----------------------------------------------------
function processOneEmail_(body, subject) {
  // Extract candidate name
  var fullName = extractCandidateName_(body);
  if (!fullName) {
    Logger.log('[Drug Trigger] No applicant name found in email.');
    return;
  }

  // Extract barcode (try multiple formats)
  var barcode = extractBarcode_(body);

  // Extract account/client hint
  var accountMatch = body.match(/Account:\s*(?:FXG|FEC)\s+VENDOR\s+(\S+)\s*\(/i);
  var clientHint = accountMatch ? accountMatch[1].trim() : 'unknown';

  // Split name into first/last
  var nameParts = fullName.split(/\s+/);
  var firstName = nameParts[0] || '';
  var lastName  = nameParts.length > 1 ? nameParts.slice(1).join(' ') : '';

  Logger.log('[Drug Trigger] Candidate: ' + fullName + ' | Barcode: ' + barcode + ' | Client hint: ' + clientHint);

  // Match candidate in Supabase
  var candidate = matchCandidate(firstName, lastName);

  if (candidate) {
    Logger.log('[Drug Trigger] Matched candidate ID ' + candidate.id + ' (' + candidate.first_name + ' ' + candidate.last_name + ')');
    updateCandidateDrugStatus(candidate.id);
    queueSMS(candidate.id, candidate.phone, candidate.first_name, barcode);
  } else {
    Logger.log('[Drug Trigger] No match for: ' + fullName);
    logUnmatched(fullName, barcode, subject);
  }
}

// -- MATCH CANDIDATE ---------------------------------------------------------
function matchCandidate(firstName, lastName) {
  // Exact first + last match only -- no first-name-only fallback (too loose, causes false positives)
  var results = sbGet_('candidates', {
    'select': 'id,phone,first_name,last_name,client_id',
    'first_name': 'ilike.' + firstName,
    'last_name': 'ilike.' + lastName,
    'status': 'not.in.(Rejected,Hired,Transferred)',
    'limit': '5'
  });

  if (results && results.length === 1) {
    return results[0];
  }

  if (results && results.length > 1) {
    Logger.log('[Drug Trigger] WARNING: Multiple matches for ' + firstName + ' ' + lastName + ' -- using first result');
    return results[0];
  }

  return null; // No match -- caller will logUnmatched
}

// -- UPDATE DRUG STATUS ------------------------------------------------------
function updateCandidateDrugStatus(candidateId) {
  sbPatch_('candidates', { 'id': 'eq.' + candidateId }, {
    'drug_test_status': 'In Progress',
    'updated_at': new Date().toISOString()
  });
  Logger.log('[Drug Trigger] Updated candidate ' + candidateId + ' -> drug_test_status = In Progress');
}

// -- QUEUE SMS ---------------------------------------------------------------
function queueSMS(candidateId, phone, firstName, barcode) {
  // Validate phone
  if (!phone || phone === '' || phone === '0000000000') {
    Logger.log('[Drug Trigger] Skipping SMS for candidate ' + candidateId + ' -- no valid phone.');
    return;
  }

  var body = TEMPLATE_BODY
    .replace('[FIRST]', firstName)
    .replace('[BARCODE]', barcode);

  sbInsert_('sms_send_queue', {
    'candidate_id': candidateId,
    'to_number': phone,
    'from_number': FROM_NUMBER,
    'body': body,
    'template_id': TEMPLATE_ID,
    'template_name': TEMPLATE_NAME,
    'status': 'pending',
    'scheduled_for': new Date().toISOString(),
    'created_by': 'fadv_drug_trigger'
  });

  Logger.log('[Drug Trigger] Queued SMS for candidate ' + candidateId + ' to ' + phone);
}

// -- LOG UNMATCHED -----------------------------------------------------------
function logUnmatched(fullName, barcode, subject) {
  var today = Utilities.formatDate(new Date(), 'America/New_York', 'yyyy-MM-dd');

  sbInsert_('forge_memory', {
    'session_date': today,
    'category': 'ops_note',
    'subject': 'FADV Drug Screen -- Unmatched: ' + fullName,
    'content': 'Drug screen email received but no candidate match found. '
      + 'Barcode: ' + barcode + (barcode === 'N/A' ? ' (extraction failed -- check original email for real barcode). ' : '. ')
      + 'Email subject: ' + subject + '. '
      + 'Manual match required in PEAKATS.',
    'target_thread': 'PEAK Ops',
    'tags': ['drug_screen', 'unmatched', 'manual_review']
  });

  Logger.log('[Drug Trigger] Logged unmatched candidate: ' + fullName);
}
