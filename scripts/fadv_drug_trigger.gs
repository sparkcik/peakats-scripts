/**
 * PEAKATS FADV Drug Screen Trigger -- HARDENED v2
 * Fixed: silent write failures, no-alert on error, thread archived on error,
 *        RC from_number, missing migration_status, label-based processing.
 *
 * Handles: FADVReports-NoReply@fadv.com "Screening Notification" emails
 * These are drug screen ORDER notifications -- sets In Progress, sends barcode SMS.
 * Separate from FADV Parser which handles results (EntAdv.DoNotReply@fadv.com).
 */

// ── CONFIG ────────────────────────────────────────────────────────────────────
const SUPABASE_URL      = 'https://eyopvsmsvbgfuffscfom.supabase.co';
const SUPABASE_KEY      = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4';
const ALERT_EMAIL       = 'kai@peakrecruitingco.com';
const DRUG_PENDING_LABEL    = 'FADV/Drug-Orders';
const DRUG_PROCESSED_LABEL  = 'FADV/Processed';
const DRUG_FAILED_LABEL     = 'FADV/Failed';
const TEMPLATE_ID       = 47;
const TEMPLATE_NAME     = 'Drug Screen Ordered -- Immediate Outreach';
const FROM_NUMBER       = '+14704704766'; // FIX: Twilio shadow number (was RC +14708574325)

const TEMPLATE_BODY =
  '[FIRST], your drug screen has been ordered. ' +
  'Check your email from First Advantage -- it has your clinic locations nearby.\n\n' +
  'Your barcode: [BARCODE]\n\n' +
  'I need you to go within the next 24 hours. Your position depends on it ' +
  'and I want to keep it held for you.\n\n' +
  'Reply here to confirm you are going today, or let me know if you need ' +
  'a little more time -- I will do my best to work with you.\n\n' +
  'Kai\nPEAKrecruiting';

// ── SUPABASE HELPERS ──────────────────────────────────────────────────────────
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
    method: 'get', headers: SB_HEADERS, muteHttpExceptions: true
  });
  if (resp.getResponseCode() !== 200) {
    Logger.log('sbGet_ failed (' + resp.getResponseCode() + '): ' + resp.getContentText().substring(0, 200));
    return null;
  }
  return JSON.parse(resp.getContentText());
}

// FIX: returns response code -- caller must check
function sbPatch_(path, params, body) {
  var qs = Object.keys(params || {}).map(function(k) {
    return encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
  }).join('&');
  var url = SUPABASE_URL + '/rest/v1/' + path + (qs ? '?' + qs : '');
  var resp = UrlFetchApp.fetch(url, {
    method: 'patch', headers: SB_HEADERS,
    payload: JSON.stringify(body), muteHttpExceptions: true
  });
  var code = resp.getResponseCode();
  if (code !== 200 && code !== 204) {
    Logger.log('sbPatch_ error (' + code + '): ' + resp.getContentText().substring(0, 300));
  }
  return code;
}

// FIX: returns response code -- caller must check
function sbInsert_(path, body) {
  var url = SUPABASE_URL + '/rest/v1/' + path;
  var resp = UrlFetchApp.fetch(url, {
    method: 'post', headers: SB_HEADERS,
    payload: JSON.stringify(body), muteHttpExceptions: true
  });
  var code = resp.getResponseCode();
  if (code !== 200 && code !== 201 && code !== 204) {
    Logger.log('sbInsert_ error (' + code + '): ' + resp.getContentText().substring(0, 300));
  }
  return code;
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

// ── LABEL HELPER ─────────────────────────────────────────────────────────────
function getOrCreateLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

// ── MAIN ENTRY POINT ─────────────────────────────────────────────────────────
function processNewDrugScreenEmails() {
  // FIX: label-based processing -- move to Drug-Orders first, then process
  // This ensures retry is possible via retryFailed()
  var incomingThreads = GmailApp.search(
    'from:FADVReports-NoReply@fadv.com subject:"Screening Notification" -label:FADV/Drug-Orders -label:FADV/Processed',
    0, 50
  );

  var drugOrdersLabel = getOrCreateLabel_(DRUG_PENDING_LABEL);
  incomingThreads.forEach(function(t) { t.addLabel(drugOrdersLabel); });

  // Now process everything in Drug-Orders
  var threads = drugOrdersLabel.getThreads(0, 50);

  if (threads.length === 0) {
    Logger.log('[Drug Trigger] No threads to process.');
    return;
  }

  var processedLabel = getOrCreateLabel_(DRUG_PROCESSED_LABEL);
  var failedLabel    = getOrCreateLabel_(DRUG_FAILED_LABEL);

  Logger.log('[Drug Trigger] Found ' + threads.length + ' thread(s) to process.');
  var processed = 0, failed = 0, skipped = 0;

  threads.forEach(function(thread) {
    var messages   = thread.getMessages();
    var threadOk   = true;

    messages.forEach(function(message) {
      try {
        var result = processOneEmail_(message);
        if (result === true)         processed++;
        else if (result === 'skip')  skipped++;
        else { failed++; threadOk = false; }
      } catch(e) {
        Logger.log('[Drug Trigger] EXCEPTION: ' + e.message);
        failed++;
        threadOk = false;
      }
    });

    // FIX: only archive to Processed if no errors
    thread.removeLabel(drugOrdersLabel);
    if (threadOk) {
      thread.addLabel(processedLabel);
      thread.markRead();
      thread.moveToArchive();
    } else {
      thread.addLabel(failedLabel);
      Logger.log('[Drug Trigger] Thread moved to FADV/Failed: ' + thread.getFirstMessageSubject());
      alertForge_('Drug Trigger failure',
        'Thread moved to FADV/Failed: ' + thread.getFirstMessageSubject() +
        '\nRe-label to FADV/Drug-Orders to retry.');
    }
  });

  Logger.log('[Drug Trigger] Done -- Processed: ' + processed + ' | Skipped: ' + skipped + ' | Failed: ' + failed);
}

// ── PROCESS SINGLE EMAIL ──────────────────────────────────────────────────────
// Returns: true = success | 'skip' = intentional | false = error
function processOneEmail_(message) {
  var body    = message.getPlainBody() || message.getBody();
  var subject = message.getSubject();

  var fullName = extractCandidateName_(body);
  if (!fullName) {
    Logger.log('[Drug Trigger] No applicant name found: ' + subject);
    return 'skip';
  }

  var barcode     = extractBarcode_(body);
  var nameParts   = fullName.split(/\s+/);
  var firstName   = nameParts[0] || '';
  var lastName    = nameParts.length > 1 ? nameParts.slice(1).join(' ') : '';

  Logger.log('[Drug Trigger] Candidate: ' + fullName + ' | Barcode: ' + barcode);

  var candidate = matchCandidate_(firstName, lastName);

  if (!candidate) {
    Logger.log('[Drug Trigger] No match for: ' + fullName);
    logUnmatched_(fullName, barcode, subject);
    return 'skip'; // Unmatched = skip not error -- logged to forge_memory for manual review
  }

  Logger.log('[Drug Trigger] Matched: ' + candidate.id + ' (' + candidate.first_name + ' ' + candidate.last_name + ')');

  // FIX: check write response before proceeding
  var patchCode = updateCandidateDrugStatus_(candidate.id);
  if (patchCode !== 200 && patchCode !== 204) {
    alertForge_('Drug Trigger DB write failed: ' + fullName,
      'Candidate ID ' + candidate.id + ' drug_test_status NOT updated.\n' +
      'Barcode: ' + barcode + '\nManual fix required in Supabase.');
    return false;
  }

  // FIX: check SMS queue insert response
  var smsCode = queueSMS_(candidate.id, candidate.phone, candidate.first_name, barcode);
  if (smsCode !== 200 && smsCode !== 201 && smsCode !== 204) {
    Logger.log('[Drug Trigger] SMS queue insert failed (' + smsCode + ') for candidate ' + candidate.id);
    alertForge_('Drug Trigger SMS queue failed: ' + fullName,
      'DB updated but SMS not queued. Candidate ID ' + candidate.id +
      '\nBarcode: ' + barcode + '\nPhone: ' + candidate.phone +
      '\nSend manually via PWA.');
    // DB was updated -- don't fail the thread, just alert
  }

  return true;
}

// ── EXTRACT BARCODE ───────────────────────────────────────────────────────────
function extractBarcode_(body) {
  var authMatch = body.match(/Authorization\s*#\s*[\r\n\s]*(\d+)/i);
  if (authMatch) return authMatch[1].trim();

  var foxMatch = body.match(/FormFox Web COC Order Registration Number:\s*(\d+)/i);
  if (foxMatch) return foxMatch[1].trim();

  var questMatch = body.match(/Quest QPassport\/Barcode #:\s*(\S+)/i);
  if (questMatch) return questMatch[1].trim();

  return 'N/A';
}

// ── EXTRACT CANDIDATE NAME ────────────────────────────────────────────────────
function extractCandidateName_(body) {
  var nameMatch = body.match(/Applicant Name:\s*([A-Z\s\-'\.]+?)(?:\r|\n)/i);
  return nameMatch ? nameMatch[1].trim() : null;
}

// ── MATCH CANDIDATE ───────────────────────────────────────────────────────────
function matchCandidate_(firstName, lastName) {
  var results = sbGet_('candidates', {
    'select': 'id,phone,first_name,last_name,client_id',
    'first_name': 'ilike.' + firstName,
    'last_name':  'ilike.' + lastName,
    'status':     'not.in.(Rejected,Hired,Transferred)',
    'limit':      '5'
  });

  if (!results || results.length === 0) return null;
  if (results.length > 1) {
    Logger.log('[Drug Trigger] WARNING: Multiple matches for ' + firstName + ' ' + lastName + ' -- using first');
  }
  return results[0];
}

// ── UPDATE DRUG STATUS ────────────────────────────────────────────────────────
function updateCandidateDrugStatus_(candidateId) {
  var code = sbPatch_('candidates', { 'id': 'eq.' + candidateId }, {
    'drug_test_status': 'In Progress',
    'updated_at': new Date().toISOString()
  });
  Logger.log('[Drug Trigger] DB patch for candidate ' + candidateId + ' returned ' + code);
  return code;
}

// ── QUEUE SMS ─────────────────────────────────────────────────────────────────
function queueSMS_(candidateId, phone, firstName, barcode) {
  if (!phone || phone === '' || phone === '0000000000') {
    Logger.log('[Drug Trigger] Skipping SMS for candidate ' + candidateId + ' -- no valid phone.');
    return 204; // Not a failure -- just no phone
  }

  var body = TEMPLATE_BODY
    .replace('[FIRST]', firstName || '')
    .replace('[BARCODE]', barcode || 'N/A');

  var code = sbInsert_('sms_send_queue', {
    'candidate_id':     candidateId,
    'to_number':        phone,
    'from_number':      FROM_NUMBER,
    'body':             body,
    'template_id':      TEMPLATE_ID,
    'template_name':    TEMPLATE_NAME,
    'status':           'pending',
    'migration_status': 'twilio_active', // FIX: was missing
    'scheduled_for':    new Date().toISOString(),
    'created_by':       'fadv_drug_trigger'
  });

  Logger.log('[Drug Trigger] SMS queue insert for candidate ' + candidateId + ' returned ' + code);
  return code;
}

// ── LOG UNMATCHED ─────────────────────────────────────────────────────────────
function logUnmatched_(fullName, barcode, subject) {
  var today = Utilities.formatDate(new Date(), 'America/New_York', 'yyyy-MM-dd');
  sbInsert_('forge_memory', {
    'session_date':  today,
    'category':      'ops_note',
    'subject':       'FADV Drug Screen -- Unmatched: ' + fullName,
    'content':       'Drug screen email received but no candidate match found. ' +
                     'Barcode: ' + barcode + '. Subject: ' + subject + '. Manual match required.',
    'target_thread': 'PEAK Ops',
    'tags':          ['drug_screen', 'unmatched', 'manual_review']
  });
  Logger.log('[Drug Trigger] Logged unmatched: ' + fullName);
}

// ── RETRY FAILED ──────────────────────────────────────────────────────────────
function retryFailed() {
  var failedLabel  = getOrCreateLabel_(DRUG_FAILED_LABEL);
  var pendingLabel = getOrCreateLabel_(DRUG_PENDING_LABEL);
  var threads = failedLabel.getThreads(0, 50);
  Logger.log('[Drug Trigger] Retrying ' + threads.length + ' failed threads');
  threads.forEach(function(t) {
    t.removeLabel(failedLabel);
    t.addLabel(pendingLabel);
  });
  if (threads.length > 0) processNewDrugScreenEmails();
}
