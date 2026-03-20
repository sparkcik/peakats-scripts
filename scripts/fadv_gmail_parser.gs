/**
 * PEAKATS FADV Gmail Parser
 * Runs on a 30-minute trigger. Finds all emails labeled FADV/Pending,
 * parses FADV status from email body, updates Supabase candidates table,
 * moves email to FADV/Processed label.
 *
 * Status maps aligned to peak_fadv_update_v6.2.py taxonomy.
 * Deploy: paste into Google Apps Script editor at script.google.com
 * Trigger: set time-driven trigger → every 30 minutes
 */

// ── CONFIG ──────────────────────────────────────────────────────────────────
const SUPABASE_URL = 'https://eyopvsmsvbgfuffscfom.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4';
const LABEL_PENDING   = 'FADV/Pending';
const LABEL_PROCESSED = 'FADV/Processed';

// ── STATUS MAPS (aligned to v6.2 taxonomy) ──────────────────────────────────
const BG_STATUS_MAP = {
  'Y-Eligible':              'Eligible',
  'Y-Eligible For Hire':     'Eligible',
  'N-In-Eligible':           'Ineligible',
  'N-In-Eligible For Hire':  'Ineligible',
  'D-Needs further review':  'Needs Further Review',
  'D-Needs Further Review':  'Needs Further Review',
  'Cancelled':               'Expired',
};

const DRUG_STATUS_MAP = {
  'Negative':        'Pass',
  'NegativeDilute':  'Fail',
  'Positive':        'Fail',
  'Complete':        'In Progress',
  'NoShow':          'No Show',
  'Pending':         'In Progress',
  'Cancelled':       'Expired',
};

const AUTO_REJECT_STATUSES = ['Ineligible', 'Fail'];

// ── MAIN ENTRY POINT ─────────────────────────────────────────────────────────
function processFadvEmails() {
  const pendingLabel = GmailApp.getUserLabelByName(LABEL_PENDING);
  if (!pendingLabel) {
    Logger.log('Label not found: ' + LABEL_PENDING + ' — create it in Gmail first');
    return;
  }

  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const threads = pendingLabel.getThreads(0, 100); // process up to 100 per run

  Logger.log('Found ' + threads.length + ' threads to process');

  let updated = 0, skipped = 0, unmatched = 0, errors = 0;

  threads.forEach(function(thread) {
    const messages = thread.getMessages();
    messages.forEach(function(message) {
      try {
        const result = processMessage_(message);
        if (result === 'updated')   updated++;
        else if (result === 'skipped')   skipped++;
        else if (result === 'unmatched') unmatched++;
      } catch(e) {
        Logger.log('ERROR processing message: ' + e.message);
        errors++;
        return; // leave in FADV/Pending so it retries next run
      }
    });

    // Move thread: remove FADV/Pending, add FADV/Processed, mark read, archive
    thread.removeLabel(pendingLabel);
    thread.addLabel(processedLabel);
    thread.markRead();
    thread.moveToArchive();
  });

  Logger.log('Run complete — Updated: ' + updated + ' | Skipped: ' + skipped +
             ' | Unmatched: ' + unmatched + ' | Errors: ' + errors);
}

// ── PARSE + UPDATE ONE MESSAGE ───────────────────────────────────────────────
function processMessage_(message) {
  const subject = message.getSubject();
  const body    = message.getPlainBody() || message.getBody();
  const msgId   = message.getId();

  // Skip non-FADV emails (safety check)
  if (!subject.includes('First Advantage Screening Alerts')) return 'skipped';

  // Parse candidate name from subject
  const nameMatch = subject.match(/Reported on (.+)$/i);
  const candidateName = nameMatch ? nameMatch[1].trim() : '';

  // Parse CID
  const cidMatch = body.match(/CID #\s*(\d+)/);
  const cid = cidMatch ? cidMatch[1] : '';

  // Detect email type and raw status value
  const scoreMatch  = body.match(/Score\s*:\s*([A-Z][^\n\r]*?)(?:\s+Click|\s*$)/m);
  const resultMatch = body.match(/Result\s*:\s*([A-Za-z]+)/m);
  const cancelMatch = body.match(/Status\s*:\s*Cancelled/i);
  const pendingMatch = body.match(/Status\s*:\s*Pending/i);

  let emailType, rawValue, mappedStatus;

  if (scoreMatch) {
    emailType    = 'background';
    rawValue     = scoreMatch[1].trim();
    mappedStatus = BG_STATUS_MAP[rawValue] || rawValue;
  } else if (resultMatch) {
    emailType    = 'drug';
    rawValue     = resultMatch[1].trim();
    mappedStatus = DRUG_STATUS_MAP[rawValue] || rawValue;
  } else if (cancelMatch) {
    emailType    = 'cancelled';
    rawValue     = 'Cancelled';
    mappedStatus = 'Expired';
  } else if (pendingMatch) {
    emailType    = 'pending';
    rawValue     = 'Pending';
    mappedStatus = 'In Progress';
  } else {
    Logger.log('Could not parse email type for: ' + candidateName + ' | ' + subject);
    logToSupabase_(msgId, candidateName, cid, 'unknown', '', '', null, 'unmatched', 0, false, 'parse_failed');
    return 'unmatched';
  }

  // Match candidate in Supabase
  const candidate = findCandidate_(cid, candidateName, emailType);
  if (!candidate) {
    Logger.log('No candidate match: ' + candidateName + ' CID: ' + cid);
    logToSupabase_(msgId, candidateName, cid, emailType, rawValue, mappedStatus, null, 'unmatched', 0, false, 'no_candidate_match');
    return 'unmatched';
  }

  // Check if already current
  const currentVal = emailType === 'background'
    ? candidate.background_status
    : candidate.drug_test_status;

  if (currentVal === mappedStatus) {
    logToSupabase_(msgId, candidateName, cid, emailType, rawValue, mappedStatus, candidate.id, candidate._match_method, candidate._match_confidence, false, 'already_current');
    return 'skipped';
  }

  // Apply update
  updateCandidate_(candidate.id, emailType, mappedStatus);
  logToSupabase_(msgId, candidateName, cid, emailType, rawValue, mappedStatus, candidate.id, candidate._match_method, candidate._match_confidence, true, null);

  return 'updated';
}

// ── FIND CANDIDATE IN SUPABASE ───────────────────────────────────────────────
function findCandidate_(cid, name, emailType) {
  // 1. Try CID match
  if (cid) {
    const cidField = emailType === 'background' ? 'background_id' : 'drug_test_id';
    const res = supabaseGet_('candidates', {
      select: 'id,first_name,last_name,background_status,drug_test_status',
      [cidField]: 'eq.' + cid,
      limit: 1
    });
    if (res && res.length > 0) {
      res[0]._match_method = 'cid';
      res[0]._match_confidence = 1.0;
      return res[0];
    }

    // Try other CID field as fallback
    const altField = emailType === 'background' ? 'drug_test_id' : 'background_id';
    const res2 = supabaseGet_('candidates', {
      select: 'id,first_name,last_name,background_status,drug_test_status',
      [altField]: 'eq.' + cid,
      limit: 1
    });
    if (res2 && res2.length > 0) {
      res2[0]._match_method = 'cid_fallback';
      res2[0]._match_confidence = 0.9;
      return res2[0];
    }
  }

  // 2. Try exact name match (UPPER normalized)
  if (name) {
    const parts = name.trim().split(/\s+/);
    if (parts.length >= 2) {
      const firstName = parts[0];
      const lastName  = parts.slice(1).join(' ');
      const res = supabaseGet_('candidates', {
        select: 'id,first_name,last_name,background_status,drug_test_status',
        'first_name': 'ilike.' + firstName,
        'last_name':  'ilike.' + lastName,
        limit: 1
      });
      if (res && res.length > 0) {
        res[0]._match_method = 'name_exact';
        res[0]._match_confidence = 1.0;
        return res[0];
      }
    }
  }

  return null;
}

// ── UPDATE CANDIDATE IN SUPABASE ─────────────────────────────────────────────
function updateCandidate_(candidateId, emailType, mappedStatus) {
  const field = emailType === 'background' ? 'background_status' : 'drug_test_status';
  const payload = {
    [field]: mappedStatus,
    fadv_last_updated: 'gmail_parser'
  };

  // Auto-reject if ineligible or fail
  if (AUTO_REJECT_STATUSES.includes(mappedStatus)) {
    payload.status = 'Rejected';
    payload.reject_reason = 'fadv_ineligible';
  }

  supabasePatch_('candidates', 'id=eq.' + candidateId, payload);
}

// ── LOG TO fadv_email_log ─────────────────────────────────────────────────────
function logToSupabase_(msgId, name, cid, type, rawScore, rawResult, candidateId, method, confidence, applied, skipReason) {
  try {
    const payload = {
      message_id:        msgId,
      candidate_name:    name,
      cid:               cid,
      email_type:        type,
      raw_score:         type === 'background' ? rawScore : null,
      raw_result:        type === 'drug' ? rawResult : null,
      mapped_status:     rawResult || rawScore,
      candidate_id:      candidateId,
      match_method:      method,
      match_confidence:  confidence,
      update_applied:    applied,
      skip_reason:       skipReason
    };
    supabasePost_('fadv_email_log', payload);
  } catch(e) {
    Logger.log('Log write failed (non-fatal): ' + e.message);
  }
}

// ── SUPABASE HELPERS ─────────────────────────────────────────────────────────
function supabaseGet_(table, params) {
  const qs = Object.entries(params).map(([k,v]) => k + '=' + encodeURIComponent(v)).join('&');
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

// ── LABEL HELPER ─────────────────────────────────────────────────────────────
function getOrCreateLabel_(labelName) {
  let label = GmailApp.getUserLabelByName(labelName);
  if (!label) label = GmailApp.createLabel(labelName);
  return label;
}

// ── ONE-TIME BACKLOG PROCESSOR ────────────────────────────────────────────────
// Run this ONCE manually to label already-processed backlog emails
function labelBacklog() {
  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const threads = GmailApp.search('from:EntAdv.DoNotReply@fadv.com -label:FADV/Pending -label:FADV/Processed');
  Logger.log('Backlog threads to label: ' + threads.length);
  threads.forEach(function(thread) {
    thread.addLabel(processedLabel);
    thread.markRead();
    thread.moveToArchive();
  });
  Logger.log('Backlog labeled complete');
}
