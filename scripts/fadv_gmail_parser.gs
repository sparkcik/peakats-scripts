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

    // Move thread: remove FADV/Pending, add FADV/Processed, strip Kai/Inbox, mark read, archive
    thread.removeLabel(pendingLabel);
    thread.addLabel(processedLabel);
    const kaiInbox = GmailApp.getUserLabelByName('Kai/Inbox');
    if (kaiInbox) thread.removeLabel(kaiInbox);
    thread.markRead();
    thread.moveToArchive();
  });

  Logger.log('Run complete — Updated: ' + updated + ' | Skipped: ' + skipped +
             ' | Unmatched: ' + unmatched + ' | Errors: ' + errors);
}

// ── HOURLY TRIGGER WRAPPER ───────────────────────────────────────────────────
function runHourly() {
  processFadvEmails();
  processFadvActionEmails();
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

  // Resolution detection: if transitioning OUT of Needs Further Review
  if (emailType === 'background' && (mappedStatus === 'Eligible' || mappedStatus === 'In Progress')) {
    const current = supabaseGet_('candidates', {
      select: 'background_status,first_name,phone,fadv_action_required',
      'id': 'eq.' + candidateId,
      limit: 1
    });
    if (current && current.length > 0 && current[0].background_status === 'Needs Further Review') {
      payload.fadv_action_required = false;
      payload.fadv_action_resolved_at = new Date().toISOString();

      // Send resolved SMS (template 45) ONLY for In Progress, not Eligible
      // Eligible has its own workflow (template 15)
      if (mappedStatus === 'In Progress' && current[0].phone) {
        var body = 'Hi ' + (current[0].first_name || '') + ', your background check is back on track -- FADV has resumed processing.\n' +
          'No further action needed from you.\n\n' +
          'Kai\nPEAKrecruiting\nQuestions? (470) 857-4325';
        sendSms_(current[0].phone, body);
      }
    }
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

// ── FADV ACTION-REQUIRED EMAIL PROCESSOR ─────────────────────────────────────
const LABEL_ACTION = 'FADV/Action-Required';

function processFadvActionEmails() {
  const actionLabel = getOrCreateLabel_(LABEL_ACTION);
  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const threads = actionLabel.getThreads(0, 50);

  Logger.log('Found ' + threads.length + ' FADV Action-Required threads');

  let sent = 0, skipped = 0, unmatched = 0, errors = 0;

  threads.forEach(function(thread) {
    const messages = thread.getMessages();
    let threadOk = true;

    messages.forEach(function(message) {
      try {
        const from = message.getFrom();
        const subject = message.getSubject();

        if (from.indexOf('FedEx.Support@fadv.com') === -1) { skipped++; return; }
        if (subject.indexOf('Requires Additional Information') === -1) { skipped++; return; }

        const body = message.getPlainBody() || message.getBody();

        // Parse fields from body
        var nameMatch = body.match(/Candidate:\s*(.+)/i);
        var orderMatch = body.match(/Order Number:\s*(\S+)/i);
        var linkMatch = body.match(/(https:\/\/pa\.fadv\.com\/#\/invite\/[^\s<"]+)/i);
        var expiryMatch = body.match(/This link expires on\s+(.+)/i);
        var reasonMatch = body.match(/Requested Information[\s\S]*?(?:<\/?\w[^>]*>|\n\s*\n)([\s\S]*?)(?:Next Step|$)/i);

        var candidateName = nameMatch ? nameMatch[1].trim() : '';
        var orderNumber = orderMatch ? orderMatch[1].trim() : '';
        var profileLink = linkMatch ? linkMatch[1].trim() : '';
        var expiryRaw = expiryMatch ? expiryMatch[1].trim() : '';
        var reason = '';

        if (reasonMatch) {
          reason = reasonMatch[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
          if (reason.length > 200) reason = reason.substring(0, 200);
        }

        // Format expiry date
        var expiryFormatted = '';
        var expiryIso = '';
        if (expiryRaw) {
          var expDate = new Date(expiryRaw);
          if (!isNaN(expDate.getTime())) {
            expiryFormatted = Utilities.formatDate(expDate, Session.getScriptTimeZone(), 'MMMM d, yyyy');
            expiryIso = Utilities.formatDate(expDate, 'UTC', "yyyy-MM-dd'T'HH:mm:ss'Z'");
          }
        }

        // Match candidate
        var candidate = null;
        if (orderNumber) {
          var res = supabaseGet_('candidates', {
            select: 'id,first_name,phone,background_status,fadv_action_required,fadv_action_sms_sent_at',
            'drug_test_id': 'eq.' + orderNumber,
            limit: 1
          });
          if (res && res.length > 0) candidate = res[0];
        }
        if (!candidate && candidateName) {
          var parts = candidateName.split(/\s+/);
          if (parts.length >= 2) {
            var res2 = supabaseGet_('candidates', {
              select: 'id,first_name,phone,background_status,fadv_action_required,fadv_action_sms_sent_at',
              'first_name': 'ilike.' + parts[0],
              'last_name': 'ilike.' + parts.slice(1).join(' '),
              limit: 1
            });
            if (res2 && res2.length > 0) candidate = res2[0];
          }
        }

        if (!candidate) {
          Logger.log('No candidate match for Action-Required: ' + candidateName + ' Order: ' + orderNumber);
          logToSupabase_(message.getId(), candidateName, orderNumber, 'action_required', '', '', null, 'unmatched', 0, false, 'no_candidate_match');
          unmatched++;
          return;
        }

        // Update Supabase
        supabasePatch_('candidates', 'id=eq.' + candidate.id, {
          background_status: 'Needs Further Review',
          fadv_action_required: true,
          fadv_action_reason: reason,
          fadv_action_link: profileLink,
          fadv_action_expires: expiryIso || null
        });

        // Send SMS (template 41)
        if (candidate.phone) {
          var smsBody = 'Hi ' + (candidate.first_name || '') + ', your background check is on hold.\n\n' +
            'FADV needs you to respond using this link -- do NOT reply to their email:\n' +
            profileLink + '\n\n' +
            'Deadline: ' + expiryFormatted + '\n\n' +
            'Reason: ' + reason + '\n\n' +
            'Kai\nPEAKrecruiting\nQuestions? (470) 857-4325';
          sendSms_(candidate.phone, smsBody);
        }

        // Mark SMS sent
        supabasePatch_('candidates', 'id=eq.' + candidate.id, {
          fadv_action_sms_sent_at: new Date().toISOString()
        });

        sent++;
      } catch(e) {
        Logger.log('ERROR processing Action-Required message: ' + e.message);
        errors++;
        threadOk = false;
      }
    });

    if (threadOk) {
      thread.removeLabel(actionLabel);
      thread.addLabel(processedLabel);
      thread.markRead();
      thread.moveToArchive();
    }
  });

  Logger.log('Action-Required run complete -- Sent: ' + sent + ' | Skipped: ' + skipped +
             ' | Unmatched: ' + unmatched + ' | Errors: ' + errors);
}

// ── FADV ACTION FUP CRON (every 6 hours) ────────────────────────────────────
function processFadvActionFups() {
  var now = new Date();
  var nowIso = now.toISOString();

  // Fetch candidates with active action-required
  var candidates = supabaseGet_('candidates', {
    select: 'id,first_name,last_name,phone,client_id,status,fadv_action_required,fadv_action_link,fadv_action_expires,fadv_action_reason,fadv_action_sms_sent_at,fadv_action_fup1_sent_at,fadv_action_fup2_sent_at,fadv_action_fup3_sent_at',
    'fadv_action_required': 'eq.true',
    'fadv_action_expires': 'gt.' + nowIso,
    'status': 'not.in.(Rejected,Hired)',
    limit: 200
  });

  if (!candidates || candidates.length === 0) {
    Logger.log('No active FADV action-required candidates');
    return;
  }

  var fup1 = 0, fup2 = 0, fup3 = 0, escalated = 0;

  candidates.forEach(function(c) {
    if (!c.fadv_action_sms_sent_at || !c.phone) return;

    var sentAt = new Date(c.fadv_action_sms_sent_at);
    var daysSince = (now.getTime() - sentAt.getTime()) / (1000 * 60 * 60 * 24);

    var link = c.fadv_action_link || '';
    var expiryFormatted = '';
    if (c.fadv_action_expires) {
      var expDate = new Date(c.fadv_action_expires);
      expiryFormatted = Utilities.formatDate(expDate, Session.getScriptTimeZone(), 'MMMM d, yyyy');
    }

    if (daysSince >= 4 && c.fadv_action_fup1_sent_at && c.fadv_action_fup2_sent_at && c.fadv_action_fup3_sent_at) {
      // Escalate: all FUPs sent, no response after 4 days
      supabasePost_('action_items', {
        priority: '🔴',
        category: 'OPS',
        task: 'FADV action required -- no response after 3 FUPs: ' + (c.first_name || '') + ' ' + (c.last_name || '') + ' (' + (c.client_id || '') + ')',
        status: 'OPEN'
      });
      escalated++;

    } else if (daysSince >= 3 && !c.fadv_action_fup3_sent_at) {
      // FUP 3 (template 44)
      var body3 = (c.first_name || '') + ', final reminder. Your background check cannot move forward until you respond to FADV.\n\n' +
        'Deadline: ' + expiryFormatted + '\n' + link + '\n\n' +
        'Reply here if you need help.\n\n' +
        'Kai\nPEAKrecruiting\nQuestions? (470) 857-4325';
      sendSms_(c.phone, body3);
      supabasePatch_('candidates', 'id=eq.' + c.id, { fadv_action_fup3_sent_at: nowIso });
      fup3++;

    } else if (daysSince >= 2 && !c.fadv_action_fup2_sent_at) {
      // FUP 2 (template 43)
      var body2 = (c.first_name || '') + ', second reminder -- background check still on hold. Deadline is ' + expiryFormatted + ':\n' + link + '\n\n' +
        'Kai\nPEAKrecruiting\nQuestions? (470) 857-4325';
      sendSms_(c.phone, body2);
      supabasePatch_('candidates', 'id=eq.' + c.id, { fadv_action_fup2_sent_at: nowIso });
      fup2++;

    } else if (daysSince >= 1 && !c.fadv_action_fup1_sent_at) {
      // FUP 1 (template 42)
      var body1 = (c.first_name || '') + ', your background check is still on hold.\n\n' +
        'FADV is waiting on your response. Deadline is ' + expiryFormatted + ':\n' + link + '\n\n' +
        'Kai\nPEAKrecruiting\nQuestions? (470) 857-4325';
      sendSms_(c.phone, body1);
      supabasePatch_('candidates', 'id=eq.' + c.id, { fadv_action_fup1_sent_at: nowIso });
      fup1++;
    }
  });

  Logger.log('FUP run complete -- FUP1: ' + fup1 + ' | FUP2: ' + fup2 + ' | FUP3: ' + fup3 + ' | Escalated: ' + escalated);
}

// ── TWILIO SMS HELPER ───────────────────────────────────────────────────────
function sendSms_(toPhone, body) {
  var props = PropertiesService.getScriptProperties();
  var sid = props.getProperty('TWILIO_ACCOUNT_SID');
  var token = props.getProperty('TWILIO_AUTH_TOKEN');
  var fromNumber = props.getProperty('TWILIO_FROM_NUMBER');

  if (!sid || !token || !fromNumber) {
    Logger.log('Twilio credentials not set in Script Properties');
    return;
  }

  // Format phone: strip non-digits, prepend +1
  var digits = toPhone.replace(/\D/g, '');
  if (digits.length === 10) digits = '1' + digits;
  var formatted = '+' + digits;

  var url = 'https://api.twilio.com/2010-04-01/Accounts/' + sid + '/Messages.json';
  var authHeader = 'Basic ' + Utilities.base64Encode(sid + ':' + token);

  try {
    var res = UrlFetchApp.fetch(url, {
      method: 'POST',
      headers: { 'Authorization': authHeader },
      payload: {
        'To': formatted,
        'From': fromNumber,
        'Body': body
      },
      muteHttpExceptions: true
    });
    var code = res.getResponseCode();
    if (code >= 200 && code < 300) {
      Logger.log('SMS sent to ' + formatted);
    } else {
      Logger.log('SMS failed (' + code + ') to ' + formatted + ': ' + res.getContentText());
    }
  } catch(e) {
    Logger.log('SMS error to ' + formatted + ': ' + e.message);
  }
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
    const kaiInbox = GmailApp.getUserLabelByName('Kai/Inbox');
    if (kaiInbox) thread.removeLabel(kaiInbox);
    thread.markRead();
    thread.moveToArchive();
  });
  Logger.log('Backlog labeled complete');
}
