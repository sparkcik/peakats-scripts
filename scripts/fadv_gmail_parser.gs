/**
 * PEAKATS FADV Gmail Parser — HARDENED v2
 * Fixed: silent write failures, bad thread archiving, false update_applied logs,
 *        added confirmed-write pattern, failure alerts, daily reconciliation.
 */

// ── CONFIG ───────────────────────────────────────────────────────────────────
const SUPABASE_URL  = 'https://eyopvsmsvbgfuffscfom.supabase.co';
const SUPABASE_KEY  = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4';
const ALERT_EMAIL   = 'kai@peakrecruitingco.com';
const LABEL_PENDING   = 'FADV/Pending';
const LABEL_PROCESSED = 'FADV/Processed';
const LABEL_FAILED    = 'FADV/Failed';

// ── STATUS MAPS ──────────────────────────────────────────────────────────────
const BG_STATUS_MAP = {
  'Y-Eligible':             'Eligible',
  'Y-Eligible For Hire':    'Eligible',
  'N-In-Eligible':          'Ineligible',
  'N-In-Eligible For Hire': 'Ineligible',
  'D-Needs further review': 'Needs Further Review',
  'D-Needs Further Review': 'Needs Further Review',
  'Cancelled':              'Expired',
};

const DRUG_STATUS_MAP = {
  'Negative':       'Pass',
  'NegativeDilute': 'Fail',
  'Positive':       'Fail',
  'Complete':       'In Progress',
  'NoShow':         'No Show',
  'Pending':        'In Progress',
  'Cancelled':      'Expired',
};

// Terminal drug statuses -- CSV processor must NOT overwrite these
const DRUG_TERMINAL = ['Pass', 'Fail', 'No Show', 'Expired'];

const AUTO_REJECT_STATUSES = ['Ineligible', 'Fail'];

// ── MAIN ENTRY POINT ─────────────────────────────────────────────────────────
function processFadvEmails() {
  const pendingLabel = GmailApp.getUserLabelByName(LABEL_PENDING);
  if (!pendingLabel) {
    Logger.log('Label not found: ' + LABEL_PENDING);
    return;
  }
  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const failedLabel    = getOrCreateLabel_(LABEL_FAILED);

  // ── SAFETY SWEEP: catch EntAdv emails that missed the Gmail filter ──────────
  // Fix (Apr 21): addLabel + getThreads in the same run has cache inconsistency.
  // Solution: process swept threads immediately in-line, don't wait for getThreads().
  var sweepSenders = [
    'from:EntAdv.DoNotReply@fadv.com',
    'from:do_not_reply@fadv.com',
    'from:Fedex.notifications@fadv.com',
    'from:DoNotReply@noti.fadv.com'
  ];
  var sweepUpdated = 0, sweepErrors = 0;
  sweepSenders.forEach(function(senderQuery) {
    var unlabeled = GmailApp.search(senderQuery + ' -label:FADV/Pending -label:FADV/Processed -label:FADV/Failed', 0, 50);
    if (unlabeled.length > 0) {
      Logger.log('[Safety sweep] Found ' + unlabeled.length + ' unlabeled thread(s) for ' + senderQuery);
      unlabeled.forEach(function(thread) {
        thread.addLabel(pendingLabel);
        // Process immediately -- do not wait for getThreads() which may miss newly-labeled threads
        thread.getMessages().forEach(function(msg) {
          try {
            var result = processMessage_(msg);
            if (result === 'updated') sweepUpdated++;
          } catch(e) {
            sweepErrors++;
            Logger.log('[Safety sweep] Error processing message: ' + e.message);
          }
        });
        // Mark as processed after in-line processing
        thread.addLabel(processedLabel);
        thread.removeLabel(pendingLabel);
      });
    }
  });
  if (sweepUpdated > 0 || sweepErrors > 0) {
    Logger.log('[Safety sweep] Complete: updated=' + sweepUpdated + ' errors=' + sweepErrors);
  }
  // ── END SAFETY SWEEP ────────────────────────────────────────────────────────

  const threads = pendingLabel.getThreads(0, 100);
  Logger.log('Found ' + threads.length + ' threads to process');

  let updated = 0, skipped = 0, unmatched = 0, errors = 0;

  threads.forEach(function(thread) {
    const messages = thread.getMessages();
    let threadHadError = false;

    messages.forEach(function(message) {
      try {
        const result = processMessage_(message);
        if (result === 'updated')        updated++;
        else if (result === 'skipped')   skipped++;
        else if (result === 'unmatched') unmatched++;
        else if (result === 'error') {
          errors++;
          threadHadError = true;
        }
      } catch(e) {
        Logger.log('EXCEPTION processing message: ' + e.message);
        errors++;
        threadHadError = true;
      }
    });

    thread.removeLabel(pendingLabel);
    if (threadHadError) {
      thread.addLabel(failedLabel);
      Logger.log('Thread left in FADV/Failed for retry: ' + thread.getFirstMessageSubject());
    } else {
      thread.addLabel(processedLabel);
      const kaiInbox = GmailApp.getUserLabelByName('Kai/Inbox');
      if (kaiInbox) thread.removeLabel(kaiInbox);
      thread.markRead();
      thread.moveToArchive();
    }
  });

  Logger.log('Run complete -- Updated: ' + updated + ' | Skipped: ' + skipped +
             ' | Unmatched: ' + unmatched + ' | Errors: ' + errors);

  if (errors > 0) {
    alertForge_('FADV Parser write failures: ' + errors,
      errors + ' message(s) failed to write to Supabase. Check FADV/Failed label in Gmail. Re-label to FADV/Pending to retry.');
  }
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

  if (!subject.includes('First Advantage Screening Alerts')) return 'skipped';

  const nameMatch = subject.match(/Reported on (.+)$/i);
  const candidateName = nameMatch ? nameMatch[1].trim() : '';

  const cidMatch = body.match(/CID #\s*(\d+)/);
  const cid = cidMatch ? cidMatch[1] : '';

  const scoreMatch   = body.match(/Score\s*:\s*([A-Z][^\n\r]*?)(?:\s+Click|\s*$)/m);
  const resultMatch  = body.match(/Result\s*:\s*([A-Za-z]+)/m);
  const cancelMatch  = body.match(/Status\s*:\s*Cancelled/i);
  const pendingMatch = body.match(/Status\s*:\s*Pending/i);

  let emailType, rawValue, mappedStatus;

  if (scoreMatch) {
    emailType    = 'background';
    rawValue     = scoreMatch[1].trim();
    // Normalize NFR variants -- FADV sends both 'D-Needs further review' and 'D-Needs Further Review'
    var normalizedRaw = rawValue.replace(/needs further review/i, 'Needs Further Review')
                                .replace(/^D-/i, 'D-');
    mappedStatus = BG_STATUS_MAP[normalizedRaw] || BG_STATUS_MAP[rawValue] || rawValue;
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
    Logger.log('Could not parse email type: ' + candidateName + ' | ' + subject);
    logToSupabase_(msgId, candidateName, cid, 'unknown', '', '', null, 'unmatched', 0, false, 'parse_failed');
    return 'unmatched';
  }

  // Extract FADV account ID from email body for client scoping
  const fadvAccountMatch = body.match(/Account[:\s#]+([A-Z0-9]+)/i) ||
                           body.match(/CSP[:\s#]+([A-Z0-9]+)/i) ||
                           body.match(/042443([A-Z0-9]+)/i);
  const fadvAccountId = fadvAccountMatch ? fadvAccountMatch[0].replace(/Account[:\s#]+|CSP[:\s#]+/i,'').trim() : null;

  const candidate = findCandidate_(cid, candidateName, emailType, fadvAccountId);
  if (!candidate) {
    Logger.log('No candidate match: ' + candidateName + ' CID: ' + cid);
    logToSupabase_(msgId, candidateName, cid, emailType, rawValue, mappedStatus, null, 'unmatched', 0, false, 'no_candidate_match');
    return 'unmatched';
  }

  const currentVal = emailType === 'background'
    ? candidate.background_status
    : candidate.drug_test_status;

  if (currentVal === mappedStatus) {
    logToSupabase_(msgId, candidateName, cid, emailType, rawValue, mappedStatus,
                   candidate.id, candidate._match_method, candidate._match_confidence, false, 'already_current');
    return 'skipped';
  }

  const writeOk = updateCandidate_(candidate.id, emailType, mappedStatus, cid);
  if (!writeOk) {
    Logger.log('WRITE FAILED for candidate ' + candidate.id + ' (' + candidateName + ')');
    logToSupabase_(msgId, candidateName, cid, emailType, rawValue, mappedStatus,
                   candidate.id, candidate._match_method, candidate._match_confidence, false, 'write_failed');
    alertForge_('FADV write failure: ' + candidateName,
      'Candidate ID ' + candidate.id + ' (' + candidateName + ')\n' +
      'Field: ' + emailType + '\nValue: ' + mappedStatus + '\nCID: ' + cid + '\n\n' +
      'Manual fix required in Supabase dashboard.');
    return 'error';
  }

  logToSupabase_(msgId, candidateName, cid, emailType, rawValue, mappedStatus,
                 candidate.id, candidate._match_method, candidate._match_confidence, true, null);
  return 'updated';
}

// ── FADV ACCOUNT TO CLIENT MAP ───────────────────────────────────────────────
const FADV_ACCOUNT_TO_CLIENT = {
  '042443JVP': 'gods_vision',
  '042443HNB': 'legacy_chattanooga',
  '042443PWO': 'cbm',
  'V0027018':  'cbm',
  'V0009926':  'cnf_services',
  '042443sdp': 'solpac',
};

// ── NAME NORMALIZATION (strips suffixes, lowercases) ─────────────────────────
function normalizeName_(name) {
  if (!name) return '';
  return name.trim()
    .replace(/\b(jr\.?|sr\.?|ii|iii|iv|v|esq\.?)\b/gi, '')
    .replace(/\s+/g, ' ').trim().toLowerCase();
}

// ── FIND CANDIDATE v3.2 -- fuzzy name + client scope ─────────────────────────
function findCandidate_(cid, name, emailType, fadvAccountId) {
  // 1. CID match -- most reliable
  if (cid) {
    var cidField = emailType === 'background' ? 'background_id' : 'drug_test_id';
    var res = supabaseGet_('candidates', {
      select: 'id,first_name,last_name,background_status,drug_test_status',
      [cidField]: 'eq.' + cid, limit: 1
    });
    if (res && res.length > 0) {
      res[0]._match_method = 'cid'; res[0]._match_confidence = 1.0; return res[0];
    }
    var altField = emailType === 'background' ? 'drug_test_id' : 'background_id';
    var res2 = supabaseGet_('candidates', {
      select: 'id,first_name,last_name,background_status,drug_test_status',
      [altField]: 'eq.' + cid, limit: 1
    });
    if (res2 && res2.length > 0) {
      res2[0]._match_method = 'cid_fallback'; res2[0]._match_confidence = 0.9; return res2[0];
    }
  }

  // 2. Name match -- normalize, scope by client, fuzzy last name fallback
  if (name) {
    var normalized = normalizeName_(name);
    var parts = normalized.split(/\s+/);
    if (parts.length < 2) return null;

    var firstName = parts[0];
    var lastName  = parts.slice(1).join(' '); // full remainder handles compound names

    // Derive client_id scope from FADV account ID
    var clientId = fadvAccountId ? (FADV_ACCOUNT_TO_CLIENT[fadvAccountId] || null) : null;

    // Exact normalized match -- with client scope if available
    var params = {
      select: 'id,first_name,last_name,background_status,drug_test_status',
      'first_name': 'ilike.' + firstName,
      'last_name':  'ilike.' + lastName,
      limit: 1
    };
    if (clientId) params['client_id'] = 'eq.' + clientId;

    var res3 = supabaseGet_('candidates', params);
    if (res3 && res3.length > 0) {
      res3[0]._match_method = clientId ? 'name_scoped' : 'name_exact';
      res3[0]._match_confidence = clientId ? 1.0 : 0.9;
      return res3[0];
    }

    // Fuzzy fallback: first name + final word of last name only
    // Catches "JHONATAN ALVAREZ CORNEJO" -> "Jhonatan Alvarez"
    // Only run with client scope to avoid false positives
    if (clientId && parts.length > 2) {
      var res4 = supabaseGet_('candidates', {
        select: 'id,first_name,last_name,background_status,drug_test_status',
        'first_name': 'ilike.' + firstName,
        'last_name':  'ilike.' + parts[1], // second word only
        'client_id':  'eq.' + clientId,
        limit: 1
      });
      if (res4 && res4.length > 0) {
        res4[0]._match_method = 'name_fuzzy_compound';
        res4[0]._match_confidence = 0.75;
        return res4[0];
      }
    }
  }

  return null;
}

// ── UPDATE CANDIDATE ─────────────────────────────────────────────────────────
function updateCandidate_(candidateId, emailType, mappedStatus, cid) {
  const field    = emailType === 'background' ? 'background_status' : 'drug_test_status';
  const cidField = emailType === 'background' ? 'background_id'     : 'drug_test_id';
  const payload = {
    [field]: mappedStatus,
    fadv_last_updated: new Date().toISOString()
  };
  // Stamp CID if provided and not already set -- never overwrite existing order ID
  if (cid) {
    const current = supabaseGet_('candidates', { select: cidField, 'id': 'eq.' + candidateId, limit: 1 });
    if (current && current.length > 0 && !current[0][cidField]) {
      payload[cidField] = cid;
      Logger.log('[updateCandidate_] Stamping ' + cidField + ' = ' + cid + ' for candidate ' + candidateId);
    }
  }

  if (AUTO_REJECT_STATUSES.includes(mappedStatus)) {
    payload.status = 'Rejected';
    payload.reject_reason = 'fadv_ineligible';
  }

  if (emailType === 'background' && (mappedStatus === 'Eligible' || mappedStatus === 'In Progress')) {
    const current = supabaseGet_('candidates', {
      select: 'background_status,first_name,phone,fadv_action_required',
      'id': 'eq.' + candidateId, limit: 1
    });
    if (current && current.length > 0 && current[0].background_status === 'Needs Further Review') {
      payload.fadv_action_required = false;
      payload.fadv_action_resolved_at = new Date().toISOString();
      if (mappedStatus === 'In Progress' && current[0].phone) {
        var smsBody = 'Hi ' + (current[0].first_name || '') + ', your background check is back on track -- FADV has resumed processing. No further action needed from you.\n\nKai\nPEAKrecruiting';
        sendSms_(current[0].phone, smsBody);
      }
    }
  }

  const code = supabasePatch_('candidates', 'id=eq.' + candidateId, payload);
  if (code !== 200 && code !== 204) {
    Logger.log('supabasePatch_ returned ' + code + ' for candidate ' + candidateId);
    return false;
  }
  return true;
}

// ── LOG TO fadv_email_log ─────────────────────────────────────────────────────
function logToSupabase_(msgId, name, cid, type, rawScore, rawResult, candidateId, method, confidence, applied, skipReason) {
  try {
    supabasePost_('fadv_email_log', {
      message_id:       msgId,
      candidate_name:   name,
      cid:              cid,
      email_type:       type,
      raw_score:        type === 'background' ? rawScore : null,
      raw_result:       type === 'drug' ? rawResult : null,
      mapped_status:    rawResult || rawScore,
      candidate_id:     candidateId,
      match_method:     method,
      match_confidence: confidence,
      update_applied:   applied,
      skip_reason:      skipReason
    });
  } catch(e) {
    Logger.log('Log write failed (non-fatal): ' + e.message);
  }
}

// ── DAILY RECONCILIATION ──────────────────────────────────────────────────────
function dailyReconciliation() {
  var cutoff = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
  var stale = supabaseGet_('candidates', {
    select: 'id,first_name,last_name,client_id,drug_test_id,drug_test_status,updated_at',
    'drug_test_id':     'not.is.null',
    'drug_test_status': 'eq.In Progress',
    'status':           'not.in.(Rejected,Hired,Transferred)',
    'updated_at':       'lt.' + cutoff,
    limit: 100
  });

  if (!stale || stale.length === 0) { Logger.log('Reconciliation: no stale drug candidates'); return; }

  var lines = stale.map(function(c) {
    return '- ' + c.first_name + ' ' + c.last_name + ' (' + c.client_id + ') drug_test_id=' + c.drug_test_id + ' last_updated=' + c.updated_at;
  });

  var msg = stale.length + ' candidate(s) with drug test In Progress older than 3 days.\n\n' +
    'These may have results in FADV that the parser missed.\nCheck FADV portal and update manually if needed.\n\n' +
    lines.join('\n');

  Logger.log('Reconciliation alert:\n' + msg);
  alertForge_('PEAK Daily Audit: ' + stale.length + ' stale drug screens', msg);
}

// ── ALERT HELPER ─────────────────────────────────────────────────────────────
function alertForge_(subject, body) {
  try {
    GmailApp.sendEmail(ALERT_EMAIL, '[PEAK ALERT] ' + subject, body);
    Logger.log('Alert sent: ' + subject);
  } catch(e) {
    Logger.log('Alert send failed: ' + e.message);
  }
}

// ── SUPABASE HELPERS ─────────────────────────────────────────────────────────
function supabaseGet_(table, params) {
  const qs = Object.entries(params).map(([k,v]) => k + '=' + encodeURIComponent(v)).join('&');
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
      'apikey': SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal'
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
      'apikey': SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  return res.getResponseCode();
}

// ── FADV ACTION-REQUIRED PROCESSOR ───────────────────────────────────────────
const LABEL_ACTION = 'FADV/Action-Required';

function processFadvActionEmails() {
  const actionLabel    = getOrCreateLabel_(LABEL_ACTION);
  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const failedLabel    = getOrCreateLabel_(LABEL_FAILED);
  const threads = actionLabel.getThreads(0, 50);
  Logger.log('Found ' + threads.length + ' FADV Action-Required threads');

  let sent = 0, skipped = 0, unmatched = 0, errors = 0;

  threads.forEach(function(thread) {
    const messages = thread.getMessages();
    let threadOk = true;

    messages.forEach(function(message) {
      try {
        const from    = message.getFrom();
        const subject = message.getSubject();
        if (from.indexOf('FedEx.Support@fadv.com') === -1) { skipped++; return; }
        if (subject.indexOf('Requires Additional Information') === -1) { skipped++; return; }

        const body = message.getPlainBody() || message.getBody();
        var nameMatch   = body.match(/Candidate:\s*(.+)/i);
        var orderMatch  = body.match(/Order Number:\s*(\S+)/i);
        var linkMatch   = body.match(/(https:\/\/pa\.fadv\.com\/#\/invite\/[^\s<"]+)/i);
        var expiryMatch = body.match(/This link expires on\s+(.+)/i);
        var reasonMatch = body.match(/Requested Information[\s\S]*?(?:<\/?\w[^>]*>|\n\s*\n)([\s\S]*?)(?:Next Step|$)/i);

        var candidateName = nameMatch  ? nameMatch[1].trim()  : '';
        var orderNumber   = orderMatch ? orderMatch[1].trim() : '';
        var profileLink   = linkMatch  ? linkMatch[1].trim()  : '';
        var expiryRaw     = expiryMatch? expiryMatch[1].trim(): '';
        var reason = reasonMatch
          ? reasonMatch[1].replace(/<[^>]+>/g,'').replace(/\s+/g,' ').trim().substring(0,200)
          : '';

        var expiryFormatted = '', expiryIso = '';
        if (expiryRaw) {
          var expDate = new Date(expiryRaw);
          if (!isNaN(expDate.getTime())) {
            expiryFormatted = Utilities.formatDate(expDate, Session.getScriptTimeZone(), 'MMMM d, yyyy');
            expiryIso = Utilities.formatDate(expDate, 'UTC', "yyyy-MM-dd'T'HH:mm:ss'Z'");
          }
        }

        var candidate = null;
        if (orderNumber) {
          var r1 = supabaseGet_('candidates', {
            select: 'id,first_name,phone,background_status,fadv_action_required,fadv_action_sms_sent_at',
            'drug_test_id': 'eq.' + orderNumber, limit: 1
          });
          if (r1 && r1.length > 0) candidate = r1[0];
        }
        if (!candidate && candidateName) {
          var parts = candidateName.split(/\s+/);
          if (parts.length >= 2) {
            var r2 = supabaseGet_('candidates', {
              select: 'id,first_name,phone,background_status,fadv_action_required,fadv_action_sms_sent_at',
              'first_name': 'ilike.' + parts[0],
              'last_name':  'ilike.' + parts.slice(1).join(' '),
              limit: 1
            });
            if (r2 && r2.length > 0) candidate = r2[0];
          }
        }

        if (!candidate) {
          Logger.log('No match for Action-Required: ' + candidateName + ' Order: ' + orderNumber);
          logToSupabase_(message.getId(), candidateName, orderNumber, 'action_required', '', '', null, 'unmatched', 0, false, 'no_candidate_match');
          unmatched++; return;
        }

        var patchCode = supabasePatch_('candidates', 'id=eq.' + candidate.id, {
          background_status:    'Needs Further Review',
          fadv_action_required: true,
          fadv_action_reason:   reason,
          fadv_action_link:     profileLink,
          fadv_action_expires:  expiryIso || null
        });

        if (patchCode !== 200 && patchCode !== 204) {
          Logger.log('Action-Required patch failed (' + patchCode + ') for candidate ' + candidate.id);
          errors++; threadOk = false; return;
        }

        if (candidate.phone) {
          var smsBody = 'Hi ' + (candidate.first_name || '') + ', your background check is on hold.\n\n' +
            'FADV needs you to respond using this link -- do NOT reply to their email:\n' +
            profileLink + '\n\nDeadline: ' + expiryFormatted +
            '\n\nReason: ' + reason +
            '\n\nKai\nPEAKrecruiting';
          sendSms_(candidate.phone, smsBody);
        }

        supabasePatch_('candidates', 'id=eq.' + candidate.id, {
          fadv_action_sms_sent_at: new Date().toISOString()
        });
        sent++;
      } catch(e) {
        Logger.log('ERROR Action-Required message: ' + e.message);
        errors++; threadOk = false;
      }
    });

    thread.removeLabel(actionLabel);
    if (threadOk) {
      thread.addLabel(processedLabel);
      const kaiInbox = GmailApp.getUserLabelByName('Kai/Inbox');
      if (kaiInbox) thread.removeLabel(kaiInbox);
      thread.markRead();
      thread.moveToArchive();
    } else {
      thread.addLabel(failedLabel);
    }
  });

  Logger.log('Action-Required done -- Sent: ' + sent + ' | Skipped: ' + skipped +
             ' | Unmatched: ' + unmatched + ' | Errors: ' + errors);
}

// ── FADV ACTION FOLLOW-UP CRON ────────────────────────────────────────────────
function processFadvActionFups() {
  var now = new Date();
  var nowIso = now.toISOString();
  var candidates = supabaseGet_('candidates', {
    select: 'id,first_name,last_name,phone,client_id,status,fadv_action_required,fadv_action_link,fadv_action_expires,fadv_action_reason,fadv_action_sms_sent_at,fadv_action_fup1_sent_at,fadv_action_fup2_sent_at,fadv_action_fup3_sent_at',
    'fadv_action_required': 'eq.true',
    'fadv_action_expires':  'gt.' + nowIso,
    'status':               'not.in.(Rejected,Hired)',
    limit: 200
  });

  if (!candidates || candidates.length === 0) { Logger.log('No active action-required candidates'); return; }

  var fup1 = 0, fup2 = 0, fup3 = 0, escalated = 0;

  candidates.forEach(function(c) {
    if (!c.fadv_action_sms_sent_at || !c.phone) return;
    var sentAt = new Date(c.fadv_action_sms_sent_at);
    var daysSince = (now - sentAt) / (1000 * 60 * 60 * 24);
    var link = c.fadv_action_link || '';
    var expiryFormatted = c.fadv_action_expires
      ? Utilities.formatDate(new Date(c.fadv_action_expires), Session.getScriptTimeZone(), 'MMMM d, yyyy')
      : '';

    if (daysSince >= 4 && c.fadv_action_fup1_sent_at && c.fadv_action_fup2_sent_at && c.fadv_action_fup3_sent_at) {
      supabasePost_('action_items', {
        priority: '🔴', domain: 'PEAK Ops', category: 'OPS',
        task: 'FADV action -- no response after 3 FUPs: ' + (c.first_name||'') + ' ' + (c.last_name||'') + ' (' + (c.client_id||'') + ')',
        status: 'PENDING'
      });
      escalated++;
    } else if (daysSince >= 3 && !c.fadv_action_fup3_sent_at) {
      sendSms_(c.phone, (c.first_name||'') + ', final reminder. Background check cannot move forward until you respond to FADV.\n\nDeadline: ' + expiryFormatted + '\n' + link + '\n\nReply if you need help.\n\nKai\nPEAKrecruiting');
      supabasePatch_('candidates', 'id=eq.' + c.id, { fadv_action_fup3_sent_at: nowIso });
      fup3++;
    } else if (daysSince >= 2 && !c.fadv_action_fup2_sent_at) {
      sendSms_(c.phone, (c.first_name||'') + ', second reminder -- background check still on hold. Deadline: ' + expiryFormatted + '\n' + link + '\n\nKai\nPEAKrecruiting');
      supabasePatch_('candidates', 'id=eq.' + c.id, { fadv_action_fup2_sent_at: nowIso });
      fup2++;
    } else if (daysSince >= 1 && !c.fadv_action_fup1_sent_at) {
      sendSms_(c.phone, (c.first_name||'') + ', your background check is still on hold.\n\nFADV is waiting on your response. Deadline: ' + expiryFormatted + '\n' + link + '\n\nKai\nPEAKrecruiting');
      supabasePatch_('candidates', 'id=eq.' + c.id, { fadv_action_fup1_sent_at: nowIso });
      fup1++;
    }
  });

  Logger.log('FUP done -- FUP1: ' + fup1 + ' | FUP2: ' + fup2 + ' | FUP3: ' + fup3 + ' | Escalated: ' + escalated);
}

// ── TWILIO SMS ────────────────────────────────────────────────────────────────
function sendSms_(toPhone, body) {
  var props = PropertiesService.getScriptProperties();
  var sid   = props.getProperty('TWILIO_ACCOUNT_SID');
  var token = props.getProperty('TWILIO_AUTH_TOKEN');
  var from  = props.getProperty('TWILIO_FROM_NUMBER');
  if (!sid || !token || !from) { Logger.log('Twilio credentials missing'); return; }
  var digits = toPhone.replace(/\D/g,'');
  if (digits.length === 10) digits = '1' + digits;
  var res = UrlFetchApp.fetch('https://api.twilio.com/2010-04-01/Accounts/' + sid + '/Messages.json', {
    method: 'POST',
    headers: { 'Authorization': 'Basic ' + Utilities.base64Encode(sid + ':' + token) },
    payload: { 'To': '+' + digits, 'From': from, 'Body': body },
    muteHttpExceptions: true
  });
  var code = res.getResponseCode();
  if (code < 200 || code >= 300) Logger.log('SMS failed (' + code + '): ' + res.getContentText().substring(0,200));
  else Logger.log('SMS sent to +' + digits);
}

// ── LABEL HELPER ─────────────────────────────────────────────────────────────
function getOrCreateLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

// ── ONE-TIME BACKLOG UTILS ────────────────────────────────────────────────────
function labelBacklog() {
  const processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  const threads = GmailApp.search('from:EntAdv.DoNotReply@fadv.com -label:FADV/Pending -label:FADV/Processed');
  Logger.log('Backlog threads: ' + threads.length);
  threads.forEach(function(t) { t.addLabel(processedLabel); t.markRead(); t.moveToArchive(); });
  Logger.log('Done');
}

function processBacklogNow() {
  const pendingLabel = getOrCreateLabel_(LABEL_PENDING);
  const threads = GmailApp.search('from:EntAdv.DoNotReply@fadv.com -label:FADV/Pending -label:FADV/Processed');
  Logger.log('Backlog threads found: ' + threads.length);
  threads.forEach(function(t) { t.addLabel(pendingLabel); });
  Logger.log('All labeled FADV/Pending -- running parser');
  processFadvEmails();
}

// ── RETRY FAILED EMAILS ───────────────────────────────────────────────────────
function retryFailed() {
  const failedLabel  = getOrCreateLabel_(LABEL_FAILED);
  const pendingLabel = getOrCreateLabel_(LABEL_PENDING);
  const threads = failedLabel.getThreads(0, 50);
  Logger.log('Retrying ' + threads.length + ' failed threads');
  threads.forEach(function(t) {
    t.removeLabel(failedLabel);
    t.addLabel(pendingLabel);
  });
  if (threads.length > 0) processFadvEmails();
}

// ── FADV PROFILE COMPLETION PARSER v2 ────────────────────────────────────────
// Fires when candidate completes their FADV profile
// Upgrades per Ops spec:
//   - Sets background_status = 'In Progress'
//   - Stamps fadv_profile_completed_at from email date (NOT NOW())
//   - If fadv_submitted_at is NULL, stamps it from email date (AO submitted directly)
//   - Creates TECH_FADV action item for portal approval
//   - SMS to Kai as secondary notification
//   - Unmatched: creates CANDIDATE_OPS action item
function processFadvProfileCompletions() {
  var SEARCH_QUERY = 'from:do_not_reply@fadv.com subject:"Application Completed Notification" -label:FADV/Processed';
  var processedLabel = getOrCreateLabel_(LABEL_PROCESSED);

  var CLIENT_MAP = {
    'cbm':                     'CBM Logistics (042443PWO)',
    'cnf_services':            'CNF Services (042443/V0009926)',
    'gods_vision':             'Gods Vision (042443JVP)',
    'legacy_chattanooga':      'Legacy Logistics (042443HNB)',
    'legacy_tuscaloosa':       'Legacy Logistics (042443HNB)',
    'legacy_chattanooga_east': 'Legacy Logistics (042443HNB)',
    'solpac':                  'Solpac (042443sdp)'
  };

  var KAI_PHONE = '4043862799';

  var threads = GmailApp.search(SEARCH_QUERY, 0, 50);
  Logger.log('[FadvProfile] Found ' + threads.length + ' completion email(s).');
  if (!threads.length) return;

  threads.forEach(function(thread) {
    var msg      = thread.getMessages()[0];
    var body     = msg.getPlainBody();
    var emailDate = msg.getDate().toISOString(); // use email date, not NOW()

    // Email format: "Brandon Anthony (email@...) completed their online profile"
    var nameEmailMatch = body.match(/([A-Za-z\s]+?)\s*\(([^\)]+)\)\s+completed/);
    var candName  = nameEmailMatch ? nameEmailMatch[1].trim() : null;
    var candEmail = nameEmailMatch ? nameEmailMatch[2].trim().toLowerCase() : null;

    Logger.log('[FadvProfile] Name: ' + candName + ' | Email: ' + candEmail + ' | Date: ' + emailDate);

    // Match candidate
    var candidate = null;
    if (candEmail) {
      var r1 = supabaseGet_('candidates', { 'personal_email': 'eq.' + candEmail, 'select': 'id,first_name,last_name,phone,client_id,fadv_submitted_at,background_status', 'limit': '1' });
      if (r1 && r1.length) candidate = r1[0];
      if (!candidate) {
        var r2 = supabaseGet_('candidates', { 'email': 'eq.' + candEmail, 'select': 'id,first_name,last_name,phone,client_id,fadv_submitted_at,background_status', 'limit': '1' });
        if (r2 && r2.length) candidate = r2[0];
      }
    }
    if (!candidate && candName) {
      var parts = candName.split(/\s+/);
      if (parts.length >= 2) {
        var r3 = supabaseGet_('candidates', { 'first_name': 'ilike.' + parts[0], 'last_name': 'ilike.' + parts[parts.length-1], 'select': 'id,first_name,last_name,phone,client_id,fadv_submitted_at,background_status', 'limit': '1' });
        if (r3 && r3.length) candidate = r3[0];
      }
    }

    // Unmatched -- create CANDIDATE_OPS action item
    if (!candidate) {
      Logger.log('[FadvProfile] No match for: ' + candName + ' / ' + candEmail);
      supabasePost_('action_items', {
        task: 'Unmatched FADV completion: ' + candName + ' -- identify and add to Supabase. Email: ' + candEmail,
        priority: '🟡',
        category: 'CANDIDATE_OPS',
        domain: 'PEAK Ops',
        status: 'PENDING'
      });
      thread.addLabel(processedLabel);
      return;
    }

    var company  = CLIENT_MAP[candidate.client_id] || candidate.client_id || 'Unknown client';
    var fullName = (candidate.first_name || '') + ' ' + (candidate.last_name || '');

    // Build patch payload
    // Idempotency guard: only advance to In Progress if not already past this stage
    var BG_ADVANCE_ALLOWED = ['Not Started', 'Intake', null, undefined, ''];
    if (candidate.background_status && BG_ADVANCE_ALLOWED.indexOf(candidate.background_status) === -1) {
      Logger.log('[FadvProfile] Skipping background_status write -- already at ' + candidate.background_status + ' for ' + fullName);
    }
    var patch = {
      fadv_profile_completed_at: emailDate,
      updated_at:               new Date().toISOString()
    };
    if (BG_ADVANCE_ALLOWED.indexOf(candidate.background_status) !== -1) {
      patch.background_status = 'In Progress';
    }

    // If fadv_submitted_at is NULL, AO submitted directly -- stamp from email date
    if (!candidate.fadv_submitted_at) {
      patch.fadv_submitted_at = emailDate;
      Logger.log('[FadvProfile] fadv_submitted_at was null -- stamping from email date (AO direct submit)');
    }

    var code = supabasePatch_('candidates', 'id=eq.' + candidate.id, patch);
    if (code !== 200 && code !== 204) {
      Logger.log('[FadvProfile] ERROR: Supabase patch failed (' + code + ') for candidate ' + candidate.id);
      alertForge_('[FadvProfile] write failed: ' + fullName, 'Patch returned ' + code + ' for candidate ' + candidate.id);
    } else {
      Logger.log('[FadvProfile] Supabase updated: ' + fullName + ' -> In Progress');
    }

    // Create TECH_FADV action item for portal approval
    supabasePost_('action_items', {
      task: 'FADV portal approval needed: ' + fullName.trim() + ' (' + (candidate.client_id || 'unknown') + ') -- log into ' + company + ' to approve.',
      priority: '🔴',
      category: 'TECH_FADV',
      domain: 'PEAK Ops',
      status: 'PENDING'
    });

    // SMS Kai
    sendSms_(KAI_PHONE, fullName.trim() + ' -- ' + company + ' -- FADV profile ready. Log in to approve.');
    Logger.log('[FadvProfile] SMS + action item created for ' + fullName);

    thread.addLabel(processedLabel);
    Logger.log('[FadvProfile] Done: ' + fullName);
  });
}

// ── SAP CANCELLATION PARSER ───────────────────────────────────────────────────
// Watches: Fedex.notifications@fadv.com subject "FedEx Order Cancellation - SAP"
// Action: set Ineligible/Rejected, create CANDIDATE_OPS action item
function processSAPCancellations() {
  var SAP_LABEL    = 'FADV/Pending';
  var processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  var failedLabel    = getOrCreateLabel_(LABEL_FAILED);

  var threads = GmailApp.search(
    'from:Fedex.notifications@fadv.com subject:"FedEx Order Cancellation - SAP" -label:FADV/Processed',
    0, 50
  );
  Logger.log('[SAP] Found ' + threads.length + ' SAP cancellation(s).');
  if (!threads.length) return;

  threads.forEach(function(thread) {
    var msg  = thread.getMessages()[0];
    var body = msg.getPlainBody() || msg.getBody();
    var subj = msg.getSubject();

    // Extract candidate name from subject or body
    var nameMatch = subj.match(/SAP[:\s]+(.+)/i) || body.match(/Candidate[:\s]+(.+)/i) || body.match(/Name[:\s]+(.+)/i);
    var candName  = nameMatch ? nameMatch[1].trim() : '';

    Logger.log('[SAP] Candidate: ' + candName);

    // Match candidate
    var candidate = null;
    if (candName) {
      var parts = candName.split(/\s+/);
      if (parts.length >= 2) {
        var r1 = supabaseGet_('candidates', {
          'first_name': 'ilike.' + parts[0],
          'last_name':  'ilike.' + parts[parts.length - 1],
          'select':     'id,first_name,last_name,client_id,phone',
          'limit':      '1'
        });
        if (r1 && r1.length) candidate = r1[0];
      }
    }

    if (!candidate) {
      Logger.log('[SAP] No match for: ' + candName);
      supabasePost_('action_items', {
        task: 'SAP cancellation -- unmatched candidate: ' + candName + ' -- find and update manually.',
        priority: '🔴',
        category: 'CANDIDATE_OPS',
        domain: 'PEAK Ops',
        status: 'PENDING'
      });
      thread.addLabel(processedLabel);
      return;
    }

    var fullName = (candidate.first_name || '') + ' ' + (candidate.last_name || '');

    // Set Ineligible + Rejected
    var code = supabasePatch_('candidates', 'id=eq.' + candidate.id, {
      background_status: 'Case Canceled',
      status:            'Rejected',
      rejection_source:  'compliance',
      reject_reason:     'SAP_disqualification',
      updated_at:        new Date().toISOString()
    });

    if (code !== 200 && code !== 204) {
      Logger.log('[SAP] ERROR: patch failed (' + code + ') for ' + candidate.id);
      thread.addLabel(failedLabel);
      return;
    }

    Logger.log('[SAP] Rejected: ' + fullName + ' (SAP disqualification)');

    // Create CANDIDATE_OPS action item to notify AO
    supabasePost_('action_items', {
      task: 'SAP cancellation -- notify AO: ' + fullName.trim() + ' (' + (candidate.client_id || 'unknown') + ') -- SAP disqualification from FADV.',
      priority: '🔴',
      category: 'CANDIDATE_OPS',
      domain: 'PEAK Ops',
      status: 'PENDING'
    });

    thread.addLabel(processedLabel);
    Logger.log('[SAP] Done: ' + fullName);
  });
}

// ── FORMFOX DRUG ORDER PARSER ─────────────────────────────────────────────────
// Watches: DoNotReply@noti.fadv.com (FormFox drug orders)
// Action: stamp drug_screen_ordered_at, note 5-day collection window
function processFormFoxOrders() {
  var processedLabel = getOrCreateLabel_(LABEL_PROCESSED);
  var failedLabel    = getOrCreateLabel_(LABEL_FAILED);

  var threads = GmailApp.search(
    'from:DoNotReply@noti.fadv.com -label:FADV/Processed',
    0, 50
  );
  Logger.log('[FormFox] Found ' + threads.length + ' FormFox order(s).');
  if (!threads.length) return;

  threads.forEach(function(thread) {
    var msg  = thread.getMessages()[0];
    var body = msg.getPlainBody() || msg.getBody();
    var emailDate = msg.getDate().toISOString();

    // Extract candidate name and order number
    var nameMatch  = body.match(/(?:Donor|Candidate|Name)[:\s]+([A-Za-z\s,]+)/i);
    var orderMatch = body.match(/(?:Order|Specimen|ID)[:\s#]+([A-Z0-9\-]+)/i);

    var candName    = nameMatch  ? nameMatch[1].trim()  : '';
    var orderNumber = orderMatch ? orderMatch[1].trim() : '';

    Logger.log('[FormFox] Name: ' + candName + ' | Order: ' + orderNumber);

    // Match candidate
    var candidate = null;
    if (orderNumber) {
      var r1 = supabaseGet_('candidates', {
        'drug_test_id': 'eq.' + orderNumber,
        'select':       'id,first_name,last_name,client_id,phone',
        'limit':        '1'
      });
      if (r1 && r1.length) candidate = r1[0];
    }
    if (!candidate && candName) {
      var parts = candName.replace(/,/g, '').split(/\s+/);
      if (parts.length >= 2) {
        var r2 = supabaseGet_('candidates', {
          'first_name': 'ilike.' + parts[0],
          'last_name':  'ilike.' + parts[parts.length - 1],
          'select':     'id,first_name,last_name,client_id,phone',
          'limit':      '1'
        });
        if (r2 && r2.length) candidate = r2[0];
      }
    }

    if (!candidate) {
      Logger.log('[FormFox] No match: ' + candName + ' / ' + orderNumber);
      supabasePost_('action_items', {
        task: 'FormFox drug order -- unmatched: ' + candName + ' Order: ' + orderNumber + ' -- 5 days to report to collection site.',
        priority: '🟡',
        category: 'CANDIDATE_OPS',
        domain: 'PEAK Ops',
        status: 'PENDING'
      });
      thread.addLabel(processedLabel);
      return;
    }

    var fullName = (candidate.first_name || '') + ' ' + (candidate.last_name || '');

    // Stamp drug_screen_ordered_at from email date
    var patch = {
      drug_screen_ordered_at: emailDate,
      drug_test_status:       'In Progress',
      updated_at:             new Date().toISOString()
    };
    if (orderNumber) patch.drug_test_id = orderNumber;

    var code = supabasePatch_('candidates', 'id=eq.' + candidate.id, patch);
    if (code !== 200 && code !== 204) {
      Logger.log('[FormFox] ERROR: patch failed (' + code + ') for ' + candidate.id);
      thread.addLabel(failedLabel);
      return;
    }

    Logger.log('[FormFox] Stamped drug_screen_ordered_at for ' + fullName + ' -- 5 days to report.');
    thread.addLabel(processedLabel);
  });
}
// deploy trigger 2026-04-16
