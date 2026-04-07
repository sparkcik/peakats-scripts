/**
 * gcic_adobe_trigger.gs -- HARDENED v5
 *
 * Fix 1: Fuzzy name matching -- strips middle names/suffixes, iterates
 *         possible last name positions, handles no-space usernames like "brandonhadley"
 * Fix 2: Auto-submit signed PDF to casedocuments@fadv.com after Supabase update
 *
 * Trigger: Time-based, every 15 minutes
 * Searches: GCIC/Pending label for Adobe Sign "Signed and Filed" emails
 */

// ── CONFIG ────────────────────────────────────────────────────────────────────
var SUPABASE_URL_G     = 'https://eyopvsmsvbgfuffscfom.supabase.co';
var SUPABASE_KEY_G     = PropertiesService.getScriptProperties().getProperty('SUPABASE_KEY');
var FADV_EMAIL         = 'casedocuments@fadv.com';
var ALERT_EMAIL_G      = 'kai@peakrecruitingco.com';
var GCIC_PENDING_LBL   = 'GCIC/Pending';
var GCIC_PROCESSED_LBL = 'AdobeSign/GCIC-Processed';
var GCIC_FAILED_LBL    = 'GCIC/Failed';

var FADV_CSP_MAP = {
  'cbm':                     '042443PWO',
  'cnf_services':            '042443/V0009926',
  'gods_vision':             '042443JVP',
  'legacy_chattanooga':      '042443HNB',
  'legacy_tuscaloosa':       '042443HNB',
  'legacy_chattanooga_east': '042443HNB',
  'solpac':                  '042443sdp'
};

var SB_HEADERS_G = {
  'apikey':        SUPABASE_KEY_G,
  'Authorization': 'Bearer ' + SUPABASE_KEY_G,
  'Content-Type':  'application/json',
  'Prefer':        'return=minimal'
};

// ── MAIN ─────────────────────────────────────────────────────────────────────
function processSignedGCICEmails() {
  var pendingLabel   = GmailApp.getUserLabelByName(GCIC_PENDING_LBL);
  var processedLabel = getOrCreateLabel_(GCIC_PROCESSED_LBL);
  var failedLabel    = getOrCreateLabel_(GCIC_FAILED_LBL);

  if (!pendingLabel) {
    Logger.log('[GCIC] GCIC/Pending label not found -- nothing to process.');
    return;
  }

  var threads = pendingLabel.getThreads(0, 50);
  Logger.log('[GCIC] ' + threads.length + ' thread(s) in GCIC/Pending.');

  threads.forEach(function(thread) {
    var msg     = thread.getMessages()[0];
    var subject = msg.getSubject();
    Logger.log('[GCIC] Processing: ' + subject);

    // Extract raw candidate name from subject
    // Format: "... between Kai M Clarke and [NAME] is Signed and Filed!"
    var nameMatch = subject.match(/between\s+Kai M\s+Clarke\s+and\s+(.+?)\s+is\s+Signed/i)
                 || subject.match(/and\s+(.+?)\s+is\s+Signed/i);

    if (!nameMatch) {
      Logger.log('[GCIC] Could not extract name from subject: ' + subject);
      moveThread_(thread, pendingLabel, failedLabel, 'Could not extract name from subject');
      return;
    }

    var rawName = nameMatch[1].trim();
    Logger.log('[GCIC] Raw name from subject: ' + rawName);

    // Fix 1: Fuzzy match handles middle names, suffixes, and no-space usernames
    var candidate = findCandidateFuzzy_(rawName);

    if (!candidate) {
      Logger.log('[GCIC] No candidate match for: ' + rawName);
      sbInsert_g('forge_memory', {
        session_date: new Date().toISOString().split('T')[0],
        category:     'unmatched_gcic',
        subject:      'Unmatched GCIC signature: ' + rawName,
        content:      'Adobe Sign GCIC email received but no candidate match. Raw name: ' + rawName + ' | Subject: ' + subject,
        target_thread: 'PEAK Ops'
      });
      alertForge_g('GCIC no candidate match: ' + rawName,
        'Adobe Sign GCIC signed but no candidate found in DB.\nSubject: ' + subject + '\nManual: find candidate, update gcic fields in Supabase.');
      moveThread_(thread, pendingLabel, failedLabel, 'No candidate match: ' + rawName);
      return;
    }

    Logger.log('[GCIC] Matched: ' + candidate.id + ' -- ' + candidate.first_name + ' ' + candidate.last_name + ' (' + candidate.client_id + ')');

    // Fix 4: Idempotency -- skip if already submitted to FADV
    if (candidate.gcic_stage === 'SUBMITTED_TO_FADV') {
      Logger.log('[GCIC] Already submitted to FADV -- skipping duplicate: ' + candidate.id);
      moveThread_(thread, pendingLabel, processedLabel, null);
      return;
    }

    // Update Supabase
    var now  = new Date().toISOString();
    var code = sbPatch_g('candidates', 'id=eq.' + candidate.id, {
      gcic_uploaded:       1,
      gcic_email_sent:     1,
      gcic_form_completed: 1,
      gcic_status:         'COMPLETE',
      gcic_stage:          'SUBMITTED_TO_FADV',
      updated_at:          now
    });

    if (code !== 200 && code !== 204) {
      Logger.log('[GCIC] ERROR: Supabase patch failed for candidate ' + candidate.id + ' code ' + code);
      moveThread_(thread, pendingLabel, failedLabel, 'Supabase patch failed: code ' + code);
      return;
    }

    Logger.log('[GCIC] Supabase updated for candidate ' + candidate.id);

    // Fix 2: Extract PDF and auto-submit to FADV
    var attachments = msg.getAttachments();
    var pdf = null;
    for (var i = 0; i < attachments.length; i++) {
      if (attachments[i].getContentType() === 'application/pdf') {
        pdf = attachments[i];
        break;
      }
    }

    if (!pdf) {
      Logger.log('[GCIC] No PDF attachment found -- cannot auto-submit to FADV.');
      alertForge_g('GCIC PDF missing -- manual submit needed',
        'Candidate: ' + candidate.first_name + ' ' + candidate.last_name +
        ' (' + candidate.client_id + ') | Supabase updated but no PDF in email.');
    } else {
      var cspId    = FADV_CSP_MAP[candidate.client_id] || 'UNKNOWN';
      var fullName = candidate.first_name + ' ' + candidate.last_name;
      var fadvSubj = 'GCIC Authorization -- ' + fullName + ' -- ' + cspId;

      try {
        GmailApp.sendEmail(
          FADV_EMAIL,
          fadvSubj,
          'Please find attached the executed GCIC authorization form for the above-referenced candidate.\n\n' +
          'This document was electronically signed via Adobe Acrobat Sign and includes IP capture per GCIC requirements.\n\n' +
          'Kai\nPEAKrecruiting',
          { attachments: [pdf] }
        );
        Logger.log('[GCIC] PDF submitted to FADV: ' + fadvSubj);
      } catch(e) {
        Logger.log('[GCIC] ERROR sending to FADV: ' + e.message);
        alertForge_g('GCIC FADV send failed',
          'Candidate: ' + fullName + ' | Error: ' + e.message + ' | Manual submit required.');
      }
    }

    // Mark processed
    moveThread_(thread, pendingLabel, processedLabel, null);
    Logger.log('[GCIC] Done: ' + candidate.first_name + ' ' + candidate.last_name);
  });
}

// ── FIX 1: Fuzzy name matching ────────────────────────────────────────────────
function findCandidateFuzzy_(rawName) {
  // Strip suffixes
  var cleaned = rawName
    .replace(/\b(jr\.?|sr\.?|ii|iii|iv|esq\.?)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim();

  // Handle no-space usernames like "brandonhadley" -- try camelCase split
  // If single token, try to split on case boundary or just use as first name search
  var parts = cleaned.split(' ');

  if (parts.length === 1) {
    // Single word -- try direct first+last split by trying substrings
    var word = parts[0].toLowerCase();
    // Try all split points
    for (var i = 3; i < word.length - 2; i++) {
      var tryFirst = word.substring(0, i);
      var tryLast  = word.substring(i);
      var r = sbGet_g('candidates', {
        'first_name': 'ilike.' + tryFirst,
        'last_name':  'ilike.' + tryLast,
        'select':     'id,first_name,last_name,phone,client_id,gcic_stage',
        'limit':      '1'
      });
      if (r && r.length) return r[0];
    }
    // Fallback: first name only
    return sbFindByFirst_(word);
  }

  var first = parts[0];
  var last  = parts[parts.length - 1];

  // Try exact first + last
  var r1 = sbGet_g('candidates', {
    'first_name': 'ilike.' + first,
    'last_name':  'ilike.' + last,
    'select':     'id,first_name,last_name,phone,client_id,gcic_stage',
    'limit':      '1'
  });
  if (r1 && r1.length) return r1[0];

  // Try all word positions as last name (handles middle names)
  for (var j = 1; j < parts.length; j++) {
    var r2 = sbGet_g('candidates', {
      'first_name': 'ilike.' + first,
      'last_name':  'ilike.' + parts[j],
      'select':     'id,first_name,last_name,phone,client_id,gcic_stage',
      'limit':      '1'
    });
    if (r2 && r2.length) return r2[0];
  }

  return null;
}

function sbFindByFirst_(first) {
  var r = sbGet_g('candidates', {
    'first_name': 'ilike.' + first,
    'select':     'id,first_name,last_name,phone,client_id,gcic_stage',
    'limit':      '1'
  });
  return r && r.length ? r[0] : null;
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function moveThread_(thread, fromLabel, toLabel, errorNote) {
  try { thread.removeLabel(fromLabel); } catch(e) {}
  try { thread.addLabel(toLabel); }     catch(e) {}
  if (errorNote) Logger.log('[GCIC] -> ' + toLabel.getName() + ': ' + errorNote);
}

function alertForge_g(subject, body) {
  try {
    GmailApp.sendEmail(ALERT_EMAIL_G, '[PEAK ALERT] ' + subject, body);
  } catch(e) {
    Logger.log('[GCIC] Alert send failed: ' + e.message);
  }
}

function getOrCreateLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

function sbGet_g(path, params) {
  var qs = Object.keys(params).map(function(k) {
    return encodeURIComponent(k) + '=' + encodeURIComponent(params[k]);
  }).join('&');
  var resp = UrlFetchApp.fetch(SUPABASE_URL_G + '/rest/v1/' + path + '?' + qs, {
    method: 'get', headers: SB_HEADERS_G, muteHttpExceptions: true
  });
  if (resp.getResponseCode() !== 200) return null;
  return JSON.parse(resp.getContentText());
}

function sbPatch_g(path, filter, body) {
  var resp = UrlFetchApp.fetch(SUPABASE_URL_G + '/rest/v1/' + path + '?' + filter, {
    method: 'patch', headers: SB_HEADERS_G,
    payload: JSON.stringify(body), muteHttpExceptions: true
  });
  return resp.getResponseCode();
}

function sbInsert_g(path, body) {
  var resp = UrlFetchApp.fetch(SUPABASE_URL_G + '/rest/v1/' + path, {
    method: 'post', headers: SB_HEADERS_G,
    payload: JSON.stringify(body), muteHttpExceptions: true
  });
  return resp.getResponseCode();
}
