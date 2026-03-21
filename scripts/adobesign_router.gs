/**
 * PEAKATS Adobe Sign Router
 * Runs on a 15-minute trigger. Searches for Adobe Sign "Signed and Filed" emails
 * and dispatches to the correct processor based on subject keyword.
 *
 * Routing table is extensible -- add new keywords/handlers as document types grow.
 * Deploy: paste into Google Apps Script editor at script.google.com
 * Trigger: set time-driven trigger -> every 15 minutes on routeAdobeSignEmails
 */

// -- CONFIG ------------------------------------------------------------------
var SUPABASE_URL = 'https://eyopvsmsvbgfuffscfom.supabase.co';
var SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4';

// -- ROUTING TABLE -----------------------------------------------------------
var ROUTES = [
  { keyword: 'GCIC', label: 'AdobeSign/GCIC-Processed', handler: processGCIC_ },
  { keyword: 'SOW',  label: 'AdobeSign/SOW-Processed',  handler: processSOW_ },
];

var UNKNOWN_LABEL = 'AdobeSign/Unknown';

// -- SUPABASE HELPERS --------------------------------------------------------
var SB_HEADERS = {
  'apikey': SUPABASE_KEY,
  'Authorization': 'Bearer ' + SUPABASE_KEY,
  'Content-Type': 'application/json',
  'Prefer': 'return=minimal'
};

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

// -- BUILD EXCLUSION QUERY ---------------------------------------------------
function buildExclusionQuery_() {
  var excludes = ROUTES.map(function(r) { return '-label:' + r.label; }).join(' ');
  return excludes + ' -label:' + UNKNOWN_LABEL;
}

// -- MAIN ENTRY POINT --------------------------------------------------------
function routeAdobeSignEmails() {
  var query = 'from:adobesign@adobesign.com subject:"Signed and Filed" ' + buildExclusionQuery_();
  var threads = GmailApp.search(query, 0, 50);

  if (threads.length === 0) {
    Logger.log('[Router] No new Adobe Sign emails found.');
    return;
  }

  Logger.log('[Router] Found ' + threads.length + ' thread(s) to route.');

  for (var t = 0; t < threads.length; t++) {
    var messages = threads[t].getMessages();
    for (var m = 0; m < messages.length; m++) {
      var msg = messages[m];
      var subject = msg.getSubject();
      var subjectUpper = subject.toUpperCase();

      // Find matching route
      var matched = false;
      for (var r = 0; r < ROUTES.length; r++) {
        if (subjectUpper.indexOf(ROUTES[r].keyword.toUpperCase()) !== -1) {
          try {
            ROUTES[r].handler(msg, subject);
          } catch (e) {
            Logger.log('[Router] Error in ' + ROUTES[r].keyword + ' handler: ' + e.message);
          }
          threads[t].addLabel(getOrCreateLabel_(ROUTES[r].label));
          matched = true;
          break;
        }
      }

      // No route matched -- unknown document type
      if (!matched) {
        Logger.log('[Router] Unknown document type: ' + subject);
        threads[t].addLabel(getOrCreateLabel_(UNKNOWN_LABEL));
        logUnknownDocument_(subject);
      }
    }
  }

  Logger.log('[Router] Done. Routed ' + threads.length + ' thread(s).');
}

// -- HANDLER: GCIC -----------------------------------------------------------
// Full logic lives in gcic_adobe_trigger.gs -- this router delegates to that processor.
// The standalone gcic_adobe_trigger.gs handles GCIC fully via its own trigger.
// This stub exists so the router can label and log GCIC emails it encounters.
function processGCIC_(msg, subject) {
  Logger.log('[Router] GCIC processor triggered for: ' + subject);
  // GCIC processing handled by gcic_adobe_trigger.gs on its own 15-min trigger.
  // This handler only applies the label so the router knows it was seen.
}

// -- HANDLER: SOW ------------------------------------------------------------
function processSOW_(msg, subject) {
  Logger.log('[Router] SOW signed: ' + subject + ' -- no automation yet, manual review required');

  // Extract client/signer name from subject
  var nameMatch = subject.match(/and\s+(.+?)\s+is\s+Signed/i);
  var signerName = nameMatch ? nameMatch[1].trim() : 'Unknown';

  var today = Utilities.formatDate(new Date(), 'America/New_York', 'yyyy-MM-dd');

  sbInsert_('forge_memory', {
    'session_date': today,
    'category': 'client_intel',
    'subject': 'SOW Signed: ' + signerName,
    'content': 'Statement of Work signed via Adobe Sign. '
      + 'Signer: ' + signerName + '. '
      + 'Email subject: ' + subject + '. '
      + 'Manual review required -- no automation for SOW processing yet.',
    'target_thread': 'PEAK Biz',
    'tags': ['sow', 'signed', 'manual_review']
  });

  Logger.log('[Router] Logged SOW signed by: ' + signerName);
}

// -- LOG UNKNOWN DOCUMENT ----------------------------------------------------
function logUnknownDocument_(subject) {
  var today = Utilities.formatDate(new Date(), 'America/New_York', 'yyyy-MM-dd');

  sbInsert_('forge_memory', {
    'session_date': today,
    'category': 'ops_note',
    'subject': 'Unknown Adobe Sign Document',
    'content': 'Adobe Sign "Signed and Filed" email received but did not match any known document type (GCIC, SOW). '
      + 'Email subject: ' + subject + '. '
      + 'Manual review required.',
    'target_thread': 'PEAK Ops',
    'tags': ['adobesign', 'unknown', 'manual_review']
  });
}
