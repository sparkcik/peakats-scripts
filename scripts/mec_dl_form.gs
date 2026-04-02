/**
 * PEAK Recruiting -- MEC/DL Form Apps Script v11 (HARDENED)
 * Attached to: MEC/DL Response Sheet (spreadsheet-bound)
 *
 * v11 fixes: dead send-sms endpoint replaced with sms_send_queue insert,
 *            sbUpdate failure now alerts and halts, sbInsert checks response.
 */

var CONFIG = {
  SUPABASE_URL: 'https://eyopvsmsvbgfuffscfom.supabase.co',
  SUPABASE_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4',
  ALERT_EMAIL:  'kai@peakrecruitingco.com',
  SHEET_ID:     '1zM8-Bh_flbnX2THnZcKa-2kcSBriPZXLLrQofSzpAq4',
  DOCS_ROOT:    '1UJfJM6ZMQo2RuVbNWrv4hkBiLnWAZkjB',
  UNMATCHED:    '1AcYp-Y98c5IYT17uNqEqHVuoA8kuv_5N',
  FROM_NUMBER:  '+14704704766'  // Twilio shadow number
};

function onFormSubmit(e) {
  try {
    var ts        = new Date().toISOString();
    var firstName = '';
    var lastName  = '';
    var phone     = '';
    var mecUrl    = '';
    var dlUrl     = '';

    Logger.log('=== MEC/DL v11 === keys: ' + Object.keys(e).join(', '));

    // PATH A: From form trigger (e.response available)
    if (e.response) {
      Logger.log('PATH A: form trigger');
      e.response.getItemResponses().forEach(function(item) {
        var title = item.getItem().getTitle().trim();
        var val   = (item.getResponse() || '').toString().trim();
        Logger.log('[' + title + '] = ' + val.substring(0, 100));
        if      (title === 'First Name')                              firstName = val;
        else if (title === 'Last Name')                               lastName  = val;
        else if (title === 'Phone Number')                            phone     = normalizePhone(val);
        else if (title === 'Medical Certificate (MEC)')               mecUrl    = val;
        else if (title === 'Driver License (front only)')             dlUrl     = val;
        else if (title === "Driver's License (photo of front only)")  dlUrl     = val;
      });

    // PATH B: From spreadsheet trigger (e.values available)
    } else if (e.values) {
      Logger.log('PATH B: spreadsheet trigger');
      var row     = e.values;
      var ss      = SpreadsheetApp.openById(CONFIG.SHEET_ID);
      var sheet   = ss.getSheets()[0];
      var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      var resp    = {};
      headers.forEach(function(h, i) { resp[h] = row[i] || ''; });
      Logger.log('Headers: ' + JSON.stringify(headers));
      firstName = (resp['First Name']  || '').trim();
      lastName  = (resp['Last Name']   || '').trim();
      phone     = normalizePhone(resp['Phone Number'] || '');
      mecUrl    = (resp['Medical Certificate (MEC)'] || '').trim();
      dlUrl     = (resp['Driver License (front only)'] || resp["Driver's License (photo of front only)"] || '').trim();

    } else {
      Logger.log('ERROR: unknown event object -- keys: ' + Object.keys(e).join(', '));
      return;
    }

    Logger.log('Name: ' + firstName + ' ' + lastName + ' | Phone: ' + phone);
    Logger.log('MEC: ' + mecUrl);
    Logger.log('DL:  ' + dlUrl);

    if (!phone && !firstName && !lastName) {
      handleMatchFailure_(firstName, lastName, phone, ts, mecUrl, dlUrl, 'No identifying info provided');
      return;
    }

    var match = findCandidate_(phone, firstName, lastName);
    Logger.log('Match: ' + JSON.stringify(match));

    if (match.found) {
      var c       = match.candidate;
      var dateStr = formatDateStamp_(new Date());

      var mecDriveUrl = null;
      var dlDriveUrl  = null;

      if (mecUrl) {
        var mecFileId = extractFileId_(mecUrl);
        if (mecFileId) {
          mecDriveUrl = renameAndMove_(mecFileId,
            buildName_(c.id, c.last_name || lastName, c.first_name || firstName, 'mec', dateStr),
            getOrCreateFolder_(getOrCreateFolder_(DriveApp.getFolderById(CONFIG.DOCS_ROOT), c.client_id), 'mec').getId()
          );
        }
      }

      if (dlUrl) {
        var dlFileId = extractFileId_(dlUrl);
        if (dlFileId) {
          dlDriveUrl = renameAndMove_(dlFileId,
            buildName_(c.id, c.last_name || lastName, c.first_name || firstName, 'dl', dateStr),
            getOrCreateFolder_(getOrCreateFolder_(DriveApp.getFolderById(CONFIG.DOCS_ROOT), c.client_id), 'dl').getId()
          );
        }
      }

      var updates = { mec_form_submitted_at: ts, mec_dl_collection_stage: 'COMPLETE' };
      if (mecDriveUrl || mecUrl) {
        updates.mec_storage_path = mecDriveUrl || mecUrl;
        updates.mec_uploaded_at  = ts;
        updates.mec_received_at  = ts;
        updates.mec_uploaded     = 1;
      }
      if (dlDriveUrl || dlUrl) {
        updates.dl_storage_path = dlDriveUrl || dlUrl;
        updates.dl_uploaded_at  = ts;
        updates.dl_received_at  = ts;
        updates.dl_verified     = 1;
      }

      // FIX: check DB write result before sending SMS
      var updateCode = sbUpdate_('candidates', c.id, updates);
      Logger.log('Update result: ' + updateCode);

      if (updateCode !== 200 && updateCode !== 204) {
        // FIX: alert and halt -- don't send SMS for a write that failed
        GmailApp.sendEmail(CONFIG.ALERT_EMAIL,
          '[PEAK ALERT] MEC/DL DB write failed: ' + firstName + ' ' + lastName,
          'Candidate ID ' + c.id + ' MEC/DL DB update failed (' + updateCode + ').\n' +
          'Files may be in Drive. Manual Supabase update required.\n' +
          'MEC: ' + (mecDriveUrl || mecUrl || 'none') + '\n' +
          'DL: ' + (dlDriveUrl || dlUrl || 'none'));
        return;
      }

      // FIX: replaced dead send-sms edge function with sms_send_queue insert
      sendConfirmationSms_(c.id, c.phone || phone, c.first_name || firstName);
      Logger.log('SUCCESS: ' + firstName + ' ' + lastName + ' -> ID ' + c.id);

    } else {
      var mecFid = mecUrl ? extractFileId_(mecUrl) : null;
      var dlFid  = dlUrl  ? extractFileId_(dlUrl)  : null;
      if (mecFid) moveTo_(mecFid, CONFIG.UNMATCHED);
      if (dlFid)  moveTo_(dlFid,  CONFIG.UNMATCHED);
      handleMatchFailure_(firstName, lastName, phone, ts, mecUrl, dlUrl, match.reason);
    }

  } catch (err) {
    Logger.log('FATAL: ' + err.toString() + '\n' + err.stack);
    GmailApp.sendEmail(CONFIG.ALERT_EMAIL, '[PEAK ALERT] MEC/DL Apps Script Error',
      'Error: ' + err.toString() + '\n\nStack: ' + err.stack);
  }
}

// ── FILE OPERATIONS ───────────────────────────────────────────────────────────

function extractFileId_(url) {
  if (!url) return null;
  var m = url.match(/[?&]id=([^&]+)/) || url.match(/\/d\/([^\/\?&]+)/);
  return m ? m[1] : null;
}

function buildName_(id, last, first, type, dateStr) {
  return [String(id),
    last.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, ''),
    first.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, ''),
    type, dateStr].join('_');
}

function getOrCreateFolder_(parent, name) {
  var iter = parent.getFoldersByName(name);
  return iter.hasNext() ? iter.next() : parent.createFolder(name);
}

function renameAndMove_(fileId, newBaseName, targetFolderId) {
  try {
    var file = DriveApp.getFileById(fileId);
    var ext  = file.getName().includes('.') ? '.' + file.getName().split('.').pop() : '';
    file.setName(newBaseName + ext);
    var target = DriveApp.getFolderById(targetFolderId);
    target.addFile(file);
    var parents = file.getParents();
    while (parents.hasNext()) {
      var p = parents.next();
      if (p.getId() !== targetFolderId) p.removeFile(file);
    }
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    return 'https://drive.google.com/file/d/' + file.getId() + '/view';
  } catch (err) {
    Logger.log('renameAndMove_ error: ' + err);
    return null;
  }
}

function moveTo_(fileId, targetFolderId) {
  try {
    var file   = DriveApp.getFileById(fileId);
    var target = DriveApp.getFolderById(targetFolderId);
    target.addFile(file);
    var parents = file.getParents();
    while (parents.hasNext()) {
      var p = parents.next();
      if (p.getId() !== targetFolderId) p.removeFile(file);
    }
  } catch (err) { Logger.log('moveTo_ error: ' + err); }
}

// ── MATCH FAILURE ─────────────────────────────────────────────────────────────

function handleMatchFailure_(firstName, lastName, phone, ts, mecUrl, dlUrl, reason) {
  // FIX: check response on insert
  var code = sbInsert_('form_match_failures', {
    form_type: 'mec_dl', submitted_phone: phone, submitted_at: ts,
    mec_drive_url: mecUrl || null, dl_drive_url: dlUrl || null, resolved: false
  });
  Logger.log('form_match_failures insert: ' + code);

  GmailApp.sendEmail(CONFIG.ALERT_EMAIL, '[PEAK ALERT] MEC/DL Form -- No Candidate Match', [
    'A candidate submitted the MEC/DL form but could not be matched.',
    '', 'Name:   ' + firstName + ' ' + lastName,
    'Phone:  ' + phone, 'Time:   ' + ts, 'Reason: ' + reason, '',
    mecUrl ? 'MEC: ' + mecUrl : 'MEC: not submitted',
    dlUrl  ? 'DL:  ' + dlUrl  : 'DL:  not submitted', '',
    'Files moved to UNMATCHED folder.',
    'ACTION: Manually update mec_storage_path and dl_storage_path on the candidate record.',
    '', '-- PEAK Automated Alert'
  ].join('\n'));
}

// ── SMS -- FIX: sms_send_queue insert (replaces dead send-sms edge function) ─

function sendConfirmationSms_(candidateId, phone, firstName) {
  if (!phone || phone.length < 10) {
    Logger.log('sendConfirmationSms_: no valid phone for candidate ' + candidateId);
    return;
  }

  var body = (firstName || '') + ', got it -- your MEC and Driver License are on file. ' +
    'You are one step closer to your start date.\n\nI will be in touch with next steps.\n\nKai\nPEAKrecruiting';

  var code = sbInsert_('sms_send_queue', {
    candidate_id:     candidateId,
    to_number:        phone,
    from_number:      CONFIG.FROM_NUMBER,
    body:             body,
    template_id:      19,
    template_name:    'MEC + DL Received - Confirmation',
    status:           'pending',
    migration_status: 'twilio_active',
    scheduled_for:    new Date().toISOString(),
    created_by:       'mec_dl_form_script'
  });

  if (code !== 200 && code !== 201 && code !== 204) {
    Logger.log('SMS queue insert failed (' + code + ') for candidate ' + candidateId);
    GmailApp.sendEmail(CONFIG.ALERT_EMAIL,
      '[PEAK ALERT] MEC/DL SMS queue failed: candidate ' + candidateId,
      'DB updated but confirmation SMS not queued. Send manually via PWA.\nPhone: ' + phone);
  } else {
    Logger.log('Confirmation SMS queued for candidate ' + candidateId);
  }
}

// ── CANDIDATE MATCHING ────────────────────────────────────────────────────────

function findCandidate_(phone, firstName, lastName) {
  if (phone && phone.length === 10) {
    var res = sbQuery_('candidates', 'id,first_name,last_name,phone,client_id',
                       'phone=eq.' + phone + '&limit=2');
    Logger.log('Phone query (' + phone + '): ' + JSON.stringify(res));
    if (Array.isArray(res) && res.length === 1) return { found: true, candidate: res[0] };
    if (Array.isArray(res) && res.length > 1) {
      var best = null, bestScore = 0;
      res.forEach(function(c) {
        var s = nameSim_(firstName, lastName, c.first_name, c.last_name);
        if (s > bestScore) { bestScore = s; best = c; }
      });
      if (best && bestScore >= 0.5) return { found: true, candidate: best };
      return { found: false, reason: 'Multiple phone matches, name ambiguous' };
    }
  }
  if (firstName && lastName) {
    var res2 = sbQuery_('candidates', 'id,first_name,last_name,phone,client_id',
                        'first_name=ilike.' + encodeURIComponent(firstName) +
                        '&last_name=ilike.'  + encodeURIComponent(lastName) + '&limit=2');
    Logger.log('Name query: ' + JSON.stringify(res2));
    if (Array.isArray(res2) && res2.length === 1) return { found: true, candidate: res2[0], lowConfidence: true };
    if (Array.isArray(res2) && res2.length > 1)  return { found: false, reason: 'Multiple name matches' };
  }
  return { found: false, reason: 'No match -- phone: ' + phone + ', name: ' + firstName + ' ' + lastName };
}

// ── SUPABASE ──────────────────────────────────────────────────────────────────

function sbHeaders_(prefer) {
  var h = {
    'apikey':        CONFIG.SUPABASE_KEY,
    'Authorization': 'Bearer ' + CONFIG.SUPABASE_KEY,
    'Content-Type':  'application/json'
  };
  if (prefer) h['Prefer'] = 'return=' + prefer;
  return h;
}

function sbQuery_(table, select, filters) {
  try {
    var r = UrlFetchApp.fetch(
      CONFIG.SUPABASE_URL + '/rest/v1/' + table + '?select=' + select + '&' + filters,
      { method: 'get', headers: sbHeaders_(), muteHttpExceptions: true }
    );
    var status = r.getResponseCode();
    var text   = r.getContentText();
    Logger.log('GET ' + status + ': ' + text.substring(0, 300));
    if (status !== 200) return [];
    var p = JSON.parse(text);
    return Array.isArray(p) ? p : [];
  } catch (err) { Logger.log('sbQuery_ error: ' + err); return []; }
}

// FIX: returns response code
function sbUpdate_(table, id, data) {
  var r = UrlFetchApp.fetch(
    CONFIG.SUPABASE_URL + '/rest/v1/' + table + '?id=eq.' + id,
    { method: 'patch', headers: sbHeaders_('minimal'), payload: JSON.stringify(data), muteHttpExceptions: true }
  );
  var code = r.getResponseCode();
  Logger.log('PATCH ' + code + ': ' + r.getContentText().substring(0, 200));
  return code;
}

// FIX: returns response code
function sbInsert_(table, data) {
  var r = UrlFetchApp.fetch(
    CONFIG.SUPABASE_URL + '/rest/v1/' + table,
    { method: 'post', headers: sbHeaders_('minimal'), payload: JSON.stringify(data), muteHttpExceptions: true }
  );
  var code = r.getResponseCode();
  if (code !== 200 && code !== 201 && code !== 204) {
    Logger.log('sbInsert_ error (' + code + ') on ' + table + ': ' + r.getContentText().substring(0, 300));
  }
  return code;
}

// ── UTILS ─────────────────────────────────────────────────────────────────────

function normalizePhone(raw) {
  var d = raw.replace(/\D/g, '');
  if (d.length === 11 && d[0] === '1') d = d.substring(1);
  return d.length === 10 ? d : '';
}

function formatDateStamp_(date) {
  return '' + date.getFullYear() +
    String(date.getMonth() + 1).padStart(2, '0') +
    String(date.getDate()).padStart(2, '0');
}

function nameSim_(fn1, ln1, fn2, ln2) {
  return (strSim_((fn1||'').toLowerCase(), (fn2||'').toLowerCase()) +
          strSim_((ln1||'').toLowerCase(), (ln2||'').toLowerCase())) / 2;
}

function strSim_(a, b) {
  if (!a || !b) return 0;
  if (a === b)  return 1;
  var len = Math.max(a.length, b.length);
  return len === 0 ? 1 : (len - editDist_(a, b)) / len;
}

function editDist_(s1, s2) {
  s1 = s1.toLowerCase(); s2 = s2.toLowerCase();
  var costs = [];
  for (var i = 0; i <= s1.length; i++) {
    var last = i;
    for (var j = 0; j <= s2.length; j++) {
      if (i === 0) { costs[j] = j; }
      else if (j > 0) {
        var nv = costs[j-1];
        if (s1[i-1] !== s2[j-1]) nv = Math.min(Math.min(nv, last), costs[j]) + 1;
        costs[j-1] = last; last = nv;
      }
    }
    if (i > 0) costs[s2.length] = last;
  }
  return costs[s2.length];
}
