/**
 * PEAK Recruiting -- GCIC Info Form Apps Script v3 (HARDENED)
 * Attached to: GCIC Info Form (from form trigger)
 *
 * v3 fixes: dead send-sms endpoint replaced with sms_send_queue insert,
 *           sbPatch failure now halts and alerts, sbPost checks response.
 */

var CONFIG = {
  SUPABASE_URL: 'https://eyopvsmsvbgfuffscfom.supabase.co',
  SUPABASE_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV5b3B2c21zdmJnZnVmZnNjZm9tIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjczNjU1NTMsImV4cCI6MjA4Mjk0MTU1M30.-DD2BRojvNfUvF9gD3GAtRXiVP61et6xs1eBc-IbOq4',
  ALERT_EMAIL:  'kai@peakrecruitingco.com',
  FROM_NUMBER:  '+14704704766'  // Twilio shadow number
};

var COLUMNS = {
  FIRST_NAME:  'First Name',
  LAST_NAME:   'Last Name',
  MIDDLE_NAME: 'Middle Name',
  PHONE:       'Phone Number',
  DOB:         'Date of Birth',
  SSN:         'Social Security Number',
  ADDRESS:     'Street Address',
  CITY:        'City',
  STATE:       'State',
  ZIP:         'ZIP Code',
  EMAIL:       'Email (personal)',
  SEX:         'Sex',
  RACE:        'Race'
};

// ── TRIGGER ───────────────────────────────────────────────────────────────────

function onFormSubmit(e) {
  try {
    var ts     = new Date().toISOString();
    var fields = {};

    Logger.log('=== GCIC Info Form v3 === keys: ' + Object.keys(e).join(', '));

    if (e.response) {
      Logger.log('PATH A: form trigger');
      e.response.getItemResponses().forEach(function(item) {
        var title = item.getItem().getTitle().trim();
        var val   = (item.getResponse() || '').toString().trim();
        Logger.log('[' + title + '] = ' + val.substring(0, 80));
        fields[title] = val;
      });
    } else if (e.values) {
      Logger.log('PATH B: spreadsheet trigger');
      var row     = e.values;
      var sheet   = e.range.getSheet();
      var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      headers.forEach(function(h, i) { fields[h] = row[i] || ''; });
      Logger.log('Headers: ' + JSON.stringify(headers));
    } else {
      Logger.log('ERROR: unknown event object'); return;
    }

    var firstName  = (fields[COLUMNS.FIRST_NAME]  || '').trim();
    var lastName   = (fields[COLUMNS.LAST_NAME]   || '').trim();
    var middleName = (fields[COLUMNS.MIDDLE_NAME] || '').trim();
    var phone      = normalizePhone(fields[COLUMNS.PHONE] || '');
    var dob        = (fields[COLUMNS.DOB]         || '').trim();
    var ssn        = (fields[COLUMNS.SSN]         || '').trim();
    var address    = (fields[COLUMNS.ADDRESS]     || '').trim();
    var city       = (fields[COLUMNS.CITY]        || '').trim();
    var state      = (fields[COLUMNS.STATE]       || '').trim();
    var zip        = (fields[COLUMNS.ZIP]         || '').trim();
    var email      = (fields[COLUMNS.EMAIL]       || '').trim();
    var sex        = (fields[COLUMNS.SEX]         || '').trim();
    var race       = (fields[COLUMNS.RACE]        || '').trim();

    Logger.log('Name: ' + firstName + ' ' + lastName + ' | Phone: ' + phone);

    if (!phone && !firstName && !lastName) {
      handleMatchFailure_(firstName, lastName, phone, ts, fields, 'No identifying info');
      return;
    }

    var match = findCandidate_(phone, firstName, lastName);
    Logger.log('Match: ' + JSON.stringify(match));

    if (match.found) {
      var c = match.candidate;

      var updates = {
        gcic_form_completed:    1,
        gcic_form_completed_at: ts,
        gcic_stage:             'FORM_COMPLETED',
        updated_at:             ts
      };

      if (middleName) updates.middle_name    = middleName;
      if (dob)        updates.dob            = dob;
      if (ssn)        updates.ssn_encrypted  = ssn;
      if (address)    updates.address_street = address;
      if (city)       updates.address_city   = city;
      if (state)      updates.address_state  = state;
      if (zip)        updates.address_zip    = zip;
      if (email)      updates.personal_email = email;
      if (sex)        updates.sex            = sex;
      if (race)       updates.race           = race;
      if (firstName)  updates.first_name     = firstName;
      if (lastName)   updates.last_name      = lastName;

      // FIX: halt on patch failure -- don't send SMS if DB write failed
      var code = sbPatch_('candidates', 'id=eq.' + c.id, updates);
      Logger.log('PATCH ' + code + ' for candidate ' + c.id);

      if (code !== 200 && code !== 204) {
        GmailApp.sendEmail(CONFIG.ALERT_EMAIL,
          '[PEAK ALERT] GCIC Info Form DB write failed: ' + firstName + ' ' + lastName,
          'Candidate ID ' + c.id + ' GCIC info patch failed (' + code + ').\n' +
          'Form data was submitted but NOT written to Supabase.\n' +
          'Manual update required in PEAKATS.\nPhone: ' + phone);
        return;
      }

      // FIX: replaced dead send-sms with sms_send_queue insert
      sendConfirmationSms_(c.id, c.phone || phone, c.first_name || firstName);
      Logger.log('SUCCESS: ' + firstName + ' ' + lastName + ' -> ID ' + c.id);

    } else {
      handleMatchFailure_(firstName, lastName, phone, ts, fields, match.reason);
    }

  } catch (err) {
    Logger.log('FATAL: ' + err + '\n' + err.stack);
    GmailApp.sendEmail(CONFIG.ALERT_EMAIL, '[PEAK ALERT] GCIC Info Form Error',
      'Error: ' + err + '\n\n' + err.stack);
  }
}

// ── MATCHER ───────────────────────────────────────────────────────────────────

function findCandidate_(phone, firstName, lastName) {
  var select = 'id,first_name,last_name,phone,personal_email,client_id';

  // 1. Exact phone
  if (phone && phone.length === 10) {
    var r1 = sbGet_('candidates', select, 'phone=eq.' + phone + '&limit=5');
    Logger.log('Phone match (' + phone + '): ' + JSON.stringify(r1));
    if (Array.isArray(r1) && r1.length === 1) return { found: true, candidate: r1[0] };
    if (Array.isArray(r1) && r1.length > 1) {
      var best = pickBestByName_(r1, firstName, lastName);
      if (best) return { found: true, candidate: best };
    }
  }

  // 2. Name match
  if (firstName && lastName) {
    var r2 = sbGet_('candidates', select,
      'first_name=ilike.' + encodeURIComponent(firstName) +
      '&last_name=ilike.'  + encodeURIComponent(lastName) + '&limit=5');
    Logger.log('Name match: ' + JSON.stringify(r2));
    if (Array.isArray(r2) && r2.length === 1) return { found: true, candidate: r2[0], via: 'name' };
    if (Array.isArray(r2) && r2.length > 1) {
      var byPhone = r2.filter(function(c) { return normalizePhone(c.phone || '') === phone; });
      if (byPhone.length === 1) return { found: true, candidate: byPhone[0], via: 'name+phone' };
      return { found: false, reason: 'Multiple name matches for ' + firstName + ' ' + lastName };
    }
  }

  // 3. Partial phone (last 7 digits)
  if (phone && phone.length >= 7) {
    var last7 = phone.slice(-7);
    var r3 = sbGet_('candidates', select, 'phone=like.*' + last7 + '&limit=5');
    Logger.log('Partial phone (last 7: ' + last7 + '): ' + JSON.stringify(r3));
    if (Array.isArray(r3) && r3.length === 1) return { found: true, candidate: r3[0], via: 'partial_phone' };
    if (Array.isArray(r3) && r3.length > 1 && firstName && lastName) {
      var best2 = pickBestByName_(r3, firstName, lastName);
      if (best2) return { found: true, candidate: best2, via: 'partial_phone+name' };
    }
  }

  return { found: false, reason: 'No match -- phone: ' + phone + ', name: ' + firstName + ' ' + lastName };
}

function pickBestByName_(candidates, firstName, lastName) {
  var best = null, bestScore = 0;
  candidates.forEach(function(c) {
    var score = nameSim_(firstName, lastName, c.first_name, c.last_name);
    if (score > bestScore) { bestScore = score; best = c; }
  });
  return (best && bestScore >= 0.5) ? best : null;
}

// ── MATCH FAILURE ─────────────────────────────────────────────────────────────

function handleMatchFailure_(firstName, lastName, phone, ts, rawData, reason) {
  // FIX: check response
  var code = sbPost_('form_match_failures', {
    form_type:       'gcic',
    submitted_phone: phone,
    submitted_at:    ts,
    raw_form_data:   rawData,
    resolved:        false
  });
  Logger.log('form_match_failures insert: ' + code);

  GmailApp.sendEmail(CONFIG.ALERT_EMAIL, '[PEAK ALERT] GCIC Info Form -- No Candidate Match', [
    'Candidate submitted GCIC info form but could not be matched.',
    '', 'Name:   ' + firstName + ' ' + lastName,
    'Phone:  ' + phone, 'Time:   ' + ts, 'Reason: ' + reason,
    '', 'ACTION: Manually update candidate record in PEAKATS.',
    '', '-- PEAK Automated Alert'
  ].join('\n'));
}

// ── SMS -- FIX: sms_send_queue insert (replaces dead send-sms edge function) ─

function sendConfirmationSms_(candidateId, phone, firstName) {
  if (!phone || phone.length < 10) {
    Logger.log('sendConfirmationSms_: no valid phone for candidate ' + candidateId);
    return;
  }

  var body = (firstName || '') + ', your background authorization form has been submitted. ' +
    'Your background check is now being processed.\n\nI will follow up as soon as I have an update.\n\nKai\nPEAKrecruiting';

  var code = sbPost_('sms_send_queue', {
    candidate_id:     candidateId,
    to_number:        phone,
    from_number:      CONFIG.FROM_NUMBER,
    body:             body,
    template_id:      14,
    template_name:    'GCIC Submitted to FADV - Candidate Confirmation',
    status:           'pending',
    migration_status: 'twilio_active',
    scheduled_for:    new Date().toISOString(),
    created_by:       'gcic_info_form_script'
  });

  if (code !== 200 && code !== 201 && code !== 204) {
    Logger.log('SMS queue insert failed (' + code + ') for candidate ' + candidateId);
    GmailApp.sendEmail(CONFIG.ALERT_EMAIL,
      '[PEAK ALERT] GCIC Info Form SMS queue failed: candidate ' + candidateId,
      'DB updated but confirmation SMS not queued. Send manually via PWA.\nPhone: ' + phone);
  } else {
    Logger.log('Confirmation SMS queued for candidate ' + candidateId);
  }
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

function sbGet_(table, select, filters) {
  try {
    var r = UrlFetchApp.fetch(
      CONFIG.SUPABASE_URL + '/rest/v1/' + table + '?select=' + select + '&' + filters,
      { method: 'get', headers: sbHeaders_(), muteHttpExceptions: true }
    );
    var status = r.getResponseCode(), text = r.getContentText();
    Logger.log('GET ' + status + ': ' + text.substring(0, 300));
    if (status !== 200) return [];
    var p = JSON.parse(text);
    return Array.isArray(p) ? p : [];
  } catch (err) { Logger.log('sbGet_ error: ' + err); return []; }
}

// FIX: returns response code
function sbPatch_(table, filter, data) {
  var r = UrlFetchApp.fetch(
    CONFIG.SUPABASE_URL + '/rest/v1/' + table + '?' + filter,
    { method: 'patch', headers: sbHeaders_('minimal'), payload: JSON.stringify(data), muteHttpExceptions: true }
  );
  var code = r.getResponseCode();
  if (code !== 200 && code !== 204) {
    Logger.log('sbPatch_ error (' + code + '): ' + r.getContentText().substring(0, 300));
  }
  return code;
}

// FIX: returns response code
function sbPost_(table, data) {
  var r = UrlFetchApp.fetch(
    CONFIG.SUPABASE_URL + '/rest/v1/' + table,
    { method: 'post', headers: sbHeaders_('minimal'), payload: JSON.stringify(data), muteHttpExceptions: true }
  );
  var code = r.getResponseCode();
  if (code !== 200 && code !== 201 && code !== 204) {
    Logger.log('sbPost_ error (' + code + ') on ' + table + ': ' + r.getContentText().substring(0, 300));
  }
  return code;
}

// ── UTILS ─────────────────────────────────────────────────────────────────────

function normalizePhone(raw) {
  var d = raw.replace(/\D/g, '');
  if (d.length === 11 && d[0] === '1') d = d.substring(1);
  return d.length === 10 ? d : d;
}

function nameSim_(fn1, ln1, fn2, ln2) {
  return (strSim_((fn1||'').toLowerCase(), (fn2||'').toLowerCase()) +
          strSim_((ln1||'').toLowerCase(), (ln2||'').toLowerCase())) / 2;
}

function strSim_(a, b) {
  if (!a || !b) return 0;
  if (a === b) return 1;
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
