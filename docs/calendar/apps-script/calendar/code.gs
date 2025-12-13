/***** SimpleMarcas – Scheduler v1 (OPTIMIZADO + INDEX TURBO)
 * Hojas:
 * - Consultores: A:consultor_id, B:nombre, C:activo, D:days_ahead_limit (opcional)
 * - Horario: A:consultor_id, B:dow(1-7), C:enabled, D:start1, E:end1, F:start2, G:end2
 * - Bloqueos: A:fecha(yyyy-MM-dd), B:consultor_id|ALL, C:motivo
 * - Agendas: A:booking_id, B:fecha, C:hora, D:consultor_id,
 *            E:nombre, F:email, G:telefono, H:marca, I:tipo,
 *            J:idpipe, K:timestamp_creado, L:cancelado, M:motivo_cancelacion,
 *            N:retraso_enviado (timestamp o vacío)
 * - AgendasIndex (NUEVO): A:key(fecha||cid), B:fecha, C:consultor_id, D:times_csv, E:count, F:updated_at
 *
 * Endpoints:
 * GET ?mode=init_booking
 * GET ?mode=consultores
 * GET ?mode=availableDays&consultor=ID|todos&from=YYYY-MM-DD&to=YYYY-MM-DD
 * GET ?mode=slots&consultor=ID|todos&date=YYYY-MM-DD
 * GET ?mode=listBookings&date=YYYY-MM-DD&consultor=ID|todos
 * GET ?mode=schedule&consultor=ID
 * GET ?mode=blocks&consultor=ID|ALL
 * GET ?mode=boot&consultor=ID|todos(&from=YYYY-MM-DD&to=YYYY-MM-DD)
 *
 * POST mode=book
 * POST mode=cancel
 * POST mode=addConsultor
 * POST mode=saveSchedule
 * POST mode=blockDate
 * POST mode=unblockDate
 *********************************************************************/
const CFG = {
  TZ: "America/Santiago",
  DAYS_AHEAD_MAX: 90,
  SLOT_MINUTES: 30,
  WEBHOOK_ZAPIER: "https://hooks.zapier.com/hooks/catch/6030955/uzu970w/"
};

// ========== PERF helpers ==========
function perfStart_(){ return Date.now(); }
function perfMark_(perf, key, t0){ perf[key] = Math.max(0, Date.now() - t0); }

// ========== Helpers generales ==========
function json_(obj){
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function getSheet_(name){
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

function todayDate_(){
  const now = new Date();
  return Utilities.formatDate(now, CFG.TZ, "yyyy-MM-dd");
}
function nowTime_(){
  const now = new Date();
  return Utilities.formatDate(now, CFG.TZ, "HH:mm");
}
function timestampISO_(){
  const now = new Date();
  return Utilities.formatDate(now, CFG.TZ, "yyyy-MM-dd'T'HH:mm:ssXXX");
}

function asStr(v){
  if (v === null || v === undefined) return "";
  if (v instanceof Date) return Utilities.formatDate(v, CFG.TZ, "yyyy-MM-dd HH:mm:ss");
  return String(v).trim();
}

function parseDate_(s){
  if (!s) return null;
  const parts = String(s).split("-");
  if (parts.length !== 3) return null;
  const y = Number(parts[0]), m = Number(parts[1])-1, d = Number(parts[2]);
  const dt = new Date(y, m, d);
  return isNaN(dt.getTime()) ? null : dt;
}
function dateToStr_(d){
  return Utilities.formatDate(d, CFG.TZ, "yyyy-MM-dd");
}
function addDays_(d, n){
  const d2 = new Date(d.getTime());
  d2.setDate(d2.getDate() + n);
  return d2;
}
function diffDays_(d1, d2){
  const ms = d2.getTime() - d1.getTime();
  return Math.floor(ms / (1000*60*60*24));
}

function timeToMinutes_(hhmm){
  if (!hhmm) return null;
  const p = String(hhmm).split(":");
  if (p.length < 2) return null;
  const h = Number(p[0]), m = Number(p[1]);
  if (isNaN(h) || isNaN(m)) return null;
  return h*60 + m;
}
function minutesToTime_(mins){
  const h = Math.floor(mins/60);
  const m = mins % 60;
  const hh = (h < 10 ? "0"+h : ""+h);
  const mm = (m < 10 ? "0"+m : ""+m);
  return hh + ":" + mm;
}

function normalizePost_(e){
  const out = {};
  if (e && e.parameter) Object.keys(e.parameter).forEach(k => out[k] = e.parameter[k]);
  if (e && e.postData && e.postData.contents){
    try { Object.assign(out, JSON.parse(e.postData.contents)); } catch(_){}
  }
  return out;
}
function normBool_(val){
  if (val === true) return true;
  if (val === false) return false;
  const s = String(val || "").toLowerCase();
  return s === "true" || s === "1" || s === "sí" || s === "si";
}

function normDaysAheadInt_(val){
  if (val === null || val === undefined) return null;
  const s = String(val).trim();
  if (!s) return null;
  const n = Number(s);
  if (!isFinite(n)) return null;
  const i = Math.floor(n);
  return i > 0 ? i : null;
}

function normDateStr_(cell){
  if (cell instanceof Date){
    return Utilities.formatDate(cell, CFG.TZ, "yyyy-MM-dd");
  }
  const s = String(cell || "").trim();
  if (!s) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const parsed = new Date(s);
  if (!isNaN(parsed.getTime())){
    return Utilities.formatDate(parsed, CFG.TZ, "yyyy-MM-dd");
  }
  return s;
}

function normTimeHHMM_(cell){
  function snapToSlotMinutes_(totalMinutes){
    if (totalMinutes == null) return null;
    const snapped = Math.round(totalMinutes / CFG.SLOT_MINUTES) * CFG.SLOT_MINUTES;
    return snapped;
  }

  if (cell instanceof Date){
    const total = cell.getHours()*60 + cell.getMinutes();
    const snapped = snapToSlotMinutes_(total);
    return snapped == null ? "" : minutesToTime_(snapped);
  }
  if (typeof cell === "number"){
    const total = cell * 24 * 60;
    const snapped = snapToSlotMinutes_(total);
    return snapped == null ? "" : minutesToTime_(snapped);
  }
  const s = String(cell || "").trim();
  if (!s) return "";
  const m = s.match(/^(\d{1,2}):(\d{2})(?::\d{1,2})?/);
  if (m){
    const h = Number(m[1]);
    const mm = Number(m[2]);
    if (!isNaN(h) && !isNaN(mm)){
      const total = h*60 + mm;
      const snapped = snapToSlotMinutes_(total);
      return snapped == null ? "" : minutesToTime_(snapped);
    }
  }
  const parsed = new Date(s);
  if (!isNaN(parsed.getTime())){
    const total = parsed.getHours()*60 + parsed.getMinutes();
    const snapped = snapToSlotMinutes_(total);
    return snapped == null ? "" : minutesToTime_(snapped);
  }
  return s;
}

function meetingDateTimeUTC_(fechaNorm, horaNorm){
  if (!fechaNorm || !horaNorm) return "";
  const dParts = String(fechaNorm).split("-");
  const tParts = String(horaNorm).split(":");
  if (dParts.length !== 3 || tParts.length < 2) return "";
  const y = Number(dParts[0]);
  const m = Number(dParts[1]);
  const d = Number(dParts[2]);
  const hh = Number(tParts[0]);
  const mm = Number(tParts[1]);
  if ([y,m,d,hh,mm].some(v => isNaN(v))) return "";
  const local = new Date(y, m-1, d, hh, mm, 0);
  return Utilities.formatDate(local, "Etc/UTC", "yyyy-MM-dd'T'HH:mm:ss'Z'");
}

// ========== Consultores & Horarios ==========
function getConsultores_(){
  const sh = getSheet_("Consultores");
  const last = sh.getLastRow();
  const out = [];
  if (last < 2) return out;
  const width = Math.max(4, sh.getLastColumn());
  const vals = sh.getRange(2,1,last-1,width).getValues();
  // asegurar header de la nueva columna sin romper setups existentes
  if (!sh.getRange(1,4).getValue()) sh.getRange(1,4).setValue("days_ahead_limit");
  for (let i=0;i<vals.length;i++){
    const [id, nombre, activo, daysAheadRaw] = vals[i];
    const days_ahead_limit = normDaysAheadInt_(daysAheadRaw);
    if (!id) continue;
    out.push({
      consultor_id: String(id),
      nombre: nombre || "",
      activo: normBool_(activo),
      days_ahead_limit
    });
  }
  return out;
}

function setConsultorActive_(cid, active){
  const sh = getSheet_("Consultores");
  const last = sh.getLastRow();
  if (last < 2) return false;
  const width = Math.max(4, sh.getLastColumn());
  const vals = sh.getRange(2,1,last-1,width).getValues();
  for (let i=0;i<vals.length;i++){
    if (String(vals[i][0]) === String(cid)){
      sh.getRange(i+2, 3).setValue(!!active);
      return true;
    }
  }
  return false;
}

function setConsultorDaysAhead_(cid, daysLimit){
  const sh = getSheet_("Consultores");
  const last = sh.getLastRow();
  if (last < 2) return false;
  const width = Math.max(4, sh.getLastColumn());
  const vals = sh.getRange(2,1,last-1,width).getValues();
  let found = false;
  for (let i=0;i<vals.length;i++){
    if (String(vals[i][0]).trim() === String(cid).trim()){
      sh.getRange(i+2, 4).setValue(daysLimit == null ? "" : daysLimit);
      found = true;
      break;
    }
  }
  if (!sh.getRange(1,4).getValue()) sh.getRange(1,4).setValue("days_ahead_limit");
  return found;
}

function effectiveDaysCap_(rawLimit){
  const n = normDaysAheadInt_(rawLimit);
  const cap = (n == null) ? CFG.DAYS_AHEAD_MAX : Math.min(CFG.DAYS_AHEAD_MAX, n);
  return cap < 1 ? 1 : cap;
}

function buildDaysAheadLimitMap_(consultores, todayD){
  const map = {};
  let maxCapDays = 0;
  const base = todayD || parseDate_(todayDate_());
  (consultores || []).forEach(c=>{
    const cid = String(c.consultor_id || "").trim();
    if (!cid) return;
    const capDays = effectiveDaysCap_(c.days_ahead_limit);
    const maxDate = addDays_(base, capDays - 1);
    map[cid] = { capDays, maxDate };
    if (capDays > maxCapDays) maxCapDays = capDays;
  });
  return { map, maxCapDays };
}

function ensureDefaultScheduleForConsultor_(cid){
  const sh = getSheet_("Horario");
  const last = sh.getLastRow();
  let hasRows = false;
  if (last >= 2){
    const vals = sh.getRange(2,1,last-1,7).getValues();
    for (let i=0;i<vals.length;i++){
      if (String(vals[i][0]) === String(cid)){
        hasRows = true;
        break;
      }
    }
  }
  if (hasRows) return;

  const rows = [];
  for (let dow=1; dow<=7; dow++){
    let enabled = false, s1="",e1="",s2="",e2="";
    if (dow >= 1 && dow <= 4){
      enabled = true; s1 = "09:30"; e1 = "13:30"; s2 = "14:30"; e2 = "19:00";
    } else if (dow === 5){
      enabled = true; s1 = "09:30"; e1 = "13:30"; s2 = "14:30"; e2 = "18:00";
    }
    rows.push([cid, dow, enabled, s1, e1, s2, e2]);
  }
  sh.getRange(sh.getLastRow()+1,1,rows.length,7).setValues(rows);
}

/**
 * LECTURA PURA (sin asegurar defaults / sin escribir).
 * Si no existe fila para el consultor, devuelve {}.
 */
function getScheduleForConsultor_(cid){
  const sh = getSheet_("Horario");
  const last = sh.getLastRow();
  const byDow = {};
  if (last >= 2){
    const vals = sh.getRange(2,1,last-1,7).getValues();
    for (let i=0;i<vals.length;i++){
      const [c, dow, enabled, s1,e1,s2,e2] = vals[i];
      if (String(c) !== String(cid)) continue;
      const k = Number(dow);
      byDow[k] = {
        dow: k,
        enabled: normBool_(enabled),
        start1: normTimeHHMM_(s1),
        end1: normTimeHHMM_(e1),
        start2: normTimeHHMM_(s2),
        end2: normTimeHHMM_(e2)
      };
    }
  }
  return byDow;
}

function saveScheduleForConsultor_(cid, daysArr){
  const sh = getSheet_("Horario");
  const last = sh.getLastRow();

  let remaining = [];
  if (last >= 2){
    const vals = sh.getRange(2,1,last-1,7).getValues();
    for (let i=0;i<vals.length;i++){
      const [c, dow, enabled, s1, e1, s2, e2] = vals[i];
      if (String(c) !== String(cid)){
        remaining.push([
          String(c),
          Number(dow),
          !!normBool_(enabled),
          normTimeHHMM_(s1),
          normTimeHHMM_(e1),
          normTimeHHMM_(s2),
          normTimeHHMM_(e2)
        ]);
      }
    }
  }

  const newRows = [];
  (daysArr || []).forEach(day => {
    const dow = Number(day.dow);
    if (!dow) return;
    newRows.push([
      String(cid),
      dow,
      !!day.enabled,
      normTimeHHMM_(day.start1 || ""),
      normTimeHHMM_(day.end1 || ""),
      normTimeHHMM_(day.start2 || ""),
      normTimeHHMM_(day.end2 || "")
    ]);
  });

  const finalRows = remaining.concat(newRows);

  sh.clearContents();
  sh.getRange(1,1,1,7).setValues([["consultor_id","dow","enabled","start1","end1","start2","end2"]]);
  if (finalRows.length){
    sh.getRange(2,1,finalRows.length,7).setValues(finalRows);
  }
}

// ========== AgendasIndex (NUEVO / TURBO por key con TextFinder) ==========
function escapeRegExp_(s){
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function enforceAgendasIndexTextFormat_(sh){
  // Fuerza texto para evitar key/date/time con formatos raros
  // (A:D)
  sh.getRange("A:D").setNumberFormat("@");
}

function getAgendasIndexSheet_(){
  const sh = getSheet_("AgendasIndex");
  if (sh.getLastRow() < 1){
    sh.getRange(1,1,1,6).setValues([[
      "key","fecha","consultor_id","times_csv","count","updated_at"
    ]]);
  }
  enforceAgendasIndexTextFormat_(sh);
  return sh;
}

function agendasIndexKey_(fechaNorm, cid){
  return String(fechaNorm || "").trim() + "||" + String(cid || "").trim();
}

function getIndexRowByKey_(sh, key){
  const last = sh.getLastRow();
  if (last < 2) return null;

  const rng = sh.getRange(2,1,last-1,1);

  // 1) Match exacto (como estaba)
  let cell = rng.createTextFinder(String(key))
    .matchEntireCell(true)
    .findNext();
  if (cell) return cell.getRow();

  // 2) Fallback por formato: tolera espacios invisibles alrededor
  const pattern = "^\\s*" + escapeRegExp_(String(key)) + "\\s*$";
  cell = rng.createTextFinder(pattern)
    .useRegularExpression(true)
    .matchEntireCell(true)
    .findNext();

  return cell ? cell.getRow() : null;
}

function parseTimesCsv_(csv){
  const s = String(csv || "").trim();
  if (!s) return [];
  return s.split(",").map(x=>String(x).trim()).filter(Boolean);
}

/**
 * Lee ocupados desde índice SOLO por llaves (TextFinder).
 * Retorna: { cid: { "HH:mm": true } }
 */
function getBookedFromIndex_(dateStr, cids){
  const sh = getAgendasIndexSheet_();
  const out = {};
  const ids = (cids || []).map(x=>String(x).trim()).filter(Boolean);
  const ds  = String(dateStr || "").trim();
  if (!ids.length || !ds) return out;

  for (let i=0;i<ids.length;i++){
    const cid = ids[i];
    const key = agendasIndexKey_(ds, cid);
    const row = getIndexRowByKey_(sh, key);
    if (!row) continue;

    const csv = sh.getRange(row, 4).getValue(); // times_csv
    const times = parseTimesCsv_(csv);
    if (!times.length) continue;

    if (!out[cid]) out[cid] = {};
    for (let j=0;j<times.length;j++){
      const t = normTimeHHMM_(times[j]); // normaliza por si viene "9:30"
      if (t) out[cid][t] = true;
    }
  }
  return out;
}

/**
 * Upsert index por key, agregando o removiendo una hora.
 * - remove=false: agrega hora (sin duplicar, ordena)
 * - remove=true : remueve hora
 * Con lock para evitar carreras.
 */
function agendasIndexUpsert_(fechaNorm, cid, horaNorm, remove){
  const sh = getAgendasIndexSheet_();
  const key = agendasIndexKey_(fechaNorm, cid);
  const ts = timestampISO_();

  const lock = LockService.getDocumentLock();
  lock.waitLock(10000);
  try{
    let rowFound = getIndexRowByKey_(sh, key);

    if (!rowFound){
      if (remove) return; // no existe, nada que remover
      const t = normTimeHHMM_(horaNorm);
      const times = t ? [t] : [];
      sh.appendRow([key, String(fechaNorm).trim(), String(cid).trim(), times.join(","), times.length, ts]);
      return;
    }

    const curTimesCsv = sh.getRange(rowFound, 4).getValue(); // D
    let times = parseTimesCsv_(curTimesCsv).map(normTimeHHMM_).filter(Boolean);

    const t = normTimeHHMM_(horaNorm);
    if (t){
      if (remove){
        times = times.filter(x => x !== t);
      } else {
        if (times.indexOf(t) === -1) times.push(t);
      }
    }

    if (times.length === 0){
      sh.getRange(rowFound, 4).setValue("");
      sh.getRange(rowFound, 5).setValue(0);
      sh.getRange(rowFound, 6).setValue(ts);
      return;
    }

    times = Array.from(new Set(times)).sort();
    sh.getRange(rowFound, 4).setValue(times.join(","));
    sh.getRange(rowFound, 5).setValue(times.length);
    sh.getRange(rowFound, 6).setValue(ts);
  } finally {
    lock.releaseLock();
  }
}

// Reconstrucción manual (por si ya tienes agenda histórica sin index)
function rebuildAgendasIndex_(){
  const idx = getAgendasIndexSheet_();
  idx.clearContents();
  idx.getRange(1,1,1,6).setValues([["key","fecha","consultor_id","times_csv","count","updated_at"]]);
  enforceAgendasIndexTextFormat_(idx);

  const sh = getSheet_("Agendas");
  const last = sh.getLastRow();
  if (last < 2) return;

  const vals = sh.getRange(2,1,last-1,13).getValues();
  const map = {}; // key => {fecha,cid,set:{}}
  for (let i=0;i<vals.length;i++){
    const row = vals[i];
    const fecha = normDateStr_(row[1]);
    const hora  = normTimeHHMM_(row[2]);
    const cid   = String(row[3] || "").trim();
    const cancel= normBool_(row[11]);
    if (!fecha || !hora || !cid || cancel) continue;
    const key = agendasIndexKey_(fecha, cid);
    if (!map[key]) map[key] = { fecha, cid, set:{} };
    map[key].set[hora] = true;
  }

  const keys = Object.keys(map).sort();
  if (!keys.length) return;

  const rows = [];
  const ts = timestampISO_();
  keys.forEach(k=>{
    const item = map[k];
    const times = Object.keys(item.set).sort();
    rows.push([k, item.fecha, item.cid, times.join(","), times.length, ts]);
  });
  idx.getRange(2,1,rows.length,6).setValues(rows);
}

// ========== Bloqueos y Slots ==========
function isDateBlocked_(dateStr, cid){
  const sh = getSheet_("Bloqueos");
  const last = sh.getLastRow();
  if (last < 2) return false;
  const vals = sh.getRange(2,1,last-1,3).getValues();
  for (let i=0;i<vals.length;i++){
    const [f, c, _motivo] = vals[i];
    const fNorm = normDateStr_(f);
    if (fNorm === dateStr && (String(c) === String(cid) || String(c) === "ALL")){
      return true;
    }
  }
  return false;
}

// booked times (por fecha y consultor) - DESDE INDEX POR KEY (TextFinder)
function getBookedTimesFor_(dateStr, cid){
  const bookedByCid = getBookedFromIndex_(dateStr, [cid]);
  return bookedByCid[String(cid).trim()] || {};
}

// Slots teóricos según horario
function countSlotsForDayConfig_(dayCfg){
  if (!dayCfg || !dayCfg.enabled) return 0;
  let total = 0;
  function spanCount(startStr, endStr){
    const sMin = timeToMinutes_(startStr);
    const eMin = timeToMinutes_(endStr);
    if (sMin == null || eMin == null) return 0;
    if (eMin <= sMin) return 0;
    return Math.floor((eMin - sMin) / CFG.SLOT_MINUTES);
  }
  if (dayCfg.start1 && dayCfg.end1) total += spanCount(dayCfg.start1, dayCfg.end1);
  if (dayCfg.start2 && dayCfg.end2) total += spanCount(dayCfg.start2, dayCfg.end2);
  return total;
}

function buildBlockedMap_(fromStr, toStr){
  const sh = getSheet_("Bloqueos");
  const last = sh.getLastRow();
  const allDates = {};
  const byKey = {};
  if (last < 2) return { allDates, byKey };

  const vals = sh.getRange(2,1,last-1,3).getValues();
  for (let i=0;i<vals.length;i++){
    const [f, c, _m] = vals[i];
    const fechaNorm = normDateStr_(f);
    if (!fechaNorm) continue;
    if (fechaNorm < fromStr || fechaNorm > toStr) continue;
    const cid = String(c || "").trim();
    if (cid === "ALL") allDates[fechaNorm] = true;
    else if (cid) byKey[fechaNorm + "||" + cid] = true;
  }
  return { allDates, byKey };
}

// ========== SLOTS TURBO (AgendasIndex + bulk schedule) ==========
function buildBlocksForDate_(dateStr){
  const sh = getSheet_("Bloqueos");
  const last = sh.getLastRow();
  const out = { all: false, byCid: {} };
  if (last < 2) return out;
  const vals = sh.getRange(2,1,last-1,3).getValues();
  for (let i=0;i<vals.length;i++){
    const [f,c,_m] = vals[i];
    const fNorm = normDateStr_(f);
    if (fNorm !== dateStr) continue;
    const cid = String(c || "").trim();
    if (cid === "ALL") out.all = true;
    else if (cid) out.byCid[cid] = true;
  }
  return out;
}

function buildScheduleBulk_(cids){
  const sh = getSheet_("Horario");
  const last = sh.getLastRow();
  const map = {};
  const set = {};
  (cids || []).forEach(cid => { map[String(cid).trim()] = {}; set[String(cid).trim()] = true; });
  if (last < 2) return map;

  const vals = sh.getRange(2,1,last-1,7).getValues();
  for (let i=0;i<vals.length;i++){
    const [c,dow,enabled,s1,e1,s2,e2] = vals[i];
    const cid = String(c || "").trim();
    if (!set[cid]) continue;
    const k = Number(dow);
    if (!k) continue;
    map[cid][k] = {
      dow: k,
      enabled: normBool_(enabled),
      start1: normTimeHHMM_(s1),
      end1: normTimeHHMM_(e1),
      start2: normTimeHHMM_(s2),
      end2: normTimeHHMM_(e2)
    };
  }
  return map;
}

function generateSlotsFromCfg_(dateStr, dayCfg, bookedMapForCid){
  if (!dayCfg || !dayCfg.enabled) return [];
  const booked = bookedMapForCid || {};
  let slots = [];

  function pushRange(startStr, endStr){
    const sMin = timeToMinutes_(startStr);
    const eMin = timeToMinutes_(endStr);
    if (sMin == null || eMin == null) return;
    for (let m = sMin; m <= eMin - CFG.SLOT_MINUTES; m += CFG.SLOT_MINUTES){
      const t = minutesToTime_(m);
      if (!booked[t]) slots.push(t);
    }
  }

  if (dayCfg.start1 && dayCfg.end1) pushRange(dayCfg.start1, dayCfg.end1);
  if (dayCfg.start2 && dayCfg.end2) pushRange(dayCfg.start2, dayCfg.end2);

  // Regla HOY: +1 hora
  const todayStr = todayDate_();
  if (dateStr === todayStr){
    const nowM = timeToMinutes_(nowTime_());
    const minAllowed = nowM + 10;
    slots = slots.filter(t=>{
      const m = timeToMinutes_(t);
      return m != null && m >= minAllowed;
    });
  }

  return Array.from(new Set(slots)).sort();
}

function slotsTurbo_(consultor, dateStr){
  const perf = { start: 1 };
  const t0 = perfStart_();
  perfMark_(perf, "slots_start", t0);

  const todayStr = todayDate_();
  const todayD = parseDate_(todayStr);
  const targetDate = parseDate_(dateStr);
  if (!targetDate){
    perfMark_(perf, "slots_after_blocks", t0);
    perfMark_(perf, "slots_after_booked", t0);
    perfMark_(perf, "slots_after_horario", t0);
    perfMark_(perf, "slots_after_compute", t0);
    perf.total_ms = perf.slots_after_compute;
    return { ok:false, error:"invalid_date", slots:[], _perf: perf };
  }

  const blocks = buildBlocksForDate_(dateStr);
  perfMark_(perf, "slots_after_blocks", t0);

  if (consultor !== "todos"){
    const cid = String(consultor || "").trim();
    if (!cid){
      perfMark_(perf, "slots_after_booked", t0);
      perfMark_(perf, "slots_after_horario", t0);
      perfMark_(perf, "slots_after_compute", t0);
      perf.total_ms = perf.slots_after_compute;
      return { ok:false, error:"missing_consultor", slots:[], _perf: perf };
    }

    const consultores = getConsultores_();
    const consultorObj = consultores.find(c => c.activo && String(c.consultor_id || "").trim() === cid);
    const { map: limitsMap } = buildDaysAheadLimitMap_([consultorObj || {}], todayD);
    const limitInfo = limitsMap[cid];
    const capDays = limitInfo ? limitInfo.capDays : CFG.DAYS_AHEAD_MAX;
    const maxAllowedDate = limitInfo ? limitInfo.maxDate : addDays_(todayD, Math.max(capDays,1) - 1);

    // booked desde index (1 búsqueda por key)
    const bookedByCid = getBookedFromIndex_(dateStr, [cid]);
    perfMark_(perf, "slots_after_booked", t0);

    if (targetDate > maxAllowedDate){
      perfMark_(perf, "slots_after_horario", t0);
      perfMark_(perf, "slots_after_compute", t0);
      perf.total_ms = perf.slots_after_compute;
      return { ok:true, consultor: cid, date: dateStr, slots:[], _perf: perf };
    }

    if (blocks.all || blocks.byCid[cid]){
      perfMark_(perf, "slots_after_horario", t0);
      perfMark_(perf, "slots_after_compute", t0);
      perf.total_ms = perf.slots_after_compute;
      return { ok:true, consultor: cid, date: dateStr, slots:[], _perf: perf };
    }

    const schedBulk = buildScheduleBulk_([cid]);
    perfMark_(perf, "slots_after_horario", t0);

    let dow = targetDate.getDay(); dow = (dow === 0 ? 7 : dow);
    const dayCfg = (schedBulk[cid] || {})[dow];
    const slots = generateSlotsFromCfg_(dateStr, dayCfg, bookedByCid[cid]);
    perfMark_(perf, "slots_after_compute", t0);
    perf.total_ms = perf.slots_after_compute;

    return { ok:true, consultor: cid, date: dateStr, slots, _perf: perf };
  }

  // consultor = todos
  const consultoresActivos = getConsultores_().filter(c => c.activo);
  perfMark_(perf, "slots_after_consultores", t0);

  if (!consultoresActivos.length || blocks.all){
    perfMark_(perf, "slots_after_booked", t0);
    perfMark_(perf, "slots_after_horario", t0);
    perfMark_(perf, "slots_after_compute", t0);
    perf.total_ms = perf.slots_after_compute;
    return { ok:true, consultor:"todos", date: dateStr, slots:[], _perf: perf };
  }

  const { map: limitsMap, maxCapDays } = buildDaysAheadLimitMap_(consultoresActivos, todayD);
  const unionMaxDate = addDays_(todayD, (maxCapDays || CFG.DAYS_AHEAD_MAX) - 1);
  if (targetDate > unionMaxDate){
    perfMark_(perf, "slots_after_booked", t0);
    perfMark_(perf, "slots_after_horario", t0);
    perfMark_(perf, "slots_after_compute", t0);
    perf.total_ms = perf.slots_after_compute;
    return { ok:true, consultor:"todos", date: dateStr, slots:[], _perf: perf };
  }

  const cids = consultoresActivos.map(c => String(c.consultor_id).trim());

  // booked desde index (1 búsqueda por key por consultor activo)
  const bookedByCid = getBookedFromIndex_(dateStr, cids);
  perfMark_(perf, "slots_after_booked", t0);

  const schedBulk = buildScheduleBulk_(cids);
  perfMark_(perf, "slots_after_horario", t0);

  let dow = targetDate.getDay(); dow = (dow === 0 ? 7 : dow);

  const union = {};
  for (let i=0;i<cids.length;i++){
    const cid = cids[i];
    const lim = limitsMap[cid];
    if (lim && targetDate > lim.maxDate) continue;
    if (blocks.byCid[cid]) continue;
    const dayCfg = (schedBulk[cid] || {})[dow];
    const arr = generateSlotsFromCfg_(dateStr, dayCfg, bookedByCid[cid]);
    for (let j=0;j<arr.length;j++) union[arr[j]] = true;
  }

  const slots = Object.keys(union).sort();
  perfMark_(perf, "slots_after_compute", t0);
  perf.total_ms = perf.slots_after_compute;

  return { ok:true, consultor:"todos", date: dateStr, slots, _perf: perf };
}

// ====== NUEVO: availableDays turbo (Horario una sola vez) ======
function availableDaysTurboResult_(consultor, fromStrParam, toStrParam, today){
  const _perf = { start: 1 };
  const t0 = perfStart_();
  perfMark_(_perf, "availableDays_start", t0);

  const fromDParam = parseDate_(fromStrParam) || parseDate_(today);
  const toDParam   = parseDate_(toStrParam)   || addDays_(fromDParam, CFG.DAYS_AHEAD_MAX);

  const todayD = parseDate_(today);
  const maxToGlobal  = addDays_(todayD, CFG.DAYS_AHEAD_MAX);
  let realTo = (toDParam > maxToGlobal ? maxToGlobal : toDParam);

  const fromDClamped = fromDParam < todayD ? todayD : fromDParam;
  if (fromDClamped > realTo){
    perfMark_(_perf, "availableDays_before_blocks", t0);
    perfMark_(_perf, "availableDays_after_blocks", t0);
    perfMark_(_perf, "availableDays_before_consultores", t0);
    perfMark_(_perf, "availableDays_after_consultores", t0);
    perfMark_(_perf, "availableDays_before_horario", t0);
    perfMark_(_perf, "availableDays_after_horario", t0);
    _perf.total_ms = Date.now() - t0;
    return { ok:true, dates: [], _perf };
  }

  const outDates = [];

  if (consultor === "todos"){
    perfMark_(_perf, "availableDays_before_consultores", t0);
    const consultoresActivos = getConsultores_().filter(c => c.activo);
    perfMark_(_perf, "availableDays_after_consultores", t0);

    if (!consultoresActivos.length){
      perfMark_(_perf, "availableDays_before_horario", t0);
      perfMark_(_perf, "availableDays_after_horario", t0);
      _perf.total_ms = Date.now() - t0;
      return { ok:true, dates: [], _perf };
    }

    const { map: limitsMap, maxCapDays } = buildDaysAheadLimitMap_(consultoresActivos, todayD);
    const unionMaxDate = addDays_(todayD, (maxCapDays || CFG.DAYS_AHEAD_MAX) - 1);
    if (realTo > unionMaxDate) realTo = unionMaxDate;

    const cids = consultoresActivos.map(c => String(c.consultor_id).trim());

    const fromStr = dateToStr_(fromDClamped);
    const toStr   = dateToStr_(realTo);

    perfMark_(_perf, "availableDays_before_blocks", t0);
    const blockedMap = buildBlockedMap_(fromStr, toStr);
    perfMark_(_perf, "availableDays_after_blocks", t0);

    perfMark_(_perf, "availableDays_before_horario", t0);
    const schedBulk = buildScheduleBulk_(cids); // <- 1 sola lectura Horario
    const slotsPerConsultorDow = {};
    for (let i=0;i<cids.length;i++){
      const cid = cids[i];
      const perDow = {};
      for (let dow=1; dow<=7; dow++){
        const cfg = (schedBulk[cid] || {})[dow] || { enabled:false };
        perDow[dow] = countSlotsForDayConfig_(cfg);
      }
      slotsPerConsultorDow[cid] = perDow;
    }
    perfMark_(_perf, "availableDays_after_horario", t0);

    for (let d = fromDClamped; d <= realTo; d = addDays_(d,1)){
      const ds = dateToStr_(d);
      if (blockedMap.allDates[ds]) continue;

      // HOY: slots reales con index + 1h
      if (ds === today){
        const s = slotsTurbo_("todos", ds);
        if ((s.slots || []).length > 0) outDates.push(ds);
        continue;
      }

      let jsDow = d.getDay(); const dow = (jsDow === 0 ? 7 : jsDow);

      let anyAvailable = false;
      for (let i=0; i<cids.length && !anyAvailable; i++){
        const cid = cids[i];
        const lim = limitsMap[cid];
        if (lim && d > lim.maxDate) continue;
        const theoretical = slotsPerConsultorDow[cid][dow] || 0;
        if (theoretical <= 0) continue;
        if (blockedMap.byKey[ds + "||" + cid]) continue;
        anyAvailable = true;
      }
      if (anyAvailable) outDates.push(ds);
    }

    _perf.total_ms = Date.now() - t0;
    return { ok:true, dates: outDates, _perf };

  } else {
    const cid = String(consultor || "").trim();

    perfMark_(_perf, "availableDays_before_consultores", t0);
    const consultores = getConsultores_();
    const consultorObj = consultores.find(c => c.activo && String(c.consultor_id).trim() === cid);
    perfMark_(_perf, "availableDays_after_consultores", t0);

    if (!consultorObj){
      perfMark_(_perf, "availableDays_before_horario", t0);
      perfMark_(_perf, "availableDays_after_horario", t0);
      _perf.total_ms = Date.now() - t0;
      return { ok:true, dates: [], _perf };
    }

    const { map: limitsMap } = buildDaysAheadLimitMap_([consultorObj], todayD);
    const lim = limitsMap[cid] || { maxDate: maxToGlobal };
    if (realTo > lim.maxDate) realTo = lim.maxDate;
    if (fromDClamped > realTo){
      perfMark_(_perf, "availableDays_before_horario", t0);
      perfMark_(_perf, "availableDays_after_horario", t0);
      _perf.total_ms = Date.now() - t0;
      return { ok:true, dates: [], _perf };
    }

    const fromStr = dateToStr_(fromDClamped);
    const toStr   = dateToStr_(realTo);

    perfMark_(_perf, "availableDays_before_blocks", t0);
    const blockedMap = buildBlockedMap_(fromStr, toStr);
    perfMark_(_perf, "availableDays_after_blocks", t0);

    perfMark_(_perf, "availableDays_before_horario", t0);
    const schedBulk = buildScheduleBulk_([cid]); // <- 1 lectura
    const slotsPerDow = {};
    for (let dow=1; dow<=7; dow++){
      const cfg = (schedBulk[cid] || {})[dow] || { enabled:false };
      slotsPerDow[dow] = countSlotsForDayConfig_(cfg);
    }
    perfMark_(_perf, "availableDays_after_horario", t0);

    for (let d = fromDClamped; d <= realTo; d = addDays_(d,1)){
      const ds = dateToStr_(d);
      if (blockedMap.allDates[ds]) continue;
      if (blockedMap.byKey[ds + "||" + cid]) continue;

      if (ds === today){
        const s = slotsTurbo_(cid, ds);
        if ((s.slots || []).length > 0) outDates.push(ds);
        continue;
      }

      let jsDow = d.getDay(); const dow = (jsDow === 0 ? 7 : jsDow);
      const theoretical = slotsPerDow[dow] || 0;
      if (theoretical <= 0) continue;
      outDates.push(ds);
    }

    _perf.total_ms = Date.now() - t0;
    return { ok:true, dates: outDates, _perf };
  }
}

// ========== Agendas (book / cancel / list) ==========
function createBooking_(data){
  const { fecha, hora, consultorSeleccionado, nombre, email, telefono, marca, tipo, idpipe } = data;

  const fechaNorm = normDateStr_(fecha);
  const horaNorm  = normTimeHHMM_(hora);

  const sh = getSheet_("Agendas");
  const booking_id = Utilities.getUuid();
  const ts = timestampISO_();

  const row = [
    booking_id,
    fechaNorm,
    horaNorm,
    String(consultorSeleccionado || "").trim(),
    nombre || "",
    email || "",
    telefono || "",
    marca || "",
    tipo || "",
    idpipe || "",
    ts,
    false,
    "",
    "" // retraso_enviado (timestamp)
  ];

  sh.appendRow(row);

  // mantener índice (upsert por key con lock)
  if (fechaNorm && horaNorm && consultorSeleccionado){
    agendasIndexUpsert_(fechaNorm, String(consultorSeleccionado || "").trim(), horaNorm, false);
  }

  const meetingUTC = meetingDateTimeUTC_(fechaNorm, horaNorm);

  const zapPayload = {
    booking_id,
    fecha: fechaNorm,
    hora: horaNorm,
    consultor_id: String(consultorSeleccionado || "").trim(),
    nombre,
    email,
    telefono,
    marca,
    tipo,
    idpipe: idpipe || "",
    timestamp: ts,
    meeting_datetime_utc: meetingUTC
  };

  try {
    const resp = UrlFetchApp.fetch(CFG.WEBHOOK_ZAPIER, {
      method: "post",
      payload: zapPayload,
      muteHttpExceptions: true
    });
    Logger.log("Zapier response code: " + resp.getResponseCode());
    Logger.log("Zapier response body: " + resp.getContentText());
  } catch (err){
    Logger.log("Zapier error: " + err);
  }

  return booking_id;
}

function listBookings_(dateStr, consultor){
  const sh = getSheet_("Agendas");
  const last = sh.getLastRow();
  const out = [];
  if (last < 2) return out;

  // Encontrar solo filas donde B == dateStr
  const colB = sh.getRange(2,2,last-1,1); // fecha
  const matches = colB.createTextFinder(String(dateStr)).matchEntireCell(true).findAll();
  if (!matches || !matches.length) return out;

  const totalCols = sh.getLastColumn();
  // Leemos solo hasta donde exista la hoja (máximo 14 columnas).
  const width = Math.min(Math.max(totalCols, 1), 14);

  for (let i=0;i<matches.length;i++){
    const rowNum = matches[i].getRow();
    let row;
    try{
      row = sh.getRange(rowNum, 1, 1, width).getValues()[0];
    }catch(errRow){
      // si hay algún problema con esta fila, la saltamos para no romper la lista
      continue;
    }

    const booking_id = row[0];
    const fecha      = normDateStr_(row[1]);
    const horaNorm   = normTimeHHMM_(row[2]);
    const c          = String(row[3] || "").trim();
    const nombre     = row[4];
    const email      = row[5];
    const telefono   = row[6];
    const marca      = row[7];
    const tipo       = row[8];
    const idpipe     = row[9];
    const ts         = row[10];
    const cancelado  = normBool_(row[11]);
    const motivo     = width >= 13 ? row[12] : "";
    const retrasoTS  = width >= 14 ? asStr(row[13]) : "";

    if (fecha !== String(dateStr).trim()) continue;
    if (consultor && consultor !== "todos" && String(c) !== String(consultor).trim()) continue;

    out.push({
      booking_id,
      fecha,
      hora: horaNorm,
      consultor_id: c,
      nombre,
      email,
      telefono,
      marca,
      tipo,
      idpipe: idpipe || "",
      timestamp_creado: ts,
      cancelado,
      motivo_cancelacion: motivo || "",
      retraso_enviado: retrasoTS
    });
  }

  out.sort((a,b)=> String(a.hora).localeCompare(String(b.hora)));
  return out;
}

function cancelBooking_(booking_id, motivo){
  const sh = getSheet_("Agendas");
  const last = sh.getLastRow();
  if (last < 2) return { ok:false, error:"not_found" };

  // Buscar booking_id en col A sin cargar toda la hoja
  const colA = sh.getRange(2,1,last-1,1);
  const cell = colA.createTextFinder(String(booking_id)).matchEntireCell(true).findNext();
  if (!cell) return { ok:false, error:"not_found" };

  const rowNum = cell.getRow();

  // leer datos necesarios antes de cancelar
  const fechaNorm = normDateStr_(sh.getRange(rowNum, 2).getValue()); // B
  const horaNorm  = normTimeHHMM_(sh.getRange(rowNum, 3).getValue()); // C
  const cid       = String(sh.getRange(rowNum, 4).getValue() || "").trim();  // D

  sh.getRange(rowNum, 12).setValue(true);         // L
  sh.getRange(rowNum, 13).setValue(motivo || ""); // M

  // actualizar índice (remover hora)
  if (fechaNorm && horaNorm && cid){
    agendasIndexUpsert_(fechaNorm, cid, horaNorm, true);
  }

  return { ok:true };
}

function markDelaySent_(booking_id){
  const sh = getSheet_("Agendas");
  const last = sh.getLastRow();
  if (last < 2) return { ok:false, error:"not_found" };

  const colA = sh.getRange(2,1,last-1,1);
  const cell = colA.createTextFinder(String(booking_id)).matchEntireCell(true).findNext();
  if (!cell) return { ok:false, error:"not_found" };

  const rowNum = cell.getRow();
  const ts = timestampISO_();
  sh.getRange(rowNum, 14).setValue(ts); // N: retraso_enviado

  return { ok:true, timestamp: ts };
}

// ========== Bloqueos helpers ==========
function blockDate_(fecha, consultorId, motivo){
  const sh = getSheet_("Bloqueos");
  const last = sh.getLastRow();
  const fechaNorm = normDateStr_(fecha);
  const cid = String(consultorId || "").trim();

  if (last >= 2){
    const vals = sh.getRange(2,1,last-1,3).getValues();
    for (let i=0;i<vals.length;i++){
      const [f,c] = vals[i];
      const fNorm = normDateStr_(f);
      if (fNorm === fechaNorm && String(c).trim() === cid){
        return;
      }
    }
  }
  sh.appendRow([fechaNorm, cid, motivo || ""]);
}

function unblockDate_(fecha, consultorId){
  const sh = getSheet_("Bloqueos");
  const last = sh.getLastRow();
  if (last < 2) return;
  const fechaNorm = normDateStr_(fecha);
  const cid = String(consultorId || "").trim();

  const vals = sh.getRange(2,1,last-1,3).getValues();
  const remaining = [];
  for (let i=0;i<vals.length;i++){
    const [f,c,m] = vals[i];
    const fNorm = normDateStr_(f);
    if (fNorm === fechaNorm && String(c).trim() === cid) {
      // skip
    } else remaining.push([f,c,m]);
  }
  sh.clearContents();
  sh.getRange(1,1,1,3).setValues([["fecha","consultor_id","motivo"]]);
  if (remaining.length){
    sh.getRange(2,1,remaining.length,3).setValues(remaining);
  }
}

function listBlocks_(consultorId){
  const sh = getSheet_("Bloqueos");
  const last = sh.getLastRow();
  const out = [];
  if (last < 2) return out;

  const cid = String(consultorId || "ALL").trim();
  const vals = sh.getRange(2,1,last-1,3).getValues();
  for (let i=0;i<vals.length;i++){
    const [f,c,m] = vals[i];
    const fNorm = normDateStr_(f);
    if (cid && cid !== "ALL" && String(c).trim() !== cid) continue;
    out.push({ fecha: fNorm, consultor_id: String(c).trim(), motivo: m || "" });
  }
  return out;
}

// ========== doGet / doPost ==========
function doGet(e){
  const mode = String((e.parameter.mode || "init_booking")).trim();
  const today = todayDate_();

  if (mode === "init_booking"){
    return json_({ ok:true, today, days_ahead_max: CFG.DAYS_AHEAD_MAX, slot_minutes: CFG.SLOT_MINUTES });
  }

  if (mode === "consultores"){
    return json_({ ok:true, consultores: getConsultores_() });
  }

  if (mode === "boot"){
    const consultor = String((e.parameter.consultor || "todos")).trim();
    let fromStrParam = String((e.parameter.from || "")).trim();
    let toStrParam   = String((e.parameter.to   || "")).trim();

    if (!fromStrParam || !toStrParam){
      const td = parseDate_(today);
      const y = td.getFullYear();
      const m = td.getMonth();
      const first = new Date(y, m, 1);
      const last  = new Date(y, m+1, 0);
      fromStrParam = fromStrParam || dateToStr_(first);
      toStrParam   = toStrParam   || dateToStr_(last);
    }

    const consultores = getConsultores_();
    const av = availableDaysTurboResult_(consultor, fromStrParam, toStrParam, today);

    return json_({
      ok:true,
      init: { today, days_ahead_max: CFG.DAYS_AHEAD_MAX, slot_minutes: CFG.SLOT_MINUTES },
      consultores,
      availableDays: { dates: av.dates || [], _perf: av._perf || {} }
    });
  }

  if (mode === "availableDays"){
    const consultor = String((e.parameter.consultor || "todos")).trim();
    const fromStrParam = String((e.parameter.from || today)).trim();
    const toStrParam   = String((e.parameter.to   || today)).trim();
    return json_(availableDaysTurboResult_(consultor, fromStrParam, toStrParam, today));
  }

  if (mode === "slots"){
    const consultor = String((e.parameter.consultor || "todos")).trim();
    const dateStr   = String((e.parameter.date || "")).trim();
    if (!dateStr) return json_({ ok:false, error:"missing_date" });

    const res = slotsTurbo_(consultor, dateStr);
    if (!res.ok && res.error) return json_(res);

    return json_({ ok:true, date: dateStr, consultor, slots: res.slots || [], _perf: res._perf || {} });
  }

  if (mode === "listBookings"){
    try{
      const dateStr   = String((e.parameter.date || today)).trim();
      const consultor = String((e.parameter.consultor || "todos")).trim();
      const bookings  = listBookings_(dateStr, consultor);
      return json_({ ok:true, date: dateStr, consultor, bookings });
    }catch(errList){
      return json_({ ok:false, error:"list_error", details: String(errList) });
    }
  }

  if (mode === "schedule"){
    const cid = String((e.parameter.consultor || "")).trim();
    if (!cid) return json_({ ok:false, error:"missing_consultor" });

    const schedMap = getScheduleForConsultor_(cid);
    const days = [];
    for (let dow=1; dow<=7; dow++){
      const cfg = schedMap[dow] || { dow, enabled:false, start1:"", end1:"", start2:"", end2:"" };
      days.push(cfg);
    }
    return json_({ ok:true, consultor_id: cid, days });
  }

  if (mode === "blocks"){
    const c = String((e.parameter.consultor || "ALL")).trim();
    return json_({ ok:true, consultor: c, blocks: listBlocks_(c) });
  }

  return json_({ ok:false, error:"unknown_mode" });
}

function doPost(e){
  const p = normalizePost_(e);
  const mode = String(p.mode || "").trim();

  if (mode === "addConsultor"){
    const cid    = String(p.consultor_id || "").trim();
    const nombre = String(p.nombre || "").trim();
    if (!cid) return json_({ ok:false, error:"missing_consultor_id" });

    const sh = getSheet_("Consultores");
    const last = sh.getLastRow();
    if (last >= 2){
      const width = Math.max(4, sh.getLastColumn());
      const vals = sh.getRange(2,1,last-1,width).getValues();
      for (let i=0;i<vals.length;i++){
        if (String(vals[i][0]).trim() === cid){
          return json_({ ok:false, error:"duplicate_consultor_id" });
        }
      }
    }
    sh.getRange(sh.getLastRow()+1,1,1,4).setValues([[cid, nombre || ("Consultor "+cid), true, ""]]);
    if (!sh.getRange(1,4).getValue()) sh.getRange(1,4).setValue("days_ahead_limit");
    ensureDefaultScheduleForConsultor_(cid); // <- aquí sí (una sola vez)
    return json_({ ok:true, consultor_id: cid });
  }

  if (mode === "deleteConsultor"){
    const cid = String(p.consultor_id || "").trim();
    if (!cid) return json_({ ok:false, error:"missing_consultor_id" });
    const ok = setConsultorActive_(cid, false);
    if (!ok) return json_({ ok:false, error:"not_found" });
    return json_({ ok:true });
  }

  if (mode === "saveSchedule"){
    const cid = String(p.consultor_id || "").trim();
    if (!cid) return json_({ ok:false, error:"missing_consultor_id" });
    let days = [];
    try{
      if (typeof p.days === "string") days = JSON.parse(p.days);
      else days = p.days || [];
    }catch(_){
      return json_({ ok:false, error:"invalid_days" });
    }
    const daysLimit = normDaysAheadInt_(p.days_ahead_limit);
    saveScheduleForConsultor_(cid, days);
    setConsultorDaysAhead_(cid, daysLimit);
    return json_({ ok:true });
  }

  if (mode === "book"){
    const fechaRaw = String(p.fecha || "").trim();
    const fecha    = normDateStr_(fechaRaw);
    const horaRaw  = String(p.hora || "").trim();
    const hora     = normTimeHHMM_(horaRaw);
    let consultor  = String(p.consultor || "todos").trim();
    const nombre   = String(p.nombre || "").trim();
    const email    = String(p.email || "").trim();
    const telefono = String(p.telefono || "").trim();
    const marca    = String(p.marca || "").trim();
    const tipo     = String(p.tipo || "").trim();
    const idpipe   = String(p.idpipe || "").trim();

    if (!fecha || !hora) return json_({ ok:false, error:"missing_date_time" });

    const d = parseDate_(fecha);
    if (!d) return json_({ ok:false, error:"invalid_date" });

    const tMin = timeToMinutes_(hora);
    if (tMin == null || (tMin % CFG.SLOT_MINUTES) !== 0){
      return json_({ ok:false, error:"invalid_time_interval" });
    }

    const todayD = parseDate_(todayDate_());
    const diff = diffDays_(todayD, d);
    if (diff < 0) return json_({ ok:false, error:"past_date" });

    if (diff === 0){
      const nowM = timeToMinutes_(nowTime_());
      if (tMin < nowM + 10) return json_({ ok:false, error:"too_soon" });
    }

    const consultoresActivos = getConsultores_().filter(c => c.activo);
    if (!consultoresActivos.length) return json_({ ok:false, error:"no_consultores" });

    const { map: limitMap } = buildDaysAheadLimitMap_(consultoresActivos, todayD);
    const capForConsultor = (cid)=>{
      const lim = limitMap[cid];
      return lim ? lim.capDays : CFG.DAYS_AHEAD_MAX;
    };

    if (consultor !== "todos"){
      const exists = consultoresActivos.some(c => String(c.consultor_id).trim() === consultor);
      if (!exists) return json_({ ok:false, error:"invalid_consultor" });
      const capDays = capForConsultor(consultor);
      if (diff > capDays - 1) return json_({ ok:false, error:"too_far_consultor" });
    }

    let assignedConsultor = null;

    if (consultor !== "todos"){
      assignedConsultor = consultor;
      const res = slotsTurbo_(assignedConsultor, fecha);
      if (!res.ok) return json_(res);
      if ((res.slots || []).indexOf(hora) === -1) return json_({ ok:false, error:"slot_not_available" });
    } else {
      const elegibles = consultoresActivos.filter(c=>{
        const cid = String(c.consultor_id || "").trim();
        const capDays = capForConsultor(cid);
        return diff <= (capDays - 1);
      });
      if (!elegibles.length) return json_({ ok:false, error:"no_consultor_for_date" });

      // candidatos: consultores donde ese horario esté disponible (usando turbo por consultor)
      const candidates = [];
      for (let i=0;i<elegibles.length;i++){
        const cid = String(elegibles[i].consultor_id || "").trim();
        const res = slotsTurbo_(cid, fecha);
        if (!res.ok) continue;
        if ((res.slots || []).indexOf(hora) !== -1) candidates.push(cid);
      }
      if (!candidates.length) return json_({ ok:false, error:"no_slot_for_any_consultor" });
      assignedConsultor = candidates[Math.floor(Math.random()*candidates.length)];
    }

    const booking_id = createBooking_({
      fecha, hora,
      consultorSeleccionado: assignedConsultor,
      nombre, email, telefono, marca, tipo, idpipe
    });

    return json_({ ok:true, booking_id, consultor_id: assignedConsultor });
  }

  if (mode === "cancel"){
    const booking_id = String(p.booking_id || "").trim();
    const motivo     = String(p.motivo || "").trim();
    if (!booking_id) return json_({ ok:false, error:"missing_booking_id" });
    return json_(cancelBooking_(booking_id, motivo));
  }

  if (mode === "markDelay"){
    const booking_id = String(p.booking_id || "").trim();
    if (!booking_id) return json_({ ok:false, error:"missing_booking_id" });
    return json_(markDelaySent_(booking_id));
  }

  if (mode === "blockDate"){
    const fecha       = String(p.fecha || "").trim();
    const consultorId = String(p.consultor_id || "ALL").trim();
    const motivo      = String(p.motivo || "").trim();
    if (!fecha) return json_({ ok:false, error:"missing_fecha" });
    blockDate_(fecha, consultorId, motivo);
    return json_({ ok:true });
  }

  if (mode === "unblockDate"){
    const fecha       = String(p.fecha || "").trim();
    const consultorId = String(p.consultor_id || "ALL").trim();
    if (!fecha) return json_({ ok:false, error:"missing_fecha" });
    unblockDate_(fecha, consultorId);
    return json_({ ok:true });
  }

  return json_({ ok:false, error:"unknown_mode" });
}
