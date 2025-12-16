/*************** [G1] CONFIG ***************/
const SHEET_NAME = "Venta";
const TZ = Session.getScriptTimeZone() || "America/Santiago";

const COL = {
  ACTIVO: 2,         // B (casilla “en trato”)
  CONSULTOR: 3,      // C
  NOMBRE: 4,         // D
  IDPIPE: 5,         // E
  EMAIL: 8,          // H
  TELEFONO: 9,       // I
  MARCA: 10,         // J
  CLASES: 12,        // L
  CANT_CLASES: 13,   // M
  PDF: 14,           // N
  OPCION: 15,        // O
  CUPON: 65,         // BM
  FECHA_OUT: 17,     // Q
  DESCUENTO: 66,     // BN
  MONTO_BASE: 67,    // BO 
  LOG_BP: 68,        // BP (LOG TRATO)
  LOG_BT: 72,        // BT (aquí van SOLO llamadas)
  TIPO: 73,          // BU
  URL_FICHA: 76,     // BX
  URL_AGENDA: 77,    // BY
  URL_CONTACTO_FALLIDO: 78, // BZ
  ESTADO: 80,        // CB
  OBS: 81,           // CC
  ULTIMO_CONTACTO: 82, // CD
  FECHA_PROX: 84,    // CF
  HORA_PROX: 85,     // CG
  ESTADO_PAGO: 86,   // CH
  URL_FICHA_VIGILANCIA: 88,   // 
  ESTADO_PAGO_VIGILANCIA: 91,  // 
  PRESENCE_LOG: 95            // CQ (IN/OUT timestamps)


};
const LAST_COL_TO_READ = 91;

function getSecret_(){
  const s = PropertiesService.getScriptProperties().getProperty("TOKEN_SECRET");
  if (!s || !s.trim()) throw new Error("missing_token_secret");
  return s.trim();
}
function b64webEncode_(bytesOrString){
  const bytes = (typeof bytesOrString === "string")
    ? Utilities.newBlob(bytesOrString).getBytes()
    : bytesOrString;
  return Utilities.base64EncodeWebSafe(bytes).replace(/=+$/,'');
}
function b64webDecodeToString_(b64){
  const bytes = Utilities.base64DecodeWebSafe(b64);
  return Utilities.newBlob(bytes).getDataAsString();
}
function verifyToken_(token){
  if (!token || token.indexOf(".") === -1) return { ok:false, err:"TOKEN_INVALID" };
  const parts = token.split(".");
  if (parts.length !== 2) return { ok:false, err:"TOKEN_INVALID" };
  const payload = parts[0];
  const sig = parts[1];
  if (!payload || !sig) return { ok:false, err:"TOKEN_INVALID" };
  const mac = Utilities.computeHmacSha256Signature(payload, getSecret_());
  const expected = b64webEncode_(mac);
  if (sig !== expected) return { ok:false, err:"TOKEN_INVALID" };
  let obj;
  try { obj = JSON.parse(b64webDecodeToString_(payload)); } catch(e){ return { ok:false, err:"TOKEN_INVALID" }; }
  const now = Math.floor(Date.now()/1000);
  if (!obj || !obj.exp || now > Number(obj.exp)) return { ok:false, err:"TOKEN_EXPIRED" };
  return { ok:true, data: obj };
}
function rateLimitOk_(key, limit, windowSec){
  try{
    const cache = CacheService.getScriptCache();
    const k = `rl:${key}`;
    const cur = Number(cache.get(k) || "0") || 0;
    if (cur >= limit) return false;
    cache.put(k, String(cur + 1), windowSec);
    return true;
  }catch(_){
    return true;
  }
}

/*************** [G1.X] CACHE IDX POR IDC ***************/
// Activa/Desactiva cache
const IDX_CACHE_ENABLED = true;

// Cada cuánto “se recalcula” el índice (TTL del cache)
const IDX_CACHE_TTL_SEC = 180; // 3 minutos  <-- cambia acá si quieres

// Horario laboral (hora Chile, 24h)
const IDX_WORK_HOUR_START = 6;   // 09:00  <-- cambia acá si quieres
const IDX_WORK_HOUR_END   = 23;  // 20:00  <-- cambia acá si quieres

// Cache de RID por idpipe (para lookups rápidos en presence_status)
const RID_CACHE_TTL_SEC = 1800; // 30 min


/*************** [G2] HELPERS ***************/
function json_(obj, callbackName){
  const json = JSON.stringify(obj);

  // JSONP (debug sin CORS): ?callback=miFuncion
  const cb = String(callbackName || "").trim();
  if (cb && /^[a-zA-Z_$][0-9a-zA-Z_$\.]*$/.test(cb)){
    return ContentService
      .createTextOutput(`${cb}(${json});`)
      .setMimeType(ContentService.MimeType.JAVASCRIPT);
  }

  // JSON normal
  return ContentService
    .createTextOutput(json)
    .setMimeType(ContentService.MimeType.JSON);
}



function getSheet_(){
  const ss = SpreadsheetApp.getActiveSpreadsheet(); // en web app es el contenedor real
  if (!ss) throw new Error("No hay ActiveSpreadsheet (webapp)");
  const sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) throw new Error(`No existe hoja "${SHEET_NAME}"`);
  return sh;
}


function asStr(v){
  if (v === null || v === undefined) return "";
  if (v instanceof Date) return Utilities.formatDate(v, TZ, "yyyy-MM-dd HH:mm:ss");
  return String(v).trim();
}

function asBool_(v){
  if (v === true) return true;
  const s = String(v || "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "sí" || s === "si";
}

function formatRecorridoLog_(recorrido, now){
  const rec = String(recorrido || "").trim();
  if (!rec) return "";

  // Si ya viene con timestamp (dd-MM-yyyy HH:mm: ...) no lo duplicamos
  const hasTs = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}\s*:/.test(rec);
  if (hasTs) return rec;

  const ts = Utilities.formatDate(now || new Date(), TZ, "dd-MM-yyyy HH:mm");
  return `${ts}: ${rec}`;
}

function normKey_(v){
  let s = asStr(v);
  if (!s) return "";
  s = s.replace(/\.0+$/,"");
  return s.trim();
}

function ridCacheKey_(target){
  const ssId = SpreadsheetApp.getActiveSpreadsheet().getId();
  return `rid:${ssId}:${SHEET_NAME}:${target}`;
}

function findRowByIdpipeCached_(sh, idpipe){
  const target = normKey_(idpipe);
  if (!target) return null;

  const cache = CacheService.getScriptCache();
  const key = ridCacheKey_(target);
  const cached = cache.get(key);
  if (cached){
    const ridCached = parseInt(cached, 10);
    if (ridCached && ridCached > 1){
      try{
        const val = sh.getRange(ridCached, COL.IDPIPE).getDisplayValue();
        if (normKey_(val) === target) return ridCached; // cache valido
      }catch(_){}
    }
  }

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return null;

  const rng = sh.getRange(2, COL.IDPIPE, lastRow-1, 1);

  // Primero intenta TextFinder exacto (rápido)
  let cell = rng.createTextFinder(target)
    .matchEntireCell(true)
    .findNext();
  let rid = cell ? cell.getRow() : null;

  // Fallback tolerante (trim) si no lo encontró
  if (!rid){
    const vals = rng.getDisplayValues();
    for (let i=0;i<vals.length;i++){
      if (normKey_(vals[i][0]) === target){
        rid = i + 2;
        break;
      }
    }
  }

  if (rid){
    try{ cache.put(key, String(rid), RID_CACHE_TTL_SEC); }catch(_){}
  }
  return rid;
}

function toTimeMs_(v){
  if (!v) return 0;
  if (v instanceof Date) return v.getTime();
  const s = String(v).trim();
  if (!s) return 0;
  const d = new Date(s);
  return isNaN(d.getTime()) ? 0 : d.getTime();
}

function isEmail_(s){
  s = String(s || "").trim();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
}

/*************** [G2.X] CACHE HELPERS (IDX por IDC) ***************/
function inWorkHours_(){
  const h = Number(Utilities.formatDate(new Date(), TZ, "H")); // 0..23 en TZ
  return (h >= IDX_WORK_HOUR_START) && (h < IDX_WORK_HOUR_END);
}

function cacheKeyIdc_(idcKey){
  // Incluimos spreadsheetId + sheet para evitar colisiones si copias el script
  const ssId = SpreadsheetApp.getActiveSpreadsheet().getId();
  return `sm_idx:${ssId}:${SHEET_NAME}:${idcKey}`;
}

// Construye el “span” (filas donde aparece el IDC + min/max) en base al sheet actual
function buildIdcSpan_(sh, n, idcKey, dbg){
  const t0 = Date.now();

  // Lee toda la col C una vez (solo cuando hay miss)
  const colC = sh.getRange(2, COL.CONSULTOR, n, 1).getDisplayValues();

  const rowsIDC = [];
  let minRow = null, maxRow = null;

  for (let i = 0; i < n; i++){
    // i=0 corresponde a row 2
    // OJO: usamos normKey_ para comparar “limpio”
    if (normKey_(colC[i][0]) !== idcKey) continue;

    const r = i + 2;
    rowsIDC.push(r);
    if (minRow === null || r < minRow) minRow = r;
    if (maxRow === null || r > maxRow) maxRow = r;
  }

  if (!rowsIDC.length) return null;

  const span = {
    idc: idcKey,
    minRow,
    maxRow,
    rowsIDC,
    builtAt: Date.now()
  };

  if (dbg){
    dbg.cache_build_ms = Date.now() - t0;
    dbg.cache_rowsIDC = rowsIDC.length;
    dbg.cache_spanN = (maxRow - minRow + 1);
  }

  return span;
}

// Intenta traer del cache; si no está y estamos en horario laboral, lo crea y lo guarda
function getIdcSpanCached_(sh, n, idcKey, dbg){
  if (!IDX_CACHE_ENABLED) return null;
  if (!idcKey) return null;

  const cache = CacheService.getScriptCache();
  const key = cacheKeyIdc_(idcKey);

  const t0 = Date.now();
  const raw = cache.get(key);

  if (raw){
    try{
      const obj = JSON.parse(raw);
      if (obj && obj.rowsIDC && obj.minRow && obj.maxRow){
        if (dbg){
          dbg.cache_hit = 1;
          dbg.cache_get_ms = Date.now() - t0;
        }
        return obj;
      }
    }catch(_){}
  }

  if (dbg){
    dbg.cache_hit = 0;
    dbg.cache_get_ms = Date.now() - t0;
  }

  // Fuera de horario laboral: NO regeneramos cache
  if (!inWorkHours_()){
    if (dbg) dbg.cache_note = "miss_offhours";
    return null;
  }

  // Miss en horario laboral: construye y guarda
  const built = buildIdcSpan_(sh, n, idcKey, dbg);
  if (built){
    cache.put(key, JSON.stringify(built), IDX_CACHE_TTL_SEC);
    if (dbg) dbg.cache_put = `ok_ttl_${IDX_CACHE_TTL_SEC}s`;
  } else {
    if (dbg) dbg.cache_put = "no_rows_for_idc";
  }

  return built;
}


/*************** [G3] PICK NEXT (FAST) ***************/
function pickNextFast_(sh, idc, skipSet){
  const t0 = Date.now();
  const sk = (skipSet instanceof Set) ? skipSet : new Set();

  const dbg = {
    lastRow: 0,
    n: 0,
    scanned: 0,
    match_idc: 0,
    excluded_out: 0,
    estado_ok: 0,
    skipped: 0,
    p1_found: 0,
    bestRow: null,
    detail: "—",

    ms_total: 0,
    ms_lastRow: 0,
    ms_readCols: 0,
    ms_scan: 0,
    ms_readDealRow: 0,
    sheet_getLastRow: "—"
  };

  let t = Date.now();
  const lastRow = sh.getLastRow();
  dbg.ms_lastRow = Date.now() - t;
  dbg.sheet_getLastRow = dbg.ms_lastRow;
  dbg.lastRow = lastRow;

  if (lastRow < 2){
    dbg.detail = "empty";
    dbg.ms_total = Date.now() - t0;
    return { bestRow:null, debug: dbg };
  }

  const n = lastRow - 1;
  dbg.n = n;

  const tReadStart = Date.now();

const target = normKey_(idc);

// 1) Intenta traer el índice del cache (rowsIDC + min/max)
let rowsIDC = null;
let minRow = null, maxRow = null;

const spanCached = getIdcSpanCached_(sh, n, target, dbg);

const tScanStart = Date.now();

if (spanCached){
  // Cache HIT (o build en horario laboral)
  rowsIDC = spanCached.rowsIDC || [];
  minRow = spanCached.minRow;
  maxRow = spanCached.maxRow;

  dbg.match_idc = rowsIDC.length;
  // scanned lo dejamos en 0 porque no recorrimos colC aquí
  dbg.scanned = 0;

} else {
  // Cache MISS fuera de horario (o desactivado): fallback a tu lógica original
  const colC = sh.getRange(2, COL.CONSULTOR, n, 1).getDisplayValues();

  rowsIDC = [];
  minRow = null;
  maxRow = null;

  for (let i=0;i<n;i++){
    dbg.scanned++;
    if (normKey_(colC[i][0]) !== target) continue;
    dbg.match_idc++;
    const r = i + 2;
    rowsIDC.push(r);
    if (minRow === null || r < minRow) minRow = r;
    if (maxRow === null || r > maxRow) maxRow = r;
  }
}

if (!rowsIDC.length){
  dbg.ms_readCols = Date.now() - tReadStart;
  dbg.ms_scan = Date.now() - tScanStart;
  dbg.detail = "no_match_idc";
  dbg.ms_total = Date.now() - t0;
  return { bestRow:null, debug: dbg };
}

const spanN = maxRow - minRow + 1;

// 2) Igual que antes: leemos Q/CB/CD en bloque (pero ya no leímos colC si hubo cache)
const qBlock  = sh.getRange(minRow, COL.FECHA_OUT, spanN, 1).getDisplayValues();
const cbBlock = sh.getRange(minRow, COL.ESTADO,    spanN, 1).getDisplayValues();
const cdBlock = sh.getRange(minRow, COL.ULTIMO_CONTACTO, spanN, 1).getValues();

dbg.ms_readCols = Date.now() - tReadStart;
dbg.ms_scan = Date.now() - tScanStart;


  const allowedEstado = new Set(["", "PEND. PAGO", "PAGADO", "PEND. PAGO REGISTRO", "REGISTRO PAGADO", "PEND. PAGO VIGILANCIA"]);
  const candidates = [];

  for (const r of rowsIDC){
    if (sk.has(r)){ dbg.skipped++; continue; }

    const off = r - minRow;

    const q = (qBlock[off]?.[0] || "").trim();
    if (q){
      dbg.excluded_out++;
      continue;
    }

    const estU = (cbBlock[off]?.[0] || "").trim().toUpperCase();
    if (!allowedEstado.has(estU)) continue;

    dbg.estado_ok++;
    candidates.push(r);
  }

  if (!candidates.length){
    dbg.ms_scan = Date.now() - tScanStart;
    dbg.detail = "no_candidates";
    dbg.ms_total = Date.now() - t0;
    return { bestRow:null, debug: dbg };
  }

  let bestRow = null;
  let bestTime = null;

  for (const r of candidates){
    const off = r - minRow;
    const cd = cdBlock[off]?.[0];
    const cdEmpty = (cd === "" || cd === null || cd === undefined);
    if (cdEmpty){
      dbg.p1_found = 1;
      bestRow = r;
      bestTime = 0;
      break;
    }
  }

  if (bestRow === null){
    for (const r of candidates){
      const off = r - minRow;
      const tms = toTimeMs_(cdBlock[off]?.[0]) || 0;
      if (bestRow === null || tms < bestTime || (tms === bestTime && r < bestRow)){
        bestRow = r;
        bestTime = tms;
      }
    }
  }

  dbg.bestRow = bestRow;
  dbg.ms_scan = Date.now() - tScanStart;
  dbg.detail = "ok";
  dbg.ms_total = Date.now() - t0;

  return { bestRow, debug: dbg };
}

/*************** [G4] DEAL SERIALIZE ***************/
function rowToDeal_(sheetRow, row){
  return {
    RID: sheetRow,
    CONSULTOR: asStr(row[COL.CONSULTOR-1]),
    NOMBRE: asStr(row[COL.NOMBRE-1]),
    IDPIPE: asStr(row[COL.IDPIPE-1]),
    EMAIL: asStr(row[COL.EMAIL-1]),
    TELEFONO: asStr(row[COL.TELEFONO-1]),
    MARCA: asStr(row[COL.MARCA-1]),
    CLASES: asStr(row[COL.CLASES-1]),
    CANT_CLASES: asStr(row[COL.CANT_CLASES-1]),
    PDF: asStr(row[COL.PDF-1]),
    OPCION: asStr(row[COL.OPCION-1]),
    CUPON: asStr(row[COL.CUPON-1]),
    MONTO_BASE: asStr(row[COL.MONTO_BASE-1]), // 
    DESCUENTO: asStr(row[COL.DESCUENTO-1]),
    LOG_BT: asStr(row[COL.LOG_BT-1]), // 
    TIPO: asStr(row[COL.TIPO-1]),
    URL_FICHA: asStr(row[COL.URL_FICHA-1]),
    URL_AGENDA: asStr(row[COL.URL_AGENDA-1]),
    URL_CONTACTO_FALLIDO: asStr(row[COL.URL_CONTACTO_FALLIDO-1]),
    ESTADO: asStr(row[COL.ESTADO-1]),
    OBS: asStr(row[COL.OBS-1]),
    ULTIMO_CONTACTO: asStr(row[COL.ULTIMO_CONTACTO-1]),
    FECHA_PROX: asStr(row[COL.FECHA_PROX-1]),
    HORA_PROX: asStr(row[COL.HORA_PROX-1]),
    FECHA_OUT: asStr(row[COL.FECHA_OUT-1]),
    ESTADO_PAGO: asStr(row[COL.ESTADO_PAGO-1]), // 
    URL_FICHA_VIGILANCIA: asStr(row[COL.URL_FICHA_VIGILANCIA-1]), // 
    ESTADO_PAGO_VIGILANCIA: asStr(row[COL.ESTADO_PAGO_VIGILANCIA-1]) // 

  };
}

/*************** [G4.1] SEARCH HELPERS ***************/
function searchByEmail_(sh, email){
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];

  const n = lastRow - 1;
  const target = String(email || "").trim().toLowerCase();

  const colH = sh.getRange(2, COL.EMAIL, n, 1).getDisplayValues();
  const colQ = sh.getRange(2, COL.FECHA_OUT, n, 1).getDisplayValues();
  const colE = sh.getRange(2, COL.IDPIPE, n, 1).getDisplayValues();
  const colJ = sh.getRange(2, COL.MARCA, n, 1).getDisplayValues();

  const out = [];
  for (let i=0;i<n;i++){
    const h = String(colH[i][0] || "").trim().toLowerCase();
    if (!h || h !== target) continue;

    const q = String(colQ[i][0] || "").trim();
    if (q) continue;

    const rid = i + 2;
    out.push({
      RID: rid,
      IDPIPE: String(colE[i][0] || "").trim(),
      MARCA: String(colJ[i][0] || "").trim()
    });
  }
  return out;
}

function searchByIdpipe_(sh, idpipe){
  const rid = findRowByIdpipeCached_(sh, idpipe);
  if (!rid) return null;

  const row = sh.getRange(rid, 1, 1, LAST_COL_TO_READ).getValues()[0];
  return rowToDeal_(rid, row);
}

function findRowByIdpipe_(sh, idpipe){
  return findRowByIdpipeCached_(sh, idpipe);
}

function parsePresenceLog_(txt){
  const lines = String(txt || "").split(/\r?\n/).map(s=> s.trim()).filter(Boolean);
  let lastInMs = 0, lastOutMs = 0;
  let lastIn = "", lastOut = "";

  lines.forEach(line=>{
    const m = line.match(/^(IN|OUT)\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/i);
    if (!m) return;
    const kind = m[1].toUpperCase();
    const tsStr = m[2];
    const ms = toTimeMs_(tsStr);
    if (!ms) return;
    if (kind === "IN" && (!lastInMs || ms >= lastInMs)){
      lastInMs = ms;
      lastIn = tsStr;
    } else if (kind === "OUT" && (!lastOutMs || ms >= lastOutMs)){
      lastOutMs = ms;
      lastOut = tsStr;
    }
  });

  return { lastIn, lastOut, lastInMs, lastOutMs };
}

function presenceStatusByIdpipe_(idpipe){
  const sh = getSheet_();
  const rid = findRowByIdpipe_(sh, idpipe);
  if (!rid) return { ok:false, error:"not_found" };

  const activeVal = sh.getRange(rid, COL.ACTIVO).getValue();
  const logVal = sh.getRange(rid, COL.PRESENCE_LOG).getValue();
  const parsed = parsePresenceLog_(logVal);

  return {
    ok: true,
    rid,
    activo: asBool_(activeVal),
    lastIn: parsed.lastIn || "",
    lastOut: parsed.lastOut || "",
    raw: asStr(logVal)
  };
}

/*************** [G-PRESENCE] PRESENCIA POPUP (COL B) ***************/
const PRESENCE_TTL_MS = 12 * 60 * 1000; // 12 min (trigger cada 10 min)
const PRESENCE_KEY = "sm_presence_sessions_v1";
const PRESENCE_MARKED_KEY = "sm_presence_marked_rids_v1";

function presReadSessions_(){
  const props = PropertiesService.getScriptProperties();
  const raw = props.getProperty(PRESENCE_KEY);
  if (!raw) return {};
  try{
    const obj = JSON.parse(raw);
    return (obj && typeof obj === "object") ? obj : {};
  }catch(_){
    return {};
  }
}

function presWriteSessions_(sessions){
  PropertiesService.getScriptProperties().setProperty(PRESENCE_KEY, JSON.stringify(sessions || {}));
}

function presReadMarked_(){
  const props = PropertiesService.getScriptProperties();
  const raw = props.getProperty(PRESENCE_MARKED_KEY) || "";
  const set = new Set();
  raw.split(",").forEach(s=>{
    const n = parseInt(String(s).trim(), 10);
    if (n && n > 1) set.add(n);
  });
  return set;
}

function presWriteMarked_(set){
  const arr = Array.from(set || []).filter(n=>n && n > 1);
  PropertiesService.getScriptProperties().setProperty(PRESENCE_MARKED_KEY, arr.join(","));
}

function presAppendLog_(sh, rid, kind){
  if (!rid || rid < 2) return;
  const k = (String(kind || "").toUpperCase() === "IN") ? "IN" : (String(kind || "").toUpperCase() === "OUT" ? "OUT" : "");
  if (!k) return;

  const rng = sh.getRange(rid, COL.PRESENCE_LOG);
  const prev = asStr(rng.getValue());
  const ts = Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd HH:mm:ss");
  const entry = `${k} ${ts}`;
  const next = prev ? `${prev}\n${entry}` : entry;

  try{ rng.setValue(next); }catch(_){}
}

function presSetCheckbox_(sh, rid, val, diag){
  if (!rid || rid < 2) return { ok:false, error:"bad_rid" };

  const rng = sh.getRange(rid, COL.ACTIVO);
  const out = {
    ok: true,
    rid: rid,
    a1: rng.getA1Notation(),
    wanted: !!val
  };

  if (diag){
    out.before = rng.getDisplayValue();
    out.beforeRaw = rng.getValue();
  }

  const prevRaw = out.beforeRaw !== undefined ? out.beforeRaw : rng.getValue();
  const prevBool = asBool_(prevRaw);
  const nextBool = !!val;

  if (prevBool === nextBool){
    out.noop = true;
    return out;
  }

  try{
    rng.setValue(nextBool);
    presAppendLog_(sh, rid, nextBool ? "IN" : "OUT");
    // SpreadsheetApp.flush();  // <- QUITADO para que sea rápido
  }catch(err){
    out.ok = false;
    out.error = String(err?.message || err);
    return out;
  }

  if (diag){
    out.after = rng.getDisplayValue();
    out.afterRaw = rng.getValue();
  }

  return out;
}



function presActiveRidsNow_(sessions, nowMs){
  const active = new Set();
  Object.keys(sessions || {}).forEach(sid=>{
    const s = sessions[sid];
    const rid = parseInt(s?.rid || "0", 10);
    const last = Number(s?.last || 0);
    if (rid > 1 && last > 0 && (nowMs - last) <= PRESENCE_TTL_MS){
      active.add(rid);
    }
  });
  return active;
}

function presIsRidActiveNow_(sessions, rid, nowMs){
  const r = parseInt(rid || "0", 10);
  if (!r || r < 2) return false;
  const active = presActiveRidsNow_(sessions, nowMs);
  return active.has(r);
}

function handlePresence_(p){
  const action = String(p.action || "").toLowerCase(); // enter|ping|switch|leave
  const sid = String(p.sid || "").trim();
  const cb  = String(p.callback || "").trim();

  if (!sid) return json_({ ok:false, error:"missing_sid" }, cb);

  const now = Date.now();
  const ridParam = parseInt(String(p.rid || "0"), 10) || 0;

  // --- PING ULTRA LIVIANO: no sheet, no marked ---
  if (action === "ping"){
    const lock = LockService.getScriptLock();
    try{
      // lock corto: si está ocupado, preferimos NO bloquear
      lock.waitLock(1500);
    }catch(_){
      return json_({ ok:true }, cb);
    }

    try{
      const sessions = presReadSessions_();
      const prev = sessions[sid] || null;

      const ridIn = parseInt(String(ridParam || (prev?.rid || "0")), 10) || 0;
      if (!ridIn || ridIn < 2){
        return json_({ ok:false, error:"missing_rid" }, cb);
      }

      if (!sessions[sid]){
        sessions[sid] = { rid: ridIn, last: now };
      } else {
        sessions[sid].last = now;
        if (sessions[sid].rid !== ridIn) sessions[sid].rid = ridIn;
      }

      presWriteSessions_(sessions);
      return json_({ ok:true }, cb);

    } finally {
      try{ lock.releaseLock(); }catch(_){}
    }
  }

  // --- ENTER / SWITCH / LEAVE: pueden tocar sheet ---
  const lock = LockService.getScriptLock();
  lock.waitLock(25000);

  try{
    const sessions = presReadSessions_();
    const prev = sessions[sid] || null;

    const ridIn = parseInt(String(ridParam || (prev?.rid || "0")), 10) || 0;
    const prevRid = parseInt(String(p.prevRid || (prev?.rid || "0")), 10) || 0;

    if (action === "enter" && ridIn > 1){
      sessions[sid] = { rid: ridIn, last: now };

    } else if (action === "switch" && ridIn > 1){
      sessions[sid] = { rid: ridIn, last: now };

    } else if (action === "leave"){
      if (sessions[sid]) delete sessions[sid];

    } else {
      return json_({ ok:false, error:"bad_action" }, cb);
    }

    // Persist sesiones primero
    presWriteSessions_(sessions);

    // Si no hay rid válido para enter/switch, no hacemos nada
    const sh = getSheet_();
    const marked = presReadMarked_();

    const diagOn = String(p.diag || "") === "1";
    let diag = null;

    // En enter/switch: asegurar TRUE
    if ((action === "enter" || action === "switch") && ridIn > 1){
      diag = presSetCheckbox_(sh, ridIn, true, diagOn);
      marked.add(ridIn);
    }

    // En switch/leave: intentar bajar el anterior si ya no está activo
    const affectedPrev = (action === "switch") ? prevRid : (action === "leave" ? prevRid : 0);
    if (affectedPrev > 1){
      const stillActive = presIsRidActiveNow_(sessions, affectedPrev, now);
      if (!stillActive){
        presSetCheckbox_(sh, affectedPrev, false);
        marked.delete(affectedPrev);
      }
    }

    presWriteMarked_(marked);
    return json_({ ok:true, diag: diag || null }, cb);

  } finally {
    try{ lock.releaseLock(); }catch(_){}
  }
}



/***************
 * Trigger time-driven (cada 10 min)
 * - Si no hay sesiones: sale al tiro
 * - Si hay sesiones: expira y desmarca rids sin presencia
 ***************/
function smPresenceSweep(){
  const lock = LockService.getScriptLock();
  lock.waitLock(25000);

  try{
    const sessions = presReadSessions_();
    const sids = Object.keys(sessions || {});
    if (!sids.length){
      // No hay nadie: salida inmediata (ultra liviana)
      return;
    }

    const now = Date.now();

    // 1) Purga expirados
    let changed = false;
    sids.forEach(sid=>{
      const last = Number(sessions[sid]?.last || 0);
      if (!last || (now - last) > PRESENCE_TTL_MS){
        delete sessions[sid];
        changed = true;
      }
    });

    // 2) Calcula rids activos
    const activeRids = presActiveRidsNow_(sessions, now);

    // 3) Marcados actuales (lo que alguna vez pusimos TRUE)
    const marked = presReadMarked_();

    // Si después de purgar quedó vacío: apaga todos los marcados y limpia
    if (activeRids.size === 0){
      if (marked.size){
        const sh = getSheet_();
        marked.forEach(rid => presSetCheckbox_(sh, rid, false));
        presWriteMarked_(new Set());
      }
      if (changed) presWriteSessions_({});
      return;
    }

    // 4) Desmarca rids que estaban marcados pero ya no están activos
    const toFalse = [];
    marked.forEach(rid => { if (!activeRids.has(rid)) toFalse.push(rid); });

    // 5) (Opcional sanidad) marca rids activos que no estén en marked
    const toTrue = [];
    activeRids.forEach(rid => { if (!marked.has(rid)) toTrue.push(rid); });

    if (toFalse.length || toTrue.length){
      const sh = getSheet_();
      toFalse.forEach(rid => presSetCheckbox_(sh, rid, false));
      toTrue.forEach(rid => presSetCheckbox_(sh, rid, true));
    }

    // 6) Persist
    const newMarked = new Set(activeRids);
    presWriteMarked_(newMarked);
    if (changed) presWriteSessions_(sessions);

  } finally {
    lock.releaseLock();
  }
}


/*************** [G5] HTTP GET ***************/
function doGet(e){
  try{
    const p = e?.parameter || {};
    const mode = String(p.mode || "").toLowerCase();
    const idc  = String(p.idc || "").trim();
    const debugOn = String(p.debug || "") === "1";
    const cb = String(p.callback || "").trim();
    const tokenRaw = String(p.token || p.t || "").trim();

    // ✅ Ultra liviano: presencia por IDPIPE (sin token)
    if (mode === "presence_status"){
      const idpipe = String(p.idpipe || p.id || "").trim();
      if (!idpipe) return json_({ ok:false, error:"missing_idpipe" }, cb);
      if (!rateLimitOk_(normKey_(idpipe) || "anon", 180, 300)) return json_({ ok:false, error:"rate_limited" }, cb);
      try{
        const res = presenceStatusByIdpipe_(idpipe);
        return json_(res, cb);
      }catch(err){
        return json_({ ok:false, error:"server_error", detail:String(err?.message || err) }, cb);
      }
    }

    const vTok = verifyToken_(tokenRaw);
    if (!vTok.ok) return json_({ ok:false, error:vTok.err }, p.callback || "");
    const tokIDC = String(vTok.data?.idc || "").trim();
    if (idc && tokIDC && idc !== tokIDC) return json_({ ok:false, error:"forbidden_idc" }, p.callback || "");
    if (!rateLimitOk_(tokIDC || "anon", 60, 300)) return json_({ ok:false, error:"rate_limited" }, p.callback || "");

    if (mode === "ping"){
      return json_({ ok:true, msg:"PING_OK" }, cb);
    }

    // ✅ Ultra liviano: solo CH
    if (mode === "paystate"){
      const rid = parseInt(String(p.rid || "0"), 10);
      if (!rid || rid < 2) return json_({ ok:false, error:"bad_rid" }, cb);
      const sh = getSheet_();
      const v = sh.getRange(rid, COL.ESTADO_PAGO).getDisplayValue();
      return json_({ ok:true, value: String(v || "").trim() }, cb);
    }

    // ✅ Ultra liviano: solo CM (Estado pago vigilancia)
    if (mode === "paystate_vig"){
      const rid = parseInt(String(p.rid || "0"), 10);
      if (!rid || rid < 2) return json_({ ok:false, error:"bad_rid" }, cb);
      const sh = getSheet_();
      const v = sh.getRange(rid, COL.ESTADO_PAGO_VIGILANCIA).getDisplayValue();
      return json_({ ok:true, value: String(v || "").trim() }, cb);
    }

    // ✅ Ultra liviano: solo BP
    if (mode === "log"){
      const rid = parseInt(String(p.rid || "0"), 10);
      if (!rid || rid < 2) return json_({ ok:false, error:"bad_rid" }, cb);
      const sh = getSheet_();
      const v = sh.getRange(rid, COL.LOG_BP).getValue();
      return json_({ ok:true, value: asStr(v) }, cb);
    }

    if (mode === "next"){
      if (!idc) return json_({ ok:false, error:"missing_idc" }, cb);

      const sh = getSheet_();

      const skipSet = new Set(
        String(p.skip || "")
          .split(",")
          .map(s => parseInt(String(s).trim(), 10))
          .filter(n => n && n > 1)
      );

      const pick = pickNextFast_(sh, idc, skipSet);

      if (!pick.bestRow){
        return json_({ ok:false, error:"no_deals", ...(debugOn ? { debug: pick.debug } : {}) }, cb);
      }

      const tRead = Date.now();
      const row = sh.getRange(pick.bestRow, 1, 1, LAST_COL_TO_READ).getValues()[0];
      pick.debug.ms_readDealRow = Date.now() - tRead;

      const deal = rowToDeal_(pick.bestRow, row);
      return json_({ ok:true, deal, ...(debugOn ? { debug: pick.debug } : {}) }, cb);
    }

    if (mode === "deal"){
      const rid = parseInt(String(p.rid || "0"), 10);
      if (!rid || rid < 2) return json_({ ok:false, error:"bad_rid" }, cb);

      const sh = getSheet_();
      const row = sh.getRange(rid, 1, 1, LAST_COL_TO_READ).getValues()[0];
      const deal = rowToDeal_(rid, row);
      return json_({ ok:true, deal }, cb);
    }

    if (mode === "search"){
      const q = String(p.q || "").trim();
      if (!q) return json_({ ok:false, error:"missing_q" }, cb);

      const sh = getSheet_();

      if (isEmail_(q)){
        const results = searchByEmail_(sh, q);
        return json_({ ok:true, type:"email", results }, cb);
      } else {
        const deal = searchByIdpipe_(sh, q);
        if (!deal) return json_({ ok:false, error:"not_found" }, cb);
        return json_({ ok:true, type:"idpipe", deal }, cb);
      }
    }

    if (mode === "presence"){
      // ✅ pasa el callback hacia handlePresence_ para que responda JSONP
      p.callback = cb;
      return handlePresence_(p);
    }

    return json_({ ok:false, error:"unknown_mode" }, cb);

  }catch(err){
    return json_({ ok:false, error:"server_error", detail:String(err?.message || err) }, "");
  }
}


/*************** [G5.9] ESTADO (CB) FROM FINAL SURVEY ***************/
function estadoFromSurvey_(code, rid){
  const c = String(code || "").trim();
  if (!c) return "";

  // Fórmulas (usa funciones en inglés para setFormula)
  const fAGENDADO = `=IF(NOW()<CO${rid},"AGENDADO","")`;

  const fREGISTRO = `=IF(NOW()<CO${rid},"AGENDADO PAGO REGISTRO",IF(CH${rid}="PAGADO","REGISTRO PAGADO","PEND. PAGO REGISTRO"))`;

  const fVIGILANCIA = `=IF(NOW()<CO${rid},"AGENDADO PAGO VIGILANCIA",IF(CM${rid}="PAGADO VIGILANCIA","VIGILANCIA PAGADA","PEND. PAGO VIGILANCIA"))`;

  const fANALISIS = `=IF(VLOOKUP($E${rid},'Estados Análisis'!A:B,2,FALSE)="No compró","NO COMPRÓ ANÁLISIS","ANÁLISIS")`;

  switch (c){

    // ===== TEL / NUMERO =====
    case "A.1.1":
    case "B.1.1":
    case "C.1.1":
    case "A.2.1":
    case "B.2.1":
    case "C.2.1":
      return "ERROR NÚMERO";

    case "A.2.6":
      return "NO CORRESPONDE";

    case "A.2.7":
      return "NUEVA SOLICITUD";

    // ===== PERDIDOS REGISTRO =====
    case "A.2.3":
    case "B.2.3":
      return "PERDIDO";

    // ===== AGENDADO (NO PODÍA HABLAR) =====
    case "A.2.4":
      return fAGENDADO;

    // ===== MOTIVO / ANALISIS / PRESUPUESTO =====
    case "A.2.2.3":
    case "B.2.2.3":
      return "PERDIDO";

    case "A.2.2.1.1":
    case "B.2.2.1.1":
      return fANALISIS;

    case "A.2.2.1.2":
    case "B.2.2.1.2":
      return "PERDIDO";

    case "A.2.2.2.1":
    case "B.2.2.2.1":
      return "OPC1";

    case "A.2.2.2.2":
    case "B.2.2.2.2":
      return "PERDIDO";

    // ===== POST-PAGO REGISTRO (AGENDAR / TRANSFER) =====
    case "A.3.2.2":
    case "A.3.1.2":
    case "B.2.4":
    case "B.2.5.2":
      return fREGISTRO;

    case "A.3.2.1":
      return "PERDIDO";

    // ===== VIGILANCIA: NO ELIGE / PIERDE =====
    case "A.4.2":
    case "B.3.2":
      return "PERDIDO VIGILANCIA";

    case "A.5.2.1":
    case "B.4.2.1":
      return "PERDIDO VIGILANCIA";

    // ===== VIGILANCIA: AGENDADO (NO PODÍA PAGAR) / TRANSFER =====
    case "A.5.2.2":
    case "A.5.1.2": // (transfer, se agenda)
    case "B.4.2.2":
    case "B.4.1.2":
    case "C.2.4":
    case "C.2.5.2":
      return fVIGILANCIA;

    // ===== VIGILANCIA: PAGADA BOTÓN =====
    case "A.5.1.1":
    case "B.4.1.1":
    case "C.2.5.1":
      return "VIGILANCIA PAGADA";

    // ===== VIGILANCIA: razones (C) =====
    case "C.2.3":
    case "C.2.2.1":
    case "C.2.2.2":
      return "PERDIDO VIGILANCIA";
  }

  // Si no está mapeado, NO tocamos CB
  return "";
}


/*************** [G6] HTTP POST ***************/
function doPost(e){
  try{
    const p = normalizePost_(e);
    const mode = String(p.mode || "").toLowerCase();
    const tokenRaw = String(p.token || p.t || "").trim();
    const vTok = verifyToken_(tokenRaw);
    if (!vTok.ok) return json_({ ok:false, error:vTok.err });
    const tokIDC = String(vTok.data?.idc || "").trim();
    if (!rateLimitOk_(tokIDC || "anon", 100, 300)) return json_({ ok:false, error:"rate_limited" });

        // ✅ PRESENCE por POST (no-cors friendly)
    if (mode === "presence"){
      p.callback = ""; // fuerza JSON normal (no JSONP)
      return handlePresence_(p);
    }


    if (mode === "finalize"){
  const sh = getSheet_();
  const rid = parseInt(p.rid || "0", 10);
  if (!rid || rid < 2) return json_({ ok:false, error:"bad_rid" });

  const obs = String(p.obs || "").trim();
  const calls = String(p.calls_text || "").trim();
  const recorrido = String(p.recorrido || p.note || "").trim(); 


  const now = new Date();

  sh.getRange(rid, COL.ULTIMO_CONTACTO).setValue(now);
  sh.getRange(rid, COL.OBS).setValue(obs);

  // ✅ LOG BT: agrega calls y debajo el recorrido (como bloque de 2 líneas)
  if (calls || recorrido){
    const btCell = sh.getRange(rid, COL.LOG_BT);
    const prevBT = asStr(btCell.getValue());
    const recLine = formatRecorridoLog_(recorrido, now);

    let entry = "";
    if (calls) entry += calls;
    if (recLine) entry += (entry ? "\n" : "") + recLine;

    const nextBT = prevBT ? (prevBT + "\n" + entry) : entry;
    btCell.setValue(nextBT);
    // <<< NUEVO: recalcular secuencia N/C/F y PERDIDO NC
    procesarSecuenciaNCPerdido_(btCell);
  }

  // ✅ setear ESTADO (CB) según respuesta final (survey)
  const surveyCode = String(p.survey || "").trim(); // ej: "A.2.4"
  const nextEstado = estadoFromSurvey_(surveyCode, rid);

  if (nextEstado){
    const cb = sh.getRange(rid, COL.ESTADO); // CB

    // ✅ 1) Guarda la validación actual (dropdown)
    const dv = cb.getDataValidation();

    // ✅ 2) Escribe valor o fórmula
    if (String(nextEstado).trim().startsWith("=")) cb.setFormula(nextEstado);
    else cb.setValue(nextEstado);

    // ✅ 3) Re-aplica la validación para que no se pierda
    if (dv) cb.setDataValidation(dv);
  }

  return json_({ ok:true });
}


    if (mode === "omit"){
      const sh = getSheet_();
      const rid = parseInt(p.rid || "0", 10);
      if (!rid || rid < 2) return json_({ ok:false, error:"bad_rid" });

      const motivo = String(p.motivo || "").trim();
      if (!motivo) return json_({ ok:false, error:"missing_motivo" });

      const estadoCell = sh.getRange(rid, COL.ESTADO);
      const estadoPrev = asStr(estadoCell.getValue());

      const now = new Date();
      const ts = Utilities.formatDate(now, TZ, "dd-MM-yyyy HH:mm:ss");

      const line = `OMITIDA: ${ts} de ${estadoPrev || "—"} a REVISION. Motivo: ${motivo}`;

      const obsCell = sh.getRange(rid, COL.OBS);
      const prevObs = asStr(obsCell.getValue());
      const nextObs = prevObs ? (prevObs + "\n" + line) : line;
      obsCell.setValue(nextObs);

      estadoCell.setValue("REVISIÓN");

      return json_({ ok:true });
    }

    return json_({ ok:false, error:"unknown_mode" });
  }catch(err){
    return json_({ ok:false, error:"server_error", detail:String(err?.message || err) });
  }
}

/*************** [G7] POST NORMALIZE ***************/
function normalizePost_(e){
  const out = {};
  if (e && e.parameter){
    Object.keys(e.parameter).forEach(k => out[k] = e.parameter[k]);
  }
  if (e && e.postData && e.postData.contents){
    try { Object.assign(out, JSON.parse(e.postData.contents)); } catch(_){}
  }
  return out;
}

function findRowByIdpipe_(sh, idpipe){
  return findRowByIdpipeCached_(sh, idpipe);
}

function parsePresenceLog_(txt){
  const lines = String(txt || "").split(/\r?\n/).map(s=> s.trim()).filter(Boolean);
  let lastInMs = 0, lastOutMs = 0;
  let lastIn = "", lastOut = "";

  lines.forEach(line=>{
    const m = line.match(/^(IN|OUT)\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/i);
    if (!m) return;
    const kind = m[1].toUpperCase();
    const tsStr = m[2];
    const ms = toTimeMs_(tsStr);
    if (!ms) return;
    if (kind === "IN" && (!lastInMs || ms >= lastInMs)){
      lastInMs = ms;
      lastIn = tsStr;
    } else if (kind === "OUT" && (!lastOutMs || ms >= lastOutMs)){
      lastOutMs = ms;
      lastOut = tsStr;
    }
  });

  return { lastIn, lastOut, lastInMs, lastOutMs };
}

/*************** REGISTRAR TIEMPO ***************/

function registrarTiempo(e) {
  const hoja = e.source.getActiveSheet();
  const celda = e.range;
  const nombreHoja = hoja.getName();
  const fila = celda.getRow();
  const ahora = new Date();
  const zona = Session.getScriptTimeZone();
  const formato = "dd-MM-yyyy HH:mm:ss";
  const horaActual = Utilities.formatDate(ahora, zona, formato);

  // ----- Configuración para Hoja 1 -----
  if (nombreHoja === "Venta") {
    const colCasilla = 2; // Columna A
    const colRegistro = 72; // Columna B
    if (celda.getColumn() !== colCasilla) return;

    const celdaRegistro = hoja.getRange(fila, colRegistro);
    let registro = celdaRegistro.getValue().toString().trim();

    if (celda.getValue() === true) {
      const lineas = registro ? registro.split("\n") : [];
      const ultima = lineas[lineas.length - 1] || "";

      if (!ultima || ultima.includes("/")) {
        lineas.push(horaActual);
        celdaRegistro.setNumberFormat("@");
        celdaRegistro.setValue(lineas.join("\n"));
      }
      return;
    }

    if (celda.getValue() === false) {
      const lineas = registro.split("\n");
      const ultima = lineas[lineas.length - 1];
      if (!ultima || ultima.includes("/")) return;

      const regex = /^(\d{2})-(\d{2})-(\d{4}) (\d{2}):(\d{2}):(\d{2})$/;
      const match = ultima.match(regex);
      if (!match) return;

      const [, dia, mes, anio, horas, minutos, segundos] = match;
      const fechaInicio = new Date(`${anio}-${mes}-${dia}T${horas}:${minutos}:${segundos}`);
      if (isNaN(fechaInicio.getTime())) return;

      const diffMs = ahora - fechaInicio;
      if (diffMs < 0) return;

      const diffSec = Math.floor(diffMs / 1000);
      const diffMin = Math.floor(diffSec / 60);
      const duracion = `${diffMin} min`;

      const nuevaLinea = `${ultima} / ${horaActual} = ${duracion}`;
      lineas[lineas.length - 1] = nuevaLinea;

      celdaRegistro.setNumberFormat("@");
      celdaRegistro.setValue(lineas.join("\n"));
    }
  }

  // ----- Configuración para Hoja 2 -----
  if (nombreHoja === "Vigilancia") {
    const colCasilla = 2; // Columna C
    const colRegistro = 18; // Columna D
    if (celda.getColumn() !== colCasilla) return;

    const celdaRegistro = hoja.getRange(fila, colRegistro);
    let registro = celdaRegistro.getValue().toString().trim();

    if (celda.getValue() === true) {
      const lineas = registro ? registro.split("\n") : [];
      const ultima = lineas[lineas.length - 1] || "";

      if (!ultima || ultima.includes("/")) {
        lineas.push(horaActual);
        celdaRegistro.setNumberFormat("@");
        celdaRegistro.setValue(lineas.join("\n"));
      }
      return;
    }

    if (celda.getValue() === false) {
      const lineas = registro.split("\n");
      const ultima = lineas[lineas.length - 1];
      if (!ultima || ultima.includes("/")) return;

      const regex = /^(\d{2})-(\d{2})-(\d{4}) (\d{2}):(\d{2}):(\d{2})$/;
      const match = ultima.match(regex);
      if (!match) return;

      const [, dia, mes, anio, horas, minutos, segundos] = match;
      const fechaInicio = new Date(`${anio}-${mes}-${dia}T${horas}:${minutos}:${segundos}`);
      if (isNaN(fechaInicio.getTime())) return;

      const diffMs = ahora - fechaInicio;
      if (diffMs < 0) return;

      const diffSec = Math.floor(diffMs / 1000);
      const diffMin = Math.floor(diffSec / 60);
      const duracion = `${diffMin} min`;

      const nuevaLinea = `${ultima} / ${horaActual} = ${duracion}`;
      lineas[lineas.length - 1] = nuevaLinea;

      celdaRegistro.setNumberFormat("@");
      celdaRegistro.setValue(lineas.join("\n"));
    }
  }
}
