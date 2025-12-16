/*************** [S1] CONFIG ***************/
const HEADERS = [
  "IDC",
  "IDPIPE",
  "FECHA COMPLETA",
  "RECORRIDO",
  "TIEMPO TOTAL EN LLAMADA",
  "TIEMPO TOTAL EN LLAMADA SEC",
  "PUNTOS",
  "PESOS",
  "TS_MS",
  "FLAG_STATUS",
  "FLAG_REASON"
];

// Tabla PUNTOS / PESOS según respuesta final
const SCORE = {
  "A.1.1":[0,0],
  "A.1.2":[3,390],
  "A.2.1":[3,390],
  "A.2.6":[3,390],
  "A.2.7":[4,520],
  "A.2.3":[3,390],
  "A.2.4":[4,520],
  "A.2.2.3":[3,390],
  "A.2.2.1.1":[5,650],
  "A.2.2.1.2":[3,390],
  "A.2.2.2.1":[5,650],
  "A.2.2.2.2":[3,390],
  "A.3.2.1":[10,1300],
  "A.3.2.2":[12,1560],
  "A.3.1.2":[15,1950],
  "A.4.2":[15,1950],
  "A.5.2.1":[15,1950],
  "A.5.2.2":[17,2210],
  "A.5.1.1":[25,3250],
  "A.5.1.2":[25,3250],

  "B.1.1":[0,0],
  "B.2.1":[3,390],
  "B.1.2":[3,390],
  "B.2.3":[3,390],
  "B.2.4":[4,520],
  "B.2.2.3":[3,390],
  "B.2.2.1.1":[5,650],
  "B.2.2.1.2":[3,390],
  "B.2.2.2.1":[5,650],
  "B.2.2.2.2":[3,390],
  "B.2.5.2":[6,780],
  "B.3.2":[6,780],
  "B.4.2.1":[6,780],
  "B.4.2.2":[8,1040],
  "B.4.1.1":[11,1430],
  "B.4.1.2":[11,1430],

  "C.1.1":[0,0],
  "C.1.2":[3,390],
  "C.2.1":[3,390],
  "C.2.3":[3,390],
  "C.2.4":[4,520],
  "C.2.2.1":[3,390],
  "C.2.2.2":[3,390],
  "C.2.5.1":[11,1430],
  "C.2.5.2":[11,1430]
};

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

/*************** [S2] ENTRY ***************/
function doPost(e){
  try{
    const p = normalizePost_(e);
    const tokenRaw = String(p.token || p.t || "").trim();
    const vTok = verifyToken_(tokenRaw);
    if (!vTok.ok) return json_({ ok:false, error:vTok.err });
    const action = String(p.action || p.mode || "").trim().toLowerCase();

    if (action === "flag"){
      return flagRow_(p, vTok.data);
    }
    if (action === "adjust"){
      return adjustRow_(p, vTok.data);
    }

    const tokIDC = String(vTok.data?.idc || "").trim();
    if (!rateLimitOk_(tokIDC || "anon", 120, 300)) return json_({ ok:false, error:"rate_limited" });

    const idc           = tokIDC || String(p.idc || "").trim();
    const idpipe        = String(p.idpipe || "").trim();
    const fechaCompleta = String(p.fecha_completa || "").trim();
    const recorrido     = String(p.recorrido || "").trim();
    const tTotal        = String(p.tiempo_total || "").trim();
    const tTotalSec     = Number(p.tiempo_total_sec || 0);
    const finalCode     = String(p.final_code || p.survey || p.numero_pregunta || "").trim();

    const [puntos, pesos] = scoreFromFinal_(finalCode);

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sh = getOrCreateMonthSheet_(ss, new Date());
    const tsMs = Number(p.ts_ms || p.ts || p.timestamp || 0) || Date.now();


    sh.appendRow([idc, idpipe, fechaCompleta, recorrido, tTotal, tTotalSec, puntos, pesos, tsMs, "", ""]);


    return json_({ ok:true, sheet: sh.getName(), puntos, pesos });
  }catch(err){
    return json_({ ok:false, error: String(err && err.message ? err.message : err) });
  }
}

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


/*************** [S3] HELPERS ***************/
function scoreFromFinal_(code){
  const key = String(code || "").trim();
  const v = SCORE[key];
  if (!v) return [0, 0];
  return [Number(v[0] || 0), Number(v[1] || 0)];
}

function getOrCreateMonthSheet_(ss, d){
  const name = monthTabName_(d); // "DIC-25"
  let sh = ss.getSheetByName(name);
  if (!sh){
    sh = ss.insertSheet(name);
    sh.getRange(1,1,1,HEADERS.length).setValues([HEADERS]);
    sh.setFrozenRows(1);
  } else {
    // por si algún día falta header
    const a1 = String(sh.getRange(1,1).getValue() || "").trim();
    if (!a1) sh.getRange(1,1,1,HEADERS.length).setValues([HEADERS]);
  }
  return sh;
}

function monthTabName_(d){
  const m = d.getMonth(); // 0-11
  const y = d.getFullYear() % 100;
  const MMM = ["ENE","FEB","MAR","ABR","MAY","JUN","JUL","AGO","SEP","OCT","NOV","DIC"][m];
  const YY = String(y).padStart(2, "0");
  return `${MMM}-${YY}`;
}

function json_(obj){
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

/*************** [S4] REPORT (GET) ***************/
function doGet(e){
  try{
    const p = (e && e.parameter) ? e.parameter : {};
    const action = String(p.action || "").trim();
    const cb = String(p.callback || "").trim(); // <-- JSONP
    const tokenRaw = String(p.token || p.t || "").trim();
    const vTok = verifyToken_(tokenRaw);
    if (!vTok.ok) return out_({ ok:false, error:vTok.err }, cb);
    const tokIDC = String(vTok.data?.idc || "").trim();
    const tokCodep = String(vTok.data?.codep || "").trim();
    if (!rateLimitOk_(tokIDC || tokCodep || "anon", 60, 300)) return out_({ ok:false, error:"rate_limited" }, cb);

    if (action !== "report"){
      return out_({ ok:true, hint:'Use ?action=report&start_ts=...&end_ts=...'}, cb);
    }

    const startTs = Number(p.start_ts || 0) || 0;
    const endTs   = Number(p.end_ts || 0) || Date.now();

    const idcFilterParam = String(p.idc || "").trim(); // opcional
    const codepParam = String(p.codep || "").trim();
    const scopeAll = String(p.scope || p.all || "").trim().toLowerCase();
    const allowAll = (scopeAll === "all" || scopeAll === "1" || scopeAll === "true") && !!tokCodep && tokCodep === codepParam;
    const idcFilter = allowAll ? "" : (tokIDC || idcFilterParam);
    const limit = Math.min(Math.max(Number(p.limit || 500) || 500, 1), 5000);

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const data = readRowsInRange_(ss, startTs, endTs, idcFilter, limit);

    return out_({ ok:true, ...data }, cb);
  }catch(err){
    const cb = (e && e.parameter && e.parameter.callback) ? String(e.parameter.callback||"").trim() : "";
    return out_({ ok:false, error: String(err && err.message ? err.message : err) }, cb);
  }
}


function readRowsInRange_(ss, startTs, endTs, idcFilter, limit){
  const tz = Session.getScriptTimeZone() || "America/Santiago";
  const monthRe = /^(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)[-\/]\d{2}$/; // <-- acepta "-" y "/"


  // tomamos todas las pestañas tipo "DIC-25"
  const sheets = ss.getSheets().filter(sh => monthRe.test(sh.getName()));

  // orden por fecha aproximada (YY + mes)
  sheets.sort((a,b)=>{
    const pa = parseMonthTab_(a.getName());
    const pb = parseMonthTab_(b.getName());
    return (pa.y*12+pa.m) - (pb.y*12+pb.m);
  });

  let rows = [];
  let totalPuntos = 0;
  let totalPesos = 0;

  // serie diaria: { 'YYYY-MM-DD': puntosDia }
  const byDay = {};

  for (const sh of sheets){
    const lastRow = sh.getLastRow();
    const lastCol = sh.getLastColumn();
    if (lastRow < 2) continue;

    // asegurar que exista TS_MS en header (si no, no podemos filtrar bien)
    // Si tu sheet viejo no tiene TS_MS, igual lo vamos a "leer" intentando parsear FECHA COMPLETA.
    const header = sh.getRange(1,1,1,lastCol).getValues()[0].map(x=>String(x||"").trim());
    const col = headerIndexMap_(header);

    const values = sh.getRange(2,1,lastRow-1,lastCol).getValues();

    for (let i=0; i<values.length; i++){
      const r = values[i];
      const obj = rowToObj_(r, col);

      // filtro IDC si viene
      if (idcFilter && String(obj.IDC||"").trim() !== idcFilter) continue;

      // timestamp: preferimos TS_MS, si no existe intentamos parsear FECHA COMPLETA
      const ts = getRowTsMs_(obj);
      if (!ts) continue;

      if (ts < startTs || ts > endTs) continue;

      const p = Number(obj.PUNTOS || 0) || 0;
      const w = Number(obj.PESOS  || 0) || 0;

      totalPuntos += p;
      totalPesos  += w;

      const dayKey = Utilities.formatDate(new Date(ts), tz, "yyyy-MM-dd");
      byDay[dayKey] = (byDay[dayKey] || 0) + p;

      // guardamos row
      obj.__ts = ts;
      obj.__sheet = sh.getName();
      obj.__row = 2 + i; // header (1) + data offset
      rows.push(obj);

      if (rows.length >= limit) break;
    }

    if (rows.length >= limit) break;
  }

  // orden por ts desc (más reciente arriba)
  rows.sort((a,b)=> (b.__ts||0) - (a.__ts||0));

  // construir serie ordenada asc para gráfico
  const dayKeys = Object.keys(byDay).sort(); // yyyy-mm-dd ordena bien
  const series = dayKeys.map(k => ({ date:k, puntos: byDay[k] }));

  return {
    meta: {
      start_ts: startTs,
      end_ts: endTs,
      tz,
      rows_count: rows.length
    },
    totals: { puntos: totalPuntos, pesos: totalPesos },
    series,
    rows
  };
}

function headerIndexMap_(headerRow){
  // Devuelve índices por nombre de columna si existe
  const m = {};
  headerRow.forEach((h, i)=> { if (h) m[h] = i; });
  return m;
}

function rowToObj_(r, col){
  // Soporta hojas viejas (sin TS_MS) si existen las otras
  const get = (name) => {
    const i = col[name];
    return (i === undefined) ? "" : r[i];
  };

  return {
    IDC: get("IDC"),
    IDPIPE: get("IDPIPE"),
    "FECHA COMPLETA": get("FECHA COMPLETA"),
    RECORRIDO: get("RECORRIDO"),
    "TIEMPO TOTAL EN LLAMADA": get("TIEMPO TOTAL EN LLAMADA"),
    "TIEMPO TOTAL EN LLAMADA SEC": get("TIEMPO TOTAL EN LLAMADA SEC"),
    PUNTOS: get("PUNTOS"),
    PESOS: get("PESOS"),
    TS_MS: get("TS_MS"),
    FLAG_STATUS: get("FLAG_STATUS"),
    FLAG_REASON: get("FLAG_REASON")
  };
}

function getRowTsMs_(obj){
  const ts = Number(obj.TS_MS || 0);
  if (ts) return ts;

  // fallback: intenta parsear FECHA COMPLETA (si viene ISO o parecido)
  const s = String(obj["FECHA COMPLETA"] || "").trim();
  if (!s) return 0;

  // si es número (ms o segundos)
  const n = Number(s);
  if (!isNaN(n) && n > 0){
    // heurística simple
    return (n < 1e12) ? (n * 1000) : n;
  }

  const d = new Date(s);
  const t = d.getTime();
  if (!isNaN(t)) return t;

  return 0;
}

function ensureFlagColumns_(sh, colMap){
  let header = sh.getRange(1,1,1, sh.getLastColumn()).getValues()[0].map(x=>String(x||"").trim());
  let changed = false;
  const need = ["FLAG_STATUS","FLAG_REASON"];
  let lastCol = header.length;
  need.forEach(name=>{
    let idx = header.indexOf(name);
    if (idx === -1){
      lastCol += 1;
      if (header.length < lastCol) header.length = lastCol;
      header[lastCol-1] = name;
      idx = lastCol-1;
      changed = true;
    }
    colMap[name] = idx;
  });
  if (changed){
    sh.getRange(1,1,1, header.length).setValues([header]);
  }
  return colMap;
}

function flagRow_(p, tokenData){
  const sheetName = String(p.sheet || p.sh || "").trim();
  const rowNum = Number(p.row || p.r || 0);
  const status = String(p.status || "").trim().toLowerCase();
  const reason = String(p.reason || "").trim();

  if (!sheetName || !rowNum || rowNum < 2) return json_({ ok:false, error:"missing_sheet_or_row" });
  if (status !== "accepted" && status !== "rejected") return json_({ ok:false, error:"invalid_status" });
  if (status === "rejected" && !reason) return json_({ ok:false, error:"missing_reason" });

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(sheetName);
  if (!sh) return json_({ ok:false, error:"sheet_not_found" });
  if (rowNum > sh.getLastRow()) return json_({ ok:false, error:"row_out_of_bounds" });

  let col = headerIndexMap_(sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0].map(x=>String(x||"").trim()));
  col = ensureFlagColumns_(sh, col);
  const colStatus = (col["FLAG_STATUS"] || 0) + 1;
  const colReason = (col["FLAG_REASON"] || 0) + 1;

  sh.getRange(rowNum, colStatus).setValue(status);
  sh.getRange(rowNum, colReason).setValue(status === "rejected" ? reason : "");

  return json_({ ok:true, sheet: sheetName, row: rowNum, status, reason });
}

function adjustRow_(p, tokenData){
  const sheetName = String(p.sheet || p.sh || "").trim();
  const rowNum = Number(p.row || p.r || 0);
  const reason = String(p.reason || "").trim();
  const pesosNew = Number(p.pesos || p.monto || p.amount || 0);

  if (!sheetName || !rowNum || rowNum < 2) return json_({ ok:false, error:"missing_sheet_or_row" });
  if (!isFinite(pesosNew)) return json_({ ok:false, error:"invalid_pesos" });
  if (!reason) return json_({ ok:false, error:"missing_reason" });

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(sheetName);
  if (!sh) return json_({ ok:false, error:"sheet_not_found" });
  if (rowNum > sh.getLastRow()) return json_({ ok:false, error:"row_out_of_bounds" });

  let col = headerIndexMap_(sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0].map(x=>String(x||"").trim()));
  col = ensureFlagColumns_(sh, col);
  const colStatus = (col["FLAG_STATUS"] || 0) + 1;
  const colReason = (col["FLAG_REASON"] || 0) + 1;
  const colPesos  = (col["PESOS"] || 0) + 1;

  sh.getRange(rowNum, colPesos).setValue(pesosNew);
  sh.getRange(rowNum, colStatus).setValue("adjusted");
  sh.getRange(rowNum, colReason).setValue(reason);

  return json_({ ok:true, sheet: sheetName, row: rowNum, pesos: pesosNew, status:"adjusted", reason });
}

function parseMonthTab_(name){
  const parts = String(name).split(/[-\/]/); // <-- acepta DIC-25 y DIC/25
  const MMM = parts[0];
  const YY = Number(parts[1] || 0);
  const map = { ENE:0,FEB:1,MAR:2,ABR:3,MAY:4,JUN:5,JUL:6,AGO:7,SEP:8,OCT:9,NOV:10,DIC:11 };
  return { m: map[MMM] ?? 0, y: 2000 + YY };
}

function out_(obj, callback){
  const txt = JSON.stringify(obj);

  // JSONP para evitar CORS en ClickFunnels
  if (callback){
    const safe = callback.replace(/[^\w$.]/g, ""); // sanitiza
    return ContentService
      .createTextOutput(`${safe}(${txt});`)
      .setMimeType(ContentService.MimeType.JAVASCRIPT);
  }

  // JSON normal
  return ContentService
    .createTextOutput(txt)
    .setMimeType(ContentService.MimeType.JSON);
}
