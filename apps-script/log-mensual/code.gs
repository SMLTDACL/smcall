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
  "TS_MS" 
];

// Tabla PUNTOS / PESOS según respuesta final
const SCORE = {
  "A.1.1":[0,0],
  "A.1.2":[3,390],
  "A.2.1":[3,390],
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

/*************** [S2] ENTRY ***************/
function doPost(e){
  try{
    const p = normalizePost_(e);

    const idc           = String(p.idc || "").trim();
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


    sh.appendRow([idc, idpipe, fechaCompleta, recorrido, tTotal, tTotalSec, puntos, pesos, tsMs]);


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

    if (action !== "report"){
      return out_({ ok:true, hint:'Use ?action=report&start_ts=...&end_ts=...'}, cb);
    }

    const startTs = Number(p.start_ts || 0) || 0;
    const endTs   = Number(p.end_ts || 0) || Date.now();

    const idcFilter = String(p.idc || "").trim(); // opcional
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

    for (const r of values){
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
    TS_MS: get("TS_MS")
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
