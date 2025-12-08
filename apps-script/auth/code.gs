/*************** [S1] CONFIG ***************/
const SHEET_NAME = "USUARIOS";

const COL = {
  EMAIL: 1,
  PASS: 2,
  NOMBRE: 3,
  APELLIDO: 4,
  IDC: 5,
  CODEP: 6,
  ACTIVO: 7
};

function prop_(k, fallback){
  const v = PropertiesService.getScriptProperties().getProperty(k);
  return (v == null || v === "") ? fallback : v;
}
function getSecret_(){
  const s = prop_("TOKEN_SECRET", "");
  if (!s) throw new Error("Falta TOKEN_SECRET");
  return s;
}
function getAppUrl_(){
  const u = prop_("APP_URL", "");
  if (!u) throw new Error("Falta APP_URL");
  return u;
}
function getTtlSeconds_(){
  return Number(prop_("TOKEN_TTL_SECONDS", "86400")) || 86400;
}

/*************** [S2] TOKEN ***************/
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
function makeToken_(payloadObj){
  const payload = b64webEncode_(JSON.stringify(payloadObj));
  const mac = Utilities.computeHmacSha256Signature(payload, getSecret_());
  const sig = b64webEncode_(mac);
  return payload + "." + sig;
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
  try { obj = JSON.parse(b64webDecodeToString_(payload)); }
  catch(e){ return { ok:false, err:"TOKEN_INVALID" }; }

  const now = Math.floor(Date.now()/1000);
  if (!obj || !obj.exp || now > Number(obj.exp)) return { ok:false, err:"TOKEN_EXPIRED" };

  return { ok:true, data: obj };
}

/*************** [S3] USERS ***************/
function normalizeEmail_(s){
  return String(s || "").trim().toLowerCase();
}
function activoOk_(v){
  const x = String(v || "").trim().toLowerCase();
  return (x === "true" || x === "si" || x === "sí" || x === "1" || x === "activo");
}
function findUserByEmail_(email){
  const sh = SpreadsheetApp.getActive().getSheetByName(SHEET_NAME);
  if (!sh) throw new Error("No existe la hoja USUARIOS");

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return null;

  const values = sh.getRange(2, 1, lastRow - 1, 7).getValues();
  for (let i = 0; i < values.length; i++){
    const row = values[i];
    if (normalizeEmail_(row[COL.EMAIL-1]) === email){
      return {
        email: normalizeEmail_(row[COL.EMAIL-1]),
        pass: String(row[COL.PASS-1] || ""),
        nombre: String(row[COL.NOMBRE-1] || ""),
        apellido: String(row[COL.APELLIDO-1] || ""),
        idc: String(row[COL.IDC-1] || ""),
        codep: String(row[COL.CODEP-1] || ""),
        activo: activoOk_(row[COL.ACTIVO-1])
      };
    }
  }
  return null;
}

/*************** [S4] JSONP CORE (blindado) ***************/
function safeCb_(s){
  s = String(s || "").trim();
  if (!s) return "";
  if (!/^[a-zA-Z_$][\w.$]*$/.test(s)) return "";
  return s;
}

function jsonpText_(cb, obj){
  const fn = safeCb_(cb) || "callback";
  const payload = JSON.stringify(obj || {});
  // Devuelve SIEMPRE JS ejecutable
  return `${fn}(${payload});`;
}

function jsonpOut_(cb, obj){
  return ContentService
    .createTextOutput(jsonpText_(cb, obj))
    .setMimeType(ContentService.MimeType.JAVASCRIPT);
}

/*************** [S5] ROUTES ***************/
function doGet(e){
  const p = (e && e.parameter) ? e.parameter : {};
  const op = String(p.op || "").trim().toLowerCase();

  // JSONP callback param (acepta cb o callback)
  const cb = p.cb || p.callback || "callback";

  try {
    if (op === "login"){
      const email = normalizeEmail_(p.email);
      const pass  = String(p.pass || "");

      if (!email) return jsonpOut_(cb, { ok:false, message:"Falta correo." });
      if (!pass)  return jsonpOut_(cb, { ok:false, message:"Falta contraseña." });

      const user = findUserByEmail_(email);
      if (!user) return jsonpOut_(cb, { ok:false, message:"Correo inválido." });
      if (!user.activo) return jsonpOut_(cb, { ok:false, message:"Usuario inactivo." });

      if (String(user.pass) !== String(pass)){
        return jsonpOut_(cb, { ok:false, message:"Contraseña incorrecta." });
      }

      const now = Math.floor(Date.now()/1000);
      const exp = now + getTtlSeconds_();

      const token = makeToken_({
        v: 1,
        exp,
        email: user.email,
        nombre: user.nombre,
        apellido: user.apellido,
        idc: user.idc,
        codep: user.codep
      });

      const appUrl = getAppUrl_();
      const join = appUrl.indexOf("?") === -1 ? "?" : "&";
      const target = appUrl + join + "t=" + encodeURIComponent(token);

      return jsonpOut_(cb, { ok:true, target });
    }

    if (op === "verify"){
      // verify normal JSON (para tu app)
      const t = String((p.t || "")).trim();
      const v = verifyToken_(t);
      return ContentService
        .createTextOutput(JSON.stringify(v))
        .setMimeType(ContentService.MimeType.JSON);
    }

    // default: también JS para no romper JSONP si alguien se equivoca de op
    return jsonpOut_(cb, { ok:false, message:"OP inválida." });

  } catch (err){
    // BLINDAJE: pase lo que pase, devolvemos JS ejecutable (nunca HTML)
    return jsonpOut_(cb, { ok:false, message:"Error interno." });
  }
}

/*************** [S6] SET PROPS ***************/
function setProps(){
  PropertiesService.getScriptProperties().setProperties({
    TOKEN_SECRET: "bF6pQmD8nK3vW2sH7tR9yX4aC1eZ0uJ5Lh8GqT2mV7wN9pS3dK6xR1cY4fH8jZ2Q",
    APP_URL: "https://portal.simplemarcas.cl/call.html",
    TOKEN_TTL_SECONDS: "86400"
  }, true);
}

