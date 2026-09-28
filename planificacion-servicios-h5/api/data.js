// Planificación de Servicios H5 — API de datos compartidos (Upstash Redis via REST)
const SEED = require("./_seed.js");

const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const P = "po112:";
const KDOCS = P + "docs", KVER = P + "v";
const SEG = /^[A-Za-z0-9_\-.~:@+]{1,200}$/;

async function redis(cmds) {
  const r = await fetch(URL_ + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error("redis " + r.status);
  const out = await r.json();
  return out.map((x) => { if (x.error) throw new Error(x.error); return x.result; });
}

async function ensureSeed() {
  const [claimed] = await redis([["SET", KVER, "1", "NX"]]);
  if (claimed === "OK") {
    const args = ["HSET", KDOCS];
    for (const k of Object.keys(SEED)) args.push(k, JSON.stringify(SEED[k]));
    await redis([args]);
  }
}

function validPath(p) {
  if (typeof p !== "string") return false;
  const s = p.split("/");
  return s.length >= 2 && s.length % 2 === 0 && s.length <= 16 && s.every((x) => SEG.test(x) && x !== "." && x !== "..");
}

async function readBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? JSON.parse(raw) : {};
}

module.exports = async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");
  if (!URL_ || !TOKEN) {
    res.status(500).json({ error: "no_db", message: "Falta conectar la base de datos (Upstash Redis) al proyecto en Vercel." });
    return;
  }
  try {
    if (req.method === "GET") {
      let [v] = await redis([["GET", KVER]]);
      if (v == null) { await ensureSeed(); [v] = await redis([["GET", KVER]]); }
      const since = req.query && req.query.v;
      if (since && String(since) === String(v)) { res.status(200).json({ v: Number(v), unchanged: true }); return; }
      const [flat] = await redis([["HGETALL", KDOCS]]);
      const docs = {};
      if (Array.isArray(flat)) for (let i = 0; i < flat.length; i += 2) { try { docs[flat[i]] = JSON.parse(flat[i + 1]); } catch (e) {} }
      res.status(200).json({ v: Number(v), docs });
      return;
    }
    if (req.method === "POST") {
      const b = await readBody(req);
      const ops = Array.isArray(b.ops) ? b.ops : [b];
      if (!ops.length || ops.length > 50) { res.status(400).json({ error: "invalid_argument" }); return; }
      const cmds = [];
      for (const o of ops) {
        if (!validPath(o.path)) { res.status(400).json({ error: "invalid_argument", message: "Ruta inválida" }); return; }
        if (o.op === "delete") { cmds.push(["HDEL", KDOCS, o.path]); continue; }
        if (!o.data || typeof o.data !== "object" || Array.isArray(o.data)) { res.status(400).json({ error: "invalid_argument" }); return; }
        let data = o.data;
        if (o.op === "update") {
          const [cur] = await redis([["HGET", KDOCS, o.path]]);
          if (cur == null) { res.status(400).json({ error: "invalid_argument", message: "El documento no existe" }); return; }
          data = Object.assign(JSON.parse(cur), o.data);
        } else if (o.op !== "set") { res.status(400).json({ error: "invalid_argument" }); return; }
        const s = JSON.stringify(data);
        if (s.length > 262144) { res.status(400).json({ error: "invalid_argument", message: "Documento demasiado grande" }); return; }
        cmds.push(["HSET", KDOCS, o.path, s]);
      }
      cmds.push(["INCR", KVER]);
      const out = await redis(cmds);
      res.status(200).json({ v: Number(out[out.length - 1]) });
      return;
    }
    res.status(405).json({ error: "method_not_allowed" });
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "unavailable", message: String(e.message || e) });
  }
};
