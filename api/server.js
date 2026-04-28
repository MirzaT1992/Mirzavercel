import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const MAIN_HOST = (process.env.REMOTE_API_HOST || "").replace(/\/$/, "");

const EXCLUDE_LIST = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

function parseClientData(req) {
  const parsed = {};
  let userIp;

  for (const [key, value] of Object.entries(req.headers)) {
    const k = key.toLowerCase();

    if (EXCLUDE_LIST.has(k)) continue;
    if (k.startsWith("x-vercel-")) continue;

    if (k === "x-real-ip") {
      userIp = value;
      continue;
    }

    if (k === "x-forwarded-for") {
      userIp ||= value;
      continue;
    }

    parsed[k] = Array.isArray(value) ? value.join(", ") : value;
  }

  if (userIp) parsed["x-forwarded-for"] = userIp;

  return parsed;
}

function generateConfig(req, h) {
  const m = req.method;
  const withBody = m !== "GET" && m !== "HEAD";

  const opt = {
    method: m,
    headers: h,
    redirect: "manual",
  };

  if (withBody) {
    opt.body = Readable.toWeb(req);
    opt.duplex = "half";
  }

  return opt;
}

function syncHeaders(source, res) {
  for (const [k, v] of source.headers) {
    if (k.toLowerCase() === "transfer-encoding") continue;
    try {
      res.setHeader(k, v);
    } catch {}
  }
}

export default async function processData(req, res) {
  if (!MAIN_HOST) {
    res.statusCode = 500;
    
    return res.end("System Error: EC-1001");
  }

  const endpointUrl = MAIN_HOST + req.url;

  try {
    const h = parseClientData(req);
    const fetchConf = generateConfig(req, h);

    const remoteResp = await fetch(endpointUrl, fetchConf);

    res.statusCode = remoteResp.status;
    syncHeaders(remoteResp, res);

    if (remoteResp.body) {
      await pipeline(Readable.fromWeb(remoteResp.body), res);
    } else {
      res.end();
    }
  } catch (err) {
    console.error("fetch_err:", err);

    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Server Error: 502 Service Unavailable");
    }
  }
}
