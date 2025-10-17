import { selectData } from "./jql";

interface Env {
  MY_BUCKET: R2Bucket;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const rawPath = url.pathname.replace(/\/{2,}/g, "/");
    const path = rawPath.replace(/^\/+|\/+$/g, "");

    const candidates: string[] = [];
    if (path) candidates.push(path);
    const last = path.split("/").pop() || "";
    const hasExt = last.includes(".");
    if (!hasExt) {
      if (path) candidates.push(`${path}/index.json`);
      else candidates.push("index.json");
      if (path) candidates.push(`${path}.json`);
      candidates.push("index.html");
    }
    if (rawPath.endsWith("/") && path) candidates.unshift(`${path}/index.json`);

    const seen = new Set<string>();
    const tryKeys = candidates.filter((k) => k && !seen.has(k) && (seen.add(k), true));

    let obj: R2ObjectBody | null = null;
    let chosenKey: string | null = null;
    for (const key of tryKeys) {
      obj = await env.MY_BUCKET.get(key);
      if (obj) { chosenKey = key; break; }
    }

    if (!obj) {
      return new Response("Not found", { status: 404, headers: { "Cache-Control": "no-store" } });
    }

    const headers = new Headers();
    obj.writeHttpMetadata(headers);
    headers.set("etag", obj.httpEtag);
    headers.set("Vary", "Accept-Encoding");
    if (!headers.has("content-type")) headers.set("content-type", guessContentType(chosenKey!));
    headers.set("Cache-Control", "public, max-age=300, s-maxage=300, stale-while-revalidate=300");

    const inm = request.headers.get("If-None-Match");
    if (inm && inm === obj.httpEtag) {
      return new Response(null, { status: 304, headers });
    }

    const method = request.method.toUpperCase();
    if (method === "GET" || method === "HEAD") {
      const cache = (caches as unknown as { default: Cache }).default;
      const cached = await cache.match(request);
      if (cached) return cached;
    }

    // —— Transformación JQL solo si el objeto es index.json y hay QS completo ——
    const isIndexJson = !!chosenKey && chosenKey.endsWith("index.json");
    const qs = url.searchParams;
    const from = qs.get("from");
    const select = qs.get("select");
    const format = (qs.get("format") || "json").toLowerCase();

    if (isIndexJson && from && select && format) {
      try {
          console.log("HOLA")
          console.log("select")
        const input = await readJsonFromR2Robust(obj, headers);
        const transformed = selectData(input as any, from, select, format);
      
     
    // Devolver DIRECTAMENTE el resultado de selectData, sin cache ni ETag:
    const isString = typeof transformed === "string";
    const body = isString
      ? transformed
      : JSON.stringify(transformed, null, 2);

    const ct =
      isString && format === "csv"
        ? "text/csv; charset=utf-8"
        : "text/plain; charset=utf-8"; // texto para ver exactamente lo que sale

    return new Response(body, {
      status: 200,
      headers: {
        "content-type": ct,
        "cache-control": "no-store" // sin caché mientras depuramos
      }
    });


      } catch (e) {
        console.error("JQL error:", e);
        return new Response(JSON.stringify({ error: String(e) }, null, 2), {
          status: 500,
          headers: { "content-type": "application/json; charset=utf-8" },
        });
      }
    }

    const res = new Response(obj.body, { headers });
    if (method === "GET" || method === "HEAD") {
      const cache = (caches as unknown as { default: Cache }).default;
      ctx.waitUntil(cache.put(request, res.clone()));
    }
    return res;
  },
} satisfies ExportedHandler<Env>;

function guessContentType(key: string): string {
  const ext = (key.split(".").pop() || "").toLowerCase();
  switch (ext) {
    case "json": return "application/json; charset=utf-8";
    case "html": return "text/html; charset=utf-8";
    case "csv":  return "text/csv; charset=utf-8";
    case "txt":  return "text/plain; charset=utf-8";
    case "js":   return "text/javascript; charset=utf-8";
    case "css":  return "text/css; charset=utf-8";
    case "png":  return "image/png";
    case "jpg":
    case "jpeg": return "image/jpeg";
    case "svg":  return "image/svg+xml";
    case "webp": return "image/webp";
    case "xml":  return "application/xml; charset=utf-8";
    default:     return "application/octet-stream";
  }
}



async function readJsonFromR2Robust(obj: R2ObjectBody, headersFromObj: Headers): Promise<unknown> {
  // 1) Intento directo
  try {
    return await obj.json();
  } catch {}

  // 2) Intento como texto “tal cual”
  try {
    const t1 = await new Response(obj.body as ReadableStream).text();
    const cleaned = t1.replace(/^\uFEFF/, "");
    return JSON.parse(cleaned);
  } catch {}

  // 3) Intento con descompresión según content-encoding
  const enc = (headersFromObj.get("content-encoding") || "").split(",")[0].trim().toLowerCase();
  if (enc === "gzip" || enc === "br" || enc === "deflate") {
    try {
      const algo = enc === "br" ? "brotli" : enc;
      const ds = new DecompressionStream(algo);
      const decompressed = (obj.body as ReadableStream).pipeThrough(ds);
      const t2 = await new Response(decompressed).text();
      const cleaned = t2.replace(/^\uFEFF/, "");
      return JSON.parse(cleaned);
    } catch {}
  }

  throw new Error("Unable to parse R2 object as JSON (raw, text, or decompressed).");
}
