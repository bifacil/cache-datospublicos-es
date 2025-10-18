import { selectData } from "./crono-json";

interface Env {
  MY_BUCKET: R2Bucket;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const rawPath = url.pathname.replace(/\/{2,}/g, "/");
    const path = rawPath.replace(/^\/+|\/+$/g, "");
    if (url.pathname === "/health") return new Response("ok");
    

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

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
    headers.set("Access-Control-Allow-Origin", "*");
    headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    headers.set("Access-Control-Allow-Headers", "Content-Type");

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
    const from = qs.get("from")?.trim() || "";
    const select = qs.get("select")?.trim() || "";
    const format = (qs.get("format") || "json").toLowerCase();
      
    if (isIndexJson && (from || select)) {
      const text = await new Response(obj.body as ReadableStream).text();
      const input = JSON.parse(text.replace(/^\uFEFF/, ""));
    
      const { body, contentType } = selectData(input, from, select, format);
    
      return new Response(body, {
        headers: { 
          "content-type": contentType, 
          "cache-control": "no-store",
          "Access-Control-Allow-Origin": "*", /* DRY */
          "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        }
      });
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
