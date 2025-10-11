export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const rawPath = url.pathname.replace(/\/{2,}/g, "/");
    const path = rawPath.replace(/^\/+|\/+$/g, "");

    // Candidatos de clave en R2
    const candidates = [];
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

    // Buscar el primer objeto existente
    const seen = new Set();
    const tryKeys = candidates.filter(k => k && !seen.has(k) && seen.add(k));

    let obj = null, chosenKey = null;
    for (const key of tryKeys) {
      obj = await env.MY_BUCKET.get(key);
      if (obj) { chosenKey = key; break; }
    }

    // 404: sin caché
    if (!obj) {
      return new Response("Not found", {
        status: 404,
        headers: { "Cache-Control": "no-store" }
      });
    }

    // HEADERS
    const headers = new Headers();
    obj.writeHttpMetadata(headers);                 // aplica metadata (incluye content-type si existe)
    headers.set("etag", obj.httpEtag);              // para 304s
    headers.set("Vary", "Accept-Encoding");         // buena higiene caché
    if (!headers.has("content-type")) {
      headers.set("content-type", guessContentType(chosenKey));
    }
    // Cache 5 minutos (navegador y edge)
    // stale-while-revalidate ayuda a servir rápido mientras refresca
    headers.set("Cache-Control", "public, max-age=300, s-maxage=300, stale-while-revalidate=300");

    // If-None-Match → 304
    const inm = request.headers.get("If-None-Match");
    if (inm && inm === obj.httpEtag) {
      return new Response(null, { status: 304, headers });
    }

    // Cache en el edge del Worker (caches.default) para GET/HEAD
    const method = request.method.toUpperCase();
    if (method === "GET" || method === "HEAD") {
      const cache = caches.default;
      const cached = await cache.match(request);
      if (cached) return cached;
      const res = new Response(obj.body, { headers });
      ctx.waitUntil(cache.put(request, res.clone()));
      return res;
    }

    return new Response(obj.body, { headers });
  }
};

function guessContentType(key) {
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
