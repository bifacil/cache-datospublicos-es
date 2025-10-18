import { ColumnValue, JsonValue } from "./types";

/**
 * formatJson:
 *  - "json"    -> cualquier JsonValue (JSON.stringify pretty)
 *  - "tabular" -> array de objetos → matriz [headers, ...rows] (pretty)
 *  - "csv"     -> igual que tabular, pero texto CSV (RFC-4180)
 *  - "ndjson"  -> array de objetos → un objeto JSON por línea (text/plain)
 *  - "sheets"  -> array de objetos → [[headers, ...rows]] (pretty)
 *  - en cualquier otro caso lanza Error
 */
export function formatJson(
  data: JsonValue,
  format: string = "json"
): { body: string; contentType: string } {
  const kind = (format || "json").toLowerCase();

  if (kind === "json") {
    return {
      body: prettyJson(data),
      contentType: "application/json; charset=utf-8",
    };
  }

  // resto de formatos: solo array de objetos
  if (!Array.isArray(data) || data.length === 0 || typeof data[0] !== "object" || Array.isArray(data[0])) {
    throw new Error("transformación de formato no implementada");
  }

  const arrayData = data as Array<Record<string, unknown>>;

  switch (kind) {
    case "ndjson":
      return {
        body: ndjsonFormat(arrayData),
        contentType: "text/plain; charset=utf-8",
      };

    case "tabular":
      return {
        body: tabularFormat(arrayData), 
        contentType: "application/json; charset=utf-8",
      };

    case "csv":
      return {
        body: csvFormat(arrayData), 
        contentType: "text/plain; charset=utf-8",
      };

    case "sheets":
      return {
        body: sheetsFormat(arrayData), 
        contentType: "application/json; charset=utf-8",
      };

    default:
      throw new Error("transformación de formato no implementada");
  }
}

/* ============================
   Helpers (devuelven string)
   ============================ */

function prettyJson(data: unknown, indent = 2): string {
  return JSON.stringify(data, null, indent);
}

function ndjsonFormat(data: Array<Record<string, unknown>>): string {
  return data.map((obj) => JSON.stringify(obj)).join("\n") + "\n";
}

function tabularFormat(data: Array<Record<string, unknown>>): string {
  const { headers, rows } = extractTable(data);
  const matrix: (string | number | boolean | null)[][] = [headers, ...rows];
  return prettyJson(matrix);
}

function sheetsFormat(data: Array<Record<string, unknown>>): string {
  const { headers, rows } = extractTable(data);
  const sheet: (string | number | boolean | null)[][] = [headers, ...rows];
  const payload = [sheet]; 
  return prettyJson(payload);
}

function csvFormat(data: Array<Record<string, unknown>>): string {
  const { headers, rows } = extractTable(data);
  const esc = (v: ColumnValue) => {
    if (v == null) return "";
    if (typeof v === "number" || typeof v === "boolean") return String(v);
    // CSV (RFC-4180): si contiene comillas, coma o salto, rodear con comillas y duplicar comillas internas
    const s = String(v);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };

  const lines = [headers.map(esc).join(","), ...rows.map((r) => r.map(esc).join(","))];
  return lines.join("\n");
}

/* ============================
   Utilidades comunes
   ============================ */

function extractTable(data: Array<Record<string, unknown>>): {
  headers: string[];
  rows: ColumnValue[][];
} {
  const headers = Object.keys(data[0]);

  const toPrimitive = (v: unknown): ColumnValue => {
    if (v === null) return null;
    if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") return v;
    return JSON.stringify(v); // objetos/arrays anidados como string JSON
  };

  const rows = data.map((row) => headers.map((h) => toPrimitive((row as any)[h])));
  return { headers, rows };
}
