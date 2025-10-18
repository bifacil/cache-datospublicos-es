
import {ColumnValue,JsonValue,JsonArray} from "./types";

/* ----------------------------------
 * formatData: acepta cualquier JsonValue cuando format === "json"
 * - "json"    -> serializa tal cual (cualquier JsonValue)
 * - "tabular" -> si es tabla (array de objetos) usa cabecera por claves; si no, 1 col "value"
 * - "csv"     -> igual que "tabular" pero en texto CSV
 * ---------------------------------------------------- */

export function formatJson(
  data: JsonValue | Array<Record<string, ColumnValue>>,
  format: string = "json"
): { body: string; contentType: string } {
  const kind = (format || "json").toLowerCase();

  // json: siempre válido para cualquier JsonValue
  if (kind === "json") {
    return {
      body: JSON.stringify(data),
      contentType: "application/json; charset=utf-8",
    };
  }

  // A partir de aquí: csv/tabular -> trabajamos con una matriz (rowsMatrix) + headers
  const isRecord = (v: unknown): v is Record<string, unknown> =>
    !!v && typeof v === "object" && !Array.isArray(v);

  const toPrimitive = (v: unknown): ColumnValue => {
    if (v === null) return null;
    const t = typeof v;
    if (t === "string" || t === "number" || t === "boolean") return v as ColumnValue;
    return JSON.stringify(v);
  };

  // Normalizamos a headers + rowsMatrix
  let headers: string[] = [];
  let rowsMatrix: ColumnValue[][] = [];

  if (Array.isArray(data)) {
    const first = (data as any[])[0];

    if (isRecord(first)) {
      // Tabla: array de objetos -> cabeceras = claves del primero
      headers = Object.keys(first as Record<string, unknown>);
      rowsMatrix = (data as Array<Record<string, unknown>>).map((row) =>
        headers.map((h) => toPrimitive((row as any)[h]))
      );
    } else {
      // No es tabla: una sola columna "value"
      headers = ["value"];
      rowsMatrix = (data as JsonArray).map((v) => [toPrimitive(v)]);
    }
  } else {
    // No es array: lo tratamos como una sola fila de una columna "value"
    headers = ["value"];
    rowsMatrix = [[toPrimitive(data)]];
  }

  if (kind === "tabular") {
    const tabular = [headers as ColumnValue[], ...rowsMatrix];
    return {
      body: JSON.stringify(tabular),
      contentType: "application/json; charset=utf-8",
    };
  }

  // kind === "csv"
  const esc = (v: ColumnValue) =>
    v === null || v === undefined
      ? ""
      : typeof v === "number" || typeof v === "boolean"
      ? String(v)
      : JSON.stringify(v);

  const lines = [headers.join(","), ...rowsMatrix.map((r) => r.map(esc).join(","))];
  return {
    body: lines.join("\n"),
    contentType: "text/plain; charset=utf-8",
  };
}