// jql.ts
import { JSONPath } from "jsonpath-plus";

import {ColumnValue,JsonValue,JsonArray,JsonObject} from "./types";

import {formatJson} from "./format";


/* ----------------------------------------------------
 * selectData: orquesta todo y DEBE devolver { body, contentType }
 * ---------------------------------------------------- */
export function selectData(
  input: JsonValue,
  rowsSelector: string,
  columnsSelector: string,
  format: string = "json"
): { body: string; contentType: string } {
 

  const rows= rowsSelector ? selectRows(input, rowsSelector) : input

  let json=rows
   if(columnsSelector) {
    if (!Array.isArray(rows)) {
      throw new TypeError("La entrada debe ser un array");
    }
    json=selectColumns(rows, columnsSelector);
  }
  
  return formatJson(json, format);
}

/* ----------------------------------------------------
 * selectRows: aplica JSONPath al documento y devuelve SIEMPRE un JsonArray
 * ---------------------------------------------------- */
export function selectRows(input: JsonValue, path: string): JsonArray {
  const matches = selectPath(input, path);
  const match = matches[0] ?? null;

  // Si hay múltiples matches, devolvemos tal cual (array de matches)
  if (matches.length !== 1) {
    return matches as unknown as JsonArray;
  }

  // Un solo match:
  if (Array.isArray(match)) {
    return match as JsonArray;
  } else if (match !== null && typeof match === "object") {
    // objeto -> diccionario a pares {Key, Value}
    return Object.entries(match as JsonObject)
      .map(([k, v]) => ({ Key: k, Value: v })) as unknown as JsonArray;
  } else {
    // escalar -> array con un elemento
    return [match] as JsonArray;
  }
}

/* ----------------------------------------------------
 * selectColumns: proyecta columnas sobre cada row
 * ---------------------------------------------------- */
export function selectColumns(
  rows: JsonArray,
  columnsSelector: string
): Array<Record<string, ColumnValue>> {
  const columns = parseColumns(columnsSelector);

  const out: Array<Record<string, ColumnValue>> = [];
  for (const row of rows) {
    const rec: Record<string, ColumnValue> = {};
    for (const { name, selector } of columns) {
      rec[name] = evaluateCell(row, selector);
    }
    out.push(rec);
  }
  return out;
}
/* ----------------------------------------------------
 * evaluateCell: aplica JSONPath sobre una "row"
 *  reglas:
 *   - 0 matches   -> null
 *   - 1 match     -> primitivo => tal cual; no-primitivo => JSON.stringify
 *   - >1 matches  -> JSON.stringify(array)
 * ---------------------------------------------------- */
export function evaluateCell(row: JsonValue, selector: string): ColumnValue {
  const matches = selectPath(row, selector);
  if (matches.length === 0) return null;

  if (matches.length === 1) {
    const m = matches[0];
    const t = typeof m;
    if (t === "string" || t === "number" || t === "boolean" || m === null) {
      return m as ColumnValue;
    }
    return JSON.stringify(m) as string;
  }

  // varios matches -> array como string
  return JSON.stringify(matches) as string;
}

/* ----------------------------------------------------
 * parseColumns: "A:$.x;B:$.y;Z" => [{name, selector}, ...]
 *  - nombre opcional: si falta, toma la última propiedad del selector
 *  - selector permite forma corta sin "$" -> se normaliza en selectPath
 * ---------------------------------------------------- */
export function parseColumns(spec: string): Array<{ name: string; selector: string }> {
  const defs = spec
    .split(";")
    .map((s) => s.trim())
    .filter(Boolean);

  const cols: Array<{ name: string; selector: string }> = [];
  for (const d of defs) {
    const idx = d.indexOf(":");
    const hasName = idx >= 0;
    const selector = hasName ? d.slice(idx + 1).trim() : d;

    let name = hasName ? d.slice(0, idx).trim() : "";
    if (!name) {
      // última parte del selector (por puntos o brackets)
      const m = selector.match(/([A-Za-z0-9_$]+)(?!.*[A-Za-z0-9_$])/);
      name = m ? m[1] : selector || "col";
    }
    cols.push({ name, selector });
  }
  return cols;
}

/* ----------------------------------------------------
 * selectPath: JSONPath con normalización de selector
 *  - si no empieza por "$" o "@", asumimos relativo: "$.<selector>"
 *  - filtra null/undefined
 * ---------------------------------------------------- */
export function selectPath(input: JsonValue, path: string): JsonValue[] {
  // normaliza: "Datos[0]" -> "$.Datos[0]"
  const norm =
    path.trim().startsWith("$") || path.trim().startsWith("@")
      ? path.trim()
      : `$.${path.trim()}`;

  try {
    const res = JSONPath({ path: norm, json: input }) as JsonValue | JsonValue[];
    const arr = Array.isArray(res) ? res : [res];
    return arr.filter((x) => x !== null && x !== undefined);
  } catch {
    return [];
  }
}



