import { JSONPath } from "jsonpath-plus";

type ColumnSpec = { name: string; selector: string };
type ColumnValue = string | number | boolean | null;

/** Punto de entrada del mini-lenguaje */
export function selectData(
  input: JsonValue,
  rowsSelector: string,
  columnsSelector: string,
  format: string = "json"
): JsonObject | JsonArray | string {
  const rows = selectRows(input, rowsSelector);
  const table = selectColumns(rows, columnsSelector); // Array<Record<string, ColumnValue>>
  return formatData(table, format);
}

function selectRows(input: JsonValue, path: string): JsonArray {
  const matches = selectPath(input, path);
  const match = matches[0] ?? null;

  if (matches.length != 1) {
    return matches as unknown as JsonArray;
  } else if (Array.isArray(match)) {
    return match as JsonArray;
  } else if (typeof match === "object") {
    return Object.entries(match as JsonObject)
      .map(([k, v]) => ({ Key: k, Value: v } as unknown as JsonValue)) as JsonArray;
  } else {
      return [match] as JsonArray; 
  }
}


function selectColumns(rows: JsonArray, spec: string): Array<Record<string, ColumnValue>> {
  const cols = parseColumns(spec);
  return rows.map((row) => {
    const out: Record<string, ColumnValue> = {};
    for (const c of cols) out[c.name] = evaluateCell(row, c.selector);
    return out;
  });
}


function evaluateCell(row: JsonValue, selector: string): ColumnValue {
  const matches = selectPath(row, selector);
  if (matches.length === 0) return null;
  if (matches.length === 1) {
    const v = matches[0];
    const t = typeof v;
    if (v === null) return null;
    if (t === "string" || t === "number" || t === "boolean") return v as ColumnValue;
    return JSON.stringify(v) as string;
  }
  return JSON.stringify(matches);
}

function parseColumns(spec: string): ColumnSpec[] {
  return spec
    .split(";")
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p, i) => {
      const idx = p.indexOf(":");
      if (idx >= 0) {
        const name = p.slice(0, idx).trim();
        const selector = p.slice(idx + 1).trim();
        return { name, selector };
      } else {
        const selector = p;
        const name = defaultColumnName(selector, i);
        return { name, selector };
      }
    });
}

function defaultColumnName(selector: string, idx: number): string {
  let s = selector.trim();
  s = s.replace(/^\$\.?/, "").replace(/^@\.?/, "");        // quita $., $ o @.
  const quoted = [...s.matchAll(/\['([^']+)'\]/g)];
  if (quoted.length) return quoted[quoted.length - 1][1];   // última clave con ['...']
  const parts = s.split(".").filter(Boolean);
  let last = parts.length ? parts[parts.length - 1] : "";
  last = last.replace(/\[.*\]$/, "");                       // quita índices [0], [*], filtros, etc.
  return last || `col${idx + 1}`;
}


function selectPath(input: JsonValue, path: string): JsonValue[] {
  let p = path.trim();
  if (p === "" || p === "$") p = "$";
  else if (!(p.startsWith("$") || p.startsWith("@"))) {
    if (p.startsWith("..") || p.startsWith("[")) p = "$" + p;
    else p = "$." + p;
  }

  try {
    const result = JSONPath({ path: p, json: input });
    const arr = Array.isArray(result) ? (result as JsonValue[]) : [result as JsonValue];
    return arr.filter((x) => x !== null && x !== undefined);
  } catch {
    return [];
  }
}


function formatData(
  table: Array<Record<string, ColumnValue>>,
  format: string
): JsonArray | string {
  const fmt = (format ?? "json").toLowerCase();

  if (fmt === "csv") {
    const headers = Object.keys(table[0] ?? {});
    const lines = [headers.join(",")];
    for (const r of table) {
      const row = headers.map(h => csvEscape(r[h])).join(",");
      lines.push(row);
    }
    return lines.join("\n");
  }

  if (fmt === "tabular") {
    const headers = Object.keys(table[0] ?? {});
    const data = table.map(r => headers.map(h => r[h] ?? null));
    return [headers, ...data] as unknown as JsonArray;
  }

  // "json": devolvemos el array de objetos tal cual (como JsonArray)
  return table as unknown as JsonArray;
}

function csvEscape(value: unknown): string {
  if (value == null) return "";
  const s = String(value);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}


/** Representa cualquier valor JSON válido. */
export type JsonValue = string | number | boolean | null | JsonObject | JsonArray;

export interface JsonObject { [key: string]: JsonValue }

export interface JsonArray extends Array<JsonValue> {}