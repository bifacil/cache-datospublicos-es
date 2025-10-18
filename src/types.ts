/* -------------------- Tipos base -------------------- */
export type JsonPrimitive = string | number | boolean | null;
export type JsonObject    = { [k: string]: JsonValue };
export type JsonArray     = JsonValue[];
export type JsonValue     = JsonPrimitive | JsonObject | JsonArray;

export type ColumnValue   = string | number | boolean | null;