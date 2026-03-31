const BASE = "/bq";
export const PROJECT = "poc-project";

export interface TableRef {
  datasetId: string;
  tableId: string;
}

export interface SchemaField {
  name: string;
  type: string;
  mode: string;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, string>[];
  totalRows: number;
}

export interface PaginatedResult extends QueryResult {
  page: number;
  pageSize: number;
  totalPages: number;
}

export async function listTables(dataset: string): Promise<TableRef[]> {
  const res = await fetch(
    `${BASE}/projects/${PROJECT}/datasets/${dataset}/tables`,
  );
  if (!res.ok) throw new Error(`Failed to list tables: ${res.status}`);
  const data = await res.json();
  const tables = (data.tables ?? []) as Array<{
    tableReference: { datasetId: string; tableId: string };
  }>;
  return tables
    .map((t) => ({
      datasetId: t.tableReference.datasetId,
      tableId: t.tableReference.tableId,
    }))
    .sort((a, b) => a.tableId.localeCompare(b.tableId));
}

export async function getSchema(
  dataset: string,
  table: string,
): Promise<SchemaField[]> {
  const res = await fetch(
    `${BASE}/projects/${PROJECT}/datasets/${dataset}/tables/${table}`,
  );
  if (!res.ok) throw new Error(`Failed to get schema: ${res.status}`);
  const data = await res.json();
  return (data.schema?.fields ?? []) as SchemaField[];
}

export async function runQuery(sql: string): Promise<QueryResult> {
  const res = await fetch(`${BASE}/projects/${PROJECT}/queries`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: sql, useLegacySql: false }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => null);
    throw new Error(err?.error?.message ?? `Query failed: ${res.status}`);
  }
  const data = await res.json();

  const columns: string[] = (data.schema?.fields ?? []).map(
    (f: { name: string }) => f.name,
  );
  const rows: Record<string, string>[] = (data.rows ?? []).map(
    (row: { f: Array<{ v: string }> }) =>
      Object.fromEntries(row.f.map((cell, i) => [columns[i], cell.v])),
  );
  return { columns, rows, totalRows: Number(data.totalRows ?? 0) };
}

async function countRows(sql: string): Promise<number> {
  const res = await runQuery(`SELECT COUNT(*) AS cnt FROM (${sql})`);
  return Number(res.rows[0]?.cnt ?? 0);
}

async function paginateQuery(
  baseSql: string,
  pageSql: string,
  page: number,
  pageSize: number,
  knownTotal?: number,
): Promise<PaginatedResult> {
  const needsCount = knownTotal === undefined;
  const [totalRows, data] = await Promise.all([
    needsCount ? countRows(baseSql) : knownTotal,
    runQuery(pageSql),
  ]);
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  return { ...data, totalRows, page, pageSize, totalPages };
}

export async function previewTablePage(
  dataset: string,
  table: string,
  page: number,
  pageSize: number,
  knownTotal?: number,
): Promise<PaginatedResult> {
  const baseSql = `SELECT * FROM \`${PROJECT}.${dataset}.${table}\``;
  const offset = (page - 1) * pageSize;
  return paginateQuery(baseSql, `${baseSql} LIMIT ${pageSize} OFFSET ${offset}`, page, pageSize, knownTotal);
}

export async function runQueryPage(
  sql: string,
  page: number,
  pageSize: number,
  knownTotal?: number,
): Promise<PaginatedResult> {
  const offset = (page - 1) * pageSize;
  return paginateQuery(sql, `SELECT * FROM (${sql}) AS _q LIMIT ${pageSize} OFFSET ${offset}`, page, pageSize, knownTotal);
}
