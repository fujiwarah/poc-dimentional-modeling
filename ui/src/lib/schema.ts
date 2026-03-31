import { listTables, getSchema, type SchemaField } from "./bq.ts";

export type ColumnRole = "pk" | "fk" | "degenerate" | "measure" | "attribute";
export type TableType = "fact" | "dim" | "stg" | "int";

export const LAYER_ORDER: TableType[] = ["stg", "int", "dim", "fact"];

export interface InferredColumn {
  name: string;
  type: string;
  mode: string;
  role: ColumnRole;
  refTable?: string;
}

export interface InferredTable {
  id: string;
  tableType: TableType;
  columns: InferredColumn[];
}

export interface InferredRelationship {
  id: string;
  sourceTable: string;
  targetTable: string;
  fkColumn: string;
}

export interface InferredSchema {
  tables: InferredTable[];
  relationships: InferredRelationship[];
}

function classifyTable(tableId: string): TableType | null {
  if (tableId.startsWith("fact_")) return "fact";
  if (tableId.startsWith("dim_")) return "dim";
  if (tableId.startsWith("stg_")) return "stg";
  if (tableId.startsWith("int_")) return "int";
  return null;
}

function inferPkColumn(tableId: string): string | null {
  if (!tableId.startsWith("dim_")) return null;
  return `${tableId.slice(4)}_key`;
}

function classifyColumn(
  field: SchemaField,
  tableType: TableType,
  pkColumn: string | null,
  dimTableIds: Set<string>,
): InferredColumn {
  if (pkColumn && field.name === pkColumn) {
    return { ...field, role: "pk" };
  }

  if (tableType === "fact" && field.name.endsWith("_key")) {
    const dimName = `dim_${field.name.replace(/_key$/, "")}`;
    if (dimTableIds.has(dimName)) {
      return { ...field, role: "fk", refTable: dimName };
    }
  }

  if (tableType === "fact") {
    const numeric = ["INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOL"];
    if (numeric.includes(field.type) && !field.name.endsWith("_id")) {
      return { ...field, role: "measure" };
    }
    return { ...field, role: "degenerate" };
  }

  return { ...field, role: "attribute" };
}

export async function fetchAndInferSchema(
  dataset: string,
): Promise<InferredSchema> {
  const tableRefs = await listTables(dataset);

  const relevant = tableRefs.filter((t) => classifyTable(t.tableId) !== null);
  const dimTableIds = new Set(
    relevant.filter((t) => t.tableId.startsWith("dim_")).map((t) => t.tableId),
  );

  const schemas = await Promise.all(
    relevant.map(async (t) => ({
      tableId: t.tableId,
      fields: await getSchema(dataset, t.tableId),
    })),
  );

  const tables: InferredTable[] = schemas.map(({ tableId, fields }) => {
    const tableType = classifyTable(tableId)!;
    const pkColumn = inferPkColumn(tableId);
    const columns = fields.map((f) =>
      classifyColumn(f, tableType, pkColumn, dimTableIds),
    );
    return { id: tableId, tableType, columns };
  });

  const relationships: InferredRelationship[] = [];
  for (const table of tables) {
    for (const col of table.columns) {
      if (col.role === "fk" && col.refTable) {
        relationships.push({
          id: `${table.id}__${col.name}`,
          sourceTable: table.id,
          targetTable: col.refTable,
          fkColumn: col.name,
        });
      }
    }
  }

  return { tables, relationships };
}

const NODE_W = 240;
const H_GAP = 80;
const V_GAP = 140;
const NODE_H = 220;

function placeRow(
  items: InferredTable[],
  y: number,
  positions: Map<string, { x: number; y: number }>,
) {
  const totalW = items.length * NODE_W + (items.length - 1) * H_GAP;
  const startX = Math.max(80, (1500 - totalW) / 2);
  items.forEach((item, i) => {
    positions.set(item.id, { x: startX + i * (NODE_W + H_GAP), y });
  });
}

export function computeLayout(
  schema: InferredSchema,
  visibleLayers: Set<TableType>,
): Map<string, { x: number; y: number }> {
  const positions = new Map<string, { x: number; y: number }>();
  const visible = schema.tables.filter((t) => visibleLayers.has(t.tableType));

  const facts = visible.filter((t) => t.tableType === "fact");
  const dims = visible.filter((t) => t.tableType === "dim");
  const stgs = visible.filter((t) => t.tableType === "stg");
  const ints = visible.filter((t) => t.tableType === "int");

  const dimToFacts = new Map<string, Set<string>>();
  for (const dim of dims) dimToFacts.set(dim.id, new Set());
  for (const rel of schema.relationships)
    dimToFacts.get(rel.targetTable)?.add(rel.sourceTable);

  const factXMap = new Map<string, number>();
  let currentY = 40;

  if (stgs.length > 0) {
    placeRow(stgs, currentY, positions);
    currentY += NODE_H + V_GAP;
  }

  if (ints.length > 0) {
    placeRow(ints, currentY, positions);
    currentY += NODE_H + V_GAP;
  }

  if (dims.length > 0 || facts.length > 0) {
    function avgFactX(dimId: string): number {
      const connected = dimToFacts.get(dimId);
      if (!connected || connected.size === 0) return Infinity;
      let sum = 0;
      for (const fId of connected) sum += factXMap.get(fId) ?? 0;
      return sum / connected.size;
    }

    const topDims = dims
      .filter((d) => (dimToFacts.get(d.id)?.size ?? 0) >= 2)
      .sort((a, b) => a.id.localeCompare(b.id));

    const bottomDims = dims
      .filter((d) => (dimToFacts.get(d.id)?.size ?? 0) < 2)
      .sort((a, b) => a.id.localeCompare(b.id));

    if (topDims.length > 0) {
      placeRow(topDims, currentY, positions);
      currentY += NODE_H + V_GAP;
    }

    if (facts.length > 0) {
      placeRow(facts, currentY, positions);
      facts.forEach((f) => {
        const pos = positions.get(f.id)!;
        factXMap.set(f.id, pos.x + NODE_W / 2);
      });
      currentY += NODE_H + V_GAP;
    }

    // Two-pass sort: initial alphabetical, then re-sort by connected fact position
    topDims.sort((a, b) => avgFactX(a.id) - avgFactX(b.id));
    bottomDims.sort((a, b) => avgFactX(a.id) - avgFactX(b.id));

    if (topDims.length > 0) {
      const topY = (positions.get(facts[0]?.id ?? "")?.y ?? currentY) - NODE_H - V_GAP;
      placeRow(topDims, topY, positions);
    }

    if (bottomDims.length > 0) {
      placeRow(bottomDims, currentY, positions);
    }
  }

  return positions;
}
