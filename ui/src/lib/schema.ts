import { listTables, getSchema, type SchemaField } from "./bq.ts";
import lineageGraph from "virtual:dbt-lineage";

export type ColumnRole = "pk" | "fk" | "degenerate" | "measure" | "attribute";
export type TableType = "raw" | "stg" | "int" | "dim" | "fact";
export type EdgeKind = "fk" | "ref" | "source";

export const LAYER_ORDER: TableType[] = ["raw", "stg", "int", "dim", "fact"];

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
  fkColumn?: string;
  edgeKind: EdgeKind;
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

export async function fetchAndInferSchema(): Promise<InferredSchema> {
  const [rawTableRefs, dwhTableRefs] = await Promise.all([
    listTables("raw"),
    listTables("dwh"),
  ]);

  const dwhRelevant = dwhTableRefs.filter(
    (t) => classifyTable(t.tableId) !== null,
  );
  const dimTableIds = new Set(
    dwhRelevant
      .filter((t) => t.tableId.startsWith("dim_"))
      .map((t) => t.tableId),
  );

  const [rawSchemas, dwhSchemas] = await Promise.all([
    Promise.all(
      rawTableRefs.map(async (t) => ({
        tableId: t.tableId,
        fields: await getSchema("raw", t.tableId),
      })),
    ),
    Promise.all(
      dwhRelevant.map(async (t) => ({
        tableId: t.tableId,
        fields: await getSchema("dwh", t.tableId),
      })),
    ),
  ]);

  const rawTables: InferredTable[] = rawSchemas.map(({ tableId, fields }) => ({
    id: tableId,
    tableType: "raw" as const,
    columns: fields.map((f: SchemaField): InferredColumn => ({ ...f, role: "attribute" })),
  }));

  const dwhTables: InferredTable[] = dwhSchemas.map(({ tableId, fields }) => {
    const tableType = classifyTable(tableId)!;
    const pkColumn = inferPkColumn(tableId);
    const columns = fields.map((f) =>
      classifyColumn(f, tableType, pkColumn, dimTableIds),
    );
    return { id: tableId, tableType, columns };
  });

  const tables = [...rawTables, ...dwhTables];
  const tableIds = new Set(tables.map((t) => t.id));

  // FK relationships (existing logic, fact → dim)
  const relationships: InferredRelationship[] = [];
  const edgePairs = new Set<string>();

  for (const table of dwhTables) {
    for (const col of table.columns) {
      if (col.role === "fk" && col.refTable) {
        const pairKey = [table.id, col.refTable].sort().join("|");
        edgePairs.add(pairKey);
        relationships.push({
          id: `${table.id}__${col.name}`,
          sourceTable: table.id,
          targetTable: col.refTable,
          fkColumn: col.name,
          edgeKind: "fk",
        });
      }
    }
  }

  // Lineage relationships from dbt ref()/source()
  for (const [model, deps] of Object.entries(lineageGraph)) {
    if (!tableIds.has(model)) continue;

    for (const ref of deps.refs) {
      if (!tableIds.has(ref)) continue;
      const pairKey = [model, ref].sort().join("|");
      if (edgePairs.has(pairKey)) continue;
      edgePairs.add(pairKey);

      relationships.push({
        id: `${ref}__ref__${model}`,
        sourceTable: ref,
        targetTable: model,
        edgeKind: "ref",
      });
    }

    for (const source of deps.sources) {
      if (!tableIds.has(source)) continue;
      const pairKey = [model, source].sort().join("|");
      if (edgePairs.has(pairKey)) continue;
      edgePairs.add(pairKey);

      relationships.push({
        id: `${source}__src__${model}`,
        sourceTable: source,
        targetTable: model,
        edgeKind: "source",
      });
    }
  }

  return { tables, relationships };
}

// --- Transitive highlight ---

export function computeTransitiveHighlight(
  schema: InferredSchema,
  selectedId: string,
): { nodeIds: Set<string>; edgeIds: Set<string> } {
  const nodeIds = new Set<string>([selectedId]);
  const edgeIds = new Set<string>();

  // Build directed adjacency based on dependency semantics:
  //   FK edges: sourceTable (fact) depends on targetTable (dim)
  //   ref/source edges: targetTable depends on sourceTable
  // "upstreamOf" maps a node to its dependencies (what it consumes)
  // "downstreamOf" maps a node to its dependents (what consumes it)
  const upstreamOf = new Map<string, { edgeId: string; node: string }[]>();
  const downstreamOf = new Map<string, { edgeId: string; node: string }[]>();

  for (const rel of schema.relationships) {
    const dependent = rel.edgeKind === "fk" ? rel.sourceTable : rel.targetTable;
    const dependency = rel.edgeKind === "fk" ? rel.targetTable : rel.sourceTable;

    if (!upstreamOf.has(dependent)) upstreamOf.set(dependent, []);
    upstreamOf.get(dependent)!.push({ edgeId: rel.id, node: dependency });

    if (!downstreamOf.has(dependency)) downstreamOf.set(dependency, []);
    downstreamOf.get(dependency)!.push({ edgeId: rel.id, node: dependent });
  }

  // BFS upstream (find all data sources)
  const upQueue = [selectedId];
  while (upQueue.length > 0) {
    const current = upQueue.shift()!;
    for (const { edgeId, node } of upstreamOf.get(current) ?? []) {
      edgeIds.add(edgeId);
      if (!nodeIds.has(node)) {
        nodeIds.add(node);
        upQueue.push(node);
      }
    }
  }

  // BFS downstream (find all data consumers)
  const downQueue = [selectedId];
  while (downQueue.length > 0) {
    const current = downQueue.shift()!;
    for (const { edgeId, node } of downstreamOf.get(current) ?? []) {
      edgeIds.add(edgeId);
      if (!nodeIds.has(node)) {
        nodeIds.add(node);
        downQueue.push(node);
      }
    }
  }

  return { nodeIds, edgeIds };
}

// --- Layout ---

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

  const raws = visible.filter((t) => t.tableType === "raw");
  const stgs = visible.filter((t) => t.tableType === "stg");
  const ints = visible.filter((t) => t.tableType === "int");
  const facts = visible.filter((t) => t.tableType === "fact");
  const dims = visible.filter((t) => t.tableType === "dim");

  // FK-based dimension-to-fact mapping (only visible facts count)
  const visibleFactIds = new Set(facts.map((f) => f.id));
  const dimToFacts = new Map<string, Set<string>>();
  for (const dim of dims) dimToFacts.set(dim.id, new Set());
  for (const rel of schema.relationships) {
    if (rel.edgeKind === "fk" && visibleFactIds.has(rel.sourceTable)) {
      dimToFacts.get(rel.targetTable)?.add(rel.sourceTable);
    }
  }

  const factXMap = new Map<string, number>();
  let currentY = 40;

  if (raws.length > 0) {
    placeRow(raws, currentY, positions);
    currentY += NODE_H + V_GAP;
  }

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

    // Two-pass: re-sort dims by connected fact X position, then re-place
    if (facts.length > 0) {
      topDims.sort((a, b) => avgFactX(a.id) - avgFactX(b.id));
      bottomDims.sort((a, b) => avgFactX(a.id) - avgFactX(b.id));

      if (topDims.length > 0) {
        const topY = positions.get(facts[0].id)!.y - NODE_H - V_GAP;
        placeRow(topDims, topY, positions);
      }
    }

    if (bottomDims.length > 0) {
      placeRow(bottomDims, currentY, positions);
    }
  }

  return positions;
}
