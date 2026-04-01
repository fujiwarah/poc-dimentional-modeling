import "@xyflow/react/dist/style.css";
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
  MarkerType,
  BackgroundVariant,
  type Node,
  type Edge,
  type NodeProps,
} from "@xyflow/react";
import { useState, useCallback, useMemo, useEffect, memo, createContext, useContext } from "react";
import { useTheme } from "../hooks/useTheme.ts";
import { cn } from "../lib/cn.ts";
import {
  fetchAndInferSchema,
  computeLayout,
  LAYER_ORDER,
  type InferredSchema,
  type InferredTable,
  type InferredColumn,
  type ColumnRole,
  type TableType,
} from "../lib/schema.ts";

const EDGE_PALETTE = ["#3b82f6", "#8b5cf6", "#f59e0b", "#10b981", "#ef4444", "#ec4899"];
const EDGE_BG_PALETTE = ["bg-blue-500", "bg-violet-500", "bg-amber-500", "bg-emerald-500", "bg-red-500", "bg-pink-500"];

function buildFactColors(tables: InferredTable[]): Map<string, { stroke: string; bg: string }> {
  const facts = tables.filter((t) => t.tableType === "fact");
  return new Map(
    facts.map((f, i) => [
      f.id,
      {
        stroke: EDGE_PALETTE[i % EDGE_PALETTE.length],
        bg: EDGE_BG_PALETTE[i % EDGE_BG_PALETTE.length],
      },
    ]),
  );
}

const ROLE_STYLE: Record<ColumnRole, { badge: string; cls: string }> = {
  pk: { badge: "PK", cls: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400" },
  fk: { badge: "FK", cls: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400" },
  measure: { badge: "M", cls: "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400" },
  degenerate: { badge: "DD", cls: "bg-zinc-100 text-zinc-500 dark:bg-zinc-800 dark:text-zinc-500" },
  attribute: { badge: "", cls: "" },
};

const TYPE_BG: Record<TableType, string> = {
  fact: "bg-gradient-to-br from-blue-50 to-indigo-50 dark:from-[#0c1a2e] dark:to-[#0f1530]",
  dim: "bg-gradient-to-br from-violet-50 to-fuchsia-50 dark:from-[#150c2e] dark:to-[#1a0c2e]",
  stg: "bg-gradient-to-br from-zinc-50 to-slate-50 dark:from-[#18181b] dark:to-[#1a1c23]",
  int: "bg-gradient-to-br from-teal-50 to-emerald-50 dark:from-[#0c2420] dark:to-[#0c2e1e]",
};

const TYPE_BADGE: Record<TableType, string> = {
  fact: "bg-gradient-to-r from-blue-500 to-indigo-500 text-white",
  dim: "bg-gradient-to-r from-violet-500 to-fuchsia-500 text-white",
  stg: "bg-gradient-to-r from-zinc-400 to-slate-400 text-white",
  int: "bg-gradient-to-r from-teal-500 to-emerald-500 text-white",
};

const TYPE_MINIMAP_COLOR: Record<TableType, string> = {
  fact: "#3b82f6",
  dim: "#8b5cf6",
  stg: "#a1a1aa",
  int: "#14b8a6",
};

const LAYER_LABELS: Record<TableType, string> = {
  stg: "Staging",
  int: "Intermediate",
  dim: "Dimension",
  fact: "Fact",
};

const NavigateCtx = createContext<(tableId: string) => void>(() => {});

interface TableNodeData {
  table: InferredTable;
  highlighted: boolean;
  dimmed: boolean;
  [key: string]: unknown;
}

type TableNodeType = Node<TableNodeData, "tableNode">;

const TableNode = memo(function TableNode({
  data,
}: NodeProps<TableNodeType>) {
  const { table, highlighted, dimmed } = data;
  const onNavigate = useContext(NavigateCtx);

  return (
    <div
      onDoubleClick={() => onNavigate(table.id)}
      className={cn(
        "rounded-lg border shadow-sm transition-opacity duration-200 select-none",
        "border-zinc-200 dark:border-zinc-700",
        TYPE_BG[table.tableType],
        dimmed && "opacity-15",
        highlighted && "ring-2 ring-blue-400 dark:ring-blue-500",
      )}
    >
      <Handle type="target" position={Position.Top} className="!opacity-0" isConnectable={false} />
      <Handle type="target" position={Position.Left} className="!opacity-0" isConnectable={false} />
      <Handle type="source" position={Position.Bottom} className="!opacity-0" isConnectable={false} />
      <Handle type="source" position={Position.Right} className="!opacity-0" isConnectable={false} />

      <div className="px-3 py-2 border-b border-zinc-100 dark:border-zinc-800 flex items-center gap-2">
        <span className="font-mono text-xs font-semibold text-zinc-800 dark:text-zinc-200 truncate">
          {table.id}
        </span>
        <span className={cn("text-[9px] px-1.5 py-0.5 rounded-full font-medium shrink-0", TYPE_BADGE[table.tableType])}>
          {table.tableType}
        </span>
      </div>

      <div className="max-h-52 overflow-y-auto py-1">
        {table.columns.map((col) => (
          <ColumnRow key={col.name} column={col} />
        ))}
      </div>
    </div>
  );
});

function ColumnRow({ column }: { column: InferredColumn }) {
  const style = ROLE_STYLE[column.role];
  return (
    <div className="flex items-center gap-1 px-3 py-px">
      {style.badge ? (
        <span className={cn("text-[7px] leading-none px-1 py-0.5 rounded font-bold shrink-0", style.cls)}>
          {style.badge}
        </span>
      ) : (
        <span className="w-[18px] shrink-0" />
      )}
      <span className="font-mono text-[10px] text-zinc-700 dark:text-zinc-300 truncate">
        {column.name}
      </span>
      {column.refTable && (
        <span className="text-[8px] text-blue-400 dark:text-blue-500 shrink-0">
          → {column.refTable}
        </span>
      )}
      <span className="text-[9px] text-zinc-400 dark:text-zinc-600 ml-auto shrink-0">
        {column.type}
      </span>
    </div>
  );
}

const nodeTypes = { tableNode: TableNode };

interface Props {
  onNavigateToBrowse: (tableId: string) => void;
}

function SchemaCanvas() {
  const { theme } = useTheme();
  const [schema, setSchema] = useState<InferredSchema | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [visibleLayers, setVisibleLayers] = useState<Set<TableType>>(
    () => new Set<TableType>(["fact", "dim"]),
  );

  useEffect(() => {
    fetchAndInferSchema("dwh")
      .then(setSchema)
      .catch((e: unknown) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
      .finally(() => setLoading(false));
  }, []);

  const toggleLayer = useCallback((layer: TableType) => {
    setVisibleLayers((prev) => {
      const next = new Set(prev);
      if (next.has(layer)) next.delete(layer);
      else next.add(layer);
      return next;
    });
    setSelectedId(null);
  }, []);

  const layerCounts = useMemo(() => {
    if (!schema) return new Map<TableType, number>();
    const counts = new Map<TableType, number>();
    for (const t of schema.tables) {
      counts.set(t.tableType, (counts.get(t.tableType) ?? 0) + 1);
    }
    return counts;
  }, [schema]);

  const factColors = useMemo(
    () => (schema ? buildFactColors(schema.tables) : new Map()),
    [schema],
  );

  const tableTypeMap = useMemo(
    () => new Map(schema?.tables.map((t) => [t.id, t.tableType]) ?? []),
    [schema],
  );

  const highlights = useMemo(() => {
    if (!schema || !selectedId) return null;
    const nodeIds = new Set<string>([selectedId]);
    const edgeIds = new Set<string>();
    for (const rel of schema.relationships) {
      if (rel.sourceTable === selectedId || rel.targetTable === selectedId) {
        nodeIds.add(rel.sourceTable);
        nodeIds.add(rel.targetTable);
        edgeIds.add(rel.id);
      }
    }
    return { nodeIds, edgeIds };
  }, [schema, selectedId]);

  const nodes: Node[] = useMemo(() => {
    if (!schema) return [];
    const layout = computeLayout(schema, visibleLayers);
    return schema.tables
      .filter((t) => visibleLayers.has(t.tableType))
      .map((table) => ({
        id: table.id,
        type: "tableNode" as const,
        position: layout.get(table.id) ?? { x: 0, y: 0 },
        data: {
          table,
          highlighted: highlights ? highlights.nodeIds.has(table.id) : false,
          dimmed: highlights ? !highlights.nodeIds.has(table.id) : false,
        },
      }));
  }, [schema, visibleLayers, highlights]);

  const edgeLabelStyle = useMemo(
    () => ({
      labelStyle: {
        fill: theme === "dark" ? "#a1a1aa" : "#71717a",
        fontSize: 9,
        fontFamily: "JetBrains Mono, monospace",
      },
      labelBgStyle: {
        fill: theme === "dark" ? "#18181b" : "#ffffff",
        fillOpacity: 0.85,
      },
    }),
    [theme],
  );

  const edges: Edge[] = useMemo(() => {
    if (!schema) return [];
    return schema.relationships
      .filter(
        (rel) =>
          visibleLayers.has(tableTypeMap.get(rel.sourceTable) ?? "fact") &&
          visibleLayers.has(tableTypeMap.get(rel.targetTable) ?? "dim"),
      )
      .map((rel) => ({
        id: rel.id,
        source: rel.sourceTable,
        target: rel.targetTable,
        type: "smoothstep",
        label: rel.fkColumn,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: {
          stroke: factColors.get(rel.sourceTable)?.stroke ?? "#6b7280",
          strokeWidth: highlights?.edgeIds.has(rel.id) ? 2.5 : 1.5,
          opacity: highlights && !highlights.edgeIds.has(rel.id) ? 0.12 : 1,
        },
        ...edgeLabelStyle,
      }));
  }, [schema, visibleLayers, tableTypeMap, highlights, factColors, edgeLabelStyle]);

  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      setSelectedId((prev) => (prev === node.id ? null : node.id));
    },
    [],
  );

  const handlePaneClick = useCallback(() => setSelectedId(null), []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full text-sm text-blue-500 dark:text-blue-400 animate-pulse">
        loading schema...
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-900/50 rounded-lg p-4 text-sm text-red-600 dark:text-red-400 font-mono max-w-lg">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="h-full relative">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodeClick={handleNodeClick}
        onPaneClick={handlePaneClick}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        nodesDraggable={false}
        nodesConnectable={false}
        colorMode={theme}
        minZoom={0.2}
        maxZoom={1.5}
        proOptions={{ hideAttribution: true }}
      >
        <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
        <Controls showInteractive={false} />
        <MiniMap
          pannable
          zoomable
          nodeColor={(node) => {
            const t = (node.data as TableNodeData).table.tableType;
            return TYPE_MINIMAP_COLOR[t] ?? "#6b7280";
          }}
        />
      </ReactFlow>

      <div className="absolute top-3 right-3 flex gap-1">
        {LAYER_ORDER.map((layer) => {
          const count = layerCounts.get(layer) ?? 0;
          if (count === 0) return null;
          const active = visibleLayers.has(layer);
          return (
            <button
              key={layer}
              onClick={() => toggleLayer(layer)}
              className={cn(
                "px-2.5 py-1 rounded-md text-[10px] font-medium transition-colors border",
                active
                  ? "bg-white dark:bg-zinc-800 text-zinc-800 dark:text-zinc-200 border-zinc-300 dark:border-zinc-600 shadow-sm"
                  : "bg-transparent text-zinc-400 dark:text-zinc-600 border-zinc-200 dark:border-zinc-800 hover:text-zinc-600 dark:hover:text-zinc-400",
              )}
            >
              {LAYER_LABELS[layer]} ({count})
            </button>
          );
        })}
      </div>

      <Legend factColors={factColors} visibleLayers={visibleLayers} />

      {selectedId && (
        <div className="absolute top-3 left-3 bg-white/90 dark:bg-zinc-900/90 backdrop-blur-sm rounded-md px-3 py-1.5 text-xs text-zinc-600 dark:text-zinc-400 border">
          <span className="font-mono font-medium text-zinc-800 dark:text-zinc-200">
            {selectedId}
          </span>
          <span className="ml-2">selected — click background to clear</span>
        </div>
      )}
    </div>
  );
}

function Legend({
  factColors,
  visibleLayers,
}: {
  factColors: Map<string, { stroke: string; bg: string }>;
  visibleLayers: Set<TableType>;
}) {
  const showEdges = visibleLayers.has("fact") && visibleLayers.has("dim");
  const items = Array.from(factColors.entries()).map(([id, c]) => ({
    color: c.bg,
    label: id,
  }));
  const roles = [
    { badge: "PK", cls: ROLE_STYLE.pk.cls },
    { badge: "FK", cls: ROLE_STYLE.fk.cls },
    { badge: "M", cls: ROLE_STYLE.measure.cls },
    { badge: "DD", cls: ROLE_STYLE.degenerate.cls },
  ];

  return (
    <div className="absolute bottom-3 right-3 bg-white/90 dark:bg-zinc-900/90 backdrop-blur-sm rounded-lg border p-3 text-[10px] space-y-2">
      {showEdges && (
        <>
          <div className="font-medium text-zinc-500 dark:text-zinc-400 uppercase tracking-wider">
            Edges
          </div>
          <div className="flex flex-col gap-1">
            {items.map((it) => (
              <div key={it.label} className="flex items-center gap-2">
                <span className={cn("w-3 h-0.5 rounded-full", it.color)} />
                <span className="text-zinc-600 dark:text-zinc-400">{it.label}</span>
              </div>
            ))}
          </div>
        </>
      )}
      <div className="font-medium text-zinc-500 dark:text-zinc-400 uppercase tracking-wider pt-1">
        Columns
      </div>
      <div className="flex flex-wrap gap-1.5">
        {roles.map((r) => (
          <span key={r.badge} className={cn("px-1.5 py-0.5 rounded text-[8px] font-bold", r.cls)}>
            {r.badge}
          </span>
        ))}
      </div>
      <div className="text-zinc-400 dark:text-zinc-600 pt-1">
        double-click node → Browse tab
      </div>
    </div>
  );
}

export default function SchemaViewer({ onNavigateToBrowse }: Props) {
  return (
    <ReactFlowProvider>
      <NavigateCtx.Provider value={onNavigateToBrowse}>
        <SchemaCanvas />
      </NavigateCtx.Provider>
    </ReactFlowProvider>
  );
}
