import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  type SortingState,
  type ColumnDef,
} from "@tanstack/react-table";
import { useMemo, useState } from "react";
import { cn } from "../lib/cn.ts";

interface PaginationProps {
  page: number;
  pageSize: number;
  totalRows: number;
  totalPages: number;
  onPageChange: (page: number) => void;
}

interface Props {
  columns: string[];
  rows: Record<string, string>[];
  pagination?: PaginationProps;
}

function getPageNumbers(current: number, total: number): (number | "...")[] {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
  const pages: (number | "...")[] = [];
  pages.push(1);
  if (current > 3) pages.push("...");
  for (let i = Math.max(2, current - 1); i <= Math.min(total - 1, current + 1); i++) {
    pages.push(i);
  }
  if (current < total - 2) pages.push("...");
  pages.push(total);
  return pages;
}

export default function DataTable({ columns, rows, pagination }: Props) {
  const [sorting, setSorting] = useState<SortingState>([]);

  const columnDefs = useMemo<ColumnDef<Record<string, string>>[]>(
    () =>
      columns.map((col) => ({
        accessorKey: col,
        header: col,
        cell: (info) => info.getValue() ?? "NULL",
      })),
    [columns],
  );

  const table = useReactTable({
    data: rows,
    columns: columnDefs,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  if (columns.length === 0) return null;

  const p = pagination;
  const rangeStart = p ? (p.page - 1) * p.pageSize + 1 : 0;
  const rangeEnd = p ? Math.min(p.page * p.pageSize, p.totalRows) : 0;

  return (
    <div className="flex flex-col rounded-lg border overflow-hidden">
      <div className="overflow-auto flex-1">
        <table className="w-full text-sm">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} className="border-b bg-zinc-50 dark:bg-zinc-900">
                {hg.headers.map((header) => (
                  <th
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                    className="px-3 py-2 text-left font-mono text-xs font-medium text-zinc-500 dark:text-zinc-400 cursor-pointer select-none hover:text-zinc-800 dark:hover:text-zinc-200 whitespace-nowrap"
                  >
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext(),
                    )}
                    {{ asc: " ↑", desc: " ↓" }[
                      header.column.getIsSorted() as string
                    ] ?? ""}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr
                key={row.id}
                className="border-b border-zinc-100 dark:border-zinc-800/50 hover:bg-zinc-50 dark:hover:bg-zinc-900/50 transition-colors"
              >
                {row.getVisibleCells().map((cell) => (
                  <td
                    key={cell.id}
                    className="px-3 py-1.5 font-mono text-xs whitespace-nowrap text-zinc-700 dark:text-zinc-300"
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {p && p.totalPages > 1 && (
        <div className="flex items-center justify-between border-t bg-zinc-50 dark:bg-zinc-900 px-3 py-2">
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            {rangeStart}–{rangeEnd} / {p.totalRows.toLocaleString()} 件
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => p.onPageChange(p.page - 1)}
              disabled={p.page <= 1}
              className="px-2 py-1 rounded text-xs text-zinc-600 dark:text-zinc-400 hover:bg-zinc-200 dark:hover:bg-zinc-800 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            >
              ←
            </button>
            {getPageNumbers(p.page, p.totalPages).map((n, i) =>
              n === "..." ? (
                <span key={`ellipsis-${i}`} className="px-1 text-xs text-zinc-400">
                  …
                </span>
              ) : (
                <button
                  key={n}
                  onClick={() => p.onPageChange(n)}
                  className={cn(
                    "min-w-[28px] px-1.5 py-1 rounded text-xs font-medium transition-colors",
                    p.page === n
                      ? "bg-blue-600 text-white"
                      : "text-zinc-600 dark:text-zinc-400 hover:bg-zinc-200 dark:hover:bg-zinc-800",
                  )}
                >
                  {n}
                </button>
              ),
            )}
            <button
              onClick={() => p.onPageChange(p.page + 1)}
              disabled={p.page >= p.totalPages}
              className="px-2 py-1 rounded text-xs text-zinc-600 dark:text-zinc-400 hover:bg-zinc-200 dark:hover:bg-zinc-800 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
            >
              →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
