import { useState, useCallback } from "react";

const STORAGE_KEY = "bq-explorer-query-history";
const MAX_ENTRIES = 30;

export interface HistoryEntry {
  sql: string;
  timestamp: number;
}

function load(): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function save(entries: HistoryEntry[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(entries));
}

export function useQueryHistory() {
  const [entries, setEntries] = useState<HistoryEntry[]>(load);

  const add = useCallback((sqlText: string) => {
    const trimmed = sqlText.trim();
    if (!trimmed) return;
    setEntries((prev) => {
      const filtered = prev.filter((e) => e.sql !== trimmed);
      const next = [{ sql: trimmed, timestamp: Date.now() }, ...filtered].slice(
        0,
        MAX_ENTRIES,
      );
      save(next);
      return next;
    });
  }, []);

  const clear = useCallback(() => {
    setEntries([]);
    localStorage.removeItem(STORAGE_KEY);
  }, []);

  return { entries, add, clear };
}
