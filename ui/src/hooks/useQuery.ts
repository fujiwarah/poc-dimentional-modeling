import { useState, useEffect } from "react";
import { runQuery, type QueryResult } from "../lib/bq.ts";

interface State {
  result: QueryResult | null;
  loading: boolean;
  error: string | null;
}

const MAX_ENTRIES = 50;
const TTL_MS = 5 * 60 * 1000;

interface CacheEntry {
  result: QueryResult;
  ts: number;
}

const cache = new Map<string, CacheEntry>();

function getCache(sql: string): QueryResult | undefined {
  const entry = cache.get(sql);
  if (!entry) return undefined;
  if (Date.now() - entry.ts > TTL_MS) {
    cache.delete(sql);
    return undefined;
  }
  return entry.result;
}

function setCache(sql: string, result: QueryResult) {
  if (cache.size >= MAX_ENTRIES) {
    const oldest = cache.keys().next().value!;
    cache.delete(oldest);
  }
  cache.set(sql, { result, ts: Date.now() });
}

export function useQuery(sql: string): State {
  const [state, setState] = useState<State>(() => {
    const cached = getCache(sql);
    return cached
      ? { result: cached, loading: false, error: null }
      : { result: null, loading: true, error: null };
  });

  useEffect(() => {
    const cached = getCache(sql);
    if (cached) {
      setState({ result: cached, loading: false, error: null });
      return;
    }

    let cancelled = false;
    setState({ result: null, loading: true, error: null });
    runQuery(sql)
      .then((result) => {
        setCache(sql, result);
        if (!cancelled) setState({ result, loading: false, error: null });
      })
      .catch((e: unknown) => {
        if (!cancelled)
          setState({
            result: null,
            loading: false,
            error: e instanceof Error ? e.message : String(e),
          });
      });
    return () => {
      cancelled = true;
    };
  }, [sql]);

  return state;
}
