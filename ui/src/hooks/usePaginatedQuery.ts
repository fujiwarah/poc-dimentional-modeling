import { useState, useCallback, useRef } from "react";
import { type PaginatedResult } from "../lib/bq.ts";

type Fetcher = (page: number, knownTotal?: number) => Promise<PaginatedResult>;

interface State {
  result: PaginatedResult | null;
  loading: boolean;
  error: string | null;
}

const INIT: State = { result: null, loading: false, error: null };

export function usePaginatedQuery() {
  const [state, setState] = useState<State>(INIT);
  const fetcherRef = useRef<Fetcher | null>(null);
  const totalRef = useRef<number | undefined>(undefined);

  const execute = useCallback(async (fetcher: Fetcher, preserveTotal?: boolean) => {
    fetcherRef.current = fetcher;
    if (!preserveTotal) totalRef.current = undefined;
    setState({ result: null, loading: true, error: null });
    try {
      const result = await fetcher(1, totalRef.current);
      totalRef.current = result.totalRows;
      setState({ result, loading: false, error: null });
    } catch (e) {
      setState({
        result: null,
        loading: false,
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }, []);

  const changePage = useCallback(async (page: number) => {
    const fetcher = fetcherRef.current;
    if (!fetcher) return;
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const result = await fetcher(page, totalRef.current);
      totalRef.current = result.totalRows;
      setState({ result, loading: false, error: null });
    } catch (e) {
      setState((s) => ({
        ...s,
        loading: false,
        error: e instanceof Error ? e.message : String(e),
      }));
    }
  }, []);

  const reset = useCallback(() => {
    fetcherRef.current = null;
    totalRef.current = undefined;
    setState(INIT);
  }, []);

  return { ...state, execute, changePage, reset };
}
