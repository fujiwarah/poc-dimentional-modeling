import { useState, useCallback, lazy, Suspense } from "react";
import TableBrowser from "./components/TableBrowser.tsx";
import { cn } from "./lib/cn.ts";
import { PROJECT } from "./lib/bq.ts";
import { useTheme } from "./hooks/useTheme.ts";

const QueryEditor = lazy(() => import("./components/QueryEditor.tsx"));
const SchemaViewer = lazy(() => import("./components/SchemaViewer.tsx"));
const Dashboard = lazy(() => import("./components/Dashboard.tsx"));
const Training = lazy(() => import("./components/Training.tsx"));

type Tab = "browse" | "query" | "schema" | "dashboard" | "training";

const TABS = [
  ["browse", "Browse"],
  ["query", "Query"],
  ["schema", "Schema"],
  ["dashboard", "Dashboard"],
  ["training", "Training"],
] as const;

function TabFallback() {
  return (
    <div className="flex items-center justify-center h-full text-sm text-blue-500 dark:text-blue-400 animate-pulse">
      loading...
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState<Tab>("browse");
  const { theme, toggle } = useTheme();

  const navigateToBrowse = useCallback((tableId: string) => {
    setTab("browse");
    void tableId;
  }, []);

  return (
    <div className="h-screen flex flex-col">
      <header className="flex items-center justify-between px-4 py-2 border-b bg-white dark:bg-zinc-950">
        <div className="flex items-center gap-3">
          <h1 className="text-sm font-semibold tracking-tight">BQ Explorer</h1>
          <span className="text-[10px] text-zinc-400 dark:text-zinc-500 font-mono">
            {PROJECT}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <nav className="flex bg-zinc-100 dark:bg-zinc-900 rounded-lg p-0.5">
            {TABS.map(([key, label]) => (
              <button
                key={key}
                onClick={() => setTab(key)}
                className={cn(
                  "px-4 py-1.5 rounded-md text-xs font-medium transition-colors",
                  tab === key
                    ? "bg-white dark:bg-zinc-700 text-zinc-900 dark:text-zinc-100 shadow-sm dark:shadow-none"
                    : "text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300",
                )}
              >
                {label}
              </button>
            ))}
          </nav>
          <button
            onClick={toggle}
            className="p-1.5 rounded-md text-zinc-500 hover:text-zinc-700 dark:hover:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
            title={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
          >
            {theme === "dark" ? (
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-4 h-4">
                <path d="M10 2a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0v-1.5A.75.75 0 0 1 10 2ZM10 15a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0v-1.5A.75.75 0 0 1 10 15ZM10 7a3 3 0 1 0 0 6 3 3 0 0 0 0-6ZM15.657 5.404a.75.75 0 1 0-1.06-1.06l-1.061 1.06a.75.75 0 0 0 1.06 1.061l1.061-1.06ZM6.464 14.596a.75.75 0 1 0-1.06-1.06l-1.061 1.06a.75.75 0 0 0 1.06 1.061l1.061-1.06ZM18 10a.75.75 0 0 1-.75.75h-1.5a.75.75 0 0 1 0-1.5h1.5A.75.75 0 0 1 18 10ZM5 10a.75.75 0 0 1-.75.75h-1.5a.75.75 0 0 1 0-1.5h1.5A.75.75 0 0 1 5 10ZM14.596 15.657a.75.75 0 0 0 1.06-1.06l-1.06-1.061a.75.75 0 1 0-1.061 1.06l1.06 1.061ZM5.404 6.464a.75.75 0 0 0 1.06-1.06l-1.06-1.061a.75.75 0 1 0-1.061 1.06l1.06 1.061Z" />
              </svg>
            ) : (
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-4 h-4">
                <path fillRule="evenodd" d="M7.455 2.004a.75.75 0 0 1 .26.77 7 7 0 0 0 9.958 7.967.75.75 0 0 1 1.067.853A8.5 8.5 0 1 1 6.647 1.921a.75.75 0 0 1 .808.083Z" clipRule="evenodd" />
              </svg>
            )}
          </button>
        </div>
      </header>

      <main className="flex-1 overflow-hidden p-4">
        <Suspense fallback={<TabFallback />}>
          {tab === "browse" && <TableBrowser />}
          {tab === "query" && <QueryEditor />}
          {tab === "schema" && <SchemaViewer onNavigateToBrowse={navigateToBrowse} />}
          {tab === "dashboard" && <Dashboard />}
          {tab === "training" && <Training />}
        </Suspense>
      </main>
    </div>
  );
}
