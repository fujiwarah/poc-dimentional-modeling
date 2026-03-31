import { useState } from "react";
import TableBrowser from "./components/TableBrowser.tsx";
import QueryEditor from "./components/QueryEditor.tsx";

type Tab = "browse" | "query";

export default function App() {
  const [tab, setTab] = useState<Tab>("browse");

  return (
    <div className="h-screen flex flex-col">
      {/* Header */}
      <header className="flex items-center justify-between px-4 py-2 border-b border-zinc-800 bg-zinc-950">
        <div className="flex items-center gap-3">
          <h1 className="text-sm font-semibold tracking-tight">
            BQ Explorer
          </h1>
          <span className="text-[10px] text-zinc-600 font-mono">
            poc-project
          </span>
        </div>
        <nav className="flex bg-zinc-900 rounded-lg p-0.5">
          {(
            [
              ["browse", "Browse"],
              ["query", "Query"],
            ] as const
          ).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setTab(key)}
              className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${
                tab === key
                  ? "bg-zinc-700 text-zinc-100"
                  : "text-zinc-500 hover:text-zinc-300"
              }`}
            >
              {label}
            </button>
          ))}
        </nav>
      </header>

      {/* Content */}
      <main className="flex-1 overflow-hidden p-4">
        {tab === "browse" && <TableBrowser />}
        {tab === "query" && <QueryEditor />}
      </main>
    </div>
  );
}
