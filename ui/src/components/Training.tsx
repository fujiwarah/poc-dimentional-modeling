import { useState, useCallback, useMemo, useRef } from "react";
import { cn } from "../lib/cn.ts";
import { runQuery, type QueryResult } from "../lib/bq.ts";
import { LESSONS, type Lesson, type Section } from "../lib/trainingContent.ts";
import DataTable from "./DataTable.tsx";
import SqlEditor from "./SqlEditor.tsx";

function ExerciseBlock({ section }: { section: Extract<Section, { type: "exercise" }> }) {
  const [sql, setSql] = useState(section.sql);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const sqlRef = useRef(sql);
  sqlRef.current = sql;

  const execute = useCallback(() => {
    setLoading(true);
    setError(null);
    runQuery(sqlRef.current)
      .then((r) => setResult(r))
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="border rounded-lg overflow-hidden bg-white dark:bg-zinc-900">
      <div className="px-4 py-3 bg-emerald-50 dark:bg-emerald-950/30 border-b border-emerald-200 dark:border-emerald-900/50">
        <h4 className="text-sm font-semibold text-emerald-800 dark:text-emerald-300">
          {section.title}
        </h4>
        <p className="text-xs text-emerald-700 dark:text-emerald-400 mt-1">
          {section.description}
        </p>
      </div>
      <div className="p-3">
        <SqlEditor value={sql} onChange={setSql} onRun={execute} />
        <div className="flex items-center gap-3 mt-2">
          <button
            onClick={execute}
            disabled={loading || !sql.trim()}
            className="px-4 py-1.5 bg-emerald-600 hover:bg-emerald-500 disabled:opacity-40 rounded-md text-sm font-medium text-white transition-colors"
          >
            {loading ? "Running..." : "Run"}
          </button>
          {result && (
            <span className="text-xs text-zinc-500 dark:text-zinc-400">
              {result.totalRows} rows
            </span>
          )}
        </div>
        {error && (
          <div className="mt-2 bg-red-50 dark:bg-red-950/50 border border-red-200 dark:border-red-900/50 rounded-lg p-3 text-sm text-red-600 dark:text-red-400 font-mono whitespace-pre-wrap">
            {error}
          </div>
        )}
        {result && (
          <div className="mt-2">
            <DataTable columns={result.columns} rows={result.rows} />
          </div>
        )}
        {section.hint && (
          <div className="mt-3 px-3 py-2 bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800/50 rounded-md text-xs text-amber-800 dark:text-amber-300">
            <span className="font-semibold">Hint: </span>
            {section.hint}
          </div>
        )}
      </div>
    </div>
  );
}

function DiagramBlock({ content }: { content: string }) {
  return (
    <div className="bg-zinc-50 dark:bg-zinc-900 border rounded-lg p-4 overflow-x-auto">
      <pre className="text-xs leading-relaxed text-zinc-700 dark:text-zinc-300 font-mono whitespace-pre">
        {content}
      </pre>
    </div>
  );
}

function parseMarkdown(content: string): React.ReactNode[] {
  const lines = content.split("\n");
  const elements: React.ReactNode[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    if (line.startsWith("## ")) {
      elements.push(
        <h2 key={`h2-${i}`} className="text-lg font-bold mt-6 mb-3 text-zinc-900 dark:text-zinc-100">
          {line.slice(3)}
        </h2>,
      );
      i++;
      continue;
    }

    if (line.startsWith("### ")) {
      elements.push(
        <h3 key={`h3-${i}`} className="text-base font-semibold mt-5 mb-2 text-zinc-800 dark:text-zinc-200">
          {line.slice(4)}
        </h3>,
      );
      i++;
      continue;
    }

    if (line.startsWith("```")) {
      const codeLines: string[] = [];
      i++;
      while (i < lines.length && !lines[i].startsWith("```")) {
        codeLines.push(lines[i]);
        i++;
      }
      elements.push(
        <pre
          key={`code-${i}`}
          className="bg-zinc-100 dark:bg-zinc-800 rounded-md p-3 text-xs font-mono overflow-x-auto my-3"
        >
          {codeLines.join("\n")}
        </pre>,
      );
      i++;
      continue;
    }

    if (line.startsWith("|") && lines[i + 1]?.match(/^\|[\s:.\-|]+$/)) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].startsWith("|")) {
        tableLines.push(lines[i]);
        i++;
      }
      const headerCells = tableLines[0]
        .split("|")
        .filter((c) => c.trim())
        .map((c) => c.trim());
      const bodyRows = tableLines.slice(2).map((row) =>
        row
          .split("|")
          .filter((c) => c.trim())
          .map((c) => c.trim()),
      );
      elements.push(
        <div key={`table-${i}`} className="overflow-x-auto my-3">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="border-b-2 border-zinc-300 dark:border-zinc-600">
                {headerCells.map((cell, ci) => (
                  <th
                    key={ci}
                    className="text-left py-2 px-3 font-semibold text-zinc-700 dark:text-zinc-300"
                  >
                    <InlineMarkdown text={cell} />
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {bodyRows.map((row, ri) => (
                <tr
                  key={ri}
                  className="border-b border-zinc-200 dark:border-zinc-700"
                >
                  {row.map((cell, ci) => (
                    <td
                      key={ci}
                      className="py-1.5 px-3 text-zinc-600 dark:text-zinc-400"
                    >
                      <InlineMarkdown text={cell} />
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>,
      );
      continue;
    }

    if (line.startsWith("- ") || line.match(/^\d+\. /)) {
      const ordered = !line.startsWith("- ");
      const pattern = ordered ? /^\d+\. / : /^- /;
      const listItems: string[] = [];
      while (i < lines.length && pattern.test(lines[i])) {
        listItems.push(ordered ? lines[i].replace(/^\d+\. /, "") : lines[i].slice(2));
        i++;
      }
      const Tag = ordered ? "ol" : "ul";
      elements.push(
        <Tag key={`list-${i}`} className={`${ordered ? "list-decimal" : "list-disc"} pl-5 my-2 space-y-1`}>
          {listItems.map((item, li) => (
            <li key={li} className="text-sm text-zinc-700 dark:text-zinc-300">
              <InlineMarkdown text={item} />
            </li>
          ))}
        </Tag>,
      );
      continue;
    }

    if (line.trim() === "") {
      i++;
      continue;
    }

    elements.push(
      <p key={`p-${i}`} className="text-sm text-zinc-700 dark:text-zinc-300 my-2 leading-relaxed">
        <InlineMarkdown text={line} />
      </p>,
    );
    i++;
  }

  return elements;
}

function MarkdownContent({ content }: { content: string }) {
  const elements = useMemo(() => parseMarkdown(content), [content]);
  return <>{elements}</>;
}

function InlineMarkdown({ text }: { text: string }) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return (
    <>
      {parts.map((part, i) => {
        if (part.startsWith("**") && part.endsWith("**")) {
          return (
            <strong key={i} className="font-semibold text-zinc-900 dark:text-zinc-100">
              {part.slice(2, -2)}
            </strong>
          );
        }
        if (part.startsWith("`") && part.endsWith("`")) {
          return (
            <code
              key={i}
              className="px-1 py-0.5 bg-zinc-100 dark:bg-zinc-800 rounded text-xs font-mono"
            >
              {part.slice(1, -1)}
            </code>
          );
        }
        return <span key={i}>{part}</span>;
      })}
    </>
  );
}

function LessonContent({ lesson }: { lesson: Lesson }) {
  return (
    <div className="space-y-4">
      <div className="mb-6">
        <h1 className="text-xl font-bold text-zinc-900 dark:text-zinc-100">
          {lesson.title}
        </h1>
        <p className="text-sm text-zinc-500 dark:text-zinc-400 mt-1">
          {lesson.subtitle}
        </p>
      </div>
      {lesson.sections.map((section, i) => {
        switch (section.type) {
          case "text":
            return <MarkdownContent key={`text-${i}`} content={section.content} />;
          case "diagram":
            return <DiagramBlock key={`dia-${i}`} content={section.content} />;
          case "exercise":
            return <ExerciseBlock key={`ex-${i}`} section={section} />;
          default:
            return section satisfies never;
        }
      })}
    </div>
  );
}

export default function Training() {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const lesson = LESSONS[selectedIndex];

  return (
    <div className="flex gap-4 h-full">
      <nav className="w-64 shrink-0 border rounded-lg bg-white dark:bg-zinc-900 overflow-y-auto">
        <div className="p-3 border-b">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-zinc-400 dark:text-zinc-500">
            Training
          </h2>
        </div>
        <ul className="p-1">
          {LESSONS.map((l, i) => (
            <li key={l.id}>
              <button
                onClick={() => setSelectedIndex(i)}
                className={cn(
                  "w-full text-left px-3 py-2 rounded-md transition-colors",
                  selectedIndex === i
                    ? "bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300"
                    : "text-zinc-600 dark:text-zinc-400 hover:bg-zinc-50 dark:hover:bg-zinc-800",
                )}
              >
                <span className="text-xs font-medium block truncate">
                  {l.title}
                </span>
                <span className="text-[10px] text-zinc-400 dark:text-zinc-500 block truncate mt-0.5">
                  {l.subtitle}
                </span>
              </button>
            </li>
          ))}
        </ul>
      </nav>

      <div className="flex-1 overflow-y-auto rounded-lg border bg-white dark:bg-zinc-950 p-6">
        <LessonContent lesson={lesson} />

        <div className="flex justify-between mt-8 pt-4 border-t">
          <button
            onClick={() => setSelectedIndex((i) => i - 1)}
            disabled={selectedIndex === 0}
            className="px-4 py-1.5 text-sm rounded-md border text-zinc-600 dark:text-zinc-400 hover:bg-zinc-50 dark:hover:bg-zinc-800 disabled:opacity-40 transition-colors"
          >
            Previous
          </button>
          <button
            onClick={() => setSelectedIndex((i) => i + 1)}
            disabled={selectedIndex === LESSONS.length - 1}
            className="px-4 py-1.5 text-sm rounded-md bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-40 transition-colors"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}
