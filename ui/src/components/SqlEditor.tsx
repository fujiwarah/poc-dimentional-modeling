import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { keymap } from "@codemirror/view";
import { useMemo } from "react";
import { useTheme } from "../hooks/useTheme.ts";

interface Props {
  value: string;
  onChange: (value: string) => void;
  onRun: () => void;
}

export default function SqlEditor({ value, onChange, onRun }: Props) {
  const { theme } = useTheme();

  const runKeymap = useMemo(
    () =>
      keymap.of([
        {
          key: "Mod-Enter",
          run: () => {
            onRun();
            return true;
          },
        },
      ]),
    [onRun],
  );

  const extensions = useMemo(
    () => [sql(), runKeymap],
    [runKeymap],
  );

  return (
    <div className="relative border rounded-lg overflow-hidden">
      <CodeMirror
        value={value}
        onChange={onChange}
        extensions={extensions}
        theme={theme === "dark" ? oneDark : undefined}
        height="192px"
        basicSetup={{
          lineNumbers: true,
          foldGutter: true,
          highlightActiveLine: true,
          bracketMatching: true,
          autocompletion: true,
        }}
        className="text-sm"
      />
      <div className="absolute bottom-2 right-3 text-xs text-zinc-400 dark:text-zinc-600 pointer-events-none">
        ⌘ + Enter
      </div>
    </div>
  );
}
