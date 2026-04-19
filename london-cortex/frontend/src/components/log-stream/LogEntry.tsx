import { LEVEL_COLORS } from "@/lib/constants";
import type { LogEntry as LogEntryType } from "@/lib/types";

export function LogEntry({ entry }: { entry: LogEntryType }) {
  const color = LEVEL_COLORS[entry.level] || "#888";

  return (
    <div className="flex gap-2 px-2 py-0.5 hover:bg-[var(--bg-card)] text-xs font-mono">
      <span className="text-[var(--text-secondary)] shrink-0 w-16">{entry.time}</span>
      <span className="shrink-0 w-14 font-semibold" style={{ color }}>
        {entry.level}
      </span>
      <span className="text-[var(--accent)] shrink-0 w-32 truncate">{entry.name}</span>
      <span className="truncate">{entry.msg}</span>
    </div>
  );
}
