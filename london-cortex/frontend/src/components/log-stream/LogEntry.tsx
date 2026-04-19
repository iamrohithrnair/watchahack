import { LEVEL_COLORS } from "@/lib/constants";
import type { LogEntry as LogEntryType } from "@/lib/types";

export function LogEntry({ entry }: { entry: LogEntryType }) {
  const color = LEVEL_COLORS[entry.level] || "#5e5e80";
  const isError = entry.level === "ERROR" || entry.level === "CRITICAL";
  const isWarning = entry.level === "WARNING";

  return (
    <div
      className={`flex gap-2 px-3 py-[3px] text-[11px] leading-relaxed transition-colors duration-100 ${
        isError
          ? "bg-[#ff174408] border-l-[2px] border-l-[var(--danger)]"
          : isWarning
            ? "bg-[#ffab0005] hover:bg-[#ffab0008]"
            : "hover:bg-[var(--bg-card)]"
      }`}
      style={{ fontFamily: 'var(--font-mono)' }}
    >
      <span className="text-[var(--text-muted)] shrink-0 w-14 text-right tabular-nums">
        {entry.time}
      </span>
      <span
        className="shrink-0 w-[52px] font-semibold uppercase tracking-wider text-[9px]"
        style={{ color }}
      >
        {entry.level}
      </span>
      <span className="text-[var(--accent)] shrink-0 w-28 truncate opacity-60">
        {entry.name}
      </span>
      <span className="text-[var(--text-secondary)] truncate">{entry.msg}</span>
    </div>
  );
}
