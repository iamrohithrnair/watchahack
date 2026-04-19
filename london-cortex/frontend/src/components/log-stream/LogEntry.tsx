import { LEVEL_COLORS } from "@/lib/constants";
import type { LogEntry as LogEntryType } from "@/lib/types";

const LEVEL_BG: Record<string, string> = {
  ERROR:    "rgba(184,80,72,0.06)",
  CRITICAL: "rgba(140,56,48,0.08)",
  WARNING:  "rgba(192,148,64,0.05)",
};

const LEVEL_BORDER: Record<string, string> = {
  ERROR:    "#B85048",
  CRITICAL: "#8C3830",
};

export function LogEntry({ entry }: { entry: LogEntryType }) {
  const color = LEVEL_COLORS[entry.level] || "#9A8E86";
  const bg = LEVEL_BG[entry.level] || "transparent";
  const borderColor = LEVEL_BORDER[entry.level];

  return (
    <div
      className="flex gap-2 px-3 py-[3px] text-[11px] leading-relaxed transition-colors duration-100"
      style={{
        fontFamily: 'var(--font-mono)',
        background: bg,
        borderLeft: borderColor ? `2px solid ${borderColor}` : '2px solid transparent',
      }}
      onMouseEnter={(e) => {
        if (!bg || bg === 'transparent') {
          e.currentTarget.style.background = 'var(--bg-card-hover)';
        }
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = bg;
      }}
    >
      <span
        className="shrink-0 w-14 text-right tabular-nums"
        style={{ color: 'var(--text-muted)' }}
      >
        {entry.time}
      </span>
      <span
        className="shrink-0 w-[50px] font-semibold uppercase tracking-wider text-[9px]"
        style={{ color }}
      >
        {entry.level}
      </span>
      <span
        className="shrink-0 w-28 truncate text-[9px]"
        style={{ color: 'var(--accent)', opacity: 0.55 }}
      >
        {entry.name}
      </span>
      <span className="truncate" style={{ color: 'var(--text-secondary)' }}>
        {entry.msg}
      </span>
    </div>
  );
}
