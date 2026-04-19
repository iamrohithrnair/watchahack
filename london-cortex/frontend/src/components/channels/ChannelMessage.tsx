import type { AgentMessage } from "@/lib/types";

const AGENT_COLORS: Record<string, string> = {
  brain: "#00e5c3",
  chronicler: "#7c4dff",
  daemon: "#ff1744",
  validator: "#00e676",
  curiosity: "#ffab00",
  discovery: "#448aff",
};

export function ChannelMessage({ msg }: { msg: AgentMessage }) {
  const color = AGENT_COLORS[msg.from_agent] || "#5e5e80";

  return (
    <div className="flex gap-2.5 px-3 py-2 border-b border-[var(--border)] hover:bg-[var(--bg-card)] transition-colors duration-100">
      <div
        className="shrink-0 w-7 h-7 rounded flex items-center justify-center text-[9px] font-bold tracking-wider"
        style={{ backgroundColor: color + "15", color, fontFamily: 'var(--font-mono)' }}
      >
        {msg.from_agent.slice(0, 2).toUpperCase()}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="text-xs font-semibold" style={{ color, fontFamily: 'var(--font-display)' }}>
            {msg.from_agent}
          </span>
          <span className="text-[10px] text-[var(--text-muted)]" style={{ fontFamily: 'var(--font-mono)' }}>
            {new Date(msg.timestamp).toLocaleTimeString()}
          </span>
        </div>
        <p className="text-[13px] text-[var(--text-secondary)] leading-relaxed break-words" style={{ fontFamily: 'var(--font-body)' }}>
          {msg.content}
        </p>
        {typeof msg.data?.type === "string" && (
          <span
            className="inline-block mt-1 px-1.5 py-0.5 text-[9px] bg-[var(--bg-card)] border border-[var(--border)] rounded-sm text-[var(--text-muted)]"
            style={{ fontFamily: 'var(--font-mono)' }}
          >
            {msg.data.type}
          </span>
        )}
      </div>
    </div>
  );
}
