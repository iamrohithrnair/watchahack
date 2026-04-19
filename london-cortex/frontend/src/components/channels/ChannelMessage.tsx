import type { AgentMessage } from "@/lib/types";

const AGENT_COLORS: Record<string, string> = {
  brain: "#6366f1",
  chronicler: "#8b5cf6",
  daemon: "#ef4444",
  validator: "#22c55e",
  curiosity: "#f59e0b",
  discovery: "#3b82f6",
};

export function ChannelMessage({ msg }: { msg: AgentMessage }) {
  const color = AGENT_COLORS[msg.from_agent] || "#888";

  return (
    <div className="flex gap-2 px-3 py-2 hover:bg-[var(--bg-card)] border-b border-[var(--border)]">
      <div
        className="shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold"
        style={{ backgroundColor: color + "30", color }}
      >
        {msg.from_agent.slice(0, 2).toUpperCase()}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-semibold text-sm" style={{ color }}>
            {msg.from_agent}
          </span>
          <span className="text-xs text-[var(--text-secondary)]">
            {new Date(msg.timestamp).toLocaleTimeString()}
          </span>
        </div>
        <p className="text-sm text-[var(--text-primary)] mt-0.5 break-words">
          {msg.content}
        </p>
        {typeof msg.data?.type === "string" && (
          <span className="inline-block mt-1 px-1.5 py-0.5 text-[10px] bg-[var(--bg-card)] rounded text-[var(--text-secondary)]">
            {msg.data.type}
          </span>
        )}
      </div>
    </div>
  );
}
