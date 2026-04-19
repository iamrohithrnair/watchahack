import type { AgentMessage } from "@/lib/types";

const AGENT_PALETTE: Record<string, { color: string; bg: string }> = {
  brain:      { color: "#B8603A", bg: "rgba(184,96,58,0.09)" },
  chronicler: { color: "#8B6FAE", bg: "rgba(139,111,174,0.09)" },
  daemon:     { color: "#B85048", bg: "rgba(184,80,72,0.09)" },
  validator:  { color: "#4D7C58", bg: "rgba(77,124,88,0.09)" },
  curiosity:  { color: "#C09440", bg: "rgba(192,148,64,0.09)" },
  discovery:  { color: "#5B7A90", bg: "rgba(91,122,144,0.09)" },
};

function agentStyle(name: string) {
  return AGENT_PALETTE[name] || { color: "#9A8E86", bg: "rgba(154,142,134,0.09)" };
}

export function ChannelMessage({ msg }: { msg: AgentMessage }) {
  const { color, bg } = agentStyle(msg.from_agent);
  const initials = msg.from_agent.slice(0, 2).toUpperCase();

  return (
    <div
      className="flex gap-3 px-4 py-3 transition-colors duration-100"
      style={{ borderBottom: '1px solid var(--border)' }}
      onMouseEnter={(e) => (e.currentTarget.style.background = 'var(--bg-card-hover)')}
      onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
    >
      {/* Agent avatar */}
      <div
        className="shrink-0 w-8 h-8 rounded-[var(--radius-md)] flex items-center justify-center text-[10px] font-bold"
        style={{
          background: bg,
          color,
          border: `1px solid ${color}28`,
          fontFamily: 'var(--font-mono)',
          letterSpacing: '0.04em',
        }}
      >
        {initials}
      </div>

      <div className="flex-1 min-w-0">
        {/* Agent name + timestamp */}
        <div className="flex items-baseline gap-2 mb-0.5">
          <span
            className="text-[11px] font-semibold"
            style={{ color, fontFamily: 'var(--font-display)' }}
          >
            {msg.from_agent}
          </span>
          <span
            className="text-[9px]"
            style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
          >
            {new Date(msg.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
          </span>
        </div>

        {/* Message body */}
        <p
          className="text-[12px] leading-relaxed break-words"
          style={{ fontFamily: 'var(--font-body)', color: 'var(--text-secondary)' }}
        >
          {msg.content}
        </p>

        {/* Data type tag */}
        {typeof msg.data?.type === "string" && (
          <span
            className="inline-block mt-1.5 px-2 py-0.5 text-[8px] rounded-full"
            style={{
              fontFamily: 'var(--font-mono)',
              color: 'var(--text-muted)',
              background: 'var(--bg-secondary)',
              border: '1px solid var(--border)',
              letterSpacing: '0.06em',
            }}
          >
            {msg.data.type}
          </span>
        )}
      </div>
    </div>
  );
}
