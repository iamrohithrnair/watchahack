"use client";

import { useState } from "react";
import { useAPI } from "@/hooks/useAPI";
import { CHANNEL_LIST } from "@/lib/constants";
import { ChannelMessage } from "./ChannelMessage";
import type { ChannelData, AgentMessage } from "@/lib/types";

export function ChannelView() {
  const [active, setActive] = useState<string>(CHANNEL_LIST[0]);
  const { data } = useAPI<ChannelData>("/api/channels", 5000);

  const messages = (data?.[active] || []) as AgentMessage[];

  return (
    <div className="flex h-full">
      <div className="w-40 border-r border-[var(--border)] bg-[var(--bg-secondary)] flex flex-col">
        {CHANNEL_LIST.map((ch) => {
          const count = (data?.[ch] || []).length;
          const isActive = active === ch;
          return (
            <button
              key={ch}
              onClick={() => setActive(ch)}
              className={`px-3 py-2 text-[11px] text-left flex justify-between items-center transition-all duration-150 ${
                isActive
                  ? "bg-[var(--accent)]/10 text-[var(--accent)] border-l-2 border-l-[var(--accent)]"
                  : "text-[var(--text-muted)] hover:text-[var(--text-secondary)] hover:bg-[var(--bg-card)] border-l-2 border-l-transparent"
              }`}
              style={{ fontFamily: 'var(--font-mono)' }}
            >
              <span className="truncate">{ch}</span>
              {count > 0 && (
                <span className={`text-[9px] tabular-nums ${isActive ? 'text-[var(--accent)]' : 'text-[var(--text-muted)]'}`}>
                  {count}
                </span>
              )}
            </button>
          );
        })}
      </div>
      <div className="flex-1 overflow-auto">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-[var(--text-muted)] text-xs" style={{ fontFamily: 'var(--font-mono)' }}>
            No messages in {active}
          </div>
        ) : (
          messages.map((msg) => <ChannelMessage key={msg.id} msg={msg} />)
        )}
      </div>
    </div>
  );
}
