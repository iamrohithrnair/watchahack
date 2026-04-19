"use client";

import { useState } from "react";
import { useAPI } from "@/hooks/useAPI";
import { CHANNEL_LIST } from "@/lib/constants";
import { ChannelMessage } from "./ChannelMessage";
import type { ChannelData, AgentMessage } from "@/lib/types";

const CHANNEL_ICONS: Record<string, string> = {
  "#discoveries": "◎",
  "#hypotheses":  "◇",
  "#anomalies":   "△",
  "#requests":    "◁",
  "#meta":        "○",
};

export function ChannelView() {
  const [active, setActive] = useState<string>(CHANNEL_LIST[0]);
  const { data } = useAPI<ChannelData>("/api/channels", 5000);

  const messages = (data?.[active] || []) as AgentMessage[];

  return (
    <div className="flex h-full">
      {/* Channel list sidebar */}
      <div
        className="w-44 shrink-0 flex flex-col py-3"
        style={{
          background: 'var(--bg-secondary)',
          borderRight: '1px solid var(--border)',
        }}
      >
        <div
          className="px-4 pb-2 mb-1 text-[8px] font-bold tracking-[0.2em] uppercase"
          style={{
            fontFamily: 'var(--font-mono)',
            color: 'var(--text-muted)',
            borderBottom: '1px solid var(--border)',
            paddingBottom: 8,
            marginBottom: 6,
          }}
        >
          Channels
        </div>

        {CHANNEL_LIST.map((ch) => {
          const count = (data?.[ch] || []).length;
          const isActive = active === ch;
          const icon = CHANNEL_ICONS[ch] || "·";

          return (
            <button
              key={ch}
              onClick={() => setActive(ch)}
              className="cursor-pointer w-full px-3 py-2 text-[11px] text-left flex items-center justify-between transition-all duration-150"
              style={{
                fontFamily: 'var(--font-mono)',
                color: isActive ? 'var(--accent)' : 'var(--text-muted)',
                background: isActive ? 'var(--accent-dim)' : 'transparent',
                borderLeft: `2px solid ${isActive ? 'var(--accent)' : 'transparent'}`,
                fontWeight: isActive ? 600 : 400,
              }}
              onMouseEnter={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'var(--bg-card)';
                  e.currentTarget.style.color = 'var(--text-secondary)';
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'transparent';
                  e.currentTarget.style.color = 'var(--text-muted)';
                }
              }}
            >
              <div className="flex items-center gap-2 min-w-0">
                <span style={{ opacity: 0.6, fontSize: 10 }}>{icon}</span>
                <span className="truncate">{ch.replace('#', '')}</span>
              </div>
              {count > 0 && (
                <span
                  className="shrink-0 text-[9px] tabular-nums px-1.5 py-0.5 rounded-full"
                  style={{
                    background: isActive ? 'var(--accent-dim)' : 'var(--bg-card)',
                    color: isActive ? 'var(--accent)' : 'var(--text-muted)',
                    border: '1px solid var(--border)',
                  }}
                >
                  {count}
                </span>
              )}
            </button>
          );
        })}
      </div>

      {/* Messages area */}
      <div className="flex-1 overflow-auto" style={{ background: 'var(--bg-card)' }}>
        {messages.length === 0 ? (
          <div
            className="flex flex-col items-center justify-center h-full gap-3"
            style={{ color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}
          >
            <div
              className="w-10 h-10 rounded-full flex items-center justify-center"
              style={{ border: '1.5px dashed var(--border)' }}
            >
              <span style={{ fontSize: 16, opacity: 0.5 }}>{CHANNEL_ICONS[active] || '·'}</span>
            </div>
            <span className="text-[11px]">No messages in {active}</span>
          </div>
        ) : (
          messages.map((msg) => <ChannelMessage key={msg.id} msg={msg} />)
        )}
      </div>
    </div>
  );
}
