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
      <div className="w-48 border-r border-[var(--border)] flex flex-col">
        {CHANNEL_LIST.map((ch) => {
          const count = (data?.[ch] || []).length;
          return (
            <button
              key={ch}
              onClick={() => setActive(ch)}
              className={`px-3 py-2 text-sm text-left flex justify-between items-center ${
                active === ch
                  ? "bg-[var(--accent)] text-white"
                  : "hover:bg-[var(--bg-card)] text-[var(--text-secondary)]"
              }`}
            >
              <span>{ch}</span>
              {count > 0 && (
                <span className="text-xs opacity-70">{count}</span>
              )}
            </button>
          );
        })}
      </div>
      <div className="flex-1 overflow-auto">
        {messages.length === 0 ? (
          <div className="text-center text-[var(--text-secondary)] mt-20">
            No messages in {active}
          </div>
        ) : (
          messages.map((msg) => <ChannelMessage key={msg.id} msg={msg} />)
        )}
      </div>
    </div>
  );
}
