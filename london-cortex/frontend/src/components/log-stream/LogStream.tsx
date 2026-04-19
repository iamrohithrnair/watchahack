"use client";

import { useRef, useCallback, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useSSE } from "@/hooks/useSSE";
import { LogEntry } from "./LogEntry";

export function LogStream() {
  const { entries, clear, connected } = useSSE("/stream");
  const parentRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [levelFilter, setLevelFilter] = useState<string>("ALL");

  const filtered = levelFilter === "ALL"
    ? entries
    : entries.filter((e) => e.level === levelFilter);

  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 22,
    overscan: 80,
  });

  const handleScroll = useCallback(() => {
    if (!parentRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = parentRef.current;
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 60);
  }, []);

  if (autoScroll && filtered.length > 0) {
    requestAnimationFrame(() => {
      virtualizer.scrollToIndex(filtered.length - 1, { align: "end" });
    });
  }

  const counts = entries.reduce(
    (acc, e) => {
      acc[e.level] = (acc[e.level] || 0) + 1;
      return acc;
    },
    {} as Record<string, number>
  );

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-1.5 border-b border-[var(--border)] bg-[var(--bg-secondary)]">
        <div className="flex gap-1">
          {[
            { key: "ALL", label: "ALL" },
            { key: "INFO", label: "INFO" },
            { key: "WARNING", label: "WARN" },
            { key: "ERROR", label: "ERR" },
          ].map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setLevelFilter(key)}
              className={`px-2 py-0.5 text-[9px] rounded-sm tracking-wider transition-all duration-150 ${
                levelFilter === key
                  ? "bg-[var(--accent)] text-[var(--bg-primary)] font-semibold"
                  : "text-[var(--text-muted)] hover:text-[var(--text-secondary)] hover:bg-[var(--bg-card)]"
              }`}
              style={{ fontFamily: 'var(--font-mono)' }}
            >
              {label}
              {key !== "ALL" && counts[key] ? (
                <span className="ml-1 opacity-60">{counts[key]}</span>
              ) : null}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-3 text-[9px] text-[var(--text-muted)]" style={{ fontFamily: 'var(--font-mono)' }}>
          <span className={`w-1.5 h-1.5 rounded-full ${connected ? "bg-[var(--accent)]" : "bg-[var(--danger)]"}`} style={!connected ? { animation: 'pulse-glow 1.5s ease-in-out infinite' } : undefined} />
          <span>{filtered.length}</span>
          <button
            onClick={clear}
            className="text-[var(--text-muted)] hover:text-[var(--danger)] transition-colors"
          >
            CLR
          </button>
          <button
            onClick={() => setAutoScroll(!autoScroll)}
            className={autoScroll ? "text-[var(--accent)]" : "text-[var(--text-muted)]"}
          >
            {autoScroll ? "AUTO" : "PAUSED"}
          </button>
        </div>
      </div>

      {/* Log entries */}
      <div ref={parentRef} onScroll={handleScroll} className="flex-1 overflow-auto">
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {virtualizer.getVirtualItems().map((item) => (
            <div
              key={item.key}
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                transform: `translateY(${item.start}px)`,
              }}
            >
              <LogEntry entry={filtered[item.index]} />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
