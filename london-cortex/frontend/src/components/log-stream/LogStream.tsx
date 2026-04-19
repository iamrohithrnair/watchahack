"use client";

import { useRef, useCallback, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useSSE } from "@/hooks/useSSE";
import { LogEntry } from "./LogEntry";

export function LogStream() {
  const { entries, clear } = useSSE("/stream");
  const parentRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [levelFilter, setLevelFilter] = useState<string>("ALL");

  const filtered = levelFilter === "ALL"
    ? entries
    : entries.filter((e) => e.level === levelFilter);

  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 24,
    overscan: 50,
  });

  const handleScroll = useCallback(() => {
    if (!parentRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = parentRef.current;
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 50);
  }, []);

  if (autoScroll && filtered.length > 0) {
    requestAnimationFrame(() => {
      virtualizer.scrollToIndex(filtered.length - 1, { align: "end" });
    });
  }

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-2 py-1 border-b border-[var(--border)]">
        <div className="flex gap-1">
          {["ALL", "INFO", "WARNING", "ERROR"].map((level) => (
            <button
              key={level}
              onClick={() => setLevelFilter(level)}
              className={`px-2 py-0.5 text-xs rounded ${
                levelFilter === level
                  ? "bg-[var(--accent)] text-white"
                  : "bg-[var(--bg-card)] text-[var(--text-secondary)]"
              }`}
            >
              {level}
            </button>
          ))}
        </div>
        <div className="flex gap-2 text-xs text-[var(--text-secondary)]">
          <span>{filtered.length} lines</span>
          <button onClick={clear} className="hover:text-[var(--text-primary)]">Clear</button>
        </div>
      </div>
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
