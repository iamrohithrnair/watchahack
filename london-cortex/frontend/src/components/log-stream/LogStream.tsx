"use client";

import { useRef, useCallback, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useSSE } from "@/hooks/useSSE";
import { LogEntry } from "./LogEntry";

const FILTERS = [
  { key: "ALL",     label: "All" },
  { key: "INFO",    label: "Info" },
  { key: "WARNING", label: "Warn" },
  { key: "ERROR",   label: "Err" },
] as const;

export function LogStream() {
  const { entries, clear, connected } = useSSE("/stream");
  const parentRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll]   = useState(true);
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
    (acc, e) => { acc[e.level] = (acc[e.level] || 0) + 1; return acc; },
    {} as Record<string, number>
  );

  return (
    <div className="flex flex-col h-full" style={{ background: 'var(--bg-card)' }}>
      {/* Panel header */}
      <div
        className="flex items-center justify-between px-3 py-2 shrink-0"
        style={{
          background: 'var(--bg-secondary)',
          borderBottom: '1px solid var(--border)',
        }}
      >
        {/* Level filter pills */}
        <div className="flex items-center gap-1">
          {FILTERS.map(({ key, label }) => {
            const active = levelFilter === key;
            return (
              <button
                key={key}
                onClick={() => setLevelFilter(key)}
                className="cursor-pointer px-2 py-0.5 rounded-full text-[9px] transition-all duration-150"
                style={{
                  fontFamily: 'var(--font-mono)',
                  background: active ? 'var(--accent)' : 'transparent',
                  color: active ? '#fff' : 'var(--text-muted)',
                  border: active ? 'none' : '1px solid var(--border)',
                  fontWeight: active ? 600 : 400,
                  letterSpacing: '0.08em',
                }}
              >
                {label}
                {key !== "ALL" && counts[key] ? (
                  <span className="ml-1" style={{ opacity: 0.75 }}>{counts[key]}</span>
                ) : null}
              </button>
            );
          })}
        </div>

        {/* Controls */}
        <div
          className="flex items-center gap-2.5"
          style={{ fontFamily: 'var(--font-mono)', fontSize: 9 }}
        >
          {/* Connection dot */}
          <div
            className="w-1.5 h-1.5 rounded-full"
            style={{
              background: connected ? 'var(--success)' : 'var(--danger)',
              boxShadow: connected
                ? '0 0 6px rgba(77,124,88,0.5)'
                : '0 0 6px rgba(184,80,72,0.5)',
              animation: !connected ? 'pulse-glow 1.5s ease-in-out infinite' : undefined,
            }}
          />
          {/* Count */}
          <span style={{ color: 'var(--text-muted)', tabularNums: true } as React.CSSProperties}>
            {filtered.length.toLocaleString()}
          </span>
          {/* Clear */}
          <button
            onClick={clear}
            className="cursor-pointer px-1.5 py-0.5 rounded transition-colors duration-150"
            style={{ color: 'var(--text-muted)' }}
            onMouseEnter={(e) => (e.currentTarget.style.color = 'var(--danger)')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'var(--text-muted)')}
          >
            clr
          </button>
          {/* Auto-scroll toggle */}
          <button
            onClick={() => setAutoScroll(!autoScroll)}
            className="cursor-pointer px-1.5 py-0.5 rounded transition-colors duration-150"
            style={{
              color: autoScroll ? 'var(--accent)' : 'var(--text-muted)',
              background: autoScroll ? 'var(--accent-dim)' : 'transparent',
            }}
          >
            {autoScroll ? 'auto' : 'paused'}
          </button>
        </div>
      </div>

      {/* Virtualised log entries */}
      <div
        ref={parentRef}
        onScroll={handleScroll}
        className="flex-1 overflow-auto"
        style={{ background: 'var(--bg-card)' }}
      >
        {filtered.length === 0 ? (
          <div
            className="flex flex-col items-center justify-center h-full gap-2"
            style={{ color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}
          >
            <div
              className="w-1.5 h-1.5 rounded-full"
              style={{
                background: connected ? 'var(--success)' : 'var(--danger)',
                animation: 'pulse-glow 2s ease-in-out infinite',
              }}
            />
            <span className="text-[10px]">{connected ? 'Listening…' : 'Disconnected'}</span>
          </div>
        ) : (
          <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
            {virtualizer.getVirtualItems().map((item) => (
              <div
                key={item.key}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  transform: `translateY(${item.start}px)`,
                }}
              >
                <LogEntry entry={filtered[item.index]} />
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
