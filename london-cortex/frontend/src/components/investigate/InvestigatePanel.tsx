"use client";

import { useState, useRef, useEffect } from "react";
import { useInvestigate } from "@/hooks/useInvestigate";
import { EvidenceCard } from "./EvidenceCard";

const SUGGESTIONS = [
  "What's happening near Canary Wharf?",
  "Any unusual air quality readings?",
  "TfL disruptions in the last hour?",
];

export function InvestigatePanel() {
  const [question, setQuestion] = useState("");
  const { result, investigate, cancel } = useInvestigate();
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [result.events]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!question.trim()) return;
    investigate(question.trim(), result.threadId || undefined);
    setQuestion("");
  };

  const handleSuggestion = (s: string) => {
    investigate(s, result.threadId || undefined);
  };

  const isEmpty = result.events.length === 0 && !result.loading;

  return (
    <div className="flex flex-col h-full">
      {/* Messages area */}
      <div className="flex-1 overflow-auto p-5 space-y-3">
        {isEmpty && (
          <div
            className="flex flex-col items-center justify-center h-full text-center"
            style={{ animation: 'fade-in 0.4s ease-out' }}
          >
            {/* Compass line-art icon */}
            <div
              className="w-14 h-14 rounded-[var(--radius-xl)] flex items-center justify-center mb-4"
              style={{
                border: '1.5px dashed var(--border)',
                background: 'var(--accent-dim)',
              }}
            >
              <svg viewBox="0 0 24 24" className="w-6 h-6" fill="none" stroke="currentColor" strokeWidth={1.25} strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--accent)' }}>
                <circle cx="12" cy="12" r="9" opacity={0.3} />
                <circle cx="12" cy="12" r="4" opacity={0.5} />
                <circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none" />
                <line x1="12" y1="3" x2="12" y2="8" />
                <line x1="12" y1="16" x2="12" y2="21" />
                <line x1="3" y1="12" x2="8" y2="12" />
                <line x1="16" y1="12" x2="20" y2="12" />
              </svg>
            </div>

            <p
              className="text-sm font-semibold mb-1"
              style={{ fontFamily: 'var(--font-display)', color: 'var(--text-primary)' }}
            >
              Ask London Cortex
            </p>
            <p
              className="text-[11px] mb-5"
              style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
            >
              Intelligence at your command
            </p>

            {/* Suggestion chips */}
            <div className="flex flex-col gap-2 w-full max-w-xs">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => handleSuggestion(s)}
                  className="cursor-pointer w-full text-left px-3 py-2 rounded-[var(--radius-md)] text-[11px] transition-all duration-150"
                  style={{
                    fontFamily: 'var(--font-body)',
                    background: 'var(--bg-card)',
                    border: '1px solid var(--border)',
                    color: 'var(--text-secondary)',
                    boxShadow: 'var(--shadow-sm)',
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.borderColor = 'var(--border-active)';
                    e.currentTarget.style.color = 'var(--accent)';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.borderColor = 'var(--border)';
                    e.currentTarget.style.color = 'var(--text-secondary)';
                  }}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {result.events.map((evt, i) => {
          // Final synthesis — the main answer
          if (evt.event === "synthesis") {
            const summary = (evt.data.summary as string) || JSON.stringify(evt.data);
            return (
              <div
                key={i}
                className="rounded-[var(--radius-md)] p-4"
                style={{
                  background: 'var(--bg-card)',
                  border: '1px solid var(--border-active)',
                  borderLeft: '3px solid var(--accent)',
                  boxShadow: 'var(--shadow-md)',
                  animation: 'slide-up 0.25s ease-out',
                }}
              >
                <div
                  className="text-[8px] font-bold tracking-[0.2em] uppercase mb-2"
                  style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent)' }}
                >
                  ◈ Analysis
                </div>
                <div
                  className="text-[13px] whitespace-pre-wrap leading-relaxed"
                  style={{ fontFamily: 'var(--font-body)', color: 'var(--text-primary)' }}
                >
                  {summary}
                </div>
              </div>
            );
          }

          // Step results — show as compact evidence cards
          if (evt.event === "step_result") {
            const result = evt.data.result as Record<string, unknown>;
            const type = result?.type as string;
            const count = result?.count as number;
            const items = result?.items as unknown[] | undefined;
            if (!count || count === 0) return null;
            return (
              <div
                key={i}
                className="rounded-[var(--radius-md)] px-3 py-2"
                style={{
                  background: 'var(--bg-secondary)',
                  border: '1px solid var(--border)',
                  animation: 'fade-in 0.2s ease-out',
                }}
              >
                <div
                  className="text-[8px] tracking-[0.15em] uppercase mb-1"
                  style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
                >
                  {type} · {count} result{count !== 1 ? 's' : ''}
                </div>
                {items?.slice(0, 3).map((item: unknown, j: number) => {
                  const it = item as Record<string, unknown>;
                  return (
                    <div key={j} className="text-[11px] leading-snug truncate" style={{ color: 'var(--text-secondary)', fontFamily: 'var(--font-body)' }}>
                      {(it.description as string) || (it.content as string) || (it.text as string) || ''}
                    </div>
                  );
                })}
              </div>
            );
          }

          // Status messages — only show non-trivial ones while loading
          if (evt.event === "status") {
            const phase = evt.data.phase as string;
            const message = evt.data.message as string;
            if (phase === "synthesizing") {
              return (
                <div key={i} className="text-[10px] px-1" style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', animation: 'fade-in 0.2s' }}>
                  · {message}
                </div>
              );
            }
            return null;
          }

          return null;
        })}

        {result.loading && (
          <div
            className="flex items-center gap-2 py-2 px-1"
            style={{ animation: 'fade-in 0.3s ease-out' }}
          >
            {[0, 1, 2].map((i) => (
              <div
                key={i}
                className="w-1.5 h-1.5 rounded-full"
                style={{
                  background: 'var(--accent)',
                  animation: `dot-bounce 1.4s ease-in-out ${i * 0.16}s infinite`,
                }}
              />
            ))}
            <span
              className="text-[11px] ml-1"
              style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
            >
              Investigating…
            </span>
          </div>
        )}

        {result.error && (
          <div
            className="text-[11px] rounded-[var(--radius-md)] p-3"
            style={{
              fontFamily: 'var(--font-mono)',
              color: 'var(--danger)',
              background: 'rgba(184,80,72,0.07)',
              border: '1px solid rgba(184,80,72,0.18)',
            }}
          >
            {result.error}
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input bar */}
      <form
        onSubmit={handleSubmit}
        className="flex gap-2 p-4 shrink-0"
        style={{
          borderTop: '1px solid var(--border)',
          background: 'var(--bg-card)',
        }}
      >
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder={result.threadId ? "Follow up…" : "Ask about London…"}
          disabled={result.loading}
          className="flex-1 px-4 py-2.5 text-sm transition-all duration-150"
          style={{
            fontFamily: 'var(--font-body)',
            background: 'var(--bg-primary)',
            border: '1px solid var(--border)',
            borderRadius: 'var(--radius-md)',
            color: 'var(--text-primary)',
            outline: 'none',
          }}
          onFocus={(e) => (e.target.style.borderColor = 'var(--border-active)')}
          onBlur={(e) => (e.target.style.borderColor = 'var(--border)')}
        />
        {result.loading ? (
          <button
            type="button"
            onClick={cancel}
            className="cursor-pointer px-4 py-2.5 rounded-[var(--radius-md)] text-[11px] font-semibold tracking-wider transition-all duration-150"
            style={{
              fontFamily: 'var(--font-mono)',
              background: 'rgba(184,80,72,0.10)',
              color: 'var(--danger)',
              border: '1px solid rgba(184,80,72,0.20)',
            }}
          >
            Stop
          </button>
        ) : (
          <button
            type="submit"
            disabled={!question.trim()}
            className="cursor-pointer px-5 py-2.5 rounded-[var(--radius-md)] text-[11px] font-semibold tracking-wider transition-all duration-150 disabled:opacity-40 disabled:cursor-not-allowed"
            style={{
              fontFamily: 'var(--font-mono)',
              background: 'var(--accent)',
              color: '#fff',
              boxShadow: '0 2px 8px rgba(184,96,58,0.28)',
              border: 'none',
            }}
            onMouseEnter={(e) => {
              if (!e.currentTarget.disabled) e.currentTarget.style.background = 'var(--accent-hover)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'var(--accent)';
            }}
          >
            Ask
          </button>
        )}
      </form>
    </div>
  );
}
