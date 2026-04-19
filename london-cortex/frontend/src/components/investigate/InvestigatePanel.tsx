"use client";

import { useState, useRef, useEffect } from "react";
import { useInvestigate } from "@/hooks/useInvestigate";
import { EvidenceCard } from "./EvidenceCard";

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

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-auto p-5 space-y-3">
        {result.events.length === 0 && !result.loading && (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <div className="w-12 h-12 rounded-full border border-[var(--border)] flex items-center justify-center mb-4">
              <svg viewBox="0 0 24 24" className="w-5 h-5 text-[var(--accent)]" fill="none" stroke="currentColor" strokeWidth={1.5}>
                <path d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </div>
            <p className="text-[var(--text-secondary)] text-sm" style={{ fontFamily: 'var(--font-body)' }}>
              Ask London Cortex anything about the city
            </p>
            <p className="text-[var(--text-muted)] text-xs mt-1" style={{ fontFamily: 'var(--font-mono)' }}>
              e.g. &quot;What&apos;s happening near Canary Wharf?&quot;
            </p>
          </div>
        )}

        {result.events.map((evt, i) => {
          if (evt.event === "evidence") {
            return <EvidenceCard key={i} data={evt.data} />;
          }
          if (evt.event === "reasoning" || evt.event === "answer") {
            return (
              <div
                key={i}
                className="bg-[var(--bg-card)] border border-[var(--border)] rounded-md p-3"
                style={{ animation: 'fade-up 0.2s ease-out' }}
              >
                <div
                  className="text-[9px] font-semibold tracking-widest uppercase mb-1.5"
                  style={{ fontFamily: 'var(--font-mono)', color: evt.event === 'answer' ? 'var(--accent)' : 'var(--text-muted)' }}
                >
                  {evt.event}
                </div>
                <div className="text-sm text-[var(--text-primary)] whitespace-pre-wrap leading-relaxed" style={{ fontFamily: 'var(--font-body)' }}>
                  {(evt.data.text as string) || (evt.data.content as string) || JSON.stringify(evt.data)}
                </div>
              </div>
            );
          }
          return null;
        })}

        {result.loading && (
          <div className="flex items-center gap-2 text-[var(--text-muted)] text-xs py-2" style={{ fontFamily: 'var(--font-mono)' }}>
            <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent)]" style={{ animation: 'pulse-glow 1.5s ease-in-out infinite' }} />
            Investigating...
          </div>
        )}

        {result.error && (
          <div className="text-[var(--danger)] text-xs border border-[var(--danger)]/20 rounded-md p-2" style={{ fontFamily: 'var(--font-mono)' }}>
            {result.error}
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <form onSubmit={handleSubmit} className="border-t border-[var(--border)] p-3 flex gap-2">
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder={result.threadId ? "Follow up..." : "Ask about London..."}
          className="flex-1 bg-[var(--bg-card)] border border-[var(--border)] rounded-md px-3 py-2 text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
          style={{ fontFamily: 'var(--font-body)' }}
          disabled={result.loading}
        />
        {result.loading ? (
          <button
            type="button"
            onClick={cancel}
            className="px-3 py-2 bg-[var(--danger)]/10 text-[var(--danger)] border border-[var(--danger)]/20 rounded-md text-xs tracking-wider hover:bg-[var(--danger)]/20 transition-colors"
            style={{ fontFamily: 'var(--font-mono)' }}
          >
            STOP
          </button>
        ) : (
          <button
            type="submit"
            className="px-4 py-2 bg-[var(--accent)] text-[var(--bg-primary)] rounded-md text-xs font-semibold tracking-wider hover:bg-[var(--accent-hover)] transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
            style={{ fontFamily: 'var(--font-mono)' }}
            disabled={!question.trim()}
          >
            ASK
          </button>
        )}
      </form>
    </div>
  );
}
