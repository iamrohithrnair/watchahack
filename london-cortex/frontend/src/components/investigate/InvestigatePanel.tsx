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
      <div className="flex-1 overflow-auto p-4 space-y-4">
        {result.events.length === 0 && !result.loading && (
          <div className="text-center text-[var(--text-secondary)] mt-20">
            <p className="text-lg">Ask London Cortex a question</p>
            <p className="text-sm mt-1">
              e.g. &quot;What is happening near Canary Wharf?&quot;
            </p>
          </div>
        )}

        {result.events.map((evt, i) => {
          if (evt.event === "evidence") {
            return <EvidenceCard key={i} data={evt.data} />;
          }
          if (evt.event === "reasoning" || evt.event === "answer") {
            return (
              <div key={i} className="bg-[var(--bg-card)] rounded-lg p-3 text-sm">
                <div className="text-[var(--accent)] text-xs font-semibold mb-1 uppercase">
                  {evt.event}
                </div>
                <div className="whitespace-pre-wrap">
                  {(evt.data.text as string) || (evt.data.content as string) || JSON.stringify(evt.data)}
                </div>
              </div>
            );
          }
          return null;
        })}

        {result.loading && (
          <div className="text-center text-[var(--text-secondary)] animate-pulse">
            Investigating...
          </div>
        )}

        {result.error && (
          <div className="text-[var(--danger)] text-sm">{result.error}</div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <form onSubmit={handleSubmit} className="border-t border-[var(--border)] p-4 flex gap-2">
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder={result.threadId ? "Follow up..." : "Ask a question..."}
          className="flex-1 bg-[var(--bg-card)] border border-[var(--border)] rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-[var(--accent)]"
          disabled={result.loading}
        />
        {result.loading ? (
          <button
            type="button"
            onClick={cancel}
            className="px-4 py-2 bg-[var(--danger)] text-white rounded-lg text-sm"
          >
            Stop
          </button>
        ) : (
          <button
            type="submit"
            className="px-4 py-2 bg-[var(--accent)] text-white rounded-lg text-sm hover:bg-[var(--accent-hover)]"
            disabled={!question.trim()}
          >
            Ask
          </button>
        )}
      </form>
    </div>
  );
}
