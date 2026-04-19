"use client";

import { useCallback, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export interface InvestigationResult {
  events: { event: string; data: Record<string, unknown> }[];
  threadId: string | null;
  loading: boolean;
  error: string | null;
}

export function useInvestigate() {
  const [result, setResult] = useState<InvestigationResult>({
    events: [],
    threadId: null,
    loading: false,
    error: null,
  });
  const abortRef = useRef<AbortController | null>(null);

  const investigate = useCallback(async (question: string, threadId?: string) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setResult({ events: [], threadId: null, loading: true, error: null });

    try {
      const params = new URLSearchParams({ q: question });
      if (threadId) params.set("thread_id", threadId);
      const url = `${API_BASE}/api/investigate?${params}`;

      const res = await fetch(url, { signal: controller.signal });
      if (!res.ok || !res.body) throw new Error(`Investigation failed: ${res.status}`);

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      const newEvents: InvestigationResult["events"] = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        let currentEvent = "";
        for (const line of lines) {
          if (line.startsWith("event: ")) {
            currentEvent = line.slice(7).trim();
          } else if (line.startsWith("data: ") && currentEvent) {
            try {
              const data = JSON.parse(line.slice(6));
              newEvents.push({ event: currentEvent, data });
              setResult((prev) => ({ ...prev, events: [...newEvents] }));
            } catch {
              // skip malformed
            }
            currentEvent = "";
          }
        }
      }

      const doneEvent = newEvents.find((e) => e.event === "done");
      setResult({
        events: newEvents,
        threadId: (doneEvent?.data?.thread_id as string) || null,
        loading: false,
        error: null,
      });
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        setResult((prev) => ({
          ...prev,
          loading: false,
          error: (err as Error).message,
        }));
      }
    }
  }, []);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    setResult((prev) => ({ ...prev, loading: false }));
  }, []);

  return { result, investigate, cancel };
}
