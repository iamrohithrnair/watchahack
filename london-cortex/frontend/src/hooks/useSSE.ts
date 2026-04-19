"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { LogEntry } from "@/lib/types";

export function useSSE(url: string) {
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const bufferRef = useRef<LogEntry[]>([]);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const es = new EventSource(url);
    esRef.current = es;

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const entry: LogEntry = data.data || data;
        bufferRef.current = [...bufferRef.current.slice(-499), entry];
        setEntries([...bufferRef.current]);
      } catch {
        // ignore malformed messages
      }
    };

    es.onerror = () => {
      es.close();
    };

    return () => {
      es.close();
      esRef.current = null;
    };
  }, [url]);

  const clear = useCallback(() => {
    bufferRef.current = [];
    setEntries([]);
  }, []);

  return { entries, clear };
}
