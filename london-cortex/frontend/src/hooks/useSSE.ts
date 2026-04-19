"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { LogEntry } from "@/lib/types";

export function useSSE(url: string) {
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const [connected, setConnected] = useState(false);
  const bufferRef = useRef<LogEntry[]>([]);
  const esRef = useRef<EventSource | null>(null);
  const reconnectRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    let alive = true;

    function connect() {
      if (!alive) return;
      const es = new EventSource(url);
      esRef.current = es;

      es.onopen = () => {
        if (alive) setConnected(true);
      };

      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          const entry: LogEntry = data.data || data;
          bufferRef.current = [...bufferRef.current.slice(-499), entry];
          if (alive) setEntries([...bufferRef.current]);
        } catch {
          // ignore malformed messages
        }
      };

      es.onerror = () => {
        if (alive) setConnected(false);
        es.close();
        // Reconnect after 3s
        reconnectRef.current = setTimeout(connect, 3000);
      };
    }

    connect();

    return () => {
      alive = false;
      clearTimeout(reconnectRef.current);
      esRef.current?.close();
      esRef.current = null;
    };
  }, [url]);

  const clear = useCallback(() => {
    bufferRef.current = [];
    setEntries([]);
  }, []);

  return { entries, clear, connected };
}
