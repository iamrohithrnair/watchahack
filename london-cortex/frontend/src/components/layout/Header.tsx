"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Stats } from "@/lib/types";

export function Header() {
  const { data: stats } = useAPI<Stats>("/api/stats", 10000);

  return (
    <header className="h-12 bg-[var(--bg-secondary)] border-b border-[var(--border)] flex items-center px-6 justify-between">
      <h1 className="text-lg font-semibold">London Cortex</h1>
      <div className="flex gap-6 text-sm text-[var(--text-secondary)]">
        <span title="Observations">
          ◈ {stats?.observations?.toLocaleString() ?? "—"}
        </span>
        <span title="Anomalies" className="text-[var(--warning)]">
          △ {stats?.anomalies?.toLocaleString() ?? "—"}
        </span>
        <span title="Connections">
          ⟷ {stats?.connections?.toLocaleString() ?? "—"}
        </span>
        <span title="Messages">
          ◫ {stats?.messages?.toLocaleString() ?? "—"}
        </span>
      </div>
    </header>
  );
}
