"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Stats } from "@/lib/types";

export function Header() {
  const { data: stats } = useAPI<Stats>("/api/stats", 10000);

  return (
    <header className="h-10 bg-[var(--bg-secondary)] border-b border-[var(--border)] flex items-center px-5 justify-between">
      <div className="flex items-center gap-3">
        <h1 className="text-sm font-bold tracking-wider uppercase" style={{ fontFamily: 'var(--font-display)', letterSpacing: '0.12em' }}>
          <span className="text-[var(--accent)]">LONDON</span>
          <span className="text-[var(--text-muted)] ml-1.5">CORTEX</span>
        </h1>
      </div>
      <div className="flex items-center gap-5" style={{ fontFamily: 'var(--font-mono)' }}>
        <StatBadge label="OBS" value={stats?.observations} />
        <StatBadge label="ANOM" value={stats?.anomalies} color="var(--warning)" />
        <StatBadge label="CONN" value={stats?.connections} />
        <StatBadge label="MSG" value={stats?.messages} />
      </div>
    </header>
  );
}

function StatBadge({ label, value, color }: { label: string; value?: number; color?: string }) {
  return (
    <span className="flex items-center gap-1.5 text-[10px] tracking-wider">
      <span className="text-[var(--text-muted)]">{label}</span>
      <span className="text-[11px] font-medium" style={{ color: color || 'var(--text-secondary)' }}>
        {value?.toLocaleString() ?? "—"}
      </span>
    </span>
  );
}
