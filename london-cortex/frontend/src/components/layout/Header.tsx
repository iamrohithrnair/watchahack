"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Stats } from "@/lib/types";

export function Header() {
  const { data: stats } = useAPI<Stats>("/api/stats", 10000);

  return (
    <header className="h-11 bg-[var(--bg-card)] border-b border-[var(--border)] flex items-center px-6 justify-between" style={{ boxShadow: 'var(--shadow-sm)' }}>
      <div className="flex items-center gap-3">
        <h1 className="text-sm font-bold tracking-wider uppercase" style={{ fontFamily: 'var(--font-display)', letterSpacing: '0.12em' }}>
          <span className="text-[var(--accent)]">LONDON</span>
          <span className="text-[var(--text-muted)] ml-1.5">CORTEX</span>
        </h1>
        <span className="text-[var(--border)] mx-2">|</span>
        <span className="text-[10px] text-[var(--text-muted)] tracking-wide" style={{ fontFamily: 'var(--font-mono)' }}>
          City Intelligence
        </span>
      </div>
      <div className="flex items-center gap-5" style={{ fontFamily: 'var(--font-mono)' }}>
        <StatBadge label="OBS" value={stats?.observations} color="var(--info)" />
        <StatBadge label="ANOM" value={stats?.anomalies} color="var(--warning)" />
        <StatBadge label="CONN" value={stats?.connections} color="var(--success)" />
        <StatBadge label="MSG" value={stats?.messages} color="var(--accent)" />
      </div>
    </header>
  );
}

function StatBadge({ label, value, color }: { label: string; value?: number; color?: string }) {
  return (
    <span className="flex items-center gap-2 text-[10px] tracking-wider">
      <span className="text-[var(--text-muted)]">{label}</span>
      <span
        className="text-[11px] font-semibold px-2 py-0.5 rounded-full"
        style={{
          color: color || 'var(--text-secondary)',
          backgroundColor: color ? color + '10' : 'transparent',
        }}
      >
        {value?.toLocaleString() ?? "—"}
      </span>
    </span>
  );
}
