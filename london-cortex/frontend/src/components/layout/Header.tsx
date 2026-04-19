"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Stats } from "@/lib/types";

export function Header() {
  const { data: stats } = useAPI<Stats>("/api/stats", 10000);

  return (
    <header
      className="h-12 flex items-center px-6 justify-between shrink-0"
      style={{
        background: 'var(--bg-card)',
        borderBottom: '1px solid var(--border)',
        boxShadow: 'var(--shadow-sm)',
      }}
    >
      {/* Branding */}
      <div className="flex items-center gap-3">
        <div className="flex items-baseline gap-1">
          <span
            className="text-[15px] font-semibold"
            style={{ fontFamily: 'var(--font-body)', color: 'var(--accent)', letterSpacing: '-0.01em' }}
          >
            London
          </span>
          <span
            className="text-[15px] font-semibold"
            style={{ fontFamily: 'var(--font-body)', color: 'var(--text-secondary)', letterSpacing: '-0.01em' }}
          >
            Cortex
          </span>
        </div>
        <div
          className="h-4 w-px mx-0.5"
          style={{ background: 'var(--border)' }}
        />
        <span
          className="text-[10px] tracking-wider"
          style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
        >
          City Intelligence
        </span>
      </div>

      {/* Live stats */}
      <div className="flex items-center gap-1.5">
        <StatPill label="Obs" value={stats?.observations} color="var(--steel)" />
        <StatPill label="Anom" value={stats?.anomalies} color="var(--warning)" />
        <StatPill label="Conn" value={stats?.connections} color="var(--success)" />
        <StatPill label="Msg" value={stats?.messages} color="var(--accent)" />
      </div>
    </header>
  );
}

function StatPill({
  label,
  value,
  color,
}: {
  label: string;
  value?: number;
  color?: string;
}) {
  return (
    <div
      className="flex items-center gap-1.5 px-2.5 py-1 rounded-full"
      style={{
        background: color ? color + '12' : 'var(--bg-secondary)',
        border: `1px solid ${color ? color + '22' : 'var(--border)'}`,
      }}
    >
      <span
        className="text-[9px] tracking-widest uppercase"
        style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
      >
        {label}
      </span>
      <span
        className="text-[11px] font-semibold tabular-nums"
        style={{ fontFamily: 'var(--font-mono)', color: color || 'var(--text-secondary)' }}
      >
        {value?.toLocaleString() ?? '—'}
      </span>
    </div>
  );
}
