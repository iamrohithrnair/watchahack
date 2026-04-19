"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Anomaly } from "@/lib/types";

const SEVERITY_META: Record<number, { color: string; label: string }> = {
  5: { color: "#B85048", label: "Critical" },
  4: { color: "#B8603A", label: "High" },
  3: { color: "#C09440", label: "Medium" },
  2: { color: "#5B7A90", label: "Low" },
  1: { color: "#4D7C58", label: "Info" },
};

export function StatsBar() {
  const { data: anomalies } = useAPI<Anomaly[]>("/api/anomalies", 3000);

  if (!anomalies) return null;

  const bySeverity = anomalies.reduce(
    (acc, a) => { acc[a.severity] = (acc[a.severity] || 0) + 1; return acc; },
    {} as Record<number, number>
  );

  const total = anomalies.length;

  return (
    <div
      className="flex items-center gap-4 px-5 py-1.5 shrink-0"
      style={{
        background: 'var(--bg-card)',
        borderBottom: '1px solid var(--border)',
        fontFamily: 'var(--font-mono)',
        boxShadow: 'var(--shadow-sm)',
      }}
    >
      <div className="flex items-center gap-2">
        <span className="text-[9px] tracking-widest uppercase" style={{ color: 'var(--text-muted)' }}>
          Anomalies
        </span>
        <span
          className="text-[12px] font-bold tabular-nums px-2 py-0.5 rounded-full"
          style={{
            color: total > 0 ? 'var(--danger)' : 'var(--text-muted)',
            background: total > 0 ? 'rgba(184,80,72,0.10)' : 'var(--bg-secondary)',
          }}
        >
          {total}
        </span>
      </div>

      <div
        className="h-3 w-px"
        style={{ background: 'var(--border)' }}
      />

      {[5, 4, 3, 2, 1].map((s) => {
        const meta = SEVERITY_META[s];
        const count = bySeverity[s] || 0;
        return (
          <div key={s} className="flex items-center gap-1.5" title={meta.label}>
            <div
              className="w-2 h-2 rounded-full shrink-0"
              style={{
                background: meta.color,
                boxShadow: count > 0 ? `0 0 5px ${meta.color}55` : 'none',
                opacity: count > 0 ? 1 : 0.3,
              }}
            />
            <span
              className="text-[10px] tabular-nums font-medium"
              style={{ color: count > 0 ? meta.color : 'var(--text-muted)' }}
            >
              {count}
            </span>
          </div>
        );
      })}
    </div>
  );
}
