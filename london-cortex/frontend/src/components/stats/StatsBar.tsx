"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Anomaly } from "@/lib/types";

export function StatsBar() {
  const { data: anomalies } = useAPI<Anomaly[]>("/api/anomalies", 3000);

  const bySeverity = anomalies?.reduce(
    (acc, a) => {
      acc[a.severity] = (acc[a.severity] || 0) + 1;
      return acc;
    },
    {} as Record<number, number>
  );

  return (
    <div className="flex gap-3 p-2 border-b border-[var(--border)]">
      {anomalies && (
        <div className="flex items-center gap-2 text-xs">
          <span className="text-[var(--text-secondary)]">Active Anomalies:</span>
          <span className="text-[var(--danger)] font-semibold">{anomalies.length}</span>
          {[5, 4, 3, 2, 1].map((s) => (
            <span key={s} className="text-[var(--text-secondary)]">
              S{s}: <span className="font-medium text-[var(--text-primary)]">{bySeverity?.[s] || 0}</span>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
