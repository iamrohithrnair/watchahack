"use client";

import { useAPI } from "@/hooks/useAPI";
import type { Anomaly } from "@/lib/types";

const SEVERITY_COLORS: Record<number, string> = {
  1: "var(--severity-1)",
  2: "var(--severity-2)",
  3: "var(--severity-3)",
  4: "var(--severity-4)",
  5: "var(--severity-5)",
};

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
    <div
      className="flex items-center gap-4 px-5 py-2 border-b border-[var(--border)] bg-[var(--bg-card)]"
      style={{ fontFamily: 'var(--font-mono)', boxShadow: 'var(--shadow-sm)' }}
    >
      {anomalies && (
        <>
          <span className="text-[10px] tracking-wider text-[var(--text-muted)] uppercase font-medium">
            Anomalies
          </span>
          <span className="text-[13px] font-bold text-[var(--danger)] tabular-nums px-2 py-0.5 rounded-full bg-[var(--danger)]/10">
            {anomalies.length}
          </span>
          <span className="text-[var(--border)] mx-1">|</span>
          {[5, 4, 3, 2, 1].map((s) => (
            <span key={s} className="flex items-center gap-1.5 text-[10px]">
              <span
                className="w-2 h-2 rounded-full"
                style={{
                  backgroundColor: SEVERITY_COLORS[s],
                  boxShadow: `0 0 4px ${SEVERITY_COLORS[s]}40`,
                }}
              />
              <span className="text-[var(--text-secondary)] tabular-nums font-medium">
                {bySeverity?.[s] || 0}
              </span>
            </span>
          ))}
        </>
      )}
    </div>
  );
}
