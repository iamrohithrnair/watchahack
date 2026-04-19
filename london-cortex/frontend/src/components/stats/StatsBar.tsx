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
    <div className="flex items-center gap-4 px-4 py-1.5 border-b border-[var(--border)] bg-[var(--bg-secondary)]" style={{ fontFamily: 'var(--font-mono)' }}>
      {anomalies && (
        <>
          <span className="text-[10px] tracking-wider text-[var(--text-muted)] uppercase">
            Anomalies
          </span>
          <span className="text-[12px] font-semibold text-[var(--danger)] tabular-nums">
            {anomalies.length}
          </span>
          <span className="text-[var(--border)]">|</span>
          {[5, 4, 3, 2, 1].map((s) => (
            <span key={s} className="flex items-center gap-1 text-[10px]">
              <span
                className="w-1.5 h-1.5 rounded-full"
                style={{ backgroundColor: SEVERITY_COLORS[s] }}
              />
              <span className="text-[var(--text-muted)] tabular-nums">
                {bySeverity?.[s] || 0}
              </span>
            </span>
          ))}
        </>
      )}
    </div>
  );
}
