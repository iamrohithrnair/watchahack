export function EvidenceCard({ data }: { data: Record<string, unknown> }) {
  const source     = (data.source as string) || "unknown";
  const content    = (data.content as string) || (data.description as string) || "";
  const confidence = data.confidence as number | undefined;
  const pct        = confidence !== undefined ? Math.round(confidence * 100) : undefined;

  return (
    <div
      className="rounded-[var(--radius-md)] overflow-hidden"
      style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border)',
        boxShadow: 'var(--shadow-md)',
        animation: 'slide-right 0.25s ease-out',
      }}
    >
      {/* Coloured top stripe */}
      <div style={{ height: 3, background: 'var(--steel)', opacity: 0.7 }} />

      <div className="p-3.5">
        <div className="flex items-center justify-between mb-2">
          <span
            className="text-[9px] font-semibold tracking-widest uppercase px-2 py-0.5 rounded-full"
            style={{
              fontFamily: 'var(--font-mono)',
              color: 'var(--accent)',
              background: 'var(--accent-dim)',
            }}
          >
            {source}
          </span>
          {pct !== undefined && (
            <div className="flex items-center gap-2">
              {/* Confidence bar */}
              <div
                className="w-16 h-1 rounded-full overflow-hidden"
                style={{ background: 'var(--bg-secondary)' }}
              >
                <div
                  className="h-full rounded-full transition-all duration-500"
                  style={{
                    width: `${pct}%`,
                    background: pct > 70 ? 'var(--success)' : pct > 40 ? 'var(--warning)' : 'var(--danger)',
                  }}
                />
              </div>
              <span
                className="text-[10px] tabular-nums"
                style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
              >
                {pct}%
              </span>
            </div>
          )}
        </div>

        <p
          className="text-[12px] leading-relaxed line-clamp-3"
          style={{ fontFamily: 'var(--font-body)', color: 'var(--text-secondary)' }}
        >
          {content}
        </p>
      </div>
    </div>
  );
}
