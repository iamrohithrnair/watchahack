export function EvidenceCard({ data }: { data: Record<string, unknown> }) {
  const source = (data.source as string) || "unknown";
  const content = (data.content as string) || (data.description as string) || "";
  const confidence = data.confidence as number | undefined;

  return (
    <div
      className="bg-[var(--bg-card)] border border-[var(--border)] rounded-md p-3 text-sm"
      style={{ animation: 'slide-in-right 0.2s ease-out' }}
    >
      <div className="flex items-center justify-between mb-1">
        <span className="text-[var(--accent)] text-[10px] font-semibold tracking-wider uppercase" style={{ fontFamily: 'var(--font-mono)' }}>
          {source}
        </span>
        {confidence !== undefined && (
          <span className="text-[10px] text-[var(--text-muted)] tabular-nums" style={{ fontFamily: 'var(--font-mono)' }}>
            {Math.round(confidence * 100)}%
          </span>
        )}
      </div>
      <p className="text-[var(--text-secondary)] text-xs leading-relaxed line-clamp-3" style={{ fontFamily: 'var(--font-body)' }}>
        {content}
      </p>
    </div>
  );
}
