export function EvidenceCard({ data }: { data: Record<string, unknown> }) {
  const source = (data.source as string) || "unknown";
  const content = (data.content as string) || (data.description as string) || "";
  const confidence = data.confidence as number | undefined;

  return (
    <div className="bg-[var(--bg-card)] border border-[var(--border)] rounded-lg p-3 text-sm">
      <div className="flex items-center justify-between mb-1">
        <span className="text-[var(--accent)] font-medium">{source}</span>
        {confidence !== undefined && (
          <span className="text-xs text-[var(--text-secondary)]">
            {Math.round(confidence * 100)}% confidence
          </span>
        )}
      </div>
      <p className="text-[var(--text-secondary)] text-xs line-clamp-3">{content}</p>
    </div>
  );
}
