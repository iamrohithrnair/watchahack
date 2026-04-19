export default function Loading() {
  return (
    <div className="flex h-full items-center justify-center">
      <div className="flex items-center gap-2 text-[var(--text-muted)] text-xs" style={{ fontFamily: 'var(--font-mono)' }}>
        <span className="w-2 h-2 rounded-full bg-[var(--accent)]" style={{ animation: 'pulse-glow 1.5s ease-in-out infinite' }} />
        Loading...
      </div>
    </div>
  );
}
