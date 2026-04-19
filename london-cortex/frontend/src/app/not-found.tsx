export default function NotFound() {
  return (
    <div className="flex h-full items-center justify-center">
      <div className="text-center">
        <div className="text-6xl font-bold text-[var(--accent)] mb-2" style={{ fontFamily: 'var(--font-display)' }}>
          404
        </div>
        <div className="text-sm text-[var(--text-muted)]" style={{ fontFamily: 'var(--font-mono)' }}>
          Route not found
        </div>
      </div>
    </div>
  );
}
