export function CameraLightbox({ name, onClose }: { name: string; onClose: () => void }) {
  return (
    <div
      className="fixed inset-0 z-50 bg-black/90 flex items-center justify-center"
      onClick={onClose}
    >
      <div className="relative max-w-4xl max-h-[80vh]" onClick={(e) => e.stopPropagation()}>
        <button
          onClick={onClose}
          className="absolute -top-2 -right-2 w-7 h-7 bg-[var(--bg-card)] border border-[var(--border)] rounded-full flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--accent)] transition-colors z-10 text-xs"
          style={{ fontFamily: 'var(--font-mono)' }}
        >
          ESC
        </button>
        <img
          src={`/images/full/${name}`}
          alt={name}
          className="max-w-full max-h-[80vh] rounded"
        />
        <div
          className="text-center text-[10px] text-[var(--text-muted)] mt-2 tracking-wider"
          style={{ fontFamily: 'var(--font-mono)' }}
        >
          {name}
        </div>
      </div>
    </div>
  );
}
