export function CameraLightbox({ name, onClose }: { name: string; onClose: () => void }) {
  return (
    <div
      className="fixed inset-0 z-50 bg-black/80 flex items-center justify-center"
      onClick={onClose}
    >
      <div className="max-w-4xl max-h-[80vh] relative" onClick={(e) => e.stopPropagation()}>
        <button
          onClick={onClose}
          className="absolute -top-3 -right-3 w-8 h-8 bg-[var(--bg-card)] border border-[var(--border)] rounded-full flex items-center justify-center text-[var(--text-secondary)] hover:text-white z-10"
        >
          x
        </button>
        <img
          src={`/images/full/${name}`}
          alt={name}
          className="max-w-full max-h-[80vh] rounded-lg"
        />
        <div className="text-center text-xs text-[var(--text-secondary)] mt-2">{name}</div>
      </div>
    </div>
  );
}
