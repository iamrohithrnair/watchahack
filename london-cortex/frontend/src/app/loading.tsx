export default function Loading() {
  return (
    <div
      className="flex h-full items-center justify-center"
      style={{ background: 'var(--bg-primary)' }}
    >
      <div className="flex flex-col items-center gap-4">
        {/* Compass ring spinner */}
        <div className="relative w-10 h-10">
          <svg
            viewBox="0 0 40 40"
            fill="none"
            stroke="currentColor"
            strokeWidth={1.25}
            className="w-10 h-10"
            style={{ color: 'var(--border)', position: 'absolute', inset: 0 }}
          >
            <circle cx="20" cy="20" r="16" />
            <circle cx="20" cy="20" r="8" />
          </svg>
          <svg
            viewBox="0 0 40 40"
            fill="none"
            stroke="currentColor"
            strokeWidth={1.5}
            strokeLinecap="round"
            className="w-10 h-10"
            style={{
              color: 'var(--accent)',
              position: 'absolute',
              inset: 0,
              animation: 'spin-slow 2s linear infinite',
            }}
          >
            <path d="M20 4 L20 10" />
            <path d="M36 20 L30 20" />
          </svg>
          {/* Centre dot */}
          <div
            className="absolute rounded-full"
            style={{
              width: 4,
              height: 4,
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              background: 'var(--accent)',
              animation: 'pulse-glow 1.5s ease-in-out infinite',
            }}
          />
        </div>

        {/* Dot bouncing loader */}
        <div className="flex items-center gap-1.5">
          {[0, 1, 2].map((i) => (
            <div
              key={i}
              className="rounded-full"
              style={{
                width: 5,
                height: 5,
                background: 'var(--accent)',
                animation: `dot-bounce 1.4s ease-in-out ${i * 0.16}s infinite`,
              }}
            />
          ))}
        </div>

        <span
          className="text-[10px] tracking-widest uppercase"
          style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
        >
          Loading
        </span>
      </div>
    </div>
  );
}
