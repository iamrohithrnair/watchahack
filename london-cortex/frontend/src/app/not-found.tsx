import Link from "next/link";

export default function NotFound() {
  return (
    <div
      className="flex h-full items-center justify-center"
      style={{ background: 'var(--bg-primary)' }}
    >
      <div className="text-center flex flex-col items-center gap-4" style={{ animation: 'fade-in 0.4s ease-out' }}>
        {/* Line-art map pin */}
        <svg viewBox="0 0 48 60" className="w-12 h-14" fill="none" stroke="currentColor" strokeWidth={1.25} strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--border)' }}>
          <path d="M24 4C13.5 4 5 12.5 5 23c0 14 19 33 19 33s19-19 19-33C43 12.5 34.5 4 24 4z" />
          <circle cx="24" cy="22" r="7" style={{ stroke: 'var(--accent)' }} />
          <line x1="24" y1="15" x2="24" y2="19" style={{ stroke: 'var(--accent)' }} />
          <line x1="24" y1="25" x2="24" y2="29" style={{ stroke: 'var(--accent)' }} />
          <line x1="17" y1="22" x2="21" y2="22" style={{ stroke: 'var(--accent)' }} />
          <line x1="27" y1="22" x2="31" y2="22" style={{ stroke: 'var(--accent)' }} />
        </svg>

        <div>
          <div
            className="text-6xl font-semibold italic mb-1"
            style={{ fontFamily: 'var(--font-display)', color: 'var(--accent)' }}
          >
            404
          </div>
          <div
            className="text-[11px] tracking-widest uppercase"
            style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
          >
            Route not found
          </div>
        </div>

        <div className="w-12 h-px" style={{ background: 'var(--border)' }} />

        <Link
          href="/"
          className="text-[11px] tracking-wider px-4 py-2 rounded-[var(--radius-md)] transition-all duration-150"
          style={{
            fontFamily: 'var(--font-mono)',
            color: 'var(--accent)',
            background: 'var(--accent-dim)',
            border: '1px solid var(--border-active)',
          }}
        >
          Return to map
        </Link>
      </div>
    </div>
  );
}
