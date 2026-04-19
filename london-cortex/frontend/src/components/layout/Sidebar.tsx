"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_ITEMS = [
  {
    href: "/",
    label: "Map",
    icon: (
      <svg viewBox="0 0 24 24" className="w-5 h-5" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round">
        <polygon points="3 6 9 3 15 6 21 3 21 18 15 21 9 18 3 21" />
        <line x1="9" y1="3" x2="9" y2="18" />
        <line x1="15" y1="6" x2="15" y2="21" />
      </svg>
    ),
  },
  {
    href: "/investigate",
    label: "Query",
    icon: (
      <svg viewBox="0 0 24 24" className="w-5 h-5" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round">
        <circle cx="11" cy="11" r="8" />
        <path d="m21 21-4.35-4.35" />
        <path d="M11 8v3M11 14h.01" />
      </svg>
    ),
  },
  {
    href: "/channels",
    label: "Feed",
    icon: (
      <svg viewBox="0 0 24 24" className="w-5 h-5" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round">
        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
      </svg>
    ),
  },
  {
    href: "/cameras",
    label: "CCTV",
    icon: (
      <svg viewBox="0 0 24 24" className="w-5 h-5" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round">
        <path d="m15 10 4.553-2.276A1 1 0 0 1 21 8.618v6.764a1 1 0 0 1-1.447.894L15 14" />
        <rect x="2" y="7" width="13" height="10" rx="2" ry="2" />
      </svg>
    ),
  },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside
      className="fixed left-0 top-0 z-40 h-screen w-[64px] flex flex-col items-center pt-5 pb-5"
      style={{
        background: 'var(--bg-card)',
        borderRight: '1px solid var(--border)',
        boxShadow: 'var(--shadow-lg)',
      }}
    >
      {/* Compass-rose logo */}
      <div className="mb-6">
        <div
          className="w-9 h-9 rounded-[var(--radius-md)] flex items-center justify-center"
          style={{
            background: 'var(--accent)',
            boxShadow: '0 2px 10px rgba(184,96,58,0.30)',
          }}
        >
          <svg viewBox="0 0 24 24" className="w-5 h-5 text-white" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="8" opacity={0.4} />
            <circle cx="12" cy="12" r="4" opacity={0.65} />
            <circle cx="12" cy="12" r="1.5" fill="white" stroke="none" />
            <line x1="12" y1="4" x2="12" y2="8" />
            <line x1="12" y1="16" x2="12" y2="20" />
            <line x1="4" y1="12" x2="8" y2="12" />
            <line x1="16" y1="12" x2="20" y2="12" />
          </svg>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex flex-col gap-0.5 flex-1">
        {NAV_ITEMS.map((item) => {
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              title={item.label}
              className="group relative flex flex-col items-center justify-center w-11 h-11 rounded-[var(--radius-md)] transition-all duration-200 cursor-pointer"
              style={{
                color: active ? 'var(--accent)' : 'var(--text-muted)',
                background: active ? 'var(--accent-dim)' : 'transparent',
              }}
              onMouseEnter={(e) => {
                if (!active) {
                  e.currentTarget.style.background = 'var(--bg-card-hover)';
                  e.currentTarget.style.color = 'var(--text-secondary)';
                }
              }}
              onMouseLeave={(e) => {
                if (!active) {
                  e.currentTarget.style.background = 'transparent';
                  e.currentTarget.style.color = 'var(--text-muted)';
                }
              }}
            >
              {/* Active indicator */}
              {active && (
                <div
                  className="absolute left-0 top-1/2 -translate-y-1/2 w-[3px] h-5 rounded-r-full"
                  style={{ background: 'var(--accent)' }}
                />
              )}
              {item.icon}
              <span
                className="text-[8px] mt-0.5 tracking-wider uppercase font-medium"
                style={{
                  fontFamily: 'var(--font-mono)',
                  fontWeight: active ? 600 : 400,
                }}
              >
                {item.label}
              </span>
            </Link>
          );
        })}
      </nav>

      {/* Live status */}
      <div className="flex flex-col items-center gap-1">
        <div
          className="w-1.5 h-1.5 rounded-full"
          style={{
            background: 'var(--success)',
            boxShadow: '0 0 8px rgba(77,124,88,0.5)',
            animation: 'pulse-glow 3s ease-in-out infinite',
          }}
        />
        <span
          className="text-[7px] tracking-widest uppercase"
          style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
        >
          live
        </span>
      </div>
    </aside>
  );
}
