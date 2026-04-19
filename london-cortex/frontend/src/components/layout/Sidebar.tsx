"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_ITEMS = [
  { href: "/", label: "Map", icon: "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1" },
  { href: "/investigate", label: "Query", icon: "M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" },
  { href: "/channels", label: "Feed", icon: "M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z" },
  { href: "/cameras", label: "CCTV", icon: "M15 10l4.553-2.276A1 1 0 0121 8.618v6.764a1 1 0 01-1.447.894L15 14M5 18h8a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-[68px] bg-[var(--bg-card)] border-r border-[var(--border)] flex flex-col items-center pt-4 gap-1" style={{ boxShadow: 'var(--shadow-lg)' }}>
      {/* Logo mark */}
      <div className="mb-5 flex flex-col items-center">
        <div
          className="w-9 h-9 rounded-[var(--radius-md)] bg-[var(--accent)] flex items-center justify-center"
          style={{ boxShadow: '0 2px 8px rgba(99, 102, 241, 0.3)' }}
        >
          <svg viewBox="0 0 24 24" className="w-4 h-4 text-white" fill="none" stroke="currentColor" strokeWidth={2.5}>
            <circle cx="12" cy="12" r="3" />
            <path d="M12 1v2m0 18v2m-9-11h2m18 0h2m-3.636-6.364l-1.414 1.414M6.05 17.95l-1.414 1.414m0-13.086l1.414 1.414M17.95 17.95l1.414 1.414" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex flex-col gap-1 flex-1">
        {NAV_ITEMS.map((item) => {
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              title={item.label}
              className={`group relative flex flex-col items-center justify-center w-12 h-12 rounded-[var(--radius-md)] transition-all duration-200 ${
                active
                  ? "bg-[var(--accent-dim)] text-[var(--accent)]"
                  : "text-[var(--text-muted)] hover:text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)]"
              }`}
            >
              <svg
                viewBox="0 0 24 24"
                className="w-[18px] h-[18px]"
                fill="none"
                stroke="currentColor"
                strokeWidth={active ? 2 : 1.5}
                strokeLinecap="round"
                strokeLinejoin="round"
                dangerouslySetInnerHTML={{ __html: item.icon }}
              />
              <span
                className={`text-[9px] mt-0.5 tracking-wider uppercase font-medium ${
                  active ? 'font-semibold' : ''
                }`}
                style={{ fontFamily: 'var(--font-mono)' }}
              >
                {item.label}
              </span>
            </Link>
          );
        })}
      </nav>

      {/* Bottom status */}
      <div className="mb-5 flex flex-col items-center gap-1.5">
        <div
          className="w-2 h-2 rounded-full bg-[var(--success)]"
          style={{ boxShadow: '0 0 8px rgba(16, 185, 129, 0.4)', animation: 'pulse-glow 3s ease-in-out infinite' }}
        />
      </div>
    </aside>
  );
}
