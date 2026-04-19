"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_ITEMS = [
  { href: "/", label: "Dashboard", icon: "◈" },
  { href: "/investigate", label: "Investigate", icon: "◎" },
  { href: "/channels", label: "Channels", icon: "◫" },
  { href: "/cameras", label: "Cameras", icon: "◧" },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-16 bg-[var(--bg-secondary)] border-r border-[var(--border)] flex flex-col items-center py-4 gap-2">
      <div className="mb-6 text-xl font-bold text-[var(--accent)]">LC</div>
      {NAV_ITEMS.map((item) => (
        <Link
          key={item.href}
          href={item.href}
          className={`flex flex-col items-center justify-center w-12 h-12 rounded-lg text-xs transition-colors ${
            pathname === item.href
              ? "bg-[var(--accent)] text-white"
              : "text-[var(--text-secondary)] hover:bg-[var(--bg-card)] hover:text-[var(--text-primary)]"
          }`}
          title={item.label}
        >
          <span className="text-lg">{item.icon}</span>
          <span className="mt-0.5 text-[10px]">{item.label}</span>
        </Link>
      ))}
    </aside>
  );
}
