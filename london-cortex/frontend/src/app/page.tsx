"use client";

import dynamic from "next/dynamic";
import { StatsBar } from "@/components/stats/StatsBar";
import { LogStream } from "@/components/log-stream/LogStream";
import { MapLegend } from "@/components/map/MapLegend";

const LiveMap = dynamic(
  () => import("@/components/map/LiveMap").then((mod) => ({ default: mod.LiveMap })),
  { ssr: false }
);

export default function DashboardPage() {
  return (
    <div className="flex flex-col h-full">
      <StatsBar />
      <div className="flex flex-1 overflow-hidden">
        {/* Map panel */}
        <div className="flex-1 relative">
          <LiveMap />
          <MapLegend />
        </div>

        {/* Log stream panel */}
        <div
          className="w-[340px] shrink-0 flex flex-col"
          style={{
            borderLeft: '1px solid var(--border)',
            background: 'var(--bg-card)',
          }}
        >
          <div
            className="px-3 py-2 shrink-0 flex items-center gap-2"
            style={{
              borderBottom: '1px solid var(--border)',
              background: 'var(--bg-secondary)',
            }}
          >
            <svg viewBox="0 0 16 16" className="w-3.5 h-3.5" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" style={{ color: 'var(--text-muted)' }}>
              <line x1="2" y1="4" x2="14" y2="4" />
              <line x1="2" y1="8" x2="11" y2="8" />
              <line x1="2" y1="12" x2="8" y2="12" />
            </svg>
            <span
              className="text-[9px] tracking-[0.16em] uppercase font-semibold"
              style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
            >
              Live Feed
            </span>
          </div>
          <LogStream />
        </div>
      </div>
    </div>
  );
}
