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
        <div className="flex-1 relative">
          <LiveMap />
          <MapLegend />
        </div>
        <div className="w-96 border-l border-[var(--border)] flex flex-col">
          <LogStream />
        </div>
      </div>
    </div>
  );
}
