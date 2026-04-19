export function MapLegend() {
  return (
    <div className="absolute bottom-4 left-4 z-[1000] bg-[var(--bg-card)] border border-[var(--border)] rounded-lg p-3 text-xs">
      <div className="font-semibold mb-1">Legend</div>
      <div className="space-y-1">
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#22c55e] inline-block" /> Discovery</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#ef4444] inline-block" /> High Severity</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#f59e0b] inline-block" /> Medium Severity</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#3b82f6] inline-block" /> Low Severity</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#6366f1] inline-block" /> Perifovea</div>
        <div className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-[#334155] inline-block" /> Peripheral</div>
      </div>
    </div>
  );
}
