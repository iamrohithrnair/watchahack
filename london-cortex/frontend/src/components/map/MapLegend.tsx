export function MapLegend() {
  return (
    <div
      className="absolute bottom-4 left-4 z-[1000] bg-[var(--bg-card)]/90 backdrop-blur-sm border border-[var(--border)] rounded-md p-2.5 text-[10px]"
      style={{ fontFamily: 'var(--font-mono)' }}
    >
      <div className="text-[9px] text-[var(--text-muted)] tracking-wider uppercase mb-1.5">Legend</div>
      <div className="space-y-1">
        <LegendRow color="#00e676" label="Discovery" />
        <LegendRow color="#ff1744" label="Critical" />
        <LegendRow color="#ffab00" label="Warning" />
        <LegendRow color="#448aff" label="Info" />
        <LegendRow color="#00e5c3" label="Fovea" />
        <LegendRow color="#3a3a58" label="Peripheral" />
      </div>
    </div>
  );
}

function LegendRow({ color, label }: { color: string; label: string }) {
  return (
    <div className="flex items-center gap-2">
      <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
      <span className="text-[var(--text-secondary)]">{label}</span>
    </div>
  );
}
