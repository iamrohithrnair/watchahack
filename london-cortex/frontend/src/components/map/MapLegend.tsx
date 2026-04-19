export function MapLegend() {
  return (
    <div
      className="absolute bottom-5 left-5 z-[1000] glass rounded-[var(--radius-lg)] p-3.5"
      style={{ boxShadow: 'var(--shadow-lg)', animation: 'fade-in 0.4s ease-out' }}
    >
      <div
        className="text-[8px] tracking-[0.18em] uppercase font-semibold mb-2.5"
        style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
      >
        Legend
      </div>
      <div className="space-y-1.5">
        <Row color="#4D7C58" label="Discovery" />
        <Row color="#B85048" label="Critical (S5)" />
        <Row color="#B8603A" label="High (S4)" />
        <Row color="#C09440" label="Medium (S3)" />
        <Row color="#5B7A90" label="Low (S2)" />
        <Row color="#4D7C58" label="Info (S1)" />
        <div className="h-px my-1.5" style={{ background: 'var(--border)' }} />
        <Row color="#B8603A" label="Focus zone" dot />
        <Row color="#647D8E" label="Awareness zone" dot />
        <Row color="#DDD7CC" label="Background" dot />
      </div>
    </div>
  );
}

function Row({ color, label, dot }: { color: string; label: string; dot?: boolean }) {
  return (
    <div className="flex items-center gap-2.5">
      <div
        className="shrink-0"
        style={{
          width: dot ? 8 : 10,
          height: dot ? 8 : 10,
          borderRadius: '50%',
          background: color,
          opacity: dot ? 0.6 : 1,
          boxShadow: dot ? 'none' : `0 0 5px ${color}45`,
          border: dot ? `1.5px solid ${color}` : 'none',
        }}
      />
      <span
        className="text-[10px]"
        style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}
      >
        {label}
      </span>
    </div>
  );
}
