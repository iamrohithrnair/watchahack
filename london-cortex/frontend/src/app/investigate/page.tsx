import { InvestigatePanel } from "@/components/investigate/InvestigatePanel";

export default function InvestigatePage() {
  return (
    <div className="h-full flex">
      {/* Left: investigation panel */}
      <div className="flex-1 max-w-2xl border-r" style={{ borderColor: 'var(--border)' }}>
        <InvestigatePanel />
      </div>

      {/* Right: context panel */}
      <div
        className="hidden lg:flex flex-col flex-1 p-8 gap-6"
        style={{ background: 'var(--bg-secondary)' }}
      >
        <div>
          <h2
            className="text-sm font-bold mb-1"
            style={{ fontFamily: 'var(--font-display)', color: 'var(--text-primary)' }}
          >
            How it works
          </h2>
          <p
            className="text-[12px] leading-relaxed"
            style={{ fontFamily: 'var(--font-body)', color: 'var(--text-muted)' }}
          >
            London Cortex analyses 63+ live data sources — traffic, air quality, weather, financial signals and more — and surfaces connections and anomalies across the city.
          </p>
        </div>

        <div className="space-y-3">
          {[
            { icon: "△", label: "Anomaly detection", desc: "Z-score deviations across 4,000 grid cells" },
            { icon: "◎", label: "Discovery synthesis", desc: "Cross-domain pattern recognition via LLM" },
            { icon: "◇", label: "Hypothesis testing", desc: "Self-directed investigation with backtesting" },
            { icon: "◁", label: "Live ingestors", desc: "63+ APIs updated every 5–3,600 seconds" },
          ].map(({ icon, label, desc }) => (
            <div
              key={label}
              className="flex gap-3 p-3 rounded-[var(--radius-md)]"
              style={{
                background: 'var(--bg-card)',
                border: '1px solid var(--border)',
                boxShadow: 'var(--shadow-sm)',
              }}
            >
              <span
                className="text-base shrink-0 mt-0.5"
                style={{ color: 'var(--accent)', fontFamily: 'var(--font-mono)' }}
              >
                {icon}
              </span>
              <div>
                <div
                  className="text-[11px] font-semibold mb-0.5"
                  style={{ fontFamily: 'var(--font-display)', color: 'var(--text-primary)' }}
                >
                  {label}
                </div>
                <div
                  className="text-[11px] leading-relaxed"
                  style={{ fontFamily: 'var(--font-body)', color: 'var(--text-muted)' }}
                >
                  {desc}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
