"use client";

import { useState } from "react";
import { useAPI } from "@/hooks/useAPI";
import { CameraLightbox } from "./CameraLightbox";
import type { ImageInfo } from "@/lib/types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export function CameraGrid() {
  const { data: images } = useAPI<ImageInfo[]>("/api/images", 30000);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div
      className="h-full overflow-auto p-5"
      style={{ background: 'var(--bg-primary)' }}
    >
      {(!images || images.length === 0) ? (
        <div className="flex flex-col items-center justify-center h-full gap-3">
          <div
            className="w-14 h-14 rounded-[var(--radius-xl)] flex items-center justify-center"
            style={{ border: '1.5px dashed var(--border)', background: 'var(--bg-card)' }}
          >
            <svg viewBox="0 0 24 24" className="w-6 h-6" fill="none" stroke="currentColor" strokeWidth={1.25} strokeLinecap="round" strokeLinejoin="round" style={{ color: 'var(--text-muted)' }}>
              <path d="m15 10 4.553-2.276A1 1 0 0 1 21 8.618v6.764a1 1 0 0 1-1.447.894L15 14" />
              <rect x="2" y="7" width="13" height="10" rx="2" ry="2" />
            </svg>
          </div>
          <span
            className="text-[11px]"
            style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}
          >
            No camera feeds available
          </span>
        </div>
      ) : (
        <div className="grid grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
          {images.map((img) => (
            <button
              key={img.name}
              onClick={() => setSelected(img.name)}
              className="cursor-pointer group aspect-video overflow-hidden rounded-[var(--radius-md)] relative"
              style={{
                background: 'var(--bg-secondary)',
                border: '1px solid var(--border)',
                boxShadow: 'var(--shadow-sm)',
                transition: 'all 0.2s ease',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.borderColor = 'rgba(184,96,58,0.35)';
                e.currentTarget.style.boxShadow = 'var(--shadow-md)';
                e.currentTarget.style.transform = 'translateY(-1px)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.borderColor = 'var(--border)';
                e.currentTarget.style.boxShadow = 'var(--shadow-sm)';
                e.currentTarget.style.transform = 'translateY(0)';
              }}
            >
              <img
                src={`${API_BASE}/images/thumbs/${img.name}`}
                alt={img.name}
                loading="lazy"
                className="w-full h-full object-cover transition-all duration-300 group-hover:scale-105"
                style={{ opacity: 0.9, filter: 'sepia(0.08) saturate(0.9)' }}
              />
              {/* Camera name overlay on hover */}
              <div
                className="absolute inset-0 flex items-end p-2 opacity-0 group-hover:opacity-100 transition-opacity duration-200"
                style={{
                  background: 'linear-gradient(to top, rgba(44,36,32,0.55) 0%, transparent 60%)',
                }}
              >
                <span
                  className="text-[9px] text-white truncate leading-tight"
                  style={{ fontFamily: 'var(--font-mono)', textShadow: '0 1px 3px rgba(0,0,0,0.4)' }}
                >
                  {img.name.replace(/\.[^.]+$/, '').replace(/_/g, ' ')}
                </span>
              </div>
            </button>
          ))}
        </div>
      )}

      {selected && (
        <CameraLightbox name={selected} onClose={() => setSelected(null)} />
      )}
    </div>
  );
}
