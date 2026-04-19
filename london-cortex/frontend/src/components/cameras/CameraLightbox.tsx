"use client";

import { useEffect } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export function CameraLightbox({ name, onClose }: { name: string; onClose: () => void }) {
  /* Close on Escape */
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{
        background: 'rgba(44,36,32,0.55)',
        backdropFilter: 'blur(8px)',
        WebkitBackdropFilter: 'blur(8px)',
        animation: 'fade-in 0.2s ease-out',
      }}
      onClick={onClose}
    >
      <div
        className="relative overflow-hidden"
        style={{
          maxWidth: '90vw',
          maxHeight: '85vh',
          background: 'var(--bg-card)',
          border: '1px solid var(--border)',
          borderRadius: 'var(--radius-xl)',
          boxShadow: 'var(--shadow-xl)',
          animation: 'slide-up 0.25s ease-out',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Top bar */}
        <div
          className="flex items-center justify-between px-5 py-3"
          style={{ borderBottom: '1px solid var(--border)' }}
        >
          <span
            className="text-[10px] tracking-wider truncate"
            style={{
              fontFamily: 'var(--font-mono)',
              color: 'var(--text-muted)',
              maxWidth: 300,
            }}
          >
            {name.replace(/\.[^.]+$/, '').replace(/_/g, ' ')}
          </span>
          <button
            onClick={onClose}
            className="cursor-pointer w-7 h-7 rounded-full flex items-center justify-center transition-all duration-150 ml-4"
            style={{
              background: 'var(--bg-secondary)',
              border: '1px solid var(--border)',
              color: 'var(--text-muted)',
              fontSize: 14,
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = 'rgba(184,80,72,0.10)';
              e.currentTarget.style.color = 'var(--danger)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'var(--bg-secondary)';
              e.currentTarget.style.color = 'var(--text-muted)';
            }}
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        {/* Image */}
        <img
          src={`${API_BASE}/images/full/${name}`}
          alt={name}
          style={{ display: 'block', maxWidth: '100%', maxHeight: '72vh', objectFit: 'contain' }}
        />

        {/* Bottom hint */}
        <div
          className="text-center py-2"
          style={{
            fontFamily: 'var(--font-mono)',
            fontSize: 9,
            color: 'var(--text-muted)',
            letterSpacing: '0.1em',
            borderTop: '1px solid var(--border)',
          }}
        >
          Press ESC or click outside to close
        </div>
      </div>
    </div>
  );
}
