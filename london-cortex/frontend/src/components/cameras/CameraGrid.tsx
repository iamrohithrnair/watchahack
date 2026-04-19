"use client";

import { useState } from "react";
import { useAPI } from "@/hooks/useAPI";
import { CameraLightbox } from "./CameraLightbox";
import type { ImageInfo } from "@/lib/types";

export function CameraGrid() {
  const { data: images } = useAPI<ImageInfo[]>("/api/images", 30000);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div className="h-full overflow-auto p-4">
      <div className="grid grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2">
        {images?.map((img) => (
          <button
            key={img.name}
            onClick={() => setSelected(img.name)}
            className="group aspect-video bg-[var(--bg-card)] border border-[var(--border)] rounded overflow-hidden hover:border-[var(--accent)]/40 transition-all duration-200"
          >
            <div className="relative w-full h-full">
              <img
                src={`/images/thumbs/${img.name}`}
                alt={img.name}
                className="w-full h-full object-cover opacity-70 group-hover:opacity-100 group-hover:scale-105 transition-all duration-300"
                loading="lazy"
              />
              <div className="absolute inset-0 bg-gradient-to-t from-[var(--bg-primary)] via-transparent to-transparent opacity-0 group-hover:opacity-60 transition-opacity" />
            </div>
          </button>
        ))}
        {(!images || images.length === 0) && (
          <div className="col-span-full flex flex-col items-center justify-center py-20 text-[var(--text-muted)] text-xs" style={{ fontFamily: 'var(--font-mono)' }}>
            No camera feeds available
          </div>
        )}
      </div>

      {selected && (
        <CameraLightbox
          name={selected}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
