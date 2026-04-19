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
      <div className="grid grid-cols-4 lg:grid-cols-6 gap-3">
        {images?.map((img) => (
          <button
            key={img.name}
            onClick={() => setSelected(img.name)}
            className="aspect-video bg-[var(--bg-card)] border border-[var(--border)] rounded-lg overflow-hidden hover:border-[var(--accent)] transition-colors"
          >
            <img
              src={`/images/thumbs/${img.name}`}
              alt={img.name}
              className="w-full h-full object-cover"
              loading="lazy"
            />
          </button>
        ))}
        {(!images || images.length === 0) && (
          <div className="col-span-full text-center text-[var(--text-secondary)] py-20">
            No camera images available
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
