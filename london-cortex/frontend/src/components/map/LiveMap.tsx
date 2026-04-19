"use client";

import { useEffect, useRef } from "react";
import L from "leaflet";
import { useAPI } from "@/hooks/useAPI";
import { LONDON_CENTER, LONDON_ZOOM } from "@/lib/constants";
import type { MapData } from "@/lib/types";

const SEVERITY_COLORS: Record<number, string> = {
  1: "#00e676",
  2: "#448aff",
  3: "#ffab00",
  4: "#ff6d00",
  5: "#ff1744",
};

export function LiveMap() {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<L.Map | null>(null);
  const markersRef = useRef<L.LayerGroup>(L.layerGroup());
  const { data } = useAPI<MapData>("/api/map_data", 5000);

  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return;

    const map = L.map(mapRef.current, {
      center: LONDON_CENTER,
      zoom: LONDON_ZOOM,
      zoomControl: false,
      attributionControl: false,
    });

    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
    }).addTo(map);

    // Subtle London boundary glow
    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
      opacity: 0.3,
    }).addTo(map);

    L.control.zoom({ position: "topright" }).addTo(map);
    markersRef.current.addTo(map);
    mapInstanceRef.current = map;

    return () => {
      map.remove();
      mapInstanceRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!data || !mapInstanceRef.current) return;

    markersRef.current.clearLayers();

    for (const a of data.anomalies) {
      if (!a.lat || !a.lon) continue;
      const color = SEVERITY_COLORS[a.severity] || "#448aff";
      const radius = 3 + a.severity * 1.5;
      L.circleMarker([a.lat, a.lon], {
        radius,
        fillColor: color,
        color: color,
        weight: 1,
        opacity: 0.9,
        fillOpacity: 0.5,
      })
        .bindPopup(`<strong>${a.source}</strong><br/><span style="opacity:0.7">${a.description}</span><br/><small>z=${a.z_score.toFixed(1)}</small>`)
        .addTo(markersRef.current);
    }

    for (const o of data.observations) {
      const zoneColor = o.zone === 2 ? "#00e5c3" : o.zone === 1 ? "#448aff" : "#1a1a3a";
      L.circleMarker([o.lat, o.lon], {
        radius: 1.5,
        fillColor: zoneColor,
        color: zoneColor,
        weight: 0,
        fillOpacity: 0.35,
      }).addTo(markersRef.current);
    }

    for (const d of data.discoveries) {
      L.circleMarker([d.lat, d.lon], {
        radius: 6,
        fillColor: "#00e676",
        color: "#00e676",
        weight: 2,
        fillOpacity: 0.2,
        opacity: 0.8,
      })
        .bindPopup(`<strong>${d.from_agent}</strong><br/><span style="opacity:0.7">${d.content.slice(0, 200)}</span>`)
        .addTo(markersRef.current);
    }
  }, [data]);

  return (
    <div ref={mapRef} className="h-full w-full" />
  );
}
