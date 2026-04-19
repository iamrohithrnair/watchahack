"use client";

import { useEffect, useRef } from "react";
import L from "leaflet";
import { useAPI } from "@/hooks/useAPI";
import { LONDON_CENTER, LONDON_ZOOM, SEVERITY_COLORS } from "@/lib/constants";
import type { MapData } from "@/lib/types";

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

    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
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
      const color = SEVERITY_COLORS[a.severity] || "#3b82f6";
      L.circleMarker([a.lat, a.lon], {
        radius: 4 + a.severity * 2,
        fillColor: color,
        color: color,
        weight: 1,
        opacity: 0.8,
        fillOpacity: 0.6,
      })
        .bindPopup(`<strong>${a.source}</strong><br/>${a.description}<br/>z=${a.z_score.toFixed(1)}`)
        .addTo(markersRef.current);
    }

    for (const o of data.observations) {
      const zoneColor = o.zone === 2 ? "#f59e0b" : o.zone === 1 ? "#6366f1" : "#334155";
      L.circleMarker([o.lat, o.lon], {
        radius: 2,
        fillColor: zoneColor,
        color: zoneColor,
        weight: 0,
        fillOpacity: 0.4,
      }).addTo(markersRef.current);
    }

    for (const d of data.discoveries) {
      L.circleMarker([d.lat, d.lon], {
        radius: 8,
        fillColor: "#22c55e",
        color: "#fff",
        weight: 2,
        fillOpacity: 0.7,
      })
        .bindPopup(`<strong>${d.from_agent}</strong><br/>${d.content.slice(0, 200)}`)
        .addTo(markersRef.current);
    }
  }, [data]);

  return (
    <div ref={mapRef} className="h-full w-full" />
  );
}
