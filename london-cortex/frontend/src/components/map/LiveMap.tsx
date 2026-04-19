"use client";

import { useEffect, useRef } from "react";
import L from "leaflet";
import { useAPI } from "@/hooks/useAPI";
import { LONDON_CENTER, LONDON_ZOOM } from "@/lib/constants";
import type { MapData } from "@/lib/types";

const SEVERITY_COLORS: Record<number, string> = {
  1: "#4D7C58",
  2: "#5B7A90",
  3: "#C09440",
  4: "#B8603A",
  5: "#B85048",
};

function anomalyIcon(color: string, severity: number): L.DivIcon {
  const core = 3 + severity * 0.8;
  const pulse = core + 5 + severity * 1.2;
  const dur = Math.max(1.4, 2.8 - severity * 0.3);
  const size = Math.ceil(pulse * 2 + 4);
  const c = size / 2;

  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" style="overflow:visible">
      <defs>
        <filter id="glow-${severity}" x="-60%" y="-60%" width="220%" height="220%">
          <feGaussianBlur in="SourceGraphic" stdDeviation="${1 + severity * 0.4}" result="blur"/>
          <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
      </defs>

      <!-- Pulse ring -->
      <circle cx="${c}" cy="${c}" r="${core}" fill="${color}" opacity="0">
        <animate attributeName="r" values="${core};${pulse};${core}" dur="${dur}s" repeatCount="indefinite"/>
        <animate attributeName="opacity" values="0.4;0;0.4" dur="${dur}s" repeatCount="indefinite"/>
      </circle>

      <!-- Core dot -->
      <circle cx="${c}" cy="${c}" r="${core}" fill="${color}" filter="url(#glow-${severity})" opacity="0.9"/>
    </svg>`;

  return L.divIcon({
    html: svg,
    className: "",
    iconSize: [size, size],
    iconAnchor: [c, c],
    popupAnchor: [0, -c],
  });
}

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

    /* CartoDB light tiles — warmed by CSS filter in globals.css */
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
      subdomains: "abcd",
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

    /* Anomaly markers — jitter stacked markers so all are visible */
    const locationCount: Record<string, number> = {};
    const locationIdx: Record<string, number> = {};
    for (const a of data.anomalies) {
      const key = `${a.lat?.toFixed(4)},${a.lon?.toFixed(4)}`;
      locationCount[key] = (locationCount[key] || 0) + 1;
    }

    for (const a of data.anomalies) {
      if (!a.lat || !a.lon) continue;
      const key = `${a.lat.toFixed(4)},${a.lon.toFixed(4)}`;
      const total = locationCount[key];
      const idx = locationIdx[key] ?? 0;
      locationIdx[key] = idx + 1;

      let lat = a.lat;
      let lon = a.lon;
      if (total > 1) {
        const angle = (2 * Math.PI * idx) / total;
        const spread = 0.003 + total * 0.0005;
        lat += Math.sin(angle) * spread;
        lon += Math.cos(angle) * spread * 1.5;
      }

      const color = SEVERITY_COLORS[a.severity] || "#5B7A90";
      L.marker([lat, lon], { icon: anomalyIcon(color, a.severity) })
        .bindPopup(
          `<strong>${a.source}</strong><br/>` +
          `<span style="color:#555">${a.description}</span><br/>` +
          `<span style="color:#999;font-size:10px;font-family:monospace">z = ${a.z_score.toFixed(2)} · severity ${a.severity}</span>`
        )
        .addTo(markersRef.current);
    }

    /* Observation zone dots */
    for (const o of data.observations) {
      const c = o.zone === 2 ? "#B8603A" : o.zone === 1 ? "#647D8E" : "#DDD7CC";
      L.circleMarker([o.lat, o.lon], {
        radius: 2,
        fillColor: c,
        color: c,
        weight: 0,
        fillOpacity: o.zone === 2 ? 0.55 : 0.35,
      }).addTo(markersRef.current);
    }

    /* Discovery markers — forest green with soft glow */
    for (const d of data.discoveries) {
      L.circleMarker([d.lat, d.lon], {
        radius: 7,
        fillColor: "#4D7C58",
        color: "#4D7C58",
        weight: 2,
        fillOpacity: 0.35,
        opacity: 0.9,
        className: "discovery-marker",
      })
        .bindPopup(
          `<strong>${d.from_agent}</strong>` +
          `<span style="color:var(--text-secondary)">${d.content.slice(0, 200)}</span>`
        )
        .addTo(markersRef.current);
    }
  }, [data]);

  return (
    <div ref={mapRef} className="h-full w-full" />
  );
}
