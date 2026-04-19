"use client";

import useSWR from "swr";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export function useAPI<T>(path: string | null, interval: number = 5000) {
  const url = path ? `${API_BASE}${path}` : null;
  return useSWR<T>(url, fetcher, {
    refreshInterval: interval,
    revalidateOnFocus: false,
    dedupingInterval: 1000,
  });
}
