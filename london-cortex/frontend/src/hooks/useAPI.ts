"use client";

import useSWR from "swr";

async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export function useAPI<T>(path: string | null, interval: number = 5000) {
  return useSWR<T>(path, fetcher, {
    refreshInterval: interval,
    revalidateOnFocus: false,
    dedupingInterval: 1000,
  });
}
