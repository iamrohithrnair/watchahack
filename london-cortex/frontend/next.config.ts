import type { NextConfig } from "next";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

const nextConfig: NextConfig = {
  outputFileTracingRoot: join(__dirname, ".."),
  async rewrites() {
    const backend = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
    return [
      { source: "/api/:path*", destination: `${backend}/api/:path*` },
      { source: "/ws", destination: `${backend}/ws` },
      { source: "/stream", destination: `${backend}/stream` },
      { source: "/images/:path*", destination: `${backend}/images/:path*` },
    ];
  },
};

export default nextConfig;
