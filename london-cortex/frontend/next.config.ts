import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  outputFileTracingRoot: require("path").join(__dirname, ".."),
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
