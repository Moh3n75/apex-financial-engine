import type { NextConfig } from "next";
import path from "node:path";

const nextConfig: NextConfig = {
  poweredByHeader: false,
  // Keep Turbopack root inside this app (repo lives under a parent folder with unrelated lockfiles).
  turbopack: {
    root: path.join(__dirname),
  },
};

export default nextConfig;
