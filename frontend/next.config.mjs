const localBackendApiUrl = process.env.LOCAL_BACKEND_API_URL?.replace(/\/$/, "");

/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    if (!localBackendApiUrl) {
      return [];
    }

    return [
      {
        source: "/proxy/:path*",
        destination: `${localBackendApiUrl}/:path*`
      }
    ];
  }
};

export default nextConfig;
