const localBackendApiUrl = process.env.LOCAL_BACKEND_API_URL?.replace(/\/$/, "");
const localBackendRootUrl = localBackendApiUrl?.replace(/\/api\/v1$/, "");
/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    workerThreads: true,
    webpackBuildWorker: false
  },
  async rewrites() {
    if (!localBackendApiUrl) {
      return [];
    }
    return [
      {
        source: "/proxy/health",
        destination: `${localBackendRootUrl}/health`
      },
      {
        source: "/proxy/:path*",
        destination: `${localBackendApiUrl}/:path*`
      }
    ];
  }
};
export default nextConfig;
