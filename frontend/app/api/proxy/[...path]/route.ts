import { NextRequest } from "next/server";

const REMOVABLE_HEADERS = [
  "connection",
  "content-length",
  "host",
  "x-forwarded-for",
  "x-forwarded-host",
  "x-forwarded-port",
  "x-forwarded-proto",
];

function getBackendRootUrl(): string {
  const configuredUrl =
    process.env.BACKEND_API_URL ??
    process.env.LOCAL_BACKEND_API_URL?.replace(/\/api\/v1$/, "") ??
    "https://auto-analytics-ai-api.onrender.com";

  return configuredUrl.replace(/\/$/, "");
}

function buildTargetUrl(path: string[], search: string): string {
  const joinedPath = path.join("/");
  return `${getBackendRootUrl()}/${joinedPath}${search}`;
}

function copyRequestHeaders(request: NextRequest): Headers {
  const headers = new Headers(request.headers);
  REMOVABLE_HEADERS.forEach((header) => headers.delete(header));
  return headers;
}

function copyResponseHeaders(response: Response): Headers {
  const headers = new Headers(response.headers);
  REMOVABLE_HEADERS.forEach((header) => headers.delete(header));
  return headers;
}

function isUploadRoute(path: string[]): boolean {
  const joinedPath = path.join("/");
  return joinedPath === "api/v1/analysis/upload" || joinedPath.endsWith("/analysis/upload");
}

function buildProxyUpstreamError(path: string[], status: number, fallback: string): string {
  if (![502, 503, 504].includes(status)) {
    return fallback;
  }

  const joinedPath = path.join("/");
  if (joinedPath === "health" || joinedPath.endsWith("/health")) {
    return "Backend is waking up or temporarily unreachable. Please wait a few seconds and try again.";
  }

  return "The backend is temporarily unavailable. Please wait a few seconds and try again.";
}

async function forwardRequest(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
  if (request.method.toUpperCase() === "POST" && isUploadRoute(context.params.path)) {
    return Response.json(
      {
        detail:
          "Direct backend upload required. Configure NEXT_PUBLIC_DIRECT_BACKEND_API_URL to point at the FastAPI service."
      },
      { status: 400 }
    );
  }

  const method = request.method.toUpperCase();
  const targetUrl = buildTargetUrl(context.params.path, request.nextUrl.search);
  const timeoutMs =
    method === "POST" && targetUrl.includes("/analysis/upload")
      ? 600_000
      : method === "POST"
        ? 120_000
        : 30_000;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(targetUrl, {
      method,
      headers: copyRequestHeaders(request),
      body: method === "GET" || method === "HEAD" ? undefined : await request.arrayBuffer(),
      cache: "no-store",
      signal: controller.signal,
    });

    if ([502, 503, 504].includes(response.status)) {
      const fallbackDetail = response.statusText || "The backend is temporarily unavailable.";
      return Response.json(
        { detail: buildProxyUpstreamError(context.params.path, response.status, fallbackDetail) },
        { status: response.status }
      );
    }

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: copyResponseHeaders(response),
    });
  } catch (error) {
    const detail =
      error instanceof DOMException && error.name === "AbortError"
        ? "The analytics service is taking longer than expected. Please try again in a moment."
        : "The analytics service is temporarily unavailable. Please retry in a few seconds.";

    return Response.json({ detail }, { status: 503 });
  } finally {
    clearTimeout(timeoutId);
  }
}

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
  return forwardRequest(request, context);
}

export async function POST(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
  return forwardRequest(request, context);
}

export async function PUT(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
  return forwardRequest(request, context);
}

export async function PATCH(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
  return forwardRequest(request, context);
}

export async function DELETE(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
  return forwardRequest(request, context);
}

export async function OPTIONS(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
  return forwardRequest(request, context);
}
