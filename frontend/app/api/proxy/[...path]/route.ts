import { NextRequest } from "next/server";

type StreamingFetchOptions = RequestInit & {
  duplex?: "half";
};

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

async function forwardRequest(
  request: NextRequest,
  context: { params: { path: string[] } }
): Promise<Response> {
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
    const requestBody = method === "GET" || method === "HEAD" ? undefined : request.body;
    const requestOptions: StreamingFetchOptions = {
      method,
      headers: copyRequestHeaders(request),
      body: requestBody,
      duplex: requestBody ? "half" : undefined,
      cache: "no-store",
      signal: controller.signal,
    };
    const response = await fetch(targetUrl, requestOptions);

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
