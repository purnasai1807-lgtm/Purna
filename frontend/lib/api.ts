import type {
  AuthResponse,
  HistoryItem,
  LoginPayload,
  ManualEntryPayload,
  ReportDetail,
  ShareLinkResponse,
  SignupPayload,
  User
} from "@/lib/types";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ??
  "/api/proxy/api/v1";
const API_ROOT_URL = API_BASE_URL.replace(/\/api\/v1$/, "");
const DIRECT_UPLOAD_API_BASE_URL =
  process.env.NEXT_PUBLIC_DIRECT_BACKEND_API_URL?.replace(/\/$/, "") ??
  (API_BASE_URL.startsWith("/")
    ? "https://auto-analytics-ai-api.onrender.com/api/v1"
    : API_BASE_URL);

type RequestOptions = {
  method?: string;
  token?: string;
  body?: BodyInit | null;
  headers?: HeadersInit;
  retries?: number;
  timeoutMs?: number;
  baseUrl?: string;
};

export class ApiError extends Error {
  status?: number;
  code?: string;
  isRetryable: boolean;

  constructor(
    message: string,
    options: {
      status?: number;
      code?: string;
      isRetryable?: boolean;
    } = {}
  ) {
    super(message);
    this.name = "ApiError";
    this.status = options.status;
    this.code = options.code;
    this.isRetryable = options.isRetryable ?? false;
  }
}

function buildUrl(path: string, baseUrl: string): string {
  return path.startsWith("http://") || path.startsWith("https://")
    ? path
    : `${baseUrl}${path}`;
}

function getDefaultTimeout(method: string, path: string): number {
  if (path.includes("/analysis/upload")) {
    return 600_000;
  }

  if (path.includes("/analysis/manual") || path.includes("/download-pdf")) {
    return 120_000;
  }

  if (method === "POST") {
    return 90_000;
  }

  return 25_000;
}

function getDefaultRetries(method: string): number {
  return method === "GET" ? 2 : 0;
}

function getFriendlyNetworkMessage(method: string): string {
  if (method === "POST") {
    return "The analytics service is temporarily unavailable. Please wait a few seconds and try again.";
  }

  return "We are reconnecting to the analytics service. Please wait a moment and try again.";
}

function getFriendlyTimeoutMessage(path: string): string {
  if (path.includes("/analysis/upload") || path.includes("/analysis/manual")) {
    return "The analysis is taking longer than expected. Please wait a moment and try again.";
  }

  return "The server took too long to respond. Please try again in a moment.";
}

async function readErrorMessage(response: Response): Promise<string> {
  const fallbackMessage = "Something went wrong. Please try again.";

  try {
    const errorPayload = (await response.json()) as { detail?: string };
    if (errorPayload.detail) {
      return errorPayload.detail;
    }
  } catch {
    try {
      const plainText = await response.text();
      if (plainText.trim()) {
        return plainText.trim();
      }
    } catch {
      return response.statusText || fallbackMessage;
    }
  }

  return response.statusText || fallbackMessage;
}

function getFriendlyHttpMessage(path: string, status: number, fallbackMessage: string): string {
  if (
    path.includes("/analysis/upload") &&
    [502, 503, 504].includes(status)
  ) {
    return "Large file uploads are taking longer than expected right now. Please wait a few seconds and try again.";
  }

  return fallbackMessage;
}

function normalizeError(error: unknown, method: string, path: string): ApiError {
  if (error instanceof ApiError) {
    return error;
  }

  if (error instanceof DOMException && error.name === "AbortError") {
    return new ApiError(getFriendlyTimeoutMessage(path), {
      code: "TIMEOUT",
      isRetryable: true
    });
  }

  if (error instanceof TypeError) {
    return new ApiError(getFriendlyNetworkMessage(method), {
      code: "NETWORK_ERROR",
      isRetryable: true
    });
  }

  if (error instanceof Error) {
    return new ApiError(error.message, { code: "UNKNOWN_ERROR" });
  }

  return new ApiError("Something went wrong. Please try again.", {
    code: "UNKNOWN_ERROR"
  });
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function warmAnalyticsService(baseUrl: string): Promise<void> {
  try {
    await fetchResponse("/health", {
      baseUrl,
      retries: 2,
      timeoutMs: 20_000
    });
  } catch {
    // If the warm-up check fails, the real upload request still gets a chance.
  }
}

function shouldRetryUpload(error: unknown): boolean {
  if (!(error instanceof ApiError)) {
    return false;
  }

  return (
    error.code === "NETWORK_ERROR" ||
    error.code === "TIMEOUT" ||
    [502, 503, 504].includes(error.status ?? 0)
  );
}

async function fetchResponse(path: string, options: RequestOptions = {}): Promise<Response> {
  const method = options.method ?? "GET";
  const timeoutMs = options.timeoutMs ?? getDefaultTimeout(method, path);
  const retries = options.retries ?? getDefaultRetries(method);
  const url = buildUrl(path, options.baseUrl ?? API_BASE_URL);

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        method,
        body: options.body,
        headers: {
          ...(options.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
          ...(options.token ? { Authorization: `Bearer ${options.token}` } : {}),
          ...options.headers
        },
        cache: "no-store",
        signal: controller.signal
      });
      window.clearTimeout(timeoutId);

      if (!response.ok) {
        const message = getFriendlyHttpMessage(
          path,
          response.status,
          await readErrorMessage(response)
        );
        throw new ApiError(message, {
          status: response.status,
          code: "HTTP_ERROR",
          isRetryable: response.status >= 500 || response.status === 429
        });
      }

      return response;
    } catch (error) {
      window.clearTimeout(timeoutId);
      const normalizedError = normalizeError(error, method, path);
      if (attempt >= retries || !normalizedError.isRetryable) {
        throw normalizedError;
      }

      await wait(900 * (attempt + 1));
    }
  }

  throw new ApiError("Something went wrong. Please try again.");
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const response = await fetchResponse(path, options);

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export function signup(payload: SignupPayload): Promise<AuthResponse> {
  return request<AuthResponse>("/auth/signup", {
    method: "POST",
    body: JSON.stringify(payload),
    timeoutMs: 120_000
  });
}

export function login(payload: LoginPayload): Promise<AuthResponse> {
  return request<AuthResponse>("/auth/login", {
    method: "POST",
    body: JSON.stringify(payload),
    timeoutMs: 120_000
  });
}

export function getCurrentUser(token: string): Promise<User> {
  return request<User>("/auth/me", { token, retries: 2, timeoutMs: 30_000 });
}

export function uploadDataset(
  file: File,
  token: string,
  input: { datasetName?: string; targetColumn?: string }
): Promise<ReportDetail> {
  const createUploadFormData = (): FormData => {
    const formData = new FormData();
    formData.append("file", file);
    if (input.datasetName) {
      formData.append("dataset_name", input.datasetName);
    }
    if (input.targetColumn) {
      formData.append("target_column", input.targetColumn);
    }
    return formData;
  };

  return (async () => {
    await warmAnalyticsService(API_ROOT_URL);
    await wait(1_200);

    try {
      return await request<ReportDetail>("/analysis/upload", {
        method: "POST",
        token,
        body: createUploadFormData(),
        timeoutMs: 600_000,
        baseUrl: DIRECT_UPLOAD_API_BASE_URL
      });
    } catch (error) {
      if (!shouldRetryUpload(error)) {
        throw error;
      }

      await wait(2_000);
      await warmAnalyticsService(API_ROOT_URL);
      return request<ReportDetail>("/analysis/upload", {
        method: "POST",
        token,
        body: createUploadFormData(),
        timeoutMs: 600_000,
        baseUrl: DIRECT_UPLOAD_API_BASE_URL
      });
    }
  })();
}

export function submitManualEntry(token: string, payload: ManualEntryPayload): Promise<ReportDetail> {
  return request<ReportDetail>("/analysis/manual", {
    method: "POST",
    token,
    body: JSON.stringify(payload),
    timeoutMs: 180_000
  });
}

export function getHistory(token: string): Promise<HistoryItem[]> {
  return request<HistoryItem[]>("/analysis/history", { token, retries: 2, timeoutMs: 30_000 });
}

export function getReport(token: string, reportId: string): Promise<ReportDetail> {
  return request<ReportDetail>(`/analysis/reports/${reportId}`, {
    token,
    retries: 2,
    timeoutMs: 30_000
  });
}

export function getSharedReport(shareToken: string): Promise<ReportDetail> {
  return request<ReportDetail>(`/analysis/shared/${shareToken}`, {
    retries: 2,
    timeoutMs: 30_000
  });
}

export function createShareLink(token: string, reportId: string): Promise<ShareLinkResponse> {
  return request<ShareLinkResponse>(`/analysis/reports/${reportId}/share`, {
    method: "POST",
    token
  });
}

export async function downloadPdf(token: string, reportId: string): Promise<Blob> {
  const response = await fetchResponse(`/analysis/reports/${reportId}/download-pdf`, {
    token,
    timeoutMs: 120_000
  });
  return response.blob();
}

export function checkApiHealth(): Promise<{ status: string }> {
  return request<{ status: string }>("/health", {
    baseUrl: API_ROOT_URL,
    retries: 2,
    timeoutMs: 15_000
  });
}

export { API_BASE_URL, API_ROOT_URL };
