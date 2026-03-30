import type {
  AuthResponse,
  HistoryItem,
  JobStatus,
  LoginPayload,
  ManualEntryPayload,
  ReportDetail,
  ReportRowsPage,
  ReportSectionResponse,
  ShareLinkResponse,
  SignupPayload,
  UploadCompletedPart,
  UploadSession,
  UploadSessionStatus,
  User
} from "@/lib/types";

const INTERNAL_PROXY_API_BASE_URL = "/api/proxy/api/v1";
const INTERNAL_PROXY_ROOT_URL = "/api/proxy";
const PUBLIC_API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? process.env.NEXT_PUBLIC_API_URL;

function normalizeApiBaseUrl(value?: string): string {
  const trimmed = value?.replace(/\/$/, "");

  if (!trimmed) {
    return INTERNAL_PROXY_API_BASE_URL;
  }

  // Support older local setups that still point the browser at `/proxy`.
  if (trimmed === "/proxy" || trimmed === "/api/proxy" || trimmed === "/proxy/api/v1") {
    return INTERNAL_PROXY_API_BASE_URL;
  }

  return trimmed;
}

function getApiRootUrl(baseUrl: string): string {
  if (baseUrl === INTERNAL_PROXY_API_BASE_URL) {
    return INTERNAL_PROXY_ROOT_URL;
  }

  return baseUrl.replace(/\/api\/v1$/, "");
}

const API_BASE_URL = normalizeApiBaseUrl(PUBLIC_API_BASE_URL);
const API_ROOT_URL = getApiRootUrl(API_BASE_URL);
const DIRECT_UPLOAD_API_BASE_URL = normalizeApiBaseUrl(
  process.env.NEXT_PUBLIC_DIRECT_BACKEND_API_URL ?? process.env.NEXT_PUBLIC_API_URL ?? API_BASE_URL
);
const DIRECT_UPLOAD_API_ROOT_URL = getApiRootUrl(DIRECT_UPLOAD_API_BASE_URL);
const DIRECT_UPLOAD_REQUIRES_DIRECT_BACKEND = DIRECT_UPLOAD_API_BASE_URL === INTERNAL_PROXY_API_BASE_URL;
const DIRECT_STORAGE_UPLOADS_ENABLED = (() => {
  const explicitValue = process.env.NEXT_PUBLIC_USE_DIRECT_STORAGE_UPLOADS?.trim().toLowerCase();
  if (explicitValue === "true") {
    return true;
  }
  if (explicitValue === "false") {
    return false;
  }
  return /^https:\/\//.test(DIRECT_UPLOAD_API_ROOT_URL);
})();
const PENDING_UPLOAD_STORAGE_KEY = "auto_analytics_pending_upload_session";

type RequestOptions = {
  method?: string;
  token?: string;
  body?: BodyInit | null;
  headers?: HeadersInit;
  retries?: number;
  timeoutMs?: number;
  baseUrl?: string;
};

type UploadDatasetInput = {
  datasetName?: string;
  targetColumn?: string;
  onUploadProgress?: (progress: number) => void;
  onStatusChange?: (message: string, progress?: number) => void;
};

export type PendingUploadSessionRecord = {
  uploadId: string;
  datasetName?: string;
  targetColumn?: string;
  uploadStrategy: string;
  storageCompleted: boolean;
  parts: UploadCompletedPart[];
  updatedAt: string;
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
  if (path.includes("/analysis/uploads/") || path.includes("/analysis/upload")) {
    return 600_000;
  }

  if (
    path.includes("/analysis/manual") ||
    path.includes("/download-pdf") ||
    path.includes("/sections/")
  ) {
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

function getFriendlyNetworkMessage(method: string, path: string): string {
  if (path.includes("/health")) {
    return "Backend is waking up or temporarily unreachable. Please wait a few seconds and try again.";
  }

  if (path.includes("/analysis/uploads/")) {
    return "The backend is temporarily unavailable while finalizing your secure upload. Your uploaded file is preserved and can resume shortly.";
  }

  if (path.includes("/analysis/upload")) {
    return "The upload could not reach the backend directly. The backend may still be waking up. Please retry in a few seconds.";
  }

  if (method === "POST") {
    return "The analytics service is temporarily unavailable. Please wait a few seconds and try again.";
  }

  return "We are reconnecting to the analytics service. Please wait a moment and try again.";
}

function getFriendlyTimeoutMessage(path: string): string {
  if (path.includes("/health")) {
    return "Backend is taking longer than expected to wake up. Please wait a few seconds and try again.";
  }

  if (path.includes("/analysis/uploads/")) {
    return "Upload finalization is taking longer than expected. Your file is already in secure storage, and the app will keep trying to resume.";
  }

  if (path.includes("/analysis/upload")) {
    return "The upload is taking longer than expected. Large datasets continue in the background once the backend is available.";
  }

  if (path.includes("/analysis/upload") || path.includes("/analysis/manual")) {
    return "The analysis is taking longer than expected. Please wait a moment and try again.";
  }

  return "The server took too long to respond. Please try again in a moment.";
}

function getFriendlyHttpMessage(path: string, status: number, fallbackMessage: string): string {
  if ([502, 503, 504].includes(status)) {
    if (path.includes("/health")) {
      return "Backend is waking up or temporarily unreachable. Please wait a few seconds and try again.";
    }

    if (path.includes("/analysis/uploads/")) {
      return "The backend is still waking up while finalizing your secure upload. The file is already stored safely and the app will retry.";
    }

    if (path.includes("/analysis/upload")) {
      return "Large dataset detected. Upload routing is correct, but the backend is still unavailable. Please retry in a few seconds.";
    }

    return "Backend is waking up or temporarily unreachable. Please wait a few seconds and try again.";
  }

  return fallbackMessage;
}

function normalizeGatewayLikeMessage(path: string, message: string): string {
  const normalizedMessage = message.trim().toLowerCase();
  if (normalizedMessage === "bad gateway") {
    return getFriendlyHttpMessage(path, 502, message);
  }
  if (normalizedMessage === "service unavailable") {
    return getFriendlyHttpMessage(path, 503, message);
  }
  if (normalizedMessage === "gateway timeout") {
    return getFriendlyHttpMessage(path, 504, message);
  }
  return message;
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
    return new ApiError(getFriendlyNetworkMessage(method, path), {
      code: "NETWORK_ERROR",
      isRetryable: true
    });
  }

  if (error instanceof Error) {
    return new ApiError(normalizeGatewayLikeMessage(path, error.message), {
      code: "UNKNOWN_ERROR",
      isRetryable: /bad gateway|service unavailable|gateway timeout/i.test(error.message),
    });
  }

  return new ApiError("Something went wrong. Please try again.", {
    code: "UNKNOWN_ERROR"
  });
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function warmAnalyticsService(): Promise<void> {
  await fetchResponse("/health", {
    baseUrl: DIRECT_UPLOAD_API_ROOT_URL,
    retries: 2,
    timeoutMs: 45_000
  });
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

function shouldRetryUploadSessionFlow(error: unknown): boolean {
  if (!(error instanceof ApiError)) {
    return false;
  }

  return (
    error.code === "NETWORK_ERROR" ||
    error.code === "TIMEOUT" ||
    [502, 503, 504].includes(error.status ?? 0)
  );
}

function readPendingUploadSessionRecord(): PendingUploadSessionRecord | null {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    const rawValue = window.localStorage.getItem(PENDING_UPLOAD_STORAGE_KEY);
    if (!rawValue) {
      return null;
    }
    return JSON.parse(rawValue) as PendingUploadSessionRecord;
  } catch {
    return null;
  }
}

function writePendingUploadSessionRecord(record: PendingUploadSessionRecord): void {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(PENDING_UPLOAD_STORAGE_KEY, JSON.stringify(record));
}

function clearPendingUploadSessionRecord(uploadId?: string): void {
  if (typeof window === "undefined") {
    return;
  }

  const existingRecord = readPendingUploadSessionRecord();
  if (uploadId && existingRecord?.uploadId && existingRecord.uploadId !== uploadId) {
    return;
  }

  window.localStorage.removeItem(PENDING_UPLOAD_STORAGE_KEY);
}

function updatePendingUploadSessionRecord(
  update: Partial<PendingUploadSessionRecord> & Pick<PendingUploadSessionRecord, "uploadId">
): void {
  const currentRecord = readPendingUploadSessionRecord();
  if (!currentRecord || currentRecord.uploadId !== update.uploadId) {
    return;
  }
  writePendingUploadSessionRecord({
    ...currentRecord,
    ...update,
    updatedAt: new Date().toISOString()
  });
}

function notifyUploadStatus(
  input: UploadDatasetInput | undefined,
  message: string,
  progress?: number
): void {
  input?.onStatusChange?.(message, progress);
  if (progress !== undefined) {
    input?.onUploadProgress?.(progress);
  }
}

async function createUploadSession(
  file: File,
  token: string,
  input: UploadDatasetInput
): Promise<UploadSession> {
  return request<UploadSession>("/analysis/uploads/session", {
    method: "POST",
    token,
    body: JSON.stringify({
      filename: file.name,
      content_type: file.type || undefined,
      file_size_bytes: file.size,
      dataset_name: input.datasetName,
      target_column: input.targetColumn
    }),
    baseUrl: DIRECT_UPLOAD_API_BASE_URL,
    retries: 1,
    timeoutMs: 120_000
  });
}

function uploadBlobWithXhr(
  url: string,
  blob: Blob,
  options: {
    headers?: Record<string, string>;
    timeoutMs?: number;
    onProgress?: (loadedBytes: number) => void;
  } = {}
): Promise<{ etag?: string | null }> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url);
    xhr.timeout = options.timeoutMs ?? 600_000;

    Object.entries(options.headers ?? {}).forEach(([headerName, headerValue]) => {
      xhr.setRequestHeader(headerName, headerValue);
    });

    xhr.upload.onprogress = (event) => {
      if (!event.lengthComputable || !options.onProgress) {
        return;
      }
      options.onProgress(event.loaded);
    };

    xhr.onerror = () => {
      reject(
        new ApiError("The secure storage upload failed. Please try again.", {
          code: "NETWORK_ERROR",
          isRetryable: true
        })
      );
    };

    xhr.ontimeout = () => {
      reject(
        new ApiError("The secure storage upload timed out. Please try again.", {
          code: "TIMEOUT",
          isRetryable: true
        })
      );
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve({
          etag: xhr.getResponseHeader("ETag") ?? xhr.getResponseHeader("etag")
        });
        return;
      }

      reject(
        new ApiError("The secure storage upload failed. Please try again.", {
          status: xhr.status,
          code: "HTTP_ERROR",
          isRetryable: xhr.status >= 500 || xhr.status === 429
        })
      );
    };

    xhr.send(blob);
  });
}

async function uploadSinglePartToStorage(
  file: File,
  session: UploadSession,
  input: UploadDatasetInput
): Promise<UploadCompletedPart[]> {
  if (!session.single_part_url) {
    throw new ApiError("The secure upload session did not include a single-part upload URL.");
  }

  await uploadBlobWithXhr(session.single_part_url, file, {
    headers: session.single_part_headers,
    onProgress: (loadedBytes) => {
      notifyUploadStatus(
        input,
        "Uploading to secure storage...",
        Math.min(100, Math.round((loadedBytes / file.size) * 100))
      );
    }
  });

  notifyUploadStatus(input, "Upload complete, finalizing analysis...", 100);
  return [];
}

async function uploadMultipartToStorage(
  file: File,
  session: UploadSession,
  input: UploadDatasetInput
): Promise<UploadCompletedPart[]> {
  if (!session.multipart_parts.length || !session.chunk_size_bytes) {
    throw new ApiError("The secure upload session did not include multipart upload instructions.");
  }

  const completedParts: UploadCompletedPart[] = [];
  let uploadedBytes = 0;

  for (let index = 0; index < session.multipart_parts.length; index += 1) {
    const uploadPart = session.multipart_parts[index];
    const start = index * session.chunk_size_bytes;
    const end = Math.min(file.size, start + session.chunk_size_bytes);
    const blob = file.slice(start, end);

    const result = await uploadBlobWithXhr(uploadPart.url, blob, {
      onProgress: (loadedBytes) => {
        const totalUploaded = Math.min(file.size, uploadedBytes + loadedBytes);
        notifyUploadStatus(
          input,
          "Uploading to secure storage...",
          Math.min(100, Math.round((totalUploaded / file.size) * 100))
        );
      }
    });

    uploadedBytes += blob.size;
    const etag = result.etag?.trim();
    if (!etag) {
      throw new ApiError(
        "The storage provider did not return a multipart upload ETag. Please verify the bucket CORS and exposed headers.",
        { code: "UPLOAD_ETAG_MISSING" }
      );
    }

    completedParts.push({ part_number: uploadPart.part_number, etag });
    notifyUploadStatus(
      input,
      "Uploading to secure storage...",
      Math.min(100, Math.round((uploadedBytes / file.size) * 100))
    );
  }

  notifyUploadStatus(input, "Upload complete, finalizing analysis...", 100);
  return completedParts;
}

async function completeUploadSession(
  token: string,
  uploadId: string,
  parts: UploadCompletedPart[]
): Promise<ReportDetail> {
  return request<ReportDetail>(`/analysis/uploads/${uploadId}/complete`, {
    method: "POST",
    token,
    body: JSON.stringify({ parts }),
    baseUrl: DIRECT_UPLOAD_API_BASE_URL,
    timeoutMs: 180_000
  });
}

async function getUploadSessionStatus(
  token: string,
  uploadId: string
): Promise<UploadSessionStatus> {
  return request<UploadSessionStatus>(`/analysis/uploads/${uploadId}`, {
    token,
    baseUrl: DIRECT_UPLOAD_API_BASE_URL,
    retries: 1,
    timeoutMs: 30_000
  });
}

async function finalizeUploadSessionWithRecovery(
  token: string,
  pendingRecord: PendingUploadSessionRecord,
  input: UploadDatasetInput
): Promise<ReportDetail> {
  let lastKnownError: unknown = null;

  for (let attempt = 0; attempt < 40; attempt += 1) {
    try {
      const sessionStatus = await getUploadSessionStatus(token, pendingRecord.uploadId);
      if (sessionStatus.report) {
        clearPendingUploadSessionRecord(pendingRecord.uploadId);
        return sessionStatus.report;
      }

      if (sessionStatus.status === "failed") {
        clearPendingUploadSessionRecord(pendingRecord.uploadId);
        throw new ApiError(
          sessionStatus.error_message ??
            sessionStatus.message ??
            "The upload failed while finalizing the analysis."
        );
      }

      if (sessionStatus.status === "expired") {
        clearPendingUploadSessionRecord(pendingRecord.uploadId);
        throw new ApiError(
          sessionStatus.error_message ?? "Upload session expired. Please upload the file again."
        );
      }

      notifyUploadStatus(
        input,
        sessionStatus.message ??
          sessionStatus.progress_message ??
          "Upload complete, finalizing analysis...",
        sessionStatus.progress
      );

      const finalizingUpdatedAt = Date.parse(sessionStatus.updated_at);
      const isFinalizingStale =
        sessionStatus.status === "finalizing" &&
        Number.isFinite(finalizingUpdatedAt) &&
        Date.now() - finalizingUpdatedAt > 30_000;

      if (
        pendingRecord.storageCompleted &&
        (["created", "uploading", "uploaded"].includes(sessionStatus.status) || isFinalizingStale)
      ) {
        try {
          const report = await completeUploadSession(token, pendingRecord.uploadId, pendingRecord.parts);
          clearPendingUploadSessionRecord(pendingRecord.uploadId);
          return report;
        } catch (error) {
          lastKnownError = error;
          if (!shouldRetryUploadSessionFlow(error)) {
            throw error;
          }
        }
      }
    } catch (error) {
      lastKnownError = error;
      if (!shouldRetryUploadSessionFlow(error)) {
        throw error;
      }
    }

    await wait(3_000);
  }

  throw normalizeError(
    lastKnownError ??
      new ApiError(
        "Upload finalization is still recovering. Your file is stored safely, and reopening the page will resume automatically."
      ),
    "POST",
    "/analysis/uploads/finalize"
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
        throw new ApiError(getFriendlyHttpMessage(path, response.status, await readErrorMessage(response)), {
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

async function uploadWithXhr(
  file: File,
  token: string,
  input: UploadDatasetInput,
  baseUrl: string
): Promise<ReportDetail> {
  return new Promise<ReportDetail>((resolve, reject) => {
    const formData = new FormData();
    formData.append("file", file);
    if (input.datasetName) {
      formData.append("dataset_name", input.datasetName);
    }
    if (input.targetColumn) {
      formData.append("target_column", input.targetColumn);
    }

    const xhr = new XMLHttpRequest();
    xhr.open("POST", buildUrl("/analysis/upload", baseUrl));
    xhr.timeout = 600_000;
    xhr.responseType = "json";
    xhr.setRequestHeader("Authorization", `Bearer ${token}`);

    xhr.upload.onprogress = (event) => {
      if (!event.lengthComputable || !input.onUploadProgress) {
        return;
      }

      input.onUploadProgress(Math.min(100, Math.round((event.loaded / event.total) * 100)));
    };

    xhr.onerror = () => {
      reject(
        new ApiError("The upload could not reach the backend directly. The backend may still be waking up. Please retry in a few seconds.", {
          code: "NETWORK_ERROR",
          isRetryable: true
        })
      );
    };

    xhr.ontimeout = () => {
      reject(
        new ApiError("The upload is taking longer than expected. Large datasets are processed in the background once the backend is available.", {
          code: "TIMEOUT",
          isRetryable: true
        })
      );
    };

    xhr.onload = () => {
      const response = xhr.response as ReportDetail | { detail?: string } | null;
      if (xhr.status >= 200 && xhr.status < 300 && response) {
        resolve(response as ReportDetail);
        return;
      }

      const detail =
        (response && typeof response === "object" && "detail" in response && response.detail) ||
        xhr.statusText ||
        "Upload failed.";
      const friendlyDetail = getFriendlyHttpMessage(
        "/analysis/upload",
        xhr.status,
        normalizeGatewayLikeMessage("/analysis/upload", detail)
      );
      reject(
        new ApiError(friendlyDetail, {
          status: xhr.status,
          code: "HTTP_ERROR",
          isRetryable: xhr.status >= 500 || xhr.status === 429
        })
      );
    };

    xhr.send(formData);
  });
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

export function loginPublicUser(): Promise<AuthResponse> {
  return request<AuthResponse>("/auth/public", {
    method: "POST",
    timeoutMs: 120_000
  });
}

export function getCurrentUser(token: string): Promise<User> {
  return request<User>("/auth/me", { token, retries: 2, timeoutMs: 30_000 });
}

export async function uploadDataset(
  file: File,
  token: string,
  input: UploadDatasetInput
): Promise<ReportDetail> {
  if (DIRECT_UPLOAD_REQUIRES_DIRECT_BACKEND) {
    throw new ApiError(
      "Direct backend uploads are required for file analysis. Configure NEXT_PUBLIC_DIRECT_BACKEND_API_URL to point at the FastAPI service.",
      { code: "UPLOAD_PROXY_DISABLED" }
    );
  }

  await warmAnalyticsService();

  if (DIRECT_STORAGE_UPLOADS_ENABLED) {
    notifyUploadStatus(input, "Preparing upload...", 5);
    const uploadSession = await createUploadSession(file, token, input);
    const pendingRecord: PendingUploadSessionRecord = {
      uploadId: uploadSession.upload_id,
      datasetName: input.datasetName,
      targetColumn: input.targetColumn,
      uploadStrategy: uploadSession.upload_strategy,
      storageCompleted: false,
      parts: [],
      updatedAt: new Date().toISOString()
    };

    writePendingUploadSessionRecord(pendingRecord);
    try {
      if (uploadSession.processing_mode === "large") {
        notifyUploadStatus(input, "Large dataset detected. Uploading to secure storage...", 5);
      } else if (uploadSession.processing_mode === "medium") {
        notifyUploadStatus(input, "Chunk-based upload detected. Uploading to secure storage...", 5);
      }

      const completedParts =
        uploadSession.upload_strategy === "single_part"
          ? await uploadSinglePartToStorage(file, uploadSession, input)
          : await uploadMultipartToStorage(file, uploadSession, input);

      updatePendingUploadSessionRecord({
        uploadId: uploadSession.upload_id,
        storageCompleted: true,
        parts: completedParts
      });

      return finalizeUploadSessionWithRecovery(
        token,
        {
          ...pendingRecord,
          storageCompleted: true,
          parts: completedParts,
          updatedAt: new Date().toISOString()
        },
        input
      );
    } catch (error) {
      const currentPendingRecord = readPendingUploadSessionRecord();
      if (!currentPendingRecord?.storageCompleted) {
        clearPendingUploadSessionRecord(uploadSession.upload_id);
      }
      throw error;
    }
  }

  try {
    return await uploadWithXhr(file, token, input, DIRECT_UPLOAD_API_BASE_URL);
  } catch (error) {
    if (!shouldRetryUpload(error)) {
      throw error;
    }

    await wait(2_000);
    await warmAnalyticsService();
    return uploadWithXhr(file, token, input, DIRECT_UPLOAD_API_BASE_URL);
  }
}

export function getPendingUploadSession(): PendingUploadSessionRecord | null {
  return readPendingUploadSessionRecord();
}

export async function resumePendingUploadSession(
  token: string,
  input: UploadDatasetInput
): Promise<ReportDetail | null> {
  const pendingRecord = readPendingUploadSessionRecord();
  if (!pendingRecord) {
    return null;
  }
  return finalizeUploadSessionWithRecovery(token, pendingRecord, input);
}

export function clearPendingUploadSession(uploadId?: string): void {
  clearPendingUploadSessionRecord(uploadId);
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

export function getJobStatus(token: string, jobId: string): Promise<JobStatus> {
  return request<JobStatus>(`/analysis/jobs/${jobId}`, {
    token,
    retries: 1,
    timeoutMs: 30_000,
    baseUrl: DIRECT_UPLOAD_API_BASE_URL
  });
}

export function getReportSection<T>(
  token: string,
  reportId: string,
  section: string
): Promise<ReportSectionResponse<T>> {
  return request<ReportSectionResponse<T>>(`/analysis/reports/${reportId}/sections/${section}`, {
    token,
    retries: 1,
    timeoutMs: 120_000
  });
}

export function getReportRows(
  token: string,
  reportId: string,
  page: number,
  pageSize: number
): Promise<ReportRowsPage> {
  return request<ReportRowsPage>(
    `/analysis/reports/${reportId}/rows?page=${page}&page_size=${pageSize}`,
    {
      token,
      retries: 1,
      timeoutMs: 90_000
    }
  );
}

export function getSharedReport(shareToken: string): Promise<ReportDetail> {
  return request<ReportDetail>(`/analysis/shared/${shareToken}`, {
    retries: 2,
    timeoutMs: 30_000
  });
}

export function getSharedReportSection<T>(
  shareToken: string,
  section: string
): Promise<ReportSectionResponse<T>> {
  return request<ReportSectionResponse<T>>(
    `/analysis/shared/${shareToken}/sections/${section}`,
    {
      retries: 1,
      timeoutMs: 120_000
    }
  );
}

export function getSharedReportRows(
  shareToken: string,
  page: number,
  pageSize: number
): Promise<ReportRowsPage> {
  return request<ReportRowsPage>(
    `/analysis/shared/${shareToken}/rows?page=${page}&page_size=${pageSize}`,
    {
      retries: 1,
      timeoutMs: 90_000
    }
  );
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
    baseUrl: DIRECT_UPLOAD_API_ROOT_URL,
    retries: 2,
    timeoutMs: 45_000
  });
}

export { API_BASE_URL, API_ROOT_URL, DIRECT_UPLOAD_API_BASE_URL };
