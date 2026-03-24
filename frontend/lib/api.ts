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
  "http://localhost:8000/api/v1";

type RequestOptions = {
  method?: string;
  token?: string;
  body?: BodyInit | null;
  headers?: HeadersInit;
};

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: options.method ?? "GET",
    body: options.body,
    headers: {
      ...(options.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
      ...(options.token ? { Authorization: `Bearer ${options.token}` } : {}),
      ...options.headers
    },
    cache: "no-store"
  });

  if (!response.ok) {
    let message = "Something went wrong. Please try again.";
    try {
      const errorPayload = (await response.json()) as { detail?: string };
      message = errorPayload.detail ?? message;
    } catch {
      message = response.statusText || message;
    }
    throw new Error(message);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export function signup(payload: SignupPayload): Promise<AuthResponse> {
  return request<AuthResponse>("/auth/signup", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function login(payload: LoginPayload): Promise<AuthResponse> {
  return request<AuthResponse>("/auth/login", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function getCurrentUser(token: string): Promise<User> {
  return request<User>("/auth/me", { token });
}

export function uploadDataset(
  file: File,
  token: string,
  input: { datasetName?: string; targetColumn?: string }
): Promise<ReportDetail> {
  const formData = new FormData();
  formData.append("file", file);
  if (input.datasetName) {
    formData.append("dataset_name", input.datasetName);
  }
  if (input.targetColumn) {
    formData.append("target_column", input.targetColumn);
  }

  return request<ReportDetail>("/analysis/upload", {
    method: "POST",
    token,
    body: formData
  });
}

export function submitManualEntry(token: string, payload: ManualEntryPayload): Promise<ReportDetail> {
  return request<ReportDetail>("/analysis/manual", {
    method: "POST",
    token,
    body: JSON.stringify(payload)
  });
}

export function getHistory(token: string): Promise<HistoryItem[]> {
  return request<HistoryItem[]>("/analysis/history", { token });
}

export function getReport(token: string, reportId: string): Promise<ReportDetail> {
  return request<ReportDetail>(`/analysis/reports/${reportId}`, { token });
}

export function getSharedReport(shareToken: string): Promise<ReportDetail> {
  return request<ReportDetail>(`/analysis/shared/${shareToken}`);
}

export function createShareLink(token: string, reportId: string): Promise<ShareLinkResponse> {
  return request<ShareLinkResponse>(`/analysis/reports/${reportId}/share`, {
    method: "POST",
    token
  });
}

export async function downloadPdf(token: string, reportId: string): Promise<Blob> {
  const response = await fetch(`${API_BASE_URL}/analysis/reports/${reportId}/download-pdf`, {
    headers: {
      Authorization: `Bearer ${token}`
    }
  });

  if (!response.ok) {
    throw new Error("Unable to download the PDF report right now.");
  }

  return response.blob();
}

export { API_BASE_URL };

