"use client";

import type { HistoryItem, ReportDetail, User } from "@/lib/types";

type AuthCacheRecord = {
  token: string;
  mode: "account" | "public";
  user?: User | null;
};

const AUTH_STORAGE_KEY = "auto-analytics-ai-auth";
const HISTORY_CACHE_PREFIX = "auto-analytics-ai-history-cache";
const REPORT_CACHE_PREFIX = "auto-analytics-ai-report-cache";

function isBrowser(): boolean {
  return typeof window !== "undefined";
}

function readJson<T>(key: string): T | null {
  if (!isBrowser()) {
    return null;
  }

  try {
    const rawValue = window.localStorage.getItem(key);
    if (!rawValue) {
      return null;
    }
    return JSON.parse(rawValue) as T;
  } catch {
    return null;
  }
}

function writeJson(key: string, value: unknown): void {
  if (!isBrowser()) {
    return;
  }

  window.localStorage.setItem(key, JSON.stringify(value));
}

function getHistoryCacheKey(scope: string): string {
  return `${HISTORY_CACHE_PREFIX}:${scope}`;
}

function getReportCacheKey(scope: string, reportId: string): string {
  return `${REPORT_CACHE_PREFIX}:${scope}:${reportId}`;
}

export function readCachedAuthState(): AuthCacheRecord | null {
  return readJson<AuthCacheRecord>(AUTH_STORAGE_KEY);
}

export function writeCachedAuthState(record: AuthCacheRecord): void {
  writeJson(AUTH_STORAGE_KEY, record);
}

export function clearCachedAuthState(): void {
  if (!isBrowser()) {
    return;
  }

  window.localStorage.removeItem(AUTH_STORAGE_KEY);
}

export function readCachedHistory(scope: string): HistoryItem[] {
  return readJson<HistoryItem[]>(getHistoryCacheKey(scope)) ?? [];
}

export function writeCachedHistory(scope: string, items: HistoryItem[]): void {
  writeJson(getHistoryCacheKey(scope), items);
}

export function readCachedReport(scope: string, reportId: string): ReportDetail | null {
  return readJson<ReportDetail>(getReportCacheKey(scope, reportId));
}

export function writeCachedReport(scope: string, report: ReportDetail): void {
  writeJson(getReportCacheKey(scope, report.id), report);
}
