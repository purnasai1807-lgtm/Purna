"use client";

import { startTransition, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

import { DatasetUploadCard } from "@/components/dashboard/dataset-upload-card";
import { ManualEntryCard } from "@/components/dashboard/manual-entry-card";
import { ReportHistory } from "@/components/dashboard/report-history";
import { useAuth } from "@/components/providers/auth-provider";
import { LoadingSpinner } from "@/components/ui/loading-spinner";
import { getHistory } from "@/lib/api";
import { readCachedHistory, writeCachedHistory, writeCachedReport } from "@/lib/client-cache";
import type { HistoryItem, ReportDetail } from "@/lib/types";

export default function DashboardPage() {
  const router = useRouter();
  const { user, token, authMode, isLoading } = useAuth();
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [isFetching, setIsFetching] = useState(true);
  const [error, setError] = useState("");
  const cacheScope = `${authMode ?? "unknown"}:${user?.id ?? "anonymous"}`;

  useEffect(() => {
    if (!isLoading && !token) {
      router.replace("/auth");
    }
  }, [isLoading, router, token]);

  useEffect(() => {
    if (!token) {
      setIsFetching(false);
      return;
    }

    const cachedHistory = readCachedHistory(cacheScope);
    if (cachedHistory.length) {
      setHistory(cachedHistory);
    }

    const currentToken = token;
    let isCancelled = false;
    let timeoutId: number | undefined;

    async function loadHistory(showSpinner: boolean) {
      if (showSpinner) {
        setIsFetching(true);
      }

      try {
        const items = await getHistory(currentToken);
        if (isCancelled) {
          return;
        }
        setHistory(items);
        writeCachedHistory(cacheScope, items);
        setError("");

        if (items.some((item) => !["completed", "failed"].includes(item.status))) {
          timeoutId = window.setTimeout(() => void loadHistory(false), 3500);
        }
      } catch (historyError) {
        if (isCancelled) {
          return;
        }
        setError(
          historyError instanceof Error
            ? historyError.message
            : "Could not load your report history."
        );
      } finally {
        if (!isCancelled) {
          setIsFetching(false);
        }
      }
    }

    void loadHistory(true);

    return () => {
      isCancelled = true;
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [cacheScope, token]);

  function handleCreated(report: ReportDetail) {
    setHistory((currentHistory) => {
      const nextHistory = [report, ...currentHistory.filter((item) => item.id !== report.id)];
      writeCachedHistory(cacheScope, nextHistory);
      return nextHistory;
    });
    writeCachedReport(cacheScope, report);
    startTransition(() => {
      router.push(`/analysis/${report.id}`);
    });
  }

  if (isLoading && !token) {
    return (
      <main className="page-shell page-shell--centered">
        <LoadingSpinner label="Loading your analytics workspace..." />
      </main>
    );
  }

  if (!token) {
    return null;
  }

  return (
    <main className="page-shell">
      <div className="shell stack stack--xl">
        <section className="hero hero--dashboard">
          <div className="hero__content">
            <div className="section-eyebrow">Workspace</div>
            <h1 className="page-title">Welcome back, {user?.full_name ?? "there"}.</h1>
            <p className="lead-copy">
              Upload a dataset or build one manually. The system now stages large files in backend storage, generates
              a quick preview first, then finishes deeper analytics in the background without freezing the browser.
              Files under 10 MB run directly, 10 MB to 50 MB use chunk-based processing, and larger uploads switch to
              optimized mode.
            </p>
          </div>
          <div className="hero-card">
            <div className="hero-card__row">
              <span>Supported uploads</span>
              <strong>CSV, Excel, JSON</strong>
            </div>
            <div className="hero-card__row">
              <span>Saved reports</span>
              <strong>{history.length}</strong>
            </div>
            <div className="hero-card__row">
              <span>Large file pipeline</span>
              <strong>10 MB direct, 10-50 MB chunked, 50+ MB optimized</strong>
            </div>
          </div>
        </section>

        {error ? <div className="notice notice--error">{error}</div> : null}

        <section className="dashboard-grid dashboard-grid--composer">
          <DatasetUploadCard token={token} onCreated={handleCreated} />
          <ManualEntryCard token={token} onCreated={handleCreated} />
        </section>

        <ReportHistory items={history} isLoading={isFetching} />
      </div>
    </main>
  );
}
