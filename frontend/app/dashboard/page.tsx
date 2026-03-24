"use client";

import { startTransition, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

import { DatasetUploadCard } from "@/components/dashboard/dataset-upload-card";
import { ManualEntryCard } from "@/components/dashboard/manual-entry-card";
import { ReportHistory } from "@/components/dashboard/report-history";
import { useAuth } from "@/components/providers/auth-provider";
import { LoadingSpinner } from "@/components/ui/loading-spinner";
import { getHistory } from "@/lib/api";
import type { HistoryItem, ReportDetail } from "@/lib/types";

export default function DashboardPage() {
  const router = useRouter();
  const { user, token, isLoading } = useAuth();
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [isFetching, setIsFetching] = useState(true);
  const [error, setError] = useState("");

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

    setIsFetching(true);
    getHistory(token)
      .then((items) => setHistory(items))
      .catch((historyError) =>
        setError(
          historyError instanceof Error
            ? historyError.message
            : "Could not load your report history."
        )
      )
      .finally(() => setIsFetching(false));
  }, [token]);

  function handleCreated(report: ReportDetail) {
    setHistory((currentHistory) => [report, ...currentHistory]);
    startTransition(() => {
      router.push(`/analysis/${report.id}`);
    });
  }

  if (isLoading || (token && isFetching)) {
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
              Upload a dataset or build one manually. The system will clean it, analyze it, generate charts, recommend models, and save the full report history for later.
            </p>
          </div>
          <div className="hero-card">
            <div className="hero-card__row">
              <span>Sample dataset</span>
              <strong>
                <a href="/sample-datasets/retail-performance.csv">Retail performance CSV</a>
              </strong>
            </div>
            <div className="hero-card__row">
              <span>Saved reports</span>
              <strong>{history.length}</strong>
            </div>
            <div className="hero-card__row">
              <span>Public sharing</span>
              <strong>Built into every saved result</strong>
            </div>
          </div>
        </section>

        {error ? <div className="notice notice--error">{error}</div> : null}

        <section className="dashboard-grid dashboard-grid--composer">
          <DatasetUploadCard token={token} onCreated={handleCreated} />
          <ManualEntryCard token={token} onCreated={handleCreated} />
        </section>

        <ReportHistory items={history} />
      </div>
    </main>
  );
}

