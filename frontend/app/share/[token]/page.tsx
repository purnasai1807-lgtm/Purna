"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";

import { AnalysisDashboard } from "@/components/analysis/analysis-dashboard";
import { LoadingSpinner } from "@/components/ui/loading-spinner";
import { getSharedReport } from "@/lib/api";
import type { ReportDetail } from "@/lib/types";

export default function SharedReportPage() {
  const params = useParams<{ token: string }>();
  const [report, setReport] = useState<ReportDetail | null>(null);
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!params?.token) {
      setIsLoading(false);
      return;
    }

    let isCancelled = false;
    let timeoutId: number | undefined;

    async function loadSharedReport(showSpinner: boolean) {
      if (showSpinner) {
        setIsLoading(true);
      }

      try {
        const nextReport = await getSharedReport(params.token);
        if (isCancelled) {
          return;
        }

        setReport(nextReport);
        setError("");

        if (!["completed", "failed"].includes(nextReport.status)) {
          timeoutId = window.setTimeout(() => void loadSharedReport(false), 3500);
        }
      } catch (sharedError) {
        if (isCancelled) {
          return;
        }

        setError(
          sharedError instanceof Error ? sharedError.message : "This shared report could not be loaded."
        );
      } finally {
        if (!isCancelled) {
          setIsLoading(false);
        }
      }
    }

    void loadSharedReport(true);

    return () => {
      isCancelled = true;
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [params?.token]);

  if (isLoading) {
    return (
      <main className="page-shell page-shell--centered">
        <LoadingSpinner label="Opening shared report..." />
      </main>
    );
  }

  return (
    <main className="page-shell">
      <div className="shell">
        {error ? <div className="notice notice--error">{error}</div> : null}
        {report ? <AnalysisDashboard report={report} isPublic shareToken={params?.token ?? null} /> : null}
      </div>
    </main>
  );
}
