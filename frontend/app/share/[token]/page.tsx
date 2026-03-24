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

    getSharedReport(params.token)
      .then((nextReport) => setReport(nextReport))
      .catch((sharedError) =>
        setError(
          sharedError instanceof Error ? sharedError.message : "This shared report could not be loaded."
        )
      )
      .finally(() => setIsLoading(false));
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
        {report ? <AnalysisDashboard report={report} isPublic /> : null}
      </div>
    </main>
  );
}
