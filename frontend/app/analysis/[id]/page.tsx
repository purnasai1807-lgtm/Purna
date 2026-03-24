"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";

import { AnalysisDashboard } from "@/components/analysis/analysis-dashboard";
import { useAuth } from "@/components/providers/auth-provider";
import { LoadingSpinner } from "@/components/ui/loading-spinner";
import { getReport } from "@/lib/api";
import type { ReportDetail } from "@/lib/types";

export default function AnalysisPage() {
  const params = useParams<{ id: string }>();
  const router = useRouter();
  const { token, isLoading } = useAuth();
  const [report, setReport] = useState<ReportDetail | null>(null);
  const [error, setError] = useState("");
  const [isFetching, setIsFetching] = useState(true);

  useEffect(() => {
    if (!isLoading && !token) {
      router.replace("/auth");
    }
  }, [isLoading, router, token]);

  useEffect(() => {
    if (!token || !params?.id) {
      setIsFetching(false);
      return;
    }

    setIsFetching(true);
    getReport(token, params.id)
      .then((nextReport) => setReport(nextReport))
      .catch((reportError) =>
        setError(
          reportError instanceof Error ? reportError.message : "Could not load this analysis report."
        )
      )
      .finally(() => setIsFetching(false));
  }, [params?.id, token]);

  if (isLoading || isFetching) {
    return (
      <main className="page-shell page-shell--centered">
        <LoadingSpinner label="Loading analysis report..." />
      </main>
    );
  }

  if (!token) {
    return null;
  }

  return (
    <main className="page-shell">
      <div className="shell">
        {error ? <div className="notice notice--error">{error}</div> : null}
        {report ? <AnalysisDashboard report={report} token={token} /> : null}
      </div>
    </main>
  );
}

