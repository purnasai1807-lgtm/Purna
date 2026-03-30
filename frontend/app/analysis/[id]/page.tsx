"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";

import { AnalysisDashboard } from "@/components/analysis/analysis-dashboard";
import { useAuth } from "@/components/providers/auth-provider";
import { LoadingSpinner } from "@/components/ui/loading-spinner";
import { getJobStatus, getReport } from "@/lib/api";
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

    const currentToken = token;
    const reportId = params.id;
    let isCancelled = false;
    let timeoutId: number | undefined;

    async function loadReport(showSpinner: boolean) {
      if (showSpinner) {
        setIsFetching(true);
      }

      try {
        const nextReport = await getReport(currentToken, reportId);
        if (isCancelled) {
          return;
        }

        setReport(nextReport);
        setError("");

        if (!["completed", "failed"].includes(nextReport.status)) {
          const jobId = nextReport.job_id;
          if (jobId) {
            timeoutId = window.setTimeout(() => void loadJobStatus(jobId), 3000);
          } else {
            timeoutId = window.setTimeout(() => void loadReport(false), 3000);
          }
        }
      } catch (reportError) {
        if (isCancelled) {
          return;
        }

        setError(
          reportError instanceof Error ? reportError.message : "Could not load this analysis report."
        );
      } finally {
        if (!isCancelled) {
          setIsFetching(false);
        }
      }
    }

    async function loadJobStatus(jobId: string) {
      try {
        const jobStatus = await getJobStatus(currentToken, jobId);
        if (isCancelled) {
          return;
        }

        setReport((currentReport) => {
          if (!currentReport) {
            return currentReport;
          }
          return {
            ...currentReport,
            dataset_name: jobStatus.dataset_name,
            status: jobStatus.status,
            progress: jobStatus.progress,
            progress_message: jobStatus.message ?? jobStatus.progress_message,
            processing_mode: jobStatus.processing_mode,
            file_type: jobStatus.file_type,
            file_size_bytes: jobStatus.file_size_bytes,
            error_message: jobStatus.error_message,
            report: jobStatus.result ?? currentReport.report
          };
        });

        if (["completed", "failed"].includes(jobStatus.status)) {
          await loadReport(false);
          return;
        }

        timeoutId = window.setTimeout(() => void loadJobStatus(jobId), 3000);
      } catch {
        if (!isCancelled) {
          timeoutId = window.setTimeout(() => void loadReport(false), 3000);
        }
      }
    }

    void loadReport(true);

    return () => {
      isCancelled = true;
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
    };
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
