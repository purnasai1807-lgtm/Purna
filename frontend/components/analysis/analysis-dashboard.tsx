"use client";

import { useEffect, useState } from "react";

import { ChartRenderer } from "@/components/charts/chart-renderer";
import {
  downloadPdf,
  getReportRows,
  getReportSection,
  getSharedReportRows,
  getSharedReportSection
} from "@/lib/api";
import type {
  ChartSpec,
  ModelingSummary,
  ReportDetail,
  ReportPayload,
  ReportRowsPage
} from "@/lib/types";
import {
  buildShareUrl,
  copyToClipboard,
  formatBytes,
  formatDate,
  formatLabel,
  formatMetric,
  formatStatus
} from "@/lib/utils";

type AnalysisDashboardProps = {
  report: ReportDetail;
  token?: string | null;
  isPublic?: boolean;
  shareToken?: string | null;
};

export function AnalysisDashboard({
  report,
  token = null,
  isPublic = false,
  shareToken = null
}: AnalysisDashboardProps) {
  const [actionState, setActionState] = useState<"idle" | "copying" | "downloading">("idle");
  const [actionError, setActionError] = useState("");
  const [charts, setCharts] = useState<ChartSpec[]>(report.report.charts ?? []);
  const [isChartsLoading, setIsChartsLoading] = useState(false);
  const [chartsError, setChartsError] = useState("");
  const [showExplorer, setShowExplorer] = useState(false);
  const [rowsPage, setRowsPage] = useState<ReportRowsPage | null>(null);
  const [rowsPageNumber, setRowsPageNumber] = useState(1);
  const [isRowsLoading, setIsRowsLoading] = useState(false);
  const [rowsError, setRowsError] = useState("");

  const payload = report.report;
  const modeling = payload.modeling;
  const previewColumns = payload.overview.columns.map((item) => item.column);
  const strongestCorrelation = getStrongestCorrelation(payload);
  const isProcessing = !["completed", "failed"].includes(report.status);
  const canLoadCharts = payload.sections.charts || charts.length > 0 || !isProcessing;
  const topMetrics = buildTopMetrics(payload, report, strongestCorrelation);

  useEffect(() => {
    setCharts(report.report.charts ?? []);
    setRowsPage(null);
    setRowsPageNumber(1);
    setShowExplorer(false);
    setChartsError("");
    setRowsError("");
  }, [report.id, report.report.charts]);

  useEffect(() => {
    if (!showExplorer) {
      return;
    }

    let isCancelled = false;

    async function loadRows() {
      setIsRowsLoading(true);
      setRowsError("");
      try {
        const nextPage = token
          ? await getReportRows(token, report.id, rowsPageNumber, 25)
          : await getSharedReportRows(shareToken ?? report.share_token, rowsPageNumber, 25);
        if (!isCancelled) {
          setRowsPage(nextPage);
        }
      } catch (error) {
        if (!isCancelled) {
          setRowsError(error instanceof Error ? error.message : "Could not load table rows.");
        }
      } finally {
        if (!isCancelled) {
          setIsRowsLoading(false);
        }
      }
    }

    void loadRows();
    return () => {
      isCancelled = true;
    };
  }, [report.id, report.share_token, rowsPageNumber, shareToken, showExplorer, token]);

  async function handleCopyShareLink() {
    setActionError("");
    setActionState("copying");
    await copyToClipboard(buildShareUrl(report.share_token, report.share_url));
    window.setTimeout(() => setActionState("idle"), 1400);
  }

  async function handleDownloadPdf() {
    if (!token) {
      return;
    }

    setActionError("");
    setActionState("downloading");
    try {
      const blob = await downloadPdf(token, report.id);
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `${report.dataset_name.replace(/\s+/g, "-").toLowerCase()}-report.pdf`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (downloadError) {
      setActionError(
        downloadError instanceof Error
          ? downloadError.message
          : "We could not download the PDF right now."
      );
    } finally {
      setActionState("idle");
    }
  }

  async function handleLoadCharts() {
    if (isChartsLoading) {
      return;
    }

    setIsChartsLoading(true);
    setChartsError("");
    try {
      const response = token
        ? await getReportSection<ChartSpec[]>(token, report.id, "charts")
        : await getSharedReportSection<ChartSpec[]>(shareToken ?? report.share_token, "charts");
      setCharts(response.data);
    } catch (error) {
      setChartsError(error instanceof Error ? error.message : "Could not load charts.");
    } finally {
      setIsChartsLoading(false);
    }
  }

  return (
    <div className="analysis-dashboard stack stack--xl">
      {actionError ? <div className="notice notice--error">{actionError}</div> : null}

      {(payload.metadata.is_preview || isProcessing) && (
        <div className="notice notice--info">
          {report.progress_message ?? "Quick preview analytics are ready."} Full analytics continue in the background,
          so some metrics and tables may still refresh as processing finishes.
        </div>
      )}

      {payload.metadata.optimized_mode ? (
        <div className="notice notice--warning">
          Large dataset detected. Processing in background with optimized mode.
          {report.job_id ? ` Job ID: ${report.job_id}.` : ""}
        </div>
      ) : null}

      {report.error_message ? <div className="notice notice--error">{report.error_message}</div> : null}

      <section className="analytics-hero">
        <article className="panel analytics-hero__panel">
          <div className="analytics-hero__topline">
            <div className="section-eyebrow">Large dataset report</div>
            <span className="pill">{formatStatus(report.status)}</span>
          </div>

          <h1 className="page-title analytics-title">{report.dataset_name}</h1>
          <p className="muted-copy analytics-hero__copy">
            Generated on {formatDate(report.created_at)} from a {report.source_type} workflow.
            {report.target_column ? ` Target column: ${report.target_column}.` : " Exploratory mode is active."}
          </p>

          <div className="analytics-badge-row">
            <span className="analytics-badge">{formatStatus(payload.metadata.processing_mode)}</span>
            <span className="analytics-badge">{formatStatus(payload.metadata.file_type ?? report.source_type)}</span>
            <span className="analytics-badge">{formatBytes(payload.metadata.file_size_bytes)}</span>
            <span className="analytics-badge">
              {payload.metadata.is_preview ? "Preview analytics" : "Full analytics"}
            </span>
          </div>

          {isProcessing ? (
            <div className="progress-card">
              <div className="progress-card__meta">
                <span>{report.progress_message ?? "Processing analytics..."}</span>
                <strong>{report.progress}%</strong>
              </div>
              <div className="progress-track">
                <span className="progress-fill" style={{ width: `${report.progress}%` }} />
              </div>
            </div>
          ) : null}

          <div className="button-row">
            <button type="button" className="button button--primary" onClick={handleCopyShareLink}>
              {actionState === "copying" ? "Share link copied" : "Copy share link"}
            </button>
            {!isPublic && token ? (
              <button type="button" className="button button--secondary" onClick={handleDownloadPdf}>
                {actionState === "downloading" ? "Preparing PDF..." : "Download PDF"}
              </button>
            ) : null}
          </div>
        </article>

        <aside className="analytics-kpi-rail analytics-kpi-rail--compact">
          {topMetrics.map((metric) => (
            <article className={`metric-tile metric-tile--${metric.tone}`} key={metric.label}>
              <span className="metric-tile__label">{metric.label}</span>
              <strong className="metric-tile__value">{metric.value}</strong>
              <p className="metric-tile__note">{metric.note}</p>
            </article>
          ))}
        </aside>
      </section>

      <section className="analytics-summary-grid analytics-summary-grid--two">
        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Insights</div>
              <h2>Decision-ready takeaways</h2>
            </div>
          </div>
          <div className="highlight-grid">
            {payload.insights.length ? (
              payload.insights.slice(0, 6).map((insight) => (
                <div className="analytics-note-card" key={insight}>
                  {insight}
                </div>
              ))
            ) : (
              <div className="analytics-note-card">The analytics engine did not generate narrative highlights yet.</div>
            )}
          </div>
        </article>

        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Recommendations</div>
              <h2>Suggested next moves</h2>
            </div>
          </div>
          {payload.recommendations.length ? (
            <ul className="bullet-list">
              {payload.recommendations.map((recommendation) => (
                <li key={recommendation}>{recommendation}</li>
              ))}
            </ul>
          ) : (
            <p className="muted-copy">No additional recommendations were generated for this dataset.</p>
          )}
        </article>
      </section>

      <section className="analytics-detail-grid">
        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Statistics</div>
              <h2>Column summary table</h2>
            </div>
            <span className="pill">{payload.summary_statistics.length} columns</span>
          </div>
          <div className="table-shell">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Column</th>
                  <th>Type</th>
                  <th>Unique</th>
                  <th>Mean / Top value</th>
                  <th>Range</th>
                </tr>
              </thead>
              <tbody>
                {payload.summary_statistics.map((stat) => (
                  <tr key={stat.column}>
                    <td>{stat.column}</td>
                    <td>{stat.dtype}</td>
                    <td>{stat.unique_values}</td>
                    <td>{stat.mean !== undefined ? formatMetric(stat.mean) : stat.top_value ?? "-"}</td>
                    <td>
                      {stat.min !== undefined || stat.max !== undefined
                        ? `${formatMetric(stat.min ?? null)} to ${formatMetric(stat.max ?? null)}`
                        : "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>

        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Signals</div>
              <h2>Correlations, outliers, and trends</h2>
            </div>
          </div>

          <div className="signal-card-grid">
            <article className="signal-card">
              <span>Strongest correlation</span>
              <strong>
                {strongestCorrelation ? strongestCorrelation.correlation.toFixed(2) : "N/A"}
              </strong>
              <p>
                {strongestCorrelation
                  ? `${strongestCorrelation.left_column} x ${strongestCorrelation.right_column}`
                  : "Need at least two numeric columns."}
              </p>
            </article>
            <article className="signal-card">
              <span>Outlier columns</span>
              <strong>{payload.outliers.length}</strong>
              <p>{payload.outliers[0]?.column ?? "No strong outliers flagged."}</p>
            </article>
            <article className="signal-card">
              <span>Trend stories</span>
              <strong>{payload.trends.length}</strong>
              <p>{payload.trends[0]?.description ?? "No trend narrative was generated yet."}</p>
            </article>
          </div>

          {payload.correlations.available ? (
            <div className="table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Column A</th>
                    <th>Column B</th>
                    <th>Correlation</th>
                  </tr>
                </thead>
                <tbody>
                  {payload.correlations.strongest_pairs.map((pair) => (
                    <tr key={`${pair.left_column}-${pair.right_column}`}>
                      <td>{pair.left_column}</td>
                      <td>{pair.right_column}</td>
                      <td>{pair.correlation}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="muted-copy">Add at least two numeric columns to unlock correlation analysis.</p>
          )}
        </article>
      </section>

      <section className="panel panel--chart-stage">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Visual board</div>
            <h2>Lazy-loaded analytics charts</h2>
          </div>
          <div className="button-row">
            <button
              type="button"
              className="button button--secondary"
              onClick={handleLoadCharts}
              disabled={isChartsLoading || (!canLoadCharts && isProcessing)}
            >
              {isChartsLoading ? "Loading charts..." : charts.length ? "Refresh charts" : "Load charts"}
            </button>
            <span className="pill">{charts.length} visuals</span>
          </div>
        </div>

        {chartsError ? <div className="notice notice--error">{chartsError}</div> : null}

        {charts.length ? (
          <div className="chart-grid chart-grid--analytics">
            {charts.map((chart) => (
              <ChartRenderer chart={chart} key={chart.id} />
            ))}
          </div>
        ) : (
          <div className="lazy-shell">
            <p className="muted-copy">
              Charts are generated only when requested so very large reports stay fast and stable.
            </p>
            {!canLoadCharts && isProcessing ? (
              <p className="muted-copy">The preview is still indexing data for chart generation.</p>
            ) : null}
          </div>
        )}
      </section>

      <section className="panel">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Data explorer</div>
            <h2>Paginated backend rows</h2>
          </div>
          <button
            type="button"
            className="button button--secondary"
            onClick={() => setShowExplorer((current) => !current)}
          >
            {showExplorer ? "Hide table" : "Open table"}
          </button>
        </div>

        {showExplorer ? (
          <div className="stack">
            {rowsError ? <div className="notice notice--error">{rowsError}</div> : null}
            {rowsPage?.is_preview ? (
              <div className="notice notice--warning">
                The backend is still indexing the full file, so this table currently shows preview rows.
              </div>
            ) : null}
            {isRowsLoading ? (
              <p className="muted-copy">Loading table page...</p>
            ) : null}
            {rowsPage ? (
              <>
                <div className="table-shell">
                  <table className="data-table">
                    <thead>
                      <tr>
                        {rowsPage.columns.map((column) => (
                          <th key={column}>{column}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rowsPage.rows.map((row, index) => (
                        <tr key={`row-${rowsPage.page}-${index}`}>
                          {rowsPage.columns.map((column) => (
                            <td key={`${index}-${column}`}>{String(row[column] ?? "-")}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="pagination-bar">
                  <span>
                    Page {rowsPage.page} of {rowsPage.total_pages}
                  </span>
                  <span>{rowsPage.total_rows.toLocaleString()} total rows</span>
                  <div className="button-row">
                    <button
                      type="button"
                      className="button button--ghost"
                      disabled={rowsPage.page <= 1 || isRowsLoading}
                      onClick={() => setRowsPageNumber((current) => Math.max(current - 1, 1))}
                    >
                      Previous
                    </button>
                    <button
                      type="button"
                      className="button button--ghost"
                      disabled={rowsPage.page >= rowsPage.total_pages || isRowsLoading}
                      onClick={() => setRowsPageNumber((current) => current + 1)}
                    >
                      Next
                    </button>
                  </div>
                </div>
              </>
            ) : null}
          </div>
        ) : (
          <p className="muted-copy">
            Large datasets stay out of browser memory. Open the table only when you need a paginated slice.
          </p>
        )}
      </section>

      <section className="panel">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Model center</div>
            <h2>Suggested ML strategy</h2>
          </div>
          <span className="pill">{formatStatus(modeling.status)}</span>
        </div>
        <ModelSummary modeling={modeling} />
      </section>

      <section className="panel">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Preview</div>
            <h2>Sample rows from the cleaned dataset</h2>
          </div>
        </div>
        <div className="table-shell">
          <table className="data-table">
            <thead>
              <tr>
                {previewColumns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {payload.overview.preview_rows.slice(0, 8).map((row, index) => (
                <tr key={`preview-${index}`}>
                  {previewColumns.map((column) => (
                    <td key={`${index}-${column}`}>{String(row[column] ?? "-")}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function ModelSummary({ modeling }: { modeling: ModelingSummary }) {
  const metrics = modeling.metrics ?? {};

  if (modeling.status !== "completed") {
    return (
      <p className="muted-copy">{modeling.reason ?? "Modeling recommendations are not available for this run."}</p>
    );
  }

  return (
    <div className="stack">
      <div className="model-metric-grid">
        {Object.entries(metrics).map(([metric, value]) => (
          <article className="model-metric-card" key={metric}>
            <span>{formatLabel(metric)}</span>
            <strong>{formatMetric(value)}</strong>
          </article>
        ))}
      </div>
      <div className="model-suggestion-grid">
        {modeling.suggestions.map((suggestion) => (
          <article className="model-suggestion-card" key={suggestion.name}>
            <span>Suggested path</span>
            <strong>{suggestion.name}</strong>
            <p>{suggestion.rationale}</p>
          </article>
        ))}
      </div>
    </div>
  );
}

function getStrongestCorrelation(
  payload: ReportPayload
): ReportPayload["correlations"]["strongest_pairs"][number] | null {
  if (!payload.correlations.available || !payload.correlations.strongest_pairs.length) {
    return null;
  }

  return payload.correlations.strongest_pairs.reduce((best, current) =>
    Math.abs(current.correlation) > Math.abs(best.correlation) ? current : best
  );
}

function buildTopMetrics(
  payload: ReportPayload,
  report: ReportDetail,
  strongestCorrelation: ReportPayload["correlations"]["strongest_pairs"][number] | null
) {
  return [
    {
      label: "Rows",
      value: report.row_count.toLocaleString(),
      note: `${report.column_count.toLocaleString()} columns profiled`,
      tone: "plum" as const
    },
    {
      label: "Missing values",
      value: payload.cleaning.missing_values_before.toLocaleString(),
      note: "Across the current analytics profile",
      tone: "gold" as const
    },
    {
      label: "Strongest pair",
      value: strongestCorrelation ? strongestCorrelation.correlation.toFixed(2) : "N/A",
      note: strongestCorrelation
        ? `${strongestCorrelation.left_column} x ${strongestCorrelation.right_column}`
        : "Need two numeric columns",
      tone: "sky" as const
    },
    {
      label: "Progress",
      value: `${report.progress}%`,
      note: report.progress_message ?? "Analytics state",
      tone: "mint" as const
    }
  ];
}
