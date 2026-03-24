"use client";

import { useState } from "react";

import { ChartRenderer } from "@/components/charts/chart-renderer";
import { downloadPdf } from "@/lib/api";
import type { ReportDetail } from "@/lib/types";
import { copyToClipboard, formatDate, formatLabel, formatMetric } from "@/lib/utils";

type AnalysisDashboardProps = {
  report: ReportDetail;
  token?: string | null;
  isPublic?: boolean;
};

export function AnalysisDashboard({
  report,
  token = null,
  isPublic = false
}: AnalysisDashboardProps) {
  const [actionState, setActionState] = useState<"idle" | "copying" | "downloading">("idle");
  const payload = report.report;
  const modeling = payload.modeling;
  const metrics = modeling.metrics ?? {};
  const previewColumns = payload.overview.columns.map((item) => item.column);

  async function handleCopyShareLink() {
    setActionState("copying");
    await copyToClipboard(report.share_url);
    window.setTimeout(() => setActionState("idle"), 1400);
  }

  async function handleDownloadPdf() {
    if (!token) {
      return;
    }

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
    } finally {
      setActionState("idle");
    }
  }

  return (
    <div className="stack stack--xl">
      <section className="hero hero--report">
        <div className="hero__content">
          <div className="section-eyebrow">Automated analysis complete</div>
          <h1 className="page-title">{report.dataset_name}</h1>
          <p className="muted-copy">
            Generated on {formatDate(report.created_at)} from a {report.source_type} workflow.
            {report.target_column
              ? ` Target column: ${report.target_column}.`
              : " No target selected, so the app suggested clustering paths."}
          </p>
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
        </div>
        <div className="stats-grid stats-grid--compact">
          <article className="stat-card">
            <span>Rows</span>
            <strong>{payload.overview.row_count}</strong>
          </article>
          <article className="stat-card">
            <span>Columns</span>
            <strong>{payload.overview.column_count}</strong>
          </article>
          <article className="stat-card">
            <span>Charts</span>
            <strong>{payload.charts.length}</strong>
          </article>
          <article className="stat-card">
            <span>Model mode</span>
            <strong>{formatLabel(modeling.mode)}</strong>
          </article>
        </div>
      </section>

      <section className="dashboard-grid">
        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Insights</div>
              <h2>Plain-English takeaways</h2>
            </div>
          </div>
          <div className="chip-list">
            {payload.insights.map((insight) => (
              <div className="insight-card" key={insight}>
                {insight}
              </div>
            ))}
          </div>
        </article>

        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Recommendations</div>
              <h2>Suggested next actions</h2>
            </div>
          </div>
          <ul className="bullet-list">
            {payload.recommendations.map((recommendation) => (
              <li key={recommendation}>{recommendation}</li>
            ))}
          </ul>
        </article>
      </section>

      <section className="panel">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Data prep</div>
            <h2>Cleaning and profiling summary</h2>
          </div>
        </div>
        <div className="stats-grid">
          <article className="stat-card">
            <span>Missing values handled</span>
            <strong>{payload.cleaning.missing_values_before - payload.cleaning.missing_values_after}</strong>
          </article>
          <article className="stat-card">
            <span>Duplicate rows removed</span>
            <strong>{payload.cleaning.duplicate_rows_removed}</strong>
          </article>
          <article className="stat-card">
            <span>Empty rows dropped</span>
            <strong>{payload.cleaning.empty_rows_dropped}</strong>
          </article>
          <article className="stat-card">
            <span>Null-only columns removed</span>
            <strong>{payload.cleaning.removed_all_null_columns.length}</strong>
          </article>
        </div>
      </section>

      <section className="panel">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Charts</div>
            <h2>Auto-generated visuals</h2>
          </div>
        </div>
        <div className="chart-grid">
          {payload.charts.map((chart) => (
            <ChartRenderer chart={chart} key={chart.id} />
          ))}
        </div>
      </section>

      <section className="dashboard-grid">
        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Correlations</div>
              <h2>Strongest relationships</h2>
            </div>
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

        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Outliers and trends</div>
              <h2>Signals worth reviewing</h2>
            </div>
          </div>
          <div className="stack">
            {payload.outliers.length ? (
              <div className="chip-list">
                {payload.outliers.map((outlier) => (
                  <div className="insight-card" key={outlier.column}>
                    {outlier.column}: {outlier.count} outliers ({outlier.percentage}%)
                  </div>
                ))}
              </div>
            ) : (
              <p className="muted-copy">No significant outliers were detected by the IQR method.</p>
            )}
            <ul className="bullet-list">
              {payload.trends.map((trend) => (
                <li key={trend.column}>{trend.description}</li>
              ))}
            </ul>
          </div>
        </article>
      </section>

      <section className="dashboard-grid">
        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Modeling</div>
              <h2>Suggested ML path</h2>
            </div>
            <span className="pill">{formatLabel(modeling.status)}</span>
          </div>

          {modeling.status === "completed" ? (
            <div className="stack">
              <div className="stats-grid">
                {Object.entries(metrics).map(([metric, value]) => (
                  <article className="stat-card" key={metric}>
                    <span>{formatLabel(metric)}</span>
                    <strong>{formatMetric(value)}</strong>
                  </article>
                ))}
              </div>

              <div className="table-shell">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Suggested model</th>
                      <th>Why it fits</th>
                    </tr>
                  </thead>
                  <tbody>
                    {modeling.suggestions.map((suggestion) => (
                      <tr key={suggestion.name}>
                        <td>{suggestion.name}</td>
                        <td>{suggestion.rationale}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {modeling.feature_importance?.length ? (
                <div className="table-shell">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Feature</th>
                        <th>Importance</th>
                      </tr>
                    </thead>
                    <tbody>
                      {modeling.feature_importance.map((feature) => (
                        <tr key={feature.feature}>
                          <td>{feature.feature}</td>
                          <td>{formatMetric(feature.importance)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : null}
            </div>
          ) : (
            <p className="muted-copy">{modeling.reason ?? "Modeling recommendations are not available for this run."}</p>
          )}
        </article>

        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Dataset preview</div>
              <h2>First records after cleaning</h2>
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
        </article>
      </section>

      <section className="panel">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Statistics</div>
            <h2>Column summary table</h2>
          </div>
        </div>
        <div className="table-shell">
          <table className="data-table">
            <thead>
              <tr>
                <th>Column</th>
                <th>Type</th>
                <th>Unique values</th>
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
      </section>
    </div>
  );
}
