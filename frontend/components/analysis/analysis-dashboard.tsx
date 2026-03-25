"use client";

import { useState } from "react";

import { ChartRenderer } from "@/components/charts/chart-renderer";
import { downloadPdf } from "@/lib/api";
import type { ModelingSummary, ReportDetail, ReportPayload } from "@/lib/types";
import { buildShareUrl, copyToClipboard, formatDate, formatLabel, formatMetric } from "@/lib/utils";

type AnalysisDashboardProps = {
  report: ReportDetail;
  token?: string | null;
  isPublic?: boolean;
};

type ExecutiveMetric = {
  label: string;
  value: string;
  note: string;
  tone: "plum" | "gold" | "sky" | "mint";
};

type HealthMetric = {
  label: string;
  value: string;
  percentage: number;
  tone: "plum" | "gold" | "sky" | "mint";
};

type RankingRow = {
  label: string;
  value: string;
  caption: string;
  width: number;
};

export function AnalysisDashboard({
  report,
  token = null,
  isPublic = false
}: AnalysisDashboardProps) {
  const [actionState, setActionState] = useState<"idle" | "copying" | "downloading">("idle");
  const [actionError, setActionError] = useState("");
  const payload = report.report;
  const modeling = payload.modeling;
  const metrics = modeling.metrics ?? {};
  const previewColumns = payload.overview.columns.map((item) => item.column);
  const highlightedInsights = payload.insights.slice(0, 4);
  const recommendedActions = payload.recommendations.slice(0, 5);
  const strongestCorrelation = getStrongestCorrelation(payload);
  const primaryMetric = getPrimaryMetric(modeling);
  const executiveMetrics = buildExecutiveMetrics(payload, modeling, strongestCorrelation, primaryMetric);
  const healthMetrics = buildHealthMetrics(payload);
  const rankingRows = buildRankingRows(modeling);
  const signalCards = buildSignalCards(payload, strongestCorrelation);
  const selectedModelLabel = modeling.selected_model ?? formatLabel(modeling.mode);

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

  return (
    <div className="analysis-dashboard stack stack--xl">
      {actionError ? <div className="notice notice--error">{actionError}</div> : null}

      <section className="analytics-hero">
        <article className="panel analytics-hero__panel">
          <div className="analytics-hero__topline">
            <div className="section-eyebrow">Executive analytics board</div>
            <span className="pill">{formatLabel(modeling.status)}</span>
          </div>

          <h1 className="page-title analytics-title">{report.dataset_name}</h1>
          <p className="muted-copy analytics-hero__copy">
            Generated on {formatDate(report.created_at)} from a {report.source_type} workflow.
            {report.target_column
              ? ` Target column: ${report.target_column}.`
              : " No target selected, so the app explored cluster-ready patterns."}
          </p>

          <div className="analytics-badge-row">
            <span className="analytics-badge">{formatLabel(report.source_type)} source</span>
            <span className="analytics-badge">
              {report.target_column ? `Target: ${report.target_column}` : "Exploratory analytics"}
            </span>
            <span className="analytics-badge">{selectedModelLabel}</span>
            {isPublic ? (
              <span className="analytics-badge analytics-badge--secondary">Shared view</span>
            ) : null}
          </div>

          <div className="analytics-story-grid">
            <article className="analytics-story-card">
              <span>Top narrative</span>
              <strong>{highlightedInsights[0] ?? "Automated analytics summary is ready for review."}</strong>
            </article>
            <article className="analytics-story-card">
              <span>Relationship spotlight</span>
              <strong>
                {strongestCorrelation
                  ? `${strongestCorrelation.left_column} and ${strongestCorrelation.right_column} move together.`
                  : "Add at least two numeric columns to unlock relationship scoring."}
              </strong>
            </article>
            <article className="analytics-story-card">
              <span>Model direction</span>
              <strong>
                {modeling.status === "completed"
                  ? `${selectedModelLabel} is the current recommended ML path.`
                  : modeling.reason ?? "Model guidance is limited for this run."}
              </strong>
            </article>
          </div>

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

        <aside className="analytics-kpi-rail">
          {executiveMetrics.map((metric) => (
            <article className={`metric-tile metric-tile--${metric.tone}`} key={metric.label}>
              <span className="metric-tile__label">{metric.label}</span>
              <strong className="metric-tile__value">{metric.value}</strong>
              <p className="metric-tile__note">{metric.note}</p>
            </article>
          ))}
        </aside>
      </section>

      <section className="analytics-summary-grid">
        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Insights</div>
              <h2>Decision-ready takeaways</h2>
            </div>
          </div>
          <div className="highlight-grid">
            {highlightedInsights.length ? (
              highlightedInsights.map((insight) => (
                <div className="analytics-note-card" key={insight}>
                  {insight}
                </div>
              ))
            ) : (
              <div className="analytics-note-card">
                The analytics engine did not generate narrative highlights for this run, but the charts and summary
                tables are still available below.
              </div>
            )}
          </div>
        </article>

        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Data health</div>
              <h2>Cleaning and retention score</h2>
            </div>
          </div>
          <div className="health-stack">
            {healthMetrics.map((item) => (
              <div className="health-row" key={item.label}>
                <div className="health-row__meta">
                  <span>{item.label}</span>
                  <strong>{item.value}</strong>
                </div>
                <div className="health-row__track">
                  <span
                    className={`health-row__fill health-row__fill--${item.tone}`}
                    style={{ width: `${item.percentage}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </article>

        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Recommendations</div>
              <h2>Suggested next moves</h2>
            </div>
          </div>
          {recommendedActions.length ? (
            <ul className="bullet-list">
              {recommendedActions.map((recommendation) => (
                <li key={recommendation}>{recommendation}</li>
              ))}
            </ul>
          ) : (
            <p className="muted-copy">No additional recommendations were generated for this dataset.</p>
          )}
        </article>
      </section>

      <section className="panel panel--chart-stage">
        <div className="panel__header">
          <div>
            <div className="section-eyebrow">Visual board</div>
            <h2>Auto-generated analytics visuals</h2>
          </div>
          <span className="pill">{payload.charts.length} visuals</span>
        </div>
        <div className="chart-grid chart-grid--analytics">
          {payload.charts.map((chart) => (
            <ChartRenderer chart={chart} key={chart.id} />
          ))}
        </div>
      </section>

      <section className="analytics-detail-grid">
        <article className="panel">
          <div className="panel__header">
            <div>
              <div className="section-eyebrow">Model center</div>
              <h2>Suggested ML strategy</h2>
            </div>
            <span className="pill">{formatLabel(modeling.status)}</span>
          </div>

          {modeling.status === "completed" ? (
            <div className="stack">
              <div className="model-hero">
                <div className="model-hero__copy">
                  <span className="model-hero__label">Recommended model</span>
                  <strong>{selectedModelLabel}</strong>
                  <p className="muted-copy">
                    {modeling.notes ?? "The analytics engine picked a practical baseline model for this dataset."}
                  </p>
                </div>
                <div className="model-metric-grid">
                  {Object.entries(metrics).map(([metric, value]) => (
                    <article className="model-metric-card" key={metric}>
                      <span>{formatLabel(metric)}</span>
                      <strong>{formatMetric(value)}</strong>
                    </article>
                  ))}
                </div>
              </div>

              <div className="model-suggestion-grid">
                {modeling.suggestions.map((suggestion, index) => (
                  <article className="model-suggestion-card" key={suggestion.name}>
                    <span>Option {index + 1}</span>
                    <strong>{suggestion.name}</strong>
                    <p>{suggestion.rationale}</p>
                  </article>
                ))}
              </div>

              {rankingRows.length ? (
                <div className="rank-list">
                  {rankingRows.map((item) => (
                    <div className="rank-row" key={item.label}>
                      <div className="rank-row__copy">
                        <span>{item.label}</span>
                        <strong>{item.value}</strong>
                      </div>
                      <div className="rank-row__track">
                        <span className="rank-row__fill" style={{ width: `${item.width}%` }} />
                      </div>
                      <small>{item.caption}</small>
                    </div>
                  ))}
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
              <div className="section-eyebrow">Signals</div>
              <h2>Relationships, outliers, and trends</h2>
            </div>
          </div>

          <div className="signal-card-grid">
            {signalCards.map((signal) => (
              <article className="signal-card" key={signal.label}>
                <span>{signal.label}</span>
                <strong>{signal.value}</strong>
                <p>{signal.note}</p>
              </article>
            ))}
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
                  {payload.correlations.strongest_pairs.slice(0, 5).map((pair) => (
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

          <div className="analytics-signal-stack">
            {payload.outliers.length ? (
              <div className="highlight-grid">
                {payload.outliers.slice(0, 4).map((outlier) => (
                  <div className="analytics-note-card" key={outlier.column}>
                    {outlier.column}: {outlier.count} outliers ({outlier.percentage}%)
                  </div>
                ))}
              </div>
            ) : (
              <p className="muted-copy">No significant outliers were detected by the IQR method.</p>
            )}

            <ul className="bullet-list">
              {payload.trends.slice(0, 5).map((trend) => (
                <li key={trend.column}>{trend.description}</li>
              ))}
            </ul>
          </div>
        </article>
      </section>

      <section className="panel">
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

function buildExecutiveMetrics(
  payload: ReportPayload,
  modeling: ModelingSummary,
  strongestCorrelation: ReportPayload["correlations"]["strongest_pairs"][number] | null,
  primaryMetric: { label: string; value: string; note: string } | null
): ExecutiveMetric[] {
  return [
    {
      label: "Rows analyzed",
      value: formatCompactValue(payload.overview.row_count),
      note: `${payload.overview.original_row_count} original records`,
      tone: "plum"
    },
    {
      label: "Columns profiled",
      value: formatCompactValue(payload.overview.column_count),
      note: `${payload.charts.length} dashboard visuals ready`,
      tone: "gold"
    },
    {
      label: "Missing values fixed",
      value: formatCompactValue(
        payload.cleaning.missing_values_before - payload.cleaning.missing_values_after
      ),
      note: `${formatPercent(
        payload.cleaning.missing_values_before === 0
          ? 1
          : (payload.cleaning.missing_values_before - payload.cleaning.missing_values_after) /
              payload.cleaning.missing_values_before
      )} resolution rate`,
      tone: "sky"
    },
    {
      label: "Duplicates removed",
      value: formatCompactValue(payload.cleaning.duplicate_rows_removed),
      note: `${payload.cleaning.empty_rows_dropped} empty rows dropped`,
      tone: "mint"
    },
    {
      label: "Strongest correlation",
      value: strongestCorrelation ? formatSignedValue(strongestCorrelation.correlation) : "N/A",
      note: strongestCorrelation
        ? `${strongestCorrelation.left_column} x ${strongestCorrelation.right_column}`
        : "Need two numeric columns",
      tone: "plum"
    },
    {
      label: primaryMetric?.label ?? "Model status",
      value: primaryMetric?.value ?? formatLabel(modeling.status),
      note: primaryMetric?.note ?? `Mode: ${formatLabel(modeling.mode)}`,
      tone: "gold"
    }
  ];
}

function buildHealthMetrics(payload: ReportPayload): HealthMetric[] {
  const rowRetention = safeRatio(
    payload.cleaning.cleaned_shape.rows,
    payload.cleaning.original_shape.rows
  );
  const columnRetention = safeRatio(
    payload.cleaning.cleaned_shape.columns,
    payload.cleaning.original_shape.columns
  );
  const missingResolution = payload.cleaning.missing_values_before
    ? safeRatio(
        payload.cleaning.missing_values_before - payload.cleaning.missing_values_after,
        payload.cleaning.missing_values_before
      )
    : 1;
  const duplicateCleanup = payload.cleaning.original_shape.rows
    ? safeRatio(payload.cleaning.duplicate_rows_removed, payload.cleaning.original_shape.rows)
    : 0;

  return [
    {
      label: "Row retention",
      value: formatPercent(rowRetention),
      percentage: clampPercentage(rowRetention * 100),
      tone: "plum"
    },
    {
      label: "Column retention",
      value: formatPercent(columnRetention),
      percentage: clampPercentage(columnRetention * 100),
      tone: "gold"
    },
    {
      label: "Missing resolution",
      value: formatPercent(missingResolution),
      percentage: clampPercentage(missingResolution * 100),
      tone: "sky"
    },
    {
      label: "Duplicate cleanup",
      value: formatPercent(duplicateCleanup),
      percentage: clampPercentage(duplicateCleanup * 100),
      tone: "mint"
    }
  ];
}

function buildRankingRows(modeling: ModelingSummary): RankingRow[] {
  if (modeling.feature_importance?.length) {
    const maxImportance = Math.max(...modeling.feature_importance.map((item) => item.importance), 0.0001);
    return modeling.feature_importance.map((item) => ({
      label: item.feature,
      value: formatMetric(item.importance),
      caption: "Relative feature impact",
      width: clampPercentage((item.importance / maxImportance) * 100)
    }));
  }

  if (modeling.class_distribution) {
    const entries = Object.entries(modeling.class_distribution)
      .sort((left, right) => right[1] - left[1])
      .slice(0, 6);
    const maxCount = Math.max(...entries.map(([, value]) => value), 1);
    return entries.map(([label, value]) => ({
      label,
      value: formatCompactValue(value),
      caption: "Records in class",
      width: clampPercentage((value / maxCount) * 100)
    }));
  }

  if (modeling.cluster_summary) {
    const entries = Object.entries(modeling.cluster_summary)
      .sort((left, right) => right[1] - left[1])
      .slice(0, 6);
    const maxCount = Math.max(...entries.map(([, value]) => value), 1);
    return entries.map(([label, value]) => ({
      label: `Cluster ${label}`,
      value: formatCompactValue(value),
      caption: "Members in segment",
      width: clampPercentage((value / maxCount) * 100)
    }));
  }

  return [];
}

function buildSignalCards(
  payload: ReportPayload,
  strongestCorrelation: ReportPayload["correlations"]["strongest_pairs"][number] | null
): ExecutiveMetric[] {
  return [
    {
      label: "Correlation spotlight",
      value: strongestCorrelation ? formatSignedValue(strongestCorrelation.correlation) : "N/A",
      note: strongestCorrelation
        ? `${strongestCorrelation.left_column} x ${strongestCorrelation.right_column}`
        : "Numeric pair analysis unavailable",
      tone: "plum"
    },
    {
      label: "Outlier columns",
      value: formatCompactValue(payload.outliers.length),
      note: payload.outliers.length
        ? `${payload.outliers[0].column} is the most flagged column`
        : "No strong outlier columns",
      tone: "gold"
    },
    {
      label: "Trend narratives",
      value: formatCompactValue(payload.trends.length),
      note: payload.trends[0]?.description ?? "Add a time or sequence signal for stronger trend analysis.",
      tone: "sky"
    }
  ];
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

function getPrimaryMetric(
  modeling: ModelingSummary
): { label: string; value: string; note: string } | null {
  const metrics = modeling.metrics ?? {};
  const prioritiesByMode: Record<string, string[]> = {
    regression: ["r2", "rmse", "mae"],
    classification: ["accuracy", "f1_score", "precision", "recall"],
    clustering: ["silhouette_score"]
  };

  const priorities = prioritiesByMode[modeling.mode] ?? Object.keys(metrics);
  const selectedMetric = priorities.find((metric) => metric in metrics);
  if (!selectedMetric) {
    return null;
  }

  return {
    label: formatLabel(selectedMetric),
    value: formatMetric(metrics[selectedMetric]),
    note: `Primary score for ${formatLabel(modeling.mode)} mode`
  };
}

function formatCompactValue(value: number): string {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: value >= 100 ? 0 : 1
  }).format(value);
}

function formatPercent(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: 0
  }).format(value);
}

function formatSignedValue(value: number): string {
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}`;
}

function safeRatio(numerator: number, denominator: number): number {
  if (!denominator) {
    return 0;
  }
  return numerator / denominator;
}

function clampPercentage(value: number): number {
  return Math.max(0, Math.min(100, value));
}
