"use client";

import Link from "next/link";
import { useState } from "react";

import type { HistoryItem } from "@/lib/types";
import { buildShareUrl, copyToClipboard, formatBytes, formatDate, formatStatus } from "@/lib/utils";

type ReportHistoryProps = {
  items: HistoryItem[];
};

export function ReportHistory({ items }: ReportHistoryProps) {
  const [copiedReportId, setCopiedReportId] = useState<string | null>(null);

  async function handleCopy(report: HistoryItem) {
    const copied = await copyToClipboard(buildShareUrl(report.share_token, report.share_url));
    setCopiedReportId(copied ? report.id : null);
    window.setTimeout(() => setCopiedReportId(null), 2000);
  }

  return (
    <section className="panel">
      <div className="panel__header">
        <div>
          <div className="section-eyebrow">Report history</div>
          <h2>Saved analytics runs</h2>
        </div>
        <span className="pill">{items.length} saved</span>
      </div>

      {items.length ? (
        <div className="history-list">
          {items.map((item) => {
            const isActive = !["completed", "failed"].includes(item.status);
            return (
              <article className="history-card" key={item.id}>
                <div className="history-card__meta">
                  <span className="pill">{formatStatus(item.status)}</span>
                  <span>{formatDate(item.created_at)}</span>
                </div>

                <div className="history-card__title-row">
                  <h3>{item.dataset_name}</h3>
                  {item.processing_mode ? <small>{formatStatus(item.processing_mode)}</small> : null}
                </div>

                <p>
                  {item.row_count.toLocaleString()} rows, {item.column_count.toLocaleString()} columns
                  {item.target_column ? `, target: ${item.target_column}` : ", exploratory mode"}
                </p>

                {item.job_id ? <p className="muted-copy">Job ID: {item.job_id}</p> : null}

                <div className="history-card__facts">
                  <span>{item.file_type ? formatStatus(item.file_type) : "Manual"}</span>
                  <span>{formatBytes(item.file_size_bytes)}</span>
                </div>

                {isActive ? (
                  <div className="progress-card progress-card--compact">
                    <div className="progress-card__meta">
                      <span>{item.progress_message ?? "Processing analytics..."}</span>
                      <strong>{item.progress}%</strong>
                    </div>
                    <div className="progress-track">
                      <span className="progress-fill" style={{ width: `${item.progress}%` }} />
                    </div>
                  </div>
                ) : null}

                {item.processing_mode === "large" && isActive ? (
                  <div className="notice notice--info">Large dataset detected. Processing in background.</div>
                ) : null}

                {item.error_message ? <div className="notice notice--error">{item.error_message}</div> : null}

                <div className="button-row">
                  <Link href={`/analysis/${item.id}`} className="button button--secondary">
                    {isActive ? "Open live report" : "Open report"}
                  </Link>
                  <button type="button" className="button button--ghost" onClick={() => handleCopy(item)}>
                    {copiedReportId === item.id ? "Copied" : "Copy share link"}
                  </button>
                </div>
              </article>
            );
          })}
        </div>
      ) : (
        <div className="empty-state">
          <h3>No reports yet</h3>
          <p>Your completed analyses will appear here with shareable links and downloadable reports.</p>
        </div>
      )}
    </section>
  );
}
