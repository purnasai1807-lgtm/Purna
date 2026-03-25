"use client";

import Link from "next/link";
import { useState } from "react";

import type { HistoryItem } from "@/lib/types";
import { buildShareUrl, copyToClipboard, formatDate } from "@/lib/utils";

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
          {items.map((item) => (
            <article className="history-card" key={item.id}>
              <div className="history-card__meta">
                <span className="pill">{item.source_type}</span>
                <span>{formatDate(item.created_at)}</span>
              </div>
              <h3>{item.dataset_name}</h3>
              <p>
                {item.row_count} rows, {item.column_count} columns
                {item.target_column ? `, target: ${item.target_column}` : ", clustering-ready"}
              </p>
              <div className="button-row">
                <Link href={`/analysis/${item.id}`} className="button button--secondary">
                  Open report
                </Link>
                <button type="button" className="button button--ghost" onClick={() => handleCopy(item)}>
                  {copiedReportId === item.id ? "Copied" : "Copy share link"}
                </button>
              </div>
            </article>
          ))}
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
