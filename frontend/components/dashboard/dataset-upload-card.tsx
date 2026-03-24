"use client";

import { FormEvent, useState } from "react";

import { uploadDataset } from "@/lib/api";
import type { ReportDetail } from "@/lib/types";

type DatasetUploadCardProps = {
  token: string;
  onCreated: (report: ReportDetail) => void;
};

export function DatasetUploadCard({ token, onCreated }: DatasetUploadCardProps) {
  const [file, setFile] = useState<File | null>(null);
  const [datasetName, setDatasetName] = useState("");
  const [targetColumn, setTargetColumn] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState("");

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file) {
      setError("Choose a CSV or Excel file before running the analysis.");
      return;
    }

    setError("");
    setIsSubmitting(true);
    try {
      const report = await uploadDataset(file, token, {
        datasetName: datasetName.trim() || undefined,
        targetColumn: targetColumn.trim() || undefined
      });
      onCreated(report);
    } catch (submissionError) {
      setError(
        submissionError instanceof Error
          ? submissionError.message
          : "We could not analyze that file."
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <section className="panel">
      <div className="panel__header">
        <div>
          <div className="section-eyebrow">Upload dataset</div>
          <h2>Analyze CSV or Excel files</h2>
        </div>
        <span className="pill">CSV / XLSX</span>
      </div>

      <form className="stack" onSubmit={handleSubmit}>
        <label className="field">
          <span>Dataset file</span>
          <input
            className="input"
            type="file"
            accept=".csv,.xlsx,.xls"
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
          />
        </label>

        <label className="field">
          <span>Display name</span>
          <input
            className="input"
            value={datasetName}
            onChange={(event) => setDatasetName(event.target.value)}
            placeholder="Quarterly sales performance"
          />
        </label>

        <label className="field">
          <span>Target column for ML</span>
          <input
            className="input"
            value={targetColumn}
            onChange={(event) => setTargetColumn(event.target.value)}
            placeholder="Optional. Example: revenue or churn_label"
          />
        </label>

        {error ? <div className="notice notice--error">{error}</div> : null}

        <button type="submit" className="button button--primary" disabled={isSubmitting}>
          {isSubmitting ? "Analyzing dataset..." : "Run automated analysis"}
        </button>
      </form>
    </section>
  );
}

