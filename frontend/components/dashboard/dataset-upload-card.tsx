"use client";

import { FormEvent, useState } from "react";

import { uploadDataset } from "@/lib/api";
import type { ReportDetail } from "@/lib/types";

type DatasetUploadCardProps = {
  token: string;
  onCreated: (report: ReportDetail) => void;
};

const MAX_UPLOAD_SIZE_BYTES = 200 * 1024 * 1024;
const SMALL_FILE_LIMIT_BYTES = 10 * 1024 * 1024;
const MEDIUM_FILE_LIMIT_BYTES = 50 * 1024 * 1024;

function classifyClientFileMode(file: File | null): "small" | "medium" | "large" | null {
  if (!file) {
    return null;
  }
  if (file.size < SMALL_FILE_LIMIT_BYTES) {
    return "small";
  }
  if (file.size <= MEDIUM_FILE_LIMIT_BYTES) {
    return "medium";
  }
  return "large";
}

export function DatasetUploadCard({ token, onCreated }: DatasetUploadCardProps) {
  const [file, setFile] = useState<File | null>(null);
  const [datasetName, setDatasetName] = useState("");
  const [targetColumn, setTargetColumn] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [error, setError] = useState("");
  const fileMode = classifyClientFileMode(file);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file) {
      setError("Choose a CSV, Excel, or JSON file before running the analysis.");
      return;
    }
    if (file.size > MAX_UPLOAD_SIZE_BYTES) {
      setError("This file is larger than the 200 MB upload limit. Please choose a smaller file.");
      return;
    }

    setError("");
    setUploadProgress(0);
    setIsSubmitting(true);
    try {
      const report = await uploadDataset(file, token, {
        datasetName: datasetName.trim() || undefined,
        targetColumn: targetColumn.trim() || undefined,
        onUploadProgress: setUploadProgress
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
          <h2>Analyze CSV, Excel, and JSON files</h2>
        </div>
        <span className="pill">Preview + background full run</span>
      </div>

      <form className="stack" onSubmit={handleSubmit}>
        <label className="field">
          <span>Dataset file</span>
          <input
            className="input"
            type="file"
            accept=".csv,.xlsx,.xls,.json"
            onChange={(event) => setFile(event.target.files?.[0] ?? null)}
          />
        </label>

        {file ? (
          <div className="notice notice--info">
            {fileMode === "large"
              ? "Large dataset detected. Processing in background with optimized mode."
              : fileMode === "medium"
                ? "Medium dataset detected. The backend will create a chunked preview first, then finish full analytics in the background."
                : "Small dataset detected. The backend can process it directly."}
          </div>
        ) : null}

        <div className="upload-hint-grid">
          <div className="upload-hint-card">
            <strong>Small files</strong>
            <p>Below 10 MB: direct analytics inside the upload request.</p>
          </div>
          <div className="upload-hint-card">
            <strong>Medium files</strong>
            <p>10 MB to 50 MB: chunk-based processing with backend optimization.</p>
          </div>
          <div className="upload-hint-card">
            <strong>Large files</strong>
            <p>Above 50 MB: sampled preview, background analytics, Parquet, and progress tracking.</p>
          </div>
        </div>

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

        {isSubmitting ? (
          <div className="progress-card">
            <div className="progress-card__meta">
              <span>Upload progress</span>
              <strong>{uploadProgress}%</strong>
            </div>
            <div className="progress-track">
              <span className="progress-fill" style={{ width: `${uploadProgress}%` }} />
            </div>
            <p className="muted-copy">
              The app saves the file first, then opens a quick preview. Medium files use chunk-based processing and
              large files continue in optimized background mode with a tracked job status.
            </p>
          </div>
        ) : null}

        {error ? <div className="notice notice--error">{error}</div> : null}

        <button type="submit" className="button button--primary" disabled={isSubmitting}>
          {isSubmitting ? "Uploading dataset..." : "Run automated analysis"}
        </button>
      </form>
    </section>
  );
}
