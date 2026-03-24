"use client";

import { FormEvent, useState } from "react";

import { submitManualEntry } from "@/lib/api";
import type { ManualEntryPayload, ReportDetail } from "@/lib/types";

type ManualEntryCardProps = {
  token: string;
  onCreated: (report: ReportDetail) => void;
};

const initialColumns = ["region", "sales", "profit", "category"];
const initialRows: Array<Record<string, string>> = [
  { region: "North", sales: "120000", profit: "32000", category: "Retail" },
  { region: "South", sales: "98000", profit: "24000", category: "Wholesale" },
  { region: "West", sales: "110500", profit: "28000", category: "Retail" }
];

export function ManualEntryCard({ token, onCreated }: ManualEntryCardProps) {
  const [datasetName, setDatasetName] = useState("Manual quick analysis");
  const [targetColumn, setTargetColumn] = useState("");
  const [columns, setColumns] = useState(initialColumns);
  const [rows, setRows] = useState<Array<Record<string, string>>>(initialRows);
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  function updateColumnName(index: number, nextValue: string) {
    const previousColumn = columns[index];
    const nextColumns = [...columns];
    nextColumns[index] = nextValue;

    const nextRows = rows.map((row) => {
      const updatedRow: Record<string, string> = {};
      nextColumns.forEach((column, columnIndex) => {
        const sourceKey = columnIndex === index ? previousColumn : columns[columnIndex];
        updatedRow[column] = row[sourceKey] ?? "";
      });
      return updatedRow;
    });

    setColumns(nextColumns);
    setRows(nextRows);
  }

  function updateCell(rowIndex: number, columnName: string, nextValue: string) {
    setRows((currentRows) =>
      currentRows.map((row, currentIndex) =>
        currentIndex === rowIndex ? { ...row, [columnName]: nextValue } : row
      )
    );
  }

  function addRow() {
    setRows((currentRows) => [
      ...currentRows,
      Object.fromEntries(columns.map((column) => [column, ""]))
    ]);
  }

  function addColumn() {
    const nextColumn = `column_${columns.length + 1}`;
    setColumns((currentColumns) => [...currentColumns, nextColumn]);
    setRows((currentRows) => currentRows.map((row) => ({ ...row, [nextColumn]: "" })));
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setIsSubmitting(true);

    const normalizedColumns = columns.map((column, index) => column.trim() || `column_${index + 1}`);
    const cleanedRows = rows
      .map((row) =>
        Object.fromEntries(
          normalizedColumns.map((column, index) => [column, row[columns[index]]?.trim() ?? ""])
        )
      )
      .filter((row) => Object.values(row).some((value) => value !== ""));

    if (!cleanedRows.length) {
      setError("Add at least one row with values before running the analysis.");
      setIsSubmitting(false);
      return;
    }

    const payload: ManualEntryPayload = {
      dataset_name: datasetName.trim() || "Manual analysis",
      columns: normalizedColumns,
      rows: cleanedRows
    };

    if (targetColumn.trim()) {
      payload.target_column = targetColumn.trim();
    }

    try {
      const report = await submitManualEntry(token, payload);
      onCreated(report);
    } catch (submissionError) {
      setError(
        submissionError instanceof Error
          ? submissionError.message
          : "Manual analysis failed."
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <section className="panel">
      <div className="panel__header">
        <div>
          <div className="section-eyebrow">Manual entry</div>
          <h2>Create a dataset without a file</h2>
        </div>
        <button type="button" className="button button--ghost" onClick={addColumn}>
          Add column
        </button>
      </div>

      <form className="stack" onSubmit={handleSubmit}>
        <div className="field-grid">
          <label className="field">
            <span>Dataset name</span>
            <input
              className="input"
              value={datasetName}
              onChange={(event) => setDatasetName(event.target.value)}
            />
          </label>
          <label className="field">
            <span>Target column</span>
            <input
              className="input"
              value={targetColumn}
              onChange={(event) => setTargetColumn(event.target.value)}
              placeholder="Optional"
            />
          </label>
        </div>

        <div className="table-shell">
          <table className="data-table">
            <thead>
              <tr>
                {columns.map((column, index) => (
                  <th key={`${column}-${index}`}>
                    <input
                      className="table-input table-input--header"
                      value={column}
                      onChange={(event) => updateColumnName(index, event.target.value)}
                    />
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, rowIndex) => (
                <tr key={`row-${rowIndex}`}>
                  {columns.map((column) => (
                    <td key={`${rowIndex}-${column}`}>
                      <input
                        className="table-input"
                        value={row[column] ?? ""}
                        onChange={(event) => updateCell(rowIndex, column, event.target.value)}
                      />
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="button-row">
          <button type="button" className="button button--ghost" onClick={addRow}>
            Add row
          </button>
          <button type="submit" className="button button--primary" disabled={isSubmitting}>
            {isSubmitting ? "Analyzing manual data..." : "Analyze manual data"}
          </button>
        </div>

        {error ? <div className="notice notice--error">{error}</div> : null}
      </form>
    </section>
  );
}
