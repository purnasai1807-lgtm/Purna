"use client";

import dynamic from "next/dynamic";

import type { ChartSpec } from "@/lib/types";
import { formatLabel } from "@/lib/utils";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

type ChartRendererProps = {
  chart: ChartSpec;
};

export function ChartRenderer({ chart }: ChartRendererProps) {
  const sourceLayout = toObject(chart.figure.layout);
  const layout = {
    autosize: true,
    ...sourceLayout,
    paper_bgcolor: "rgba(0, 0, 0, 0)",
    plot_bgcolor: "rgba(255, 247, 214, 0.54)",
    margin: {
      l: 34,
      r: 18,
      t: 18,
      b: 34,
      ...toObject(sourceLayout.margin)
    },
    legend: {
      orientation: "h",
      x: 0,
      y: -0.2,
      bgcolor: "rgba(0,0,0,0)",
      ...toObject(sourceLayout.legend)
    },
    font: {
      family: "Manrope, sans-serif",
      size: 12,
      color: "#34195e",
      ...toObject(sourceLayout.font)
    },
    hoverlabel: {
      bgcolor: "#351761",
      bordercolor: "#f9d76a",
      font: { color: "#fff8e1" },
      ...toObject(sourceLayout.hoverlabel)
    },
    xaxis: {
      gridcolor: "rgba(77, 39, 123, 0.08)",
      linecolor: "rgba(77, 39, 123, 0.18)",
      zerolinecolor: "rgba(77, 39, 123, 0.12)",
      ...toObject(sourceLayout.xaxis)
    },
    yaxis: {
      gridcolor: "rgba(77, 39, 123, 0.08)",
      linecolor: "rgba(77, 39, 123, 0.18)",
      zerolinecolor: "rgba(77, 39, 123, 0.12)",
      ...toObject(sourceLayout.yaxis)
    },
    title: undefined
  };

  return (
    <div className={getChartCardClassName(chart.type)}>
      <div className="chart-card__header">
        <div>
          <div className="chart-card__eyebrow">{formatLabel(chart.type)}</div>
          <h3>{chart.title}</h3>
          <p>{chart.description}</p>
        </div>
        <span className="pill">{formatLabel(chart.type)}</span>
      </div>
      <div className="chart-canvas">
        <Plot
          data={chart.figure.data as never}
          layout={layout as never}
          config={{
            responsive: true,
            displaylogo: false,
            displayModeBar: false,
            modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"]
          }}
          useResizeHandler
          style={{ width: "100%", height: "100%" }}
        />
      </div>
    </div>
  );
}

function getChartCardClassName(chartType: string): string {
  const classes = ["chart-card"];
  if (chartType === "heatmap" || chartType === "line") {
    classes.push("chart-card--wide");
  }
  if (chartType === "pie" || chartType === "box") {
    classes.push("chart-card--compact");
  }
  return classes.join(" ");
}

function toObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}
