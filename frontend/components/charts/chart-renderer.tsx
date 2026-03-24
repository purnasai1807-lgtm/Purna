"use client";

import dynamic from "next/dynamic";

import type { ChartSpec } from "@/lib/types";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

type ChartRendererProps = {
  chart: ChartSpec;
};

export function ChartRenderer({ chart }: ChartRendererProps) {
  return (
    <div className="chart-card">
      <div className="chart-card__header">
        <div>
          <h3>{chart.title}</h3>
          <p>{chart.description}</p>
        </div>
        <span className="pill">{chart.type}</span>
      </div>
      <div className="chart-canvas">
        <Plot
          data={chart.figure.data as never}
          layout={{
            autosize: true,
            ...chart.figure.layout
          }}
          config={{
            responsive: true,
            displaylogo: false,
            modeBarButtonsToRemove: ["select2d", "lasso2d"]
          }}
          useResizeHandler
          style={{ width: "100%", height: "100%" }}
        />
      </div>
    </div>
  );
}
