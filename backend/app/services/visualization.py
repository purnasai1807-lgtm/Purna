from __future__ import annotations
import json
from typing import Any
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pandas.api.types import is_datetime64_any_dtype
MAX_VISUALIZATION_ROWS = 5000
DASHBOARD_COLORS = [
    "#7C3AED",
    "#4F46E5",
    "#EC4899",
    "#F59E0B",
    "#14B8A6",
    "#3B82F6",
    "#EF4444",
    "#8B5CF6",
]
HEATMAP_SCALE = [
    [0.0, "#FFF5C3"],
    [0.5, "#D6C2F7"],
    [1.0, "#5B21B6"],
]
def get_chart_frame(dataframe: pd.DataFrame, max_rows: int = MAX_VISUALIZATION_ROWS) -> pd.DataFrame:
    if len(dataframe) <= max_rows:
        return dataframe
    return dataframe.sample(n=max_rows, random_state=42).sort_index()
def generate_chart_specs(dataframe: pd.DataFrame, correlations: dict[str, Any]) -> list[dict[str, Any]]:
    charts: list[dict[str, Any]] = []
    chart_frame = get_chart_frame(dataframe)
    numeric_columns = list(dataframe.select_dtypes(include=[np.number]).columns)
    datetime_columns = [column for column in dataframe.columns if is_datetime64_any_dtype(dataframe[column])]
    categorical_columns = [
        column
        for column in dataframe.columns
        if column not in numeric_columns and column not in datetime_columns
    ]
    if categorical_columns:
        categorical_column = categorical_columns[0]
        counts = (
            dataframe[categorical_column]
            .astype(str)
            .value_counts()
            .head(10)
            .rename_axis(categorical_column)
            .reset_index(name="count")
            .sort_values("count", ascending=True)
        )
        bar_figure = px.bar(
            counts,
            x="count",
            y=categorical_column,
            orientation="h",
            color="count",
            text="count",
            title=f"Top values in {categorical_column}",
            color_continuous_scale=["#DCCBFF", "#7C3AED"],
        )
        bar_figure.update_traces(textposition="outside", cliponaxis=False)
        style_figure(bar_figure)
        charts.append(
            build_chart_payload(
                chart_id=f"bar-{categorical_column}",
                chart_type="bar",
                title=f"Bar chart for {categorical_column}",
                description="Shows the most common categories in the dataset.",
                figure=bar_figure,
            )
        )
        pie_counts = counts.head(5)
        pie_figure = px.pie(
            pie_counts,
            names=categorical_column,
            values="count",
            title=f"Category share for {categorical_column}",
            hole=0.58,
            color_discrete_sequence=DASHBOARD_COLORS,
        )
        pie_figure.update_traces(
            textinfo="label+percent",
            pull=[0.06 if index == 0 else 0 for index in range(len(pie_counts))],
            marker={"line": {"color": "#fff8df", "width": 2}},
        )
        style_figure(pie_figure)
        charts.append(
            build_chart_payload(
                chart_id=f"pie-{categorical_column}",
                chart_type="pie",
                title=f"Pie chart for {categorical_column}",
                description="Highlights the category distribution across the most frequent labels.",
                figure=pie_figure,
            )
        )
    if numeric_columns:
        numeric_column = numeric_columns[0]
        histogram_figure = px.histogram(
            chart_frame,
            x=numeric_column,
            nbins=min(30, max(10, len(chart_frame) // 5)),
            title=f"Distribution of {numeric_column}",
            color_discrete_sequence=["#7C3AED"],
        )
        histogram_figure.update_traces(marker_line={"color": "#F8E8AF", "width": 1})
        style_figure(histogram_figure)
        charts.append(
            build_chart_payload(
                chart_id=f"histogram-{numeric_column}",
                chart_type="histogram",
                title=f"Histogram for {numeric_column}",
                description="Displays how numeric values are distributed.",
                figure=histogram_figure,
            )
        )
        box_figure = px.box(
            chart_frame,
            y=numeric_column,
            points="outliers",
            title=f"Outlier view for {numeric_column}",
            color_discrete_sequence=["#14B8A6"],
        )
        box_figure.update_traces(
            fillcolor="rgba(20, 184, 166, 0.16)",
            marker={"color": "#7C3AED"},
            line={"color": "#14B8A6"},
        )
        style_figure(box_figure)
        charts.append(
            build_chart_payload(
                chart_id=f"box-{numeric_column}",
                chart_type="box",
                title=f"Box plot for {numeric_column}",
                description="Useful for spotting spread, quartiles, and outliers.",
                figure=box_figure,
            )
        )
    if len(numeric_columns) >= 2:
        scatter_figure = px.scatter(
            chart_frame,
            x=numeric_columns[0],
            y=numeric_columns[1],
            title=f"{numeric_columns[0]} vs {numeric_columns[1]}",
            color_discrete_sequence=["#4F46E5"],
        )
        scatter_figure.update_traces(
            marker={"size": 9, "opacity": 0.72, "line": {"color": "#FFF5C3", "width": 1}}
        )
        style_figure(scatter_figure)
        charts.append(
            build_chart_payload(
                chart_id=f"scatter-{numeric_columns[0]}-{numeric_columns[1]}",
                chart_type="scatter",
                title=f"Scatter plot for {numeric_columns[0]} and {numeric_columns[1]}",
                description="Helps identify relationships between two numeric signals.",
                figure=scatter_figure,
            )
        )

    if numeric_columns:
        if datetime_columns:
            date_column = datetime_columns[0]
            trend_frame = (
                chart_frame[[date_column, numeric_columns[0]]]
                .dropna()
                .sort_values(date_column)
                .groupby(date_column, as_index=False)[numeric_columns[0]]
                .mean()
            )
            line_figure = px.line(
                trend_frame,
                x=date_column,
                y=numeric_columns[0],
                markers=True,
                title=f"{numeric_columns[0]} over {date_column}",
                color_discrete_sequence=["#7C3AED"],
            )
        else:
            trend_frame = chart_frame[[numeric_columns[0]]].copy()
            trend_frame["record_index"] = range(1, len(trend_frame) + 1)
            line_figure = px.line(
                trend_frame,
                x="record_index",
                y=numeric_columns[0],
                markers=True,
                title=f"{numeric_columns[0]} across dataset order",
                color_discrete_sequence=["#7C3AED"],
            )
        line_figure.update_traces(
            line={"width": 3},
            marker={"size": 7, "color": "#F59E0B", "line": {"color": "#FFF5C3", "width": 1}},
            fill="tozeroy",
            fillcolor="rgba(124, 58, 237, 0.12)",
        )
        style_figure(line_figure)
        charts.append(
            build_chart_payload(
                chart_id=f"line-{numeric_columns[0]}",
                chart_type="line",
                title=f"Line chart for {numeric_columns[0]}",
                description="Visualizes directional movement or change over time/order.",
                figure=line_figure,
            )
        )

    if correlations.get("available"):
        matrix = np.array(correlations["matrix"], dtype=float)
        heatmap_figure = go.Figure(
            data=[
                go.Heatmap(
                    z=matrix,
                    x=correlations["columns"],
                    y=correlations["columns"],
                    colorscale=HEATMAP_SCALE,
                    zmin=-1,
                    zmax=1,
                    hoverongaps=False,
                )
            ]
        )
        heatmap_figure.update_layout(title="Correlation heatmap")
        style_figure(heatmap_figure)
        charts.append(
            build_chart_payload(
                chart_id="heatmap-correlations",
                chart_type="heatmap",
                title="Correlation heatmap",
                description="Highlights strong positive and negative relationships between numeric columns.",
                figure=heatmap_figure,
            )
        )

    return charts


def build_chart_payload(
    chart_id: str,
    chart_type: str,
    title: str,
    description: str,
    figure,
) -> dict[str, Any]:
    return {
        "id": chart_id,
        "type": chart_type,
        "title": title,
        "description": description,
        "figure": json.loads(figure.to_json()),
    }


def style_figure(figure) -> None:
    figure.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,247,214,0.54)",
        margin={"l": 24, "r": 16, "t": 54, "b": 30},
        legend={"orientation": "h", "y": -0.2, "x": 0},
        font={"family": "Manrope, sans-serif", "color": "#34195e", "size": 12},
        hoverlabel={"bgcolor": "#351761", "bordercolor": "#f9d76a", "font": {"color": "#fff8e1"}},
    )
    figure.update_xaxes(
        showline=True,
        linecolor="rgba(77,39,123,0.18)",
        gridcolor="rgba(77,39,123,0.08)",
        zerolinecolor="rgba(77,39,123,0.12)",
    )
    figure.update_yaxes(
        showline=True,
        linecolor="rgba(77,39,123,0.18)",
        gridcolor="rgba(77,39,123,0.08)",
        zerolinecolor="rgba(77,39,123,0.12)",
    )