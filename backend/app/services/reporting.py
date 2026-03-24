from __future__ import annotations

from html import escape
from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from app.db.models import AnalysisReport


def build_pdf_report(report: AnalysisReport) -> BytesIO:
    payload = report.report_payload or {}
    buffer = BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=0.65 * inch,
        rightMargin=0.65 * inch,
        topMargin=0.65 * inch,
        bottomMargin=0.65 * inch,
    )

    styles = getSampleStyleSheet()
    body_style = ParagraphStyle(
        "BodySmall",
        parent=styles["BodyText"],
        fontSize=10,
        leading=14,
        spaceAfter=6,
    )

    story = [
        Paragraph("Auto Analytics AI Report", styles["Title"]),
        Spacer(1, 10),
        Paragraph(
            f"Dataset: <b>{escape(report.dataset_name)}</b> | Source: <b>{escape(report.source_type.title())}</b>",
            body_style,
        ),
        Paragraph(
            f"Rows: <b>{report.row_count}</b> | Columns: <b>{report.column_count}</b> | "
            f"Generated: <b>{report.created_at.strftime('%Y-%m-%d %H:%M UTC')}</b>",
            body_style,
        ),
        Spacer(1, 10),
    ]

    story.append(Paragraph("Overview", styles["Heading2"]))
    story.append(
        build_key_value_table(
            [
                ["Target column", report.target_column or "Not selected"],
                ["Duplicates removed", payload.get("cleaning", {}).get("duplicate_rows_removed", 0)],
                ["Missing values handled", payload.get("cleaning", {}).get("missing_values_before", 0)],
                ["Share token", report.share_token],
            ]
        )
    )
    story.append(Spacer(1, 12))

    insights = payload.get("insights", [])
    if insights:
        story.append(Paragraph("Plain-English Insights", styles["Heading2"]))
        for insight in insights[:6]:
            story.append(Paragraph(f"&bull; {escape(str(insight))}", body_style))
        story.append(Spacer(1, 8))

    recommendations = payload.get("recommendations", [])
    if recommendations:
        story.append(Paragraph("Recommendations", styles["Heading2"]))
        for recommendation in recommendations[:6]:
            story.append(Paragraph(f"&bull; {escape(str(recommendation))}", body_style))
        story.append(Spacer(1, 8))

    modeling = payload.get("modeling", {})
    if modeling:
        story.append(Paragraph("Model Summary", styles["Heading2"]))
        metrics = modeling.get("metrics", {})
        story.append(
            build_key_value_table(
                [
                    ["Mode", modeling.get("mode", "n/a")],
                    ["Selected model", modeling.get("selected_model", "n/a")],
                    ["Status", modeling.get("status", "n/a")],
                    ["Metrics", ", ".join(f"{key}: {value}" for key, value in metrics.items()) or "n/a"],
                ]
            )
        )
        story.append(Spacer(1, 12))

    summary_rows = [["Column", "Type", "Unique", "Highlights"]]
    for item in payload.get("summary_statistics", [])[:8]:
        highlight_parts = []
        for key in ("mean", "top_value", "min", "max"):
            if key in item:
                highlight_parts.append(f"{key}: {item[key]}")
        summary_rows.append(
            [
                str(item.get("column", "")),
                str(item.get("dtype", "")),
                str(item.get("unique_values", "")),
                ", ".join(highlight_parts) or "n/a",
            ]
        )
    if len(summary_rows) > 1:
        story.append(Paragraph("Summary Statistics Snapshot", styles["Heading2"]))
        story.append(build_key_value_table(summary_rows, header=True))

    document.build(story)
    buffer.seek(0)
    return buffer


def build_key_value_table(rows: list[list[object]], header: bool = False) -> Table:
    table = Table(rows, colWidths=[1.8 * inch, 4.7 * inch] if not header else None)
    styles = [
        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#CBD5E1")),
        ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#E2E8F0")),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EFF6FF") if header else colors.white),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#0F172A")),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold" if header else "Helvetica"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("PADDING", (0, 0), (-1, -1), 8),
    ]
    table.setStyle(TableStyle(styles))
    return table

