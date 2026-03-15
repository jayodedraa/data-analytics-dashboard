"""
Simple analytics web application (local-only) for uploading a CSV and viewing
cleaned summaries and visualizations.

Usage (from project root):
    .venv\Scripts\activate          # on Windows
    python webapp/app.py

Then open http://127.0.0.1:5000 in your browser.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import plotly.express as px
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    send_file,
)
from io import StringIO, BytesIO


app = Flask(__name__)

# Shared Plotly config so zoom/pan are available via the toolbar,
# but not activated by default mouse drag, and the mode bar only appears on hover.
PLOT_CONFIG = {
    "displaylogo": False,
    "scrollZoom": False,
    "displayModeBar": "hover",
    "modeBarButtonsToRemove": [
        "zoom2d",
        "zoomIn2d",
        "zoomOut2d",
        "autoScale2d",
        "lasso2d",
        "select2d",
    ],
}

# Simple in-memory store for last uploaded dataset and presets (local demo only)
LAST_DF_BASE: Optional[pd.DataFrame] = None
LAST_DF_FILTERED: Optional[pd.DataFrame] = None
FILTER_PRESETS: List[Tuple[str, Dict[str, str]]] = []
app.secret_key = "local-dev-secret-key"  # OK for local-only usage


@dataclass
class AnalysisResult:
    """Container for cleaned data and derived insights."""

    numeric_summary_html: str
    special_metrics: Dict[str, Any]
    plots: Dict[str, str]  # slot -> html
    active_filters: Dict[str, Optional[str]]
    trend_metrics: Dict[str, Any]


def read_uploaded_csv(file_storage) -> pd.DataFrame:
    """
    Load an uploaded CSV into a DataFrame.

    Tries a small set of common encodings so that typical Excel/CSV exports
    with non-UTF8 characters are handled gracefully.
    """
    file_storage.stream.seek(0)
    encodings_to_try = ["utf-8-sig", "cp1252", "latin1"]
    last_error: Exception | None = None

    for enc in encodings_to_try:
        try:
            file_storage.stream.seek(0)
            return pd.read_csv(file_storage, encoding=enc)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

    # Final fallback: replace undecodable characters instead of failing
    file_storage.stream.seek(0)
    return pd.read_csv(file_storage, encoding="latin1", encoding_errors="replace")


def clean_generic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply generic cleaning steps that work for many tabular datasets:
    - Drop exact duplicates
    - Strip whitespace from string columns
    - Convert Postal Code to string if present
    - Coerce known numeric columns to numeric (if they exist)
    - Parse known date columns (if they exist)
    - Add simple date parts if Order Date exists
    """
    df = df.drop_duplicates().copy()

    # Strip whitespace
    obj_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in obj_cols:
        df[col] = df[col].astype("string").str.strip()

    # Postal code as text
    if "Postal Code" in df.columns:
        df["Postal Code"] = df["Postal Code"].astype("string")

    # Coerce common numeric columns if they exist
    numeric_candidates = ["Sales", "Profit", "Quantity", "Discount"]
    for col in numeric_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Parse date-like columns
    date_cols = ["Order Date", "Ship Date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=False)

    # Add simple date parts if we have Order Date
    if "Order Date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["Order Date"]):
        if "Order Year" not in df.columns:
            df["Order Year"] = df["Order Date"].dt.year
        if "Order MonthNum" not in df.columns:
            df["Order MonthNum"] = df["Order Date"].dt.month
        if "Order Month" not in df.columns:
            df["Order Month"] = df["Order Date"].dt.month_name()

    return df


def build_plots(df: pd.DataFrame) -> Dict[str, str]:
    """Create a set of key plots as interactive Plotly HTML snippets mapped by slot name."""
    plots: Dict[str, str] = {}

    # Teal / purple modern palette
    primary_color = "#14B8A6"
    secondary_color = "#8B5CF6"

    # Monthly sales trend (line) if dates available
    if {"Order Date", "Sales"}.issubset(df.columns):
        monthly = (
            df.dropna(subset=["Order Date"])
            .assign(Year=lambda x: x["Order Date"].dt.year, Month=lambda x: x["Order Date"].dt.month)
            .groupby(["Year", "Month"], as_index=False)["Sales"]
            .sum()
        )
        monthly["YearMonth"] = pd.to_datetime(
            monthly[["Year", "Month"]].assign(DAY=1)
        )
        fig = px.line(
            monthly,
            x="YearMonth",
            y="Sales",
            title="Monthly Sales Trend 📈",
            markers=True,
            color_discrete_sequence=[primary_color],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=40),
            dragmode=False,
            xaxis_title="Month",
            yaxis_title="Sales",
        )
        plots["trend"] = fig.to_html(
            include_plotlyjs=False,
            full_html=False,
            config=PLOT_CONFIG,
        )

    # Histogram of Sales
    if "Sales" in df.columns:
        fig = px.histogram(
            df,
            x="Sales",
            nbins=30,
            title="Distribution of Sales",
            color_discrete_sequence=[primary_color],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=20),
            dragmode=False,
        )
        plots["sales_hist"] = fig.to_html(
            include_plotlyjs=False,
            full_html=False,
            config=PLOT_CONFIG,
        )

    # Histogram of Profit
    if "Profit" in df.columns:
        fig = px.histogram(
            df,
            x="Profit",
            nbins=30,
            title="Distribution of Profit",
            color_discrete_sequence=[secondary_color],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=20),
            dragmode=False,
        )
        plots["profit_hist"] = fig.to_html(
            include_plotlyjs=False,
            full_html=False,
            config=PLOT_CONFIG,
        )

    # Sales by Region
    if {"Region", "Sales"}.issubset(df.columns):
        grouped = (
            df.groupby("Region", as_index=False)["Sales"]
            .sum()
            .sort_values("Sales", ascending=False)
        )
        fig = px.bar(
            grouped,
            x="Region",
            y="Sales",
            title="Total Sales by Region",
            color_discrete_sequence=[primary_color],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=20),
            dragmode=False,
        )
        plots["region_bar"] = fig.to_html(
            include_plotlyjs=False,
            full_html=False,
            config=PLOT_CONFIG,
        )

        # Region sales share (pie)
        fig_pie = px.pie(
            grouped,
            names="Region",
            values="Sales",
            title="Sales Share by Region 🥧",
            color_discrete_sequence=px.colors.sequential.Teal,
        )
        fig_pie.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=20),
            dragmode=False,
        )
        plots["region_pie"] = fig_pie.to_html(
            include_plotlyjs=False,
            full_html=False,
            config=PLOT_CONFIG,
        )

    # Profit by Sub-Category
    if {"Sub-Category", "Profit"}.issubset(df.columns):
        grouped = (
            df.groupby("Sub-Category", as_index=False)["Profit"]
            .sum()
            .sort_values("Profit", ascending=True)
        )
        fig = px.bar(
            grouped,
            x="Sub-Category",
            y="Profit",
            title="Profit by Sub-Category (Ascending)",
            color_discrete_sequence=[secondary_color],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=80),
            xaxis_tickangle=-45,
            dragmode=False,
        )
        plots["subcat_profit"] = fig.to_html(
            include_plotlyjs=False,
            full_html=False,
            config=PLOT_CONFIG,
        )

    # Top 10 customers by Sales
    customer_col = None
    if "Customer Name" in df.columns:
        customer_col = "Customer Name"
    elif "Customer ID" in df.columns:
        customer_col = "Customer ID"

    if customer_col and "Sales" in df.columns:
        grouped = (
            df.groupby(customer_col, as_index=False)["Sales"]
            .sum()
            .sort_values("Sales", ascending=False)
            .head(10)
        )
        fig = px.bar(
            grouped,
            x=customer_col,
            y="Sales",
            title="Top 10 Customers by Sales",
            color_discrete_sequence=[primary_color],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=80),
            xaxis_tickangle=-45,
            dragmode=False,
        )
        plots["top_customers"] = fig.to_html(
            include_plotlyjs=False,
            full_html=False,
            config=PLOT_CONFIG,
        )

    return plots


def _compute_trend_metrics(base_df: pd.DataFrame, filtered_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute simple month-over-month trend metrics for Sales and Profit.

    Uses Order Year / Order MonthNum from the base dataset to find the latest
    and previous month, then calculates percentage change on the filtered slice.
    """
    metrics: Dict[str, Any] = {
        "sales_change_pct": None,
        "profit_change_pct": None,
        "comparison_label": "",
    }

    if not {"Order Year", "Order MonthNum"}.issubset(base_df.columns):
        return metrics

    # Determine latest and previous month from the base dataset
    month_keys = (
        base_df[["Order Year", "Order MonthNum"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["Order Year", "Order MonthNum"])
    )
    if len(month_keys) < 2:
        return metrics

    latest_year, latest_month = month_keys.iloc[-1]
    prev_year, prev_month = month_keys.iloc[-2]

    metrics["comparison_label"] = f"{int(latest_year)}/{int(latest_month):02d} vs {int(prev_year)}/{int(prev_month):02d}"

    def _period_slice(df: pd.DataFrame, year: float, month: float) -> pd.DataFrame:
        return df[(df["Order Year"] == int(year)) & (df["Order MonthNum"] == int(month))]

    current_slice = _period_slice(filtered_df, latest_year, latest_month)
    prev_slice = _period_slice(filtered_df, prev_year, prev_month)

    if "Sales" in filtered_df.columns:
        current_sales = current_slice["Sales"].sum()
        prev_sales = prev_slice["Sales"].sum()
        if prev_sales != 0:
            metrics["sales_change_pct"] = (current_sales - prev_sales) / abs(prev_sales) * 100.0

    if "Profit" in filtered_df.columns:
        current_profit = current_slice["Profit"].sum()
        prev_profit = prev_slice["Profit"].sum()
        if prev_profit != 0:
            metrics["profit_change_pct"] = (current_profit - prev_profit) / abs(prev_profit) * 100.0

    return metrics


def summarize_dataframe(df: pd.DataFrame, base_df: Optional[pd.DataFrame]) -> AnalysisResult:
    """Build tables, metrics and plots for display."""

    # Numeric summary
    numeric_df = df.select_dtypes(include="number")
    if not numeric_df.empty:
        numeric_summary = numeric_df.describe().T.round(0)
        numeric_summary_html = numeric_summary.to_html(
            classes="table table-sm table-striped", border=0
        )
    else:
        numeric_summary_html = "<p>No numeric columns detected.</p>"

    # Special metrics if Sales/Profit exist
    special_metrics: Dict[str, Any] = {}
    if "Sales" in df.columns:
        special_metrics["Total Sales"] = float(df["Sales"].sum(skipna=True))
    if "Profit" in df.columns:
        special_metrics["Total Profit"] = float(df["Profit"].sum(skipna=True))
        loss_mask = df["Profit"] < 0
        special_metrics["Loss Orders"] = int(loss_mask.sum())
        special_metrics["Total Loss Amount"] = float(df.loc[loss_mask, "Profit"].sum())

    plots = build_plots(df)

    # Filter metadata – for now just echo back which columns exist
    active_filters: Dict[str, Optional[str]] = {
        "region": "Region" if "Region" in df.columns else None,
        "year": "Order Year" if "Order Year" in df.columns else None,
        "segment": "Segment" if "Segment" in df.columns else None,
    }

    trend_metrics = _compute_trend_metrics(base_df, df) if base_df is not None else {}

    return AnalysisResult(
        numeric_summary_html=numeric_summary_html,
        special_metrics=special_metrics,
        plots=plots,
        active_filters=active_filters,
        trend_metrics=trend_metrics,
    )


@app.route("/", methods=["GET", "POST"])
def index():
    """Landing page with file upload."""
    global LAST_DF_BASE, LAST_DF_FILTERED, FILTER_PRESETS

    if request.method == "POST":
        uploaded = request.files.get("file")
        if uploaded is None or uploaded.filename == "":
            flash("Please choose a CSV file to upload.", "warning")
            return redirect(url_for("index"))

        try:
            df_raw = read_uploaded_csv(uploaded)
        except Exception as exc:  # noqa: BLE001
            flash(f"Failed to read CSV: {exc}", "danger")
            return redirect(url_for("index"))

        if df_raw.empty:
            flash("Uploaded file appears to be empty.", "warning")
            return redirect(url_for("index"))

        LAST_DF_BASE = clean_generic(df_raw)
        LAST_DF_FILTERED = LAST_DF_BASE.copy()
        FILTER_PRESETS = []  # reset presets for new dataset
        return redirect(url_for("dashboard"))

    return render_template("index.html")


@app.route("/dashboard")
def dashboard():
    """Dashboard view with optional filters applied."""
    global LAST_DF_BASE, LAST_DF_FILTERED, FILTER_PRESETS

    if LAST_DF_BASE is None:
        flash("Please upload a CSV file first.", "warning")
        return redirect(url_for("index"))

    base_df = LAST_DF_BASE

    # Build filter option lists from the base dataset
    filter_options: Dict[str, list] = {}
    if "Order Year" in base_df.columns:
        filter_options["years"] = sorted(int(y) for y in base_df["Order Year"].dropna().unique())
    if "Order Month" in base_df.columns:
        # preserve natural month order via MonthNum if available
        if "Order MonthNum" in base_df.columns:
            month_order = (
                base_df[["Order Month", "Order MonthNum"]]
                .dropna()
                .drop_duplicates()
                .sort_values("Order MonthNum")
            )
            filter_options["months"] = month_order["Order Month"].tolist()
        else:
            filter_options["months"] = sorted(base_df["Order Month"].dropna().unique())
    if "Region" in base_df.columns:
        filter_options["regions"] = sorted(base_df["Region"].dropna().unique())
    if "Category" in base_df.columns:
        filter_options["categories"] = sorted(base_df["Category"].dropna().unique())

    # Apply filters based on query parameters or a saved preset
    df_filtered = base_df.copy()
    selected = {
        "year": request.args.get("year") or "",
        "month": request.args.get("month") or "",
        "region": request.args.get("region") or "",
        "category": request.args.get("category") or "",
        "preset": request.args.get("preset") or "",
    }

    # Apply preset if chosen
    if selected["preset"]:
        for name, values in FILTER_PRESETS:
            if name == selected["preset"]:
                for key in ("year", "month", "region", "category"):
                    if values.get(key):
                        selected[key] = values[key]
                break

    if selected["year"] and "Order Year" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["Order Year"] == int(selected["year"])]
    if selected["month"] and "Order Month" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["Order Month"] == selected["month"]]
    if selected["region"] and "Region" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["Region"] == selected["region"]]
    if selected["category"] and "Category" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["Category"] == selected["category"]]

    LAST_DF_FILTERED = df_filtered.copy()
    analysis = summarize_dataframe(df_filtered, base_df)

    return render_template(
        "results.html",
        analysis=analysis,
        filters=filter_options,
        selected=selected,
        presets=FILTER_PRESETS,
    )


@app.route("/save_preset", methods=["POST"])
def save_preset() -> Any:
    """Save the current filter selection as an in-memory preset."""
    global FILTER_PRESETS

    name = request.form.get("preset_name", "").strip()
    if not name:
        flash("Please provide a name for the preset.", "warning")
        return redirect(url_for("dashboard", **request.args))

    # Capture current filters from querystring
    values = {
        "year": request.args.get("year") or "",
        "month": request.args.get("month") or "",
        "region": request.args.get("region") or "",
        "category": request.args.get("category") or "",
    }
    FILTER_PRESETS.append((name, values))
    flash(f"Preset '{name}' saved.", "success")
    return redirect(url_for("dashboard", **request.args))


@app.route("/download")
def download_current() -> Any:
    """Download the currently filtered dataset as CSV."""
    global LAST_DF_FILTERED

    if LAST_DF_FILTERED is None:
        flash("No filtered data available to download.", "warning")
        return redirect(url_for("index"))

    csv_buffer = BytesIO()
    LAST_DF_FILTERED.to_csv(csv_buffer, index=False, mode='wb')
    csv_buffer.seek(0)

    return send_file(
        csv_buffer,
        mimetype="text/csv",
        as_attachment=True,
        download_name="filtered_sales_export.csv",
    )


if __name__ == "__main__":
    # Run in debug mode for local development
    app.run(debug=True)

