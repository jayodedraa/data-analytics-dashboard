"""
Schema-agnostic analytics web application (local-only).

Works with ANY CSV file – not just the original data_augmented.csv schema.
It auto-detects column types and produces whatever charts are possible given
the available data.

Usage (from project root):
    .venv\\Scripts\\activate          # on Windows
    python webapp/app.py

Then open http://127.0.0.1:5000 in your browser.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
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
from io import BytesIO


app = Flask(__name__)

# Shared Plotly config – toolbar on hover, no aggressive zoom-by-drag
PLOT_CONFIG = {
    "displaylogo": False,
    "scrollZoom": False,
    "displayModeBar": "hover",
    "modeBarButtonsToRemove": [
        "zoom2d", "zoomIn2d", "zoomOut2d",
        "autoScale2d", "lasso2d", "select2d",
    ],
}

# In-memory store (local demo only)
LAST_DF_BASE: Optional[pd.DataFrame] = None
LAST_DF_FILTERED: Optional[pd.DataFrame] = None
FILTER_PRESETS: List[Tuple[str, Dict[str, str]]] = []
LAST_SCHEMA: Optional[Dict[str, Any]] = None   # detected column roles
app.secret_key = "local-dev-secret-key"

# ─── Known-column aliases ─────────────────────────────────────────────────
# Map common column name variants → a canonical "role" key.
# First match wins (case-insensitive).
ROLE_ALIASES: Dict[str, List[str]] = {
    "sales":    ["sales", "revenue", "amount", "total", "price", "value", "income"],
    "profit":   ["profit", "margin", "gain", "net"],
    "quantity": ["quantity", "qty", "units", "count"],
    "discount": ["discount", "disc", "rebate"],
    "date":     ["order date", "date", "transaction date", "invoice date",
                 "ship date", "created at", "timestamp", "time"],
    "category": ["category", "cat", "product category", "type", "group"],
    "subcategory": ["sub-category", "subcategory", "sub category",
                    "product type", "subtype"],
    "region":   ["region", "area", "zone", "territory"],
    "segment":  ["segment", "customer segment", "market segment"],
    "customer": ["customer name", "customer", "client", "client name",
                 "buyer", "customer id"],
    "product":  ["product name", "product", "item", "sku", "description"],
    "city":     ["city", "town"],
    "state":    ["state", "province", "county"],
    "country":  ["country", "nation"],
}

# ─── Colour palette ────────────────────────────────────────────────────────
PRIMARY   = "#14B8A6"
SECONDARY = "#8B5CF6"
WARM      = "#F59E0B"
DANGER    = "#EF4444"
PALETTE   = [PRIMARY, SECONDARY, WARM, "#EC4899", "#3B82F6", "#10B981"]


# ──────────────────────────────────────────────────────────────────────────
# CSV reading
# ──────────────────────────────────────────────────────────────────────────

def read_uploaded_csv(file_storage) -> pd.DataFrame:
    """Load CSV handling common encodings gracefully."""
    encodings = ["utf-8-sig", "cp1252", "latin1"]
    for enc in encodings:
        try:
            file_storage.stream.seek(0)
            return pd.read_csv(file_storage, encoding=enc)
        except UnicodeDecodeError:
            continue
    file_storage.stream.seek(0)
    return pd.read_csv(file_storage, encoding="latin1", encoding_errors="replace")


# ──────────────────────────────────────────────────────────────────────────
# Schema detection
# ──────────────────────────────────────────────────────────────────────────

def detect_schema(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Inspect a DataFrame and return a schema dict describing detected roles.

    Schema keys:
        role_map       – {role: actual_column_name} for recognised columns
        numeric_cols   – list of numeric column names
        date_cols      – list of datetime column names
        categorical_cols – list of low-cardinality object columns (≤50 unique)
        high_card_cols – list of high-cardinality text columns
        total_rows     – int
        total_cols     – int
    """
    col_lower_map = {c.strip().lower(): c for c in df.columns}

    role_map: Dict[str, str] = {}
    for role, aliases in ROLE_ALIASES.items():
        for alias in aliases:
            if alias in col_lower_map:
                col = col_lower_map[alias]
                # Only assign if not already taken by another role
                if col not in role_map.values():
                    role_map[role] = col
                break

    # Re-classify columns by dtype after cleaning
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    date_cols    = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    obj_cols     = df.select_dtypes(include=["object", "string"]).columns.tolist()

    categorical_cols: List[str] = []
    high_card_cols:   List[str] = []
    for col in obj_cols:
        n_unique = df[col].nunique(dropna=True)
        if n_unique <= 50:
            categorical_cols.append(col)
        else:
            high_card_cols.append(col)

    return {
        "role_map":         role_map,
        "numeric_cols":     numeric_cols,
        "date_cols":        date_cols,
        "categorical_cols": categorical_cols,
        "high_card_cols":   high_card_cols,
        "total_rows":       len(df),
        "total_cols":       len(df.columns),
        "all_columns":      df.columns.tolist(),
    }


# ──────────────────────────────────────────────────────────────────────────
# Cleaning
# ──────────────────────────────────────────────────────────────────────────

def clean_generic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generic cleaning that works on any DataFrame:
    - Drop exact duplicates
    - Strip whitespace from string columns
    - Auto-coerce numeric-looking object columns
    - Auto-parse date-looking object columns
    - Add Year / MonthNum / Month helper columns for the first date column found
    """
    df = df.drop_duplicates().copy()

    # Strip whitespace from string / object columns
    obj_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    for col in obj_cols:
        df[col] = df[col].astype("string").str.strip()

    # Try to coerce object columns that look numeric
    for col in obj_cols:
        if df[col].str.replace(r"[\$,£€₹%\s]", "", regex=True).str.match(
            r"^-?[\d]+\.?[\d]*$"
        ).dropna().all():
            df[col] = pd.to_numeric(
                df[col].str.replace(r"[\$,£€₹%,\s]", "", regex=True),
                errors="coerce",
            )

    # Parse date-looking columns (heuristic: name contains "date"/"time"/"created")
    date_keywords = ["date", "time", "created", "updated", "timestamp", "dt"]
    for col in df.select_dtypes(include=["object", "string"]).columns:
        if any(kw in col.lower() for kw in date_keywords):
            parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=False)
            # Accept if at least 50 % of non-null values parsed successfully
            if parsed.notna().sum() >= 0.5 * df[col].notna().sum():
                df[col] = parsed

    # For the FIRST datetime column, inject Year / MonthNum / Month helpers
    dt_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    if dt_cols:
        primary_date = dt_cols[0]
        if "_Year" not in df.columns and f"{primary_date}_Year" not in df.columns:
            df["_Year"]     = df[primary_date].dt.year
            df["_MonthNum"] = df[primary_date].dt.month
            df["_Month"]    = df[primary_date].dt.month_name()
            df["_PrimaryDate"] = df[primary_date]  # store reference

    return df


# ──────────────────────────────────────────────────────────────────────────
# Plot builder (schema-agnostic)
# ──────────────────────────────────────────────────────────────────────────

def _fig_to_html(fig) -> str:
    fig.update_layout(dragmode=False)
    return fig.to_html(include_plotlyjs=False, full_html=False, config=PLOT_CONFIG)


def build_plots(df: pd.DataFrame, schema: Dict[str, Any]) -> Dict[str, str]:
    """
    Build as many meaningful charts as the data allows.

    Returns a dict  slot_name → HTML snippet.  Slots are generated dynamically
    so the template just iterates over whatever is present.
    """
    plots: Dict[str, str] = {}
    rm = schema["role_map"]

    # ── 1. Time-series trend for every numeric column against the primary date ──
    if "_PrimaryDate" in df.columns:
        num_cols = schema["numeric_cols"]
        for nc in num_cols[:3]:   # limit to first 3 numeric columns
            valid = df.dropna(subset=["_PrimaryDate", nc])
            if valid.empty:
                continue
            monthly = (
                valid.assign(
                    _TY=lambda x: x["_PrimaryDate"].dt.year,
                    _TM=lambda x: x["_PrimaryDate"].dt.month,
                )
                .groupby(["_TY", "_TM"], as_index=False)[nc]
                .sum()
            )
            monthly["_YM"] = pd.to_datetime(
                monthly[["_TY", "_TM"]].assign(DAY=1).rename(
                    columns={"_TY": "year", "_TM": "month"}
                )
            )
            fig = px.line(
                monthly, x="_YM", y=nc,
                title=f"Monthly {nc} Trend 📈",
                markers=True,
                color_discrete_sequence=[PRIMARY],
            )
            fig.update_layout(
                template="plotly_white",
                margin=dict(l=20, r=20, t=40, b=40),
                xaxis_title="Month", yaxis_title=nc,
            )
            plots[f"trend_{nc}"] = _fig_to_html(fig)

    # ── 2. Histogram for every numeric column ──────────────────────────────
    num_cols = schema["numeric_cols"]
    for i, nc in enumerate(num_cols[:6]):   # cap at 6
        color = PALETTE[i % len(PALETTE)]
        fig = px.histogram(
            df, x=nc, nbins=30,
            title=f"Distribution of {nc}",
            color_discrete_sequence=[color],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=20),
        )
        plots[f"hist_{nc}"] = _fig_to_html(fig)

    # ── 3. Bar + pie for each categorical × first numeric ─────────────────
    if num_cols:
        primary_num = rm.get("sales") or rm.get("profit") or num_cols[0]
        if primary_num not in df.columns:
            primary_num = num_cols[0]

        cat_cols = schema["categorical_cols"]
        for i, cat in enumerate(cat_cols[:4]):   # cap at 4 categories
            grouped = (
                df.groupby(cat, as_index=False)[primary_num]
                .sum()
                .sort_values(primary_num, ascending=False)
            )
            color = PALETTE[i % len(PALETTE)]

            # Bar
            fig = px.bar(
                grouped, x=cat, y=primary_num,
                title=f"{primary_num} by {cat}",
                color_discrete_sequence=[color],
            )
            fig.update_layout(
                template="plotly_white",
                margin=dict(l=20, r=20, t=40, b=80),
                xaxis_tickangle=-45,
            )
            plots[f"bar_{cat}"] = _fig_to_html(fig)

            # Pie (only if ≤ 15 unique values for readability)
            if grouped[cat].nunique() <= 15:
                fig_pie = px.pie(
                    grouped, names=cat, values=primary_num,
                    title=f"{primary_num} share by {cat} 🥧",
                    color_discrete_sequence=px.colors.sequential.Teal,
                )
                fig_pie.update_layout(
                    template="plotly_white",
                    margin=dict(l=20, r=20, t=40, b=20),
                )
                plots[f"pie_{cat}"] = _fig_to_html(fig_pie)

    # ── 4. Top-10 "customer / product" bar chart ──────────────────────────
    top_col = rm.get("customer") or rm.get("product")
    if top_col and top_col in df.columns and num_cols:
        primary_num = rm.get("sales") or rm.get("profit") or num_cols[0]
        if primary_num not in df.columns:
            primary_num = num_cols[0]
        grouped = (
            df.groupby(top_col, as_index=False)[primary_num]
            .sum()
            .sort_values(primary_num, ascending=False)
            .head(10)
        )
        fig = px.bar(
            grouped, x=top_col, y=primary_num,
            title=f"Top 10 {top_col} by {primary_num}",
            color_discrete_sequence=[PRIMARY],
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=80),
            xaxis_tickangle=-45,
        )
        plots["top_entities"] = _fig_to_html(fig)

    # ── 5. Scatter: first two numeric columns ─────────────────────────────
    if len(num_cols) >= 2:
        x_col, y_col = num_cols[0], num_cols[1]
        color_col = schema["categorical_cols"][0] if schema["categorical_cols"] else None
        fig = px.scatter(
            df.sample(min(2000, len(df)), random_state=42),
            x=x_col, y=y_col,
            color=color_col,
            title=f"{x_col} vs {y_col}",
            opacity=0.6,
            color_discrete_sequence=PALETTE,
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=40, b=20),
        )
        plots["scatter"] = _fig_to_html(fig)

    # ── 6. Correlation heat-map (if ≥ 3 numeric cols) ─────────────────────
    if len(num_cols) >= 3:
        corr = df[num_cols].corr().round(2)
        import plotly.figure_factory as ff
        try:
            fig = ff.create_annotated_heatmap(
                z=corr.values.tolist(),
                x=corr.columns.tolist(),
                y=corr.index.tolist(),
                colorscale="Teal",
                showscale=True,
            )
            fig.update_layout(
                title="Correlation Heat-map 🔥",
                template="plotly_white",
                margin=dict(l=60, r=20, t=60, b=60),
            )
            plots["correlation"] = _fig_to_html(fig)
        except Exception:
            pass   # Silently skip if ff fails

    return plots


# ──────────────────────────────────────────────────────────────────────────
# Trend metrics (schema-agnostic)
# ──────────────────────────────────────────────────────────────────────────

def _compute_trend_metrics(base_df: pd.DataFrame, filtered_df: pd.DataFrame) -> Dict[str, Any]:
    """Month-over-month change for the primary numeric column (if dates available)."""
    metrics: Dict[str, Any] = {
        "pct_changes": [],   # list of {label, value, col}
        "comparison_label": "",
    }

    if "_Year" not in base_df.columns or "_MonthNum" not in base_df.columns:
        return metrics

    month_keys = (
        base_df[["_Year", "_MonthNum"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["_Year", "_MonthNum"])
    )
    if len(month_keys) < 2:
        return metrics

    ly, lm = month_keys.iloc[-1]
    py, pm = month_keys.iloc[-2]
    metrics["comparison_label"] = f"{int(ly)}/{int(lm):02d} vs {int(py)}/{int(pm):02d}"

    def _slice(df, y, m):
        return df[(df["_Year"] == int(y)) & (df["_MonthNum"] == int(m))]

    cur  = _slice(filtered_df, ly, lm)
    prev = _slice(filtered_df, py, pm)

    for nc in filtered_df.select_dtypes(include="number").columns:
        if nc.startswith("_"):
            continue
        c_val = cur[nc].sum()
        p_val = prev[nc].sum()
        if p_val != 0:
            pct = (c_val - p_val) / abs(p_val) * 100.0
            metrics["pct_changes"].append({"col": nc, "value": round(pct, 1)})

    return metrics


# ──────────────────────────────────────────────────────────────────────────
# Summarise
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class AnalysisResult:
    numeric_summary_html: str
    special_metrics: Dict[str, Any]
    plots: Dict[str, str]
    active_filters: Dict[str, Any]
    trend_metrics: Dict[str, Any]
    schema: Dict[str, Any]


def summarize_dataframe(
    df: pd.DataFrame,
    base_df: Optional[pd.DataFrame],
    schema: Dict[str, Any],
) -> AnalysisResult:
    """Build tables, metrics and plots for display."""

    # Numeric summary (hide internal _ columns)
    visible_num = [c for c in schema["numeric_cols"] if not c.startswith("_")]
    if visible_num:
        num_sum = df[visible_num].describe().T.round(2)
        numeric_summary_html = num_sum.to_html(
            classes="table table-sm table-striped", border=0
        )
    else:
        numeric_summary_html = "<p>No numeric columns detected.</p>"

    # KPI metrics: sum/count for recognised roles
    special_metrics: Dict[str, Any] = {}
    rm = schema["role_map"]
    for role in ("sales", "profit", "quantity"):
        col = rm.get(role)
        if col and col in df.columns:
            special_metrics[f"Total {col}"] = float(df[col].sum(skipna=True))

    # Loss orders (if profit-like column exists)
    profit_col = rm.get("profit")
    if profit_col and profit_col in df.columns:
        loss_mask = df[profit_col] < 0
        special_metrics["Loss Orders"]       = int(loss_mask.sum())
        special_metrics["Total Loss Amount"] = float(df.loc[loss_mask, profit_col].sum())
        special_metrics["_profit_col"]       = profit_col

    # Generic: total row count
    special_metrics["Total Rows"] = len(df)

    plots = build_plots(df, schema)

    # Filters present in this dataset
    active_filters: Dict[str, Any] = {
        "has_year":     "_Year"     in df.columns,
        "has_month":    "_Month"    in df.columns,
        "has_category": rm.get("category") and rm["category"] in df.columns,
        "has_region":   rm.get("region")   and rm["region"]   in df.columns,
    }

    trend_metrics = (
        _compute_trend_metrics(base_df, df) if base_df is not None else {}
    )

    return AnalysisResult(
        numeric_summary_html=numeric_summary_html,
        special_metrics=special_metrics,
        plots=plots,
        active_filters=active_filters,
        trend_metrics=trend_metrics,
        schema=schema,
    )


# ──────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET", "POST"])
def index():
    global LAST_DF_BASE, LAST_DF_FILTERED, FILTER_PRESETS, LAST_SCHEMA

    if request.method == "POST":
        uploaded = request.files.get("file")
        if uploaded is None or uploaded.filename == "":
            flash("Please choose a CSV file to upload.", "warning")
            return redirect(url_for("index"))

        try:
            df_raw = read_uploaded_csv(uploaded)
        except Exception as exc:
            flash(f"Failed to read CSV: {exc}", "danger")
            return redirect(url_for("index"))

        if df_raw.empty:
            flash("Uploaded file appears to be empty.", "warning")
            return redirect(url_for("index"))

        LAST_DF_BASE     = clean_generic(df_raw)
        LAST_DF_FILTERED = LAST_DF_BASE.copy()
        LAST_SCHEMA      = detect_schema(LAST_DF_BASE)
        FILTER_PRESETS   = []
        return redirect(url_for("dashboard"))

    return render_template("index.html")


@app.route("/dashboard")
def dashboard():
    global LAST_DF_BASE, LAST_DF_FILTERED, FILTER_PRESETS, LAST_SCHEMA

    if LAST_DF_BASE is None:
        flash("Please upload a CSV file first.", "warning")
        return redirect(url_for("index"))

    base_df = LAST_DF_BASE
    schema  = LAST_SCHEMA or detect_schema(base_df)
    rm      = schema["role_map"]

    # ── Build dynamic filter dropdowns ────────────────────────────────────
    filter_options: Dict[str, Any] = {}

    if "_Year" in base_df.columns:
        filter_options["years"] = sorted(
            int(y) for y in base_df["_Year"].dropna().unique()
        )
    if "_Month" in base_df.columns and "_MonthNum" in base_df.columns:
        month_order = (
            base_df[["_Month", "_MonthNum"]]
            .dropna()
            .drop_duplicates()
            .sort_values("_MonthNum")
        )
        filter_options["months"] = month_order["_Month"].tolist()

    # Up to 4 low-cardinality categorical columns become filter dropdowns
    cat_cols = schema["categorical_cols"]
    for cat in cat_cols[:4]:
        filter_options[f"cat_{cat}"] = sorted(base_df[cat].dropna().unique())

    # ── Apply filters ──────────────────────────────────────────────────────
    df_filtered = base_df.copy()
    selected: Dict[str, Any] = {
        "year":   request.args.get("year")   or "",
        "month":  request.args.get("month")  or "",
        "preset": request.args.get("preset") or "",
    }
    # Dynamic category filters
    for cat in cat_cols[:4]:
        selected[f"cat_{cat}"] = request.args.get(f"cat_{cat}") or ""

    # Apply preset if chosen
    if selected["preset"]:
        for name, values in FILTER_PRESETS:
            if name == selected["preset"]:
                selected.update({k: v for k, v in values.items() if v})
                break

    if selected["year"] and "_Year" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["_Year"] == int(selected["year"])]
    if selected["month"] and "_Month" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["_Month"] == selected["month"]]
    for cat in cat_cols[:4]:
        val = selected.get(f"cat_{cat}", "")
        if val and cat in df_filtered.columns:
            df_filtered = df_filtered[df_filtered[cat] == val]

    LAST_DF_FILTERED = df_filtered.copy()
    analysis = summarize_dataframe(df_filtered, base_df, schema)

    return render_template(
        "results.html",
        analysis=analysis,
        filters=filter_options,
        selected=selected,
        presets=FILTER_PRESETS,
        schema=schema,
    )


@app.route("/save_preset", methods=["POST"])
def save_preset():
    global FILTER_PRESETS

    name = request.form.get("preset_name", "").strip()
    if not name:
        flash("Please provide a name for the preset.", "warning")
        return redirect(url_for("dashboard", **request.args))

    values = {k: v for k, v in request.args.items()}
    FILTER_PRESETS.append((name, values))
    flash(f"Preset '{name}' saved.", "success")
    return redirect(url_for("dashboard", **request.args))


@app.route("/download")
def download_current():
    global LAST_DF_FILTERED

    if LAST_DF_FILTERED is None:
        flash("No filtered data available to download.", "warning")
        return redirect(url_for("index"))

    # Drop internal helper columns from the export
    export_df = LAST_DF_FILTERED.drop(
        columns=[c for c in LAST_DF_FILTERED.columns if c.startswith("_")],
        errors="ignore",
    )
    csv_buffer = BytesIO()
    export_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    return send_file(
        csv_buffer,
        mimetype="text/csv",
        as_attachment=True,
        download_name="filtered_export.csv",
    )


if __name__ == "__main__":
    app.run(debug=True)
