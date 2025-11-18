import json
import time
import threading
import logging
import os
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Any

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RECENT_MESSAGES: deque = deque(maxlen=500)
LATEST_BY_TYPE: Dict[str, Dict[str, Any]] = {}
BY_TYPE_MESSAGE_COUNT: Dict[str, int] = {}
LOCK = threading.Lock()
TOTAL_MESSAGES = 0
LAST_SUCCESSFUL_CONSUME: float = 0.0


DF_REGION_YEAR = pd.DataFrame(columns=[
    "year", "ocean_region", "environment_type", "avg_concentration", "total_observations", "unit"
])

DF_RISK_YEAR = pd.DataFrame(columns=[
    "year", "ocean_region", "environment_type", "avg_pollution", "species_count", "total_microplastics", "risk_raw", "risk_index"
])

# config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "microplastics-metrics")

ALLOWED_TYPES = {
    "avg_concentration_by_region",
    "ecological_risk_index",
}

# refresco < 1s
REFRESH_INTERVAL_MS = int(os.getenv("DASH_POLL_INTERVAL_MS", "500"))


def consumer_loop(bootstrap_servers=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC):
    global TOTAL_MESSAGES, LAST_SUCCESSFUL_CONSUME, DF_REGION_YEAR, DF_RISK_YEAR
    backoff = 1
    logger.info(f"Connecting to Kafka {bootstrap_servers} topic {topic}")

    while True:
        try:
            from kafka import KafkaConsumer

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers.split(","),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            backoff = 1
            logger.info("Consumer online")

            for msg in consumer:
                try:
                    payload = msg.value
                    if not isinstance(payload, dict):
                        continue

                    metric_type = payload.get("metric_type")
                    if metric_type not in ALLOWED_TYPES:
                        continue

                    year = payload.get("year")  # producer incluye year
                    ts = payload.get("timestamp")

                    with LOCK:
                        RECENT_MESSAGES.appendleft(payload)
                        LATEST_BY_TYPE[metric_type] = payload
                        TOTAL_MESSAGES += 1
                        BY_TYPE_MESSAGE_COUNT[metric_type] = BY_TYPE_MESSAGE_COUNT.get(metric_type, 0) + 1

                        # actualizar timestamp de último mensaje (usa timestamp del mensaje si existe)
                        try:
                            if ts:
                                dt = datetime.fromisoformat(ts)
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                LAST_SUCCESSFUL_CONSUME = dt.timestamp()
                            else:
                                LAST_SUCCESSFUL_CONSUME = time.time()
                        except:
                            LAST_SUCCESSFUL_CONSUME = time.time()

                        # UPDATES por tipo (solo anual)
                        if metric_type == "avg_concentration_by_region":
                            rows = payload.get("data") or []
                            if rows:
                                df_new = pd.DataFrame(rows)
                                # ensure columns
                                expected = {"ocean_region", "environment_type", "avg_concentration", "total_observations"}
                                if expected.issubset(df_new.columns):
                                    df_new = df_new.assign(year=year)[[
                                        "year", "ocean_region", "environment_type", "avg_concentration", "total_observations", "unit"
                                    ]].copy()
                                    # upsert by year+ocean+environment+unit
                                    for _, r in df_new.iterrows():
                                        mask = (
                                            (DF_REGION_YEAR["year"] == int(r["year"])) &
                                            (DF_REGION_YEAR["ocean_region"] == r["ocean_region"]) &
                                            (DF_REGION_YEAR["environment_type"] == r["environment_type"]) &
                                            (DF_REGION_YEAR["unit"] == r.get("unit"))
                                        )
                                        DF_REGION_YEAR = DF_REGION_YEAR[~mask]
                                    DF_REGION_YEAR = pd.concat([DF_REGION_YEAR, df_new], ignore_index=True, sort=False)

                        elif metric_type == "ecological_risk_index":
                            rows = payload.get("data") or []
                            if rows:
                                df_new = pd.DataFrame(rows)
                                # expected columns include total_microplastics, avg_pollution, species_count, risk_raw, risk_index
                                expected = {"ocean_region", "environment_type", "avg_pollution", "species_count", "risk_index"}
                                if expected.issubset(df_new.columns):
                                    # ensure total_microplastics and risk_raw present (may or may not be)
                                    cols = ["year", "ocean_region", "environment_type", "avg_pollution", "species_count", "total_microplastics", "risk_raw", "risk_index"]
                                    df_new = df_new.assign(year=year)
                                    # keep only columns that exist in df_new
                                    cols_present = [c for c in cols if c in df_new.columns or c == "year"]
                                    df_new = df_new[cols_present].copy()
                                    # upsert by year+ocean+environment
                                    for _, r in df_new.iterrows():
                                        mask = (
                                            (DF_RISK_YEAR["year"] == int(r["year"])) &
                                            (DF_RISK_YEAR["ocean_region"] == r["ocean_region"]) &
                                            (DF_RISK_YEAR["environment_type"] == r["environment_type"])
                                        )
                                        DF_RISK_YEAR = DF_RISK_YEAR[~mask]
                                    DF_RISK_YEAR = pd.concat([DF_RISK_YEAR, df_new], ignore_index=True, sort=False)

                except Exception:
                    logger.exception("Error procesando mensaje")
                    continue

        except Exception:
            logger.exception(f"Kafka connection failed, retrying in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


def _latest_year_from_region():
    with LOCK:
        if DF_REGION_YEAR.empty and DF_RISK_YEAR.empty:
            return None
        years = []
        if not DF_REGION_YEAR.empty:
            years.append(int(DF_REGION_YEAR["year"].max()))
        if not DF_RISK_YEAR.empty:
            years.append(int(DF_RISK_YEAR["year"].max()))
    return max(years) if years else None


def _build_kpis():
    with LOCK:
        total = TOTAL_MESSAGES
        dfr = DF_RISK_YEAR.copy()
        dfr_region = DF_REGION_YEAR.copy()

    current_year = _latest_year_from_region()
    current_label = "N/A"
    total_obs = "N/A"
    avg_conc = "N/A"
    avg_risk = "N/A"

    if current_year is not None:
        current_label = str(current_year)
        # total observations: sum region totals for that year (if available)
        if not dfr_region.empty and current_year in dfr_region["year"].astype(int).values:
            df_sel = dfr_region[dfr_region["year"] == current_year]
            total_obs = int(df_sel["total_observations"].sum())
            avg_conc = float(df_sel["avg_concentration"].mean()) if not df_sel["avg_concentration"].isna().all() else "N/A"
        else:
            total_obs = 0
            avg_conc = "N/A"

        # avg risk: average risk_index for that year across oceans (if available)
        if (not dfr.empty) and (current_year in dfr["year"].astype(int).values):
            avg_risk = float(dfr[dfr["year"] == current_year]["risk_index"].astype(float).mean())
        else:
            avg_risk = "N/A"

    card_style = {
        "backgroundColor": "#fff",
        "borderRadius": "10px",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.1)",
        "padding": "8px 14px",
        "minWidth": "160px",
    }

    children = []

    children.append(html.Div([
        html.Div("Total messages", style={"fontWeight": "600"}),
        html.Div(str(total))
    ], style=card_style))

    children.append(html.Div([
        html.Div("Current year", style={"fontWeight": "600"}),
        html.Div(current_label)
    ], style=card_style))

    children.append(html.Div([
        html.Div("Total observations (year)", style={"fontWeight": "600"}),
        html.Div(str(total_obs))
    ], style=card_style))

    children.append(html.Div([
        html.Div("Avg concentration (year)", style={"fontWeight": "600"}),
        html.Div(f"{avg_conc if avg_conc != 'N/A' else avg_conc}")
    ], style=card_style))

    children.append(html.Div([
        html.Div("Avg ecological risk (year)", style={"fontWeight": "600"}),
        html.Div(f"{avg_risk if avg_risk != 'N/A' else avg_risk}")
    ], style=card_style))

    return html.Div(children, style={
        "display": "flex",
        "flexWrap": "wrap",
        "gap": "14px",
        "marginBottom": "10px"
    })


def build_dash_app():
    app = Dash(__name__)

    app.layout = html.Div([
        html.H2("Microplastics – Real-time dashboard"),
        html.Div("Series anuales por región y riesgo (cada mensaje corresponde a un año).", style={"color": "#555"}),

        html.Div(id="kpi-cards"),

        html.Div([
            html.Div([dcc.Graph(id="avg-region-graph")],
                     style={"width": "49%", "backgroundColor": "#fff", "borderRadius": "10px",
                            "boxShadow": "0 2px 6px rgba(0,0,0,0.06)", "padding": "10px"}),

            html.Div([dcc.Graph(id="risk-graph")],
                     style={"width": "49%", "backgroundColor": "#fff", "borderRadius": "10px",
                            "boxShadow": "0 2px 6px rgba(0,0,0,0.06)", "padding": "10px"}),
        ], style={"display": "flex", "gap": "12px"}),

        html.Div([dcc.Graph(id="obs-graph")],
                 style={"marginTop": "20px", "backgroundColor": "#fff", "borderRadius": "10px",
                        "boxShadow": "0 2px 6px rgba(0,0,0,0.06)", "padding": "10px"}),

        html.H4("Últimos mensajes (crudos)"),
        html.Div(id="recent-messages-table",
                 style={"backgroundColor": "#fff", "borderRadius": "10px", "padding": "10px",
                        "fontFamily": "monospace", "fontSize": "12px"}),

        dcc.Interval(id="interval-refresh", interval=REFRESH_INTERVAL_MS, n_intervals=0),
    ], style={"padding": "16px", "backgroundColor": "#f5f6fa"})

    @app.callback(
        Output("kpi-cards", "children"),
        Output("avg-region-graph", "figure"),
        Output("risk-graph", "figure"),
        Output("obs-graph", "figure"),
        Output("recent-messages-table", "children"),
        Input("interval-refresh", "n_intervals"),
    )
    def refresh(_):
        kpis = _build_kpis()

        # AVG by region: stacked bars per year (each segment = ocean)
        with LOCK:
            df_reg = DF_REGION_YEAR.copy()
        fig_avg = go.Figure()
        if not df_reg.empty:
            df_reg["year"] = df_reg["year"].astype(int)
            df_reg["region_label"] = df_reg["ocean_region"].fillna("") 
            pivot = df_reg.pivot_table(index="year", columns="region_label", values="avg_concentration", aggfunc="mean", fill_value=0)
            years = list(pivot.index.astype(str))
            # stacked bars: add a trace per ocean
            for col in pivot.columns:
                fig_avg.add_bar(x=years, y=pivot[col].values, name=str(col))
            fig_avg.update_layout(barmode="stack", title="Avg concentration by ocean (annual, stacked)")

        with LOCK:
            df_risk = DF_RISK_YEAR.copy()
        fig_risk = go.Figure()
        if not df_risk.empty:
            df_risk["year"] = df_risk["year"].astype(int)
            oceans = df_risk["ocean_region"].dropna().unique()
            for ocean in oceans:
                df_o = df_risk[df_risk["ocean_region"] == ocean].sort_values("year")

                custom_cols = []
                for col in ["environment_type", "species_count", "total_microplastics", "avg_pollution", "risk_raw"]:
                    if col in df_o.columns:
                        custom_cols.append(df_o[col].astype(object).values)
                    else:
                        custom_cols.append([None] * len(df_o))

                import numpy as _np
                customdata = _np.column_stack(custom_cols)
                fig_risk.add_scatter(
                    x=df_o["year"].astype(str),
                    y=df_o["risk_index"].astype(float),
                    mode="lines+markers",
                    name=str(ocean),
                    customdata=customdata,
                    hovertemplate=(
                        "<b>Ocean:</b> " + str(ocean) + "<br>" +
                        "<b>Year:</b> %{x}<br>" +
                        "<b>Risk index:</b> %{y:.2f}<br>" +
                        "<b>Environment:</b> %{customdata[0]}<br>" +
                        "<b>Species count:</b> %{customdata[1]}<br>" +
                        "<b>Total microplastics:</b> %{customdata[2]}<br>" +
                        "<b>Avg pollution:</b> %{customdata[3]:.2f}<br>" +
                        "<b>Risk raw:</b> %{customdata[4]:.2f}<extra></extra>"
                    )
                )
            fig_risk.update_layout(title="Ecological risk index by ocean (annual)")

        with LOCK:
            df_reg_for_obs = DF_REGION_YEAR.copy()
        fig_obs = go.Figure()
        if not df_reg_for_obs.empty:
            df_reg_for_obs["year"] = df_reg_for_obs["year"].astype(int)
            grouped = df_reg_for_obs.groupby("year")["total_observations"].sum().reset_index()
            grouped = grouped.sort_values("year")
            fig_obs.add_scatter(
                x=grouped["year"].astype(str),
                y=grouped["total_observations"],
                mode="lines+markers",
                name="Total observations"
            )
            avg_val = grouped["total_observations"].mean()
            fig_obs.add_hline(y=avg_val, line_dash="dash", annotation_text=f"Avg = {avg_val:.1f}", annotation_position="top left")
            fig_obs.update_layout(
                title="Total observations per year",
                xaxis_title="Year",
                yaxis_title="Total observations",
                margin=dict(t=40, b=40, l=40, r=20)
            )

        with LOCK:
            recent = list(RECENT_MESSAGES)[:20]
        if recent:
            recent_div = html.Div([
                html.Div([
                    html.Span(m.get("metric_type", "-"), style={"fontWeight": "600"}),
                    html.Span(" — "),
                    html.Span(m.get("timestamp", "-")),
                    html.Span(f" (records: {m.get('record_count', '-')})")
                ]) for m in recent
            ])
        else:
            recent_div = html.Div("No recent messages")
        return kpis, fig_avg, fig_risk, fig_obs, recent_div
    return app


def run_server():
    t = threading.Thread(target=consumer_loop, daemon=True)
    t.start()
    app = build_dash_app()
    app.run(host="127.0.0.1", port=8051, debug=False)


if __name__ == "__main__":
    run_server()