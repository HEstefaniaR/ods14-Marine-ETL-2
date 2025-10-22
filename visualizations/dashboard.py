import os
from datetime import datetime
import mysql.connector
import pandas as pd
import dash
from dash import dcc, html, dash_table, Input, Output, State
import plotly.express as px
import subprocess

# ================================
# Config DB (mismo setup que load.py)
# ================================
DB_CONFIG = {
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),      
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB", "marineDB"),
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def execute_query(sql: str, params=None) -> pd.DataFrame:
    """Ejecuta SQL con mysql.connector y devuelve DataFrame."""
    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


# ================================
# Consultas base
# ================================
Q_SUMMARY = """
    SELECT 
        COUNT(DISTINCT fm.observation_id) AS total_observaciones,
        COUNT(DISTINCT dl.location_id) AS total_ubicaciones,
        COUNT(DISTINCT dd.date_id) AS total_fechas,
        (SELECT COUNT(*) FROM dim_species) AS total_especies,
        COUNT(DISTINCT dc.climate_id) AS total_climas,
        AVG(fm.measurement) AS avg_medicion,
        MAX(fm.measurement) AS max_medicion,
        SUM(COALESCE(fm.volunteers_count,0)) AS total_voluntarios
    FROM fact_microplastics fm
    LEFT JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
    LEFT JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
"""

Q_OCEANS = """
    SELECT 
        dl.ocean,
        COUNT(fm.observation_id) AS conteo_observaciones,
        AVG(fm.measurement) AS avg_medicion
    FROM fact_microplastics fm
    JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
    WHERE dl.ocean IS NOT NULL
    GROUP BY dl.ocean
    ORDER BY conteo_observaciones DESC
"""

Q_TOP_LOCATIONS = """
    SELECT 
        dl.latitude,
        dl.longitude,
        dl.ocean,
        dl.marine_setting,
        COUNT(fm.observation_id) AS conteo,
        AVG(fm.measurement) AS avg_medicion
    FROM fact_microplastics fm
    JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
    WHERE dl.latitude IS NOT NULL AND dl.longitude IS NOT NULL
    GROUP BY dl.latitude, dl.longitude, dl.ocean, dl.marine_setting
    ORDER BY conteo DESC
    LIMIT 1000
"""

Q_TEMPORAL_BASE = """
    SELECT 
        dd.full_date,
        dd.year,
        dd.month,
        COUNT(fm.observation_id) AS conteo_observaciones,
        AVG(fm.measurement) AS avg_medicion
    FROM fact_microplastics fm
    JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
    WHERE dd.full_date BETWEEN %s AND %s
    GROUP BY dd.full_date, dd.year, dd.month
    ORDER BY dd.full_date
"""

Q_SAMPLING = """
    SELECT 
        dsm.sampling_method,
        dsm.mesh_size_mm,
        COUNT(fm.observation_id) AS conteo_uso,
        AVG(fm.measurement) AS avg_medicion
    FROM fact_microplastics fm
    JOIN dim_sampling_method dsm ON fm.dim_sampling_method_sampling_method_id = dsm.sampling_method_id
    WHERE dsm.sampling_method IS NOT NULL
    GROUP BY dsm.sampling_method, dsm.mesh_size_mm
    ORDER BY conteo_uso DESC
"""

Q_RAW = """
    SELECT 
        fm.observation_id,
        fm.measurement,
        fm.unit,
        dl.latitude,
        dl.longitude,
        dl.ocean,
        dd.full_date,
        dsm.sampling_method
    FROM fact_microplastics fm
    LEFT JOIN dim_location dl  ON fm.dim_location_location_id = dl.location_id
    LEFT JOIN dim_date dd      ON fm.dim_date_date_id = dd.date_id
    LEFT JOIN dim_sampling_method dsm ON fm.dim_sampling_method_sampling_method_id = dsm.sampling_method_id
    ORDER BY fm.observation_id DESC
    LIMIT 200
"""

# ================================
# App Dash
# ================================
app = dash.Dash(__name__, title="Dashboard MarineDB", suppress_callback_exceptions=True)

app.layout = html.Div([
    # Header
    html.Div([
        html.H1("🌊 Dashboard MarineDB - Esquema Estrella",
                style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 10}),
        html.P("Análisis de microplásticos marinos basado en MySQL",
               style={'textAlign': 'center', 'color': '#7f8c8d', 'marginBottom': 30}),
    ]),

    # Filtros
    html.Div([
        html.Div([
            html.Label("Rango de Fechas:", style={'fontWeight': 'bold'}),
            dcc.DatePickerRange(
                id='date-range',
                start_date=datetime(2010, 1, 1),
                end_date=datetime.now(),
                display_format='YYYY-MM-DD'
            )
        ], style={'display': 'inline-block', 'marginRight': '20px'}),

        html.Div([
            html.Label("Océano:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='ocean-filter',
                options=[],  
                multi=True,
                placeholder="Seleccionar océanos..."
            )
        ], style={'display': 'inline-block', 'width': '220px', 'marginRight': '20px'}),

        html.Button('🔄 Actualizar Datos', id='update-button', n_clicks=0,
                    style={'backgroundColor': '#3498db', 'color': 'white', 'border': 'none',
                           'padding': '10px 20px', 'borderRadius': '5px', 'cursor': 'pointer'})
    ], style={'backgroundColor': 'white', 'padding': '20px', 'borderRadius': '10px', 'marginBottom': '20px'}),

    # Métricas
    html.Div(id='metrics-cards', style={'marginBottom': '30px'}),

    # Tabs (sin especies)
    dcc.Tabs(id='tabs', value='tab-overview', children=[
        dcc.Tab(label='📊 Resumen General', value='tab-overview'),
        dcc.Tab(label='🌍 Análisis Geográfico', value='tab-geo'),
        dcc.Tab(label='📈 Análisis Temporal', value='tab-temporal'),
        dcc.Tab(label='🔬 Métodos de Muestreo', value='tab-sampling'),
        dcc.Tab(label='📋 Datos Crudos', value='tab-raw'),
    ]),
    html.Div(id='tabs-content')
])

# ================================
# Callbacks
# ================================

# Dropdown océanos
@app.callback(
    Output('ocean-filter', 'options'),
    Input('update-button', 'n_clicks')
)
def update_ocean_options(_):
    df = execute_query("SELECT DISTINCT ocean FROM dim_location WHERE ocean IS NOT NULL")
    if df.empty:
        return []
    return [{'label': o, 'value': o} for o in df['ocean'].dropna().unique()]

# Métricas
@app.callback(
    Output('metrics-cards', 'children'),
    Input('update-button', 'n_clicks')
)
def update_metrics(_):
    df = execute_query(Q_SUMMARY)
    if df.empty:
        return html.Div("Sin datos aún.")
    s = df.iloc[0].fillna(0)

    def card(emoji, value, label, color):
        return html.Div([
            html.Div(emoji, style={'fontSize': '2em', 'marginBottom': '10px'}),
            html.H3(f"{value}", style={'color': color, 'margin': '0'}),
            html.P(label, style={'margin': '0', 'color': '#7f8c8d'})
        ], style={
            'padding': '20px', 'backgroundColor': 'white', 'borderRadius': '10px',
            'textAlign': 'center', 'margin': '10px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'flex': '1', 'minWidth': '200px'
        })

    return html.Div([
        card("📊", f"{int(s['total_observaciones']):,}", "Observaciones", "#3498db"),
        card("🌎", f"{int(s['total_ubicaciones']):,}", "Ubicaciones", "#2ecc71"),
        card("🐠", f"{int(s['total_especies']):,}", "Especies", "#e74c3c"),
        card("📏", f"{float(s['avg_medicion']):.2f}", "Medición Promedio", "#9b59b6"),
    ], style={'display': 'flex', 'justifyContent': 'center', 'flexWrap': 'wrap'})

# Contenido tabs
@app.callback(
    Output('tabs-content', 'children'),
    [Input('tabs', 'value'), Input('update-button', 'n_clicks')],
    [State('date-range', 'start_date'), State('date-range', 'end_date'),
     State('ocean-filter', 'value')]
)
def render_tab(tab, n_clicks, start_date, end_date, oceans):
    if not n_clicks:
        return html.Div("Haz clic en 'Actualizar Datos' para cargar la información")
    try:
        if tab == 'tab-overview':
            return tab_overview()
        if tab == 'tab-geo':
            return tab_geo(oceans)
        if tab == 'tab-temporal':
            return tab_temporal(start_date, end_date)
        if tab == 'tab-sampling':
            return tab_sampling()
        if tab == 'tab-raw':
            return tab_raw()
        return html.Div("Tab no reconocido.")
    except Exception as e:
        return html.Div(f"⚠️ Error cargando datos: {e}")

# ================================
# Funciones de tabs
# ================================
def tab_overview():
    df_o = execute_query(Q_OCEANS)
    df_t = execute_query("""
        SELECT dd.year, dd.month, dd.full_date,
               COUNT(fm.observation_id) AS conteo_observaciones
        FROM fact_microplastics fm
        JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
        WHERE dd.year IS NOT NULL
        GROUP BY dd.year, dd.month, dd.full_date
        ORDER BY dd.full_date
    """)
    if df_o.empty and df_t.empty:
        return html.Div("Sin datos para mostrar todavía.")

    comps = []
    if not df_o.empty:
        fig_o = px.pie(df_o, values='conteo_observaciones', names='ocean',
                       title='Distribución de Observaciones por Océano', hole=0.4)
        comps.append(dcc.Graph(figure=fig_o, style={'width': '48%', 'display': 'inline-block'}))

    if not df_t.empty:
        yearly = df_t.groupby('year', as_index=False)['conteo_observaciones'].sum()
        fig_t = px.line(yearly, x='year', y='conteo_observaciones',
                        title='Evolución Temporal de Observaciones', markers=True)
        comps.append(dcc.Graph(figure=fig_t, style={'width': '48%', 'display': 'inline-block', 'float': 'right'}))

    return html.Div(comps)

def tab_geo(oceans):
    df = execute_query(Q_TOP_LOCATIONS)
    if oceans:
        df = df[df['ocean'].isin(oceans)]
    if df.empty:
        return html.Div("Sin puntos para el mapa.")
    fig = px.scatter_geo(
        df, lat='latitude', lon='longitude', color='ocean', size='conteo',
        hover_name='marine_setting',
        hover_data={'avg_medicion': ':.2f', 'conteo': True},
        title='Distribución Geográfica de Observaciones',
        projection='natural earth'
    )
    return dcc.Graph(figure=fig)

def tab_temporal(start_date, end_date):
    if not start_date or not end_date:
        start_date = "2000-01-01"
        end_date = datetime.now().date().isoformat()
    df = execute_query(Q_TEMPORAL_BASE, (start_date, end_date))
    if df.empty:
        return html.Div("Sin observaciones en ese rango.")
    fig = px.line(df, x='full_date', y='conteo_observaciones',
                  title='Observaciones por Fecha', markers=True)
    return dcc.Graph(figure=fig)

def tab_sampling():
    df = execute_query(Q_SAMPLING)
    if df.empty:
        return html.Div("Sin métodos de muestreo.")
    fig = px.bar(df.head(10), x='sampling_method', y='conteo_uso',
                 title='Métodos de Muestreo Más Utilizados',
                 color='avg_medicion', color_continuous_scale='Viridis')
    return dcc.Graph(figure=fig)

def tab_raw():
    df = execute_query(Q_RAW)
    if df.empty:
        return html.Div("Sin datos crudos para mostrar.")
    return dash_table.DataTable(
        data=df.to_dict('records'),
        columns=[{'name': c, 'id': c} for c in df.columns],
        page_size=10,
        style_table={'overflowX': 'auto'},
        style_header={'backgroundColor': '#3498db', 'color': 'white', 'fontWeight': 'bold'},
        style_cell={'textAlign': 'left', 'padding': '10px'},
    )

# ================================
# Run
# ================================
if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)