import os
from datetime import datetime, timedelta
import mysql.connector
import pandas as pd
import dash
from dash import dcc, html, dash_table, Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# ================================
# Config DB
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
    """Ejecuta SQL y devuelve DataFrame."""
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
# Consultas SQL Optimizadas
# ================================

Q_KPI_SUMMARY = """
    SELECT 
        COUNT(DISTINCT fm.observation_id) AS total_observaciones,
        COUNT(DISTINCT dl.location_id) AS total_ubicaciones,
        COUNT(DISTINCT dd.date_id) AS total_fechas,
        COUNT(DISTINCT ds.species_id) AS total_especies,
        COUNT(DISTINCT dc.climate_id) AS total_registros_clima,
        AVG(fm.measurement) AS avg_microplasticos,
        MAX(fm.measurement) AS max_microplasticos,
        MIN(fm.measurement) AS min_microplasticos,
        STDDEV(fm.measurement) AS std_microplasticos,
        AVG(dc.wave_height_max) AS avg_wave_height,
        AVG(dc.swell_wave_height_max) AS avg_swell_height
    FROM fact_microplastics fm
    LEFT JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
    LEFT JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
"""

Q_CRITICAL_ZONES = """
    SELECT 
        dl.latitude,
        dl.longitude,
        dl.ocean,
        dl.marine_setting,
        COUNT(DISTINCT fm.observation_id) AS num_observaciones,
        AVG(fm.measurement) AS avg_microplasticos,
        MAX(fm.measurement) AS max_microplasticos,
        COUNT(DISTINCT ds.species_id) AS num_especies,
        AVG(dc.wave_height_max) AS avg_wave_height,
        AVG(dc.swell_wave_height_max) AS avg_swell_height,
        AVG(dc.wave_period_max) AS avg_wave_period
    FROM fact_microplastics fm
    JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dl.latitude IS NOT NULL AND dl.longitude IS NOT NULL
    GROUP BY dl.latitude, dl.longitude, dl.ocean, dl.marine_setting
    HAVING avg_microplasticos > (SELECT AVG(measurement) FROM fact_microplastics)
    ORDER BY avg_microplasticos DESC, num_especies ASC
    LIMIT 100
"""

Q_TEMPORAL_TRENDS = """
    SELECT 
        dd.year,
        dd.month,
        dd.quarter,
        dd.decade,
        COUNT(DISTINCT fm.observation_id) AS num_observaciones,
        AVG(fm.measurement) AS avg_microplasticos,
        COUNT(DISTINCT ds.species_id) AS num_especies,
        AVG(dc.wave_height_max) AS avg_wave_height,
        AVG(dc.swell_wave_height_max) AS avg_swell_height,
        AVG(dc.wave_period_max) AS avg_wave_period,
        AVG(dc.wave_direction_dominant) AS avg_wave_direction
    FROM fact_microplastics fm
    JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dd.year >= %s AND dd.year <= %s
    GROUP BY dd.year, dd.month, dd.quarter, dd.decade
    ORDER BY dd.year, dd.month
"""

Q_OCEAN_COMPARISON = """
    SELECT 
        dl.ocean,
        dl.marine_setting,
        COUNT(DISTINCT fm.observation_id) AS num_observaciones,
        AVG(fm.measurement) AS avg_microplasticos,
        STDDEV(fm.measurement) AS std_microplasticos,
        COUNT(DISTINCT ds.species_id) AS num_especies,
        COUNT(DISTINCT ds.phylum) AS num_phylums,
        AVG(dc.wave_height_max) AS avg_wave_height,
        AVG(dc.swell_wave_height_max) AS avg_swell_height
    FROM fact_microplastics fm
    JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dl.ocean IS NOT NULL
    GROUP BY dl.ocean, dl.marine_setting
    ORDER BY avg_microplasticos DESC
"""

Q_SPECIES_BIODIVERSITY = """
    SELECT 
        ds.kingdom,
        ds.phylum,
        ds.class,
        ds.order_name,
        ds.family,
        COUNT(DISTINCT ds.species_id) AS num_especies,
        COUNT(DISTINCT fm.observation_id) AS num_observaciones,
        AVG(fm.measurement) AS avg_microplasticos,
        AVG(dc.wave_height_max) AS avg_wave_height
    FROM dim_species ds
    JOIN microplastics_species_bridge msb ON ds.species_id = msb.dim_species_species_id
    JOIN fact_microplastics fm ON msb.fact_microplastics_observation_id = fm.observation_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    WHERE ds.kingdom IS NOT NULL
    GROUP BY ds.kingdom, ds.phylum, ds.class, ds.order_name, ds.family
    ORDER BY num_especies DESC
    LIMIT 100
"""

Q_CLIMATE_CORRELATION = """
    SELECT 
        dc.wave_height_max,
        dc.wave_period_max,
        dc.swell_wave_height_max,
        dc.wave_direction_dominant,
        dc.wind_wave_direction_dominant,
        AVG(fm.measurement) AS avg_microplasticos,
        COUNT(fm.observation_id) AS num_observaciones,
        COUNT(DISTINCT ds.species_id) AS num_especies
    FROM fact_microplastics fm
    JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dc.wave_height_max IS NOT NULL OR dc.swell_wave_height_max IS NOT NULL
    GROUP BY dc.wave_height_max, dc.wave_period_max, dc.swell_wave_height_max, 
             dc.wave_direction_dominant, dc.wind_wave_direction_dominant
    HAVING num_observaciones >= 2
    ORDER BY avg_microplasticos DESC
"""

Q_SAMPLING_EFFECTIVENESS = """
    SELECT 
        dsm.sampling_method,
        dsm.concentration_class_range,
        dsm.concentration_class_text,
        COUNT(DISTINCT fm.observation_id) AS num_observaciones,
        AVG(fm.measurement) AS avg_microplasticos,
        MIN(fm.measurement) AS min_microplasticos,
        MAX(fm.measurement) AS max_microplasticos,
        STDDEV(fm.measurement) AS std_microplasticos,
        COUNT(DISTINCT ds.species_id) AS num_especies,
        AVG(dc.wave_height_max) AS avg_wave_height
    FROM fact_microplastics fm
    JOIN dim_sampling_method dsm ON fm.dim_sampling_method_sampling_method_id = dsm.sampling_method_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dsm.sampling_method IS NOT NULL
    GROUP BY dsm.sampling_method, dsm.concentration_class_range, dsm.concentration_class_text
    ORDER BY num_observaciones DESC
"""

Q_CLIMATE_SPECIES_IMPACT = """
    SELECT 
        CASE 
            WHEN dc.wave_height_max < 2 THEN 'Bajo (< 2m)'
            WHEN dc.wave_height_max < 4 THEN 'Medio (2-4m)'
            ELSE 'Alto (> 4m)'
        END AS oleaje_categoria,
        COUNT(DISTINCT ds.species_id) AS num_especies,
        AVG(fm.measurement) AS avg_microplasticos,
        COUNT(DISTINCT fm.observation_id) AS num_observaciones,
        AVG(dc.wave_height_max) AS avg_wave_height,
        AVG(dc.swell_wave_height_max) AS avg_swell
    FROM fact_microplastics fm
    JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dc.wave_height_max IS NOT NULL
    GROUP BY oleaje_categoria
    ORDER BY avg_wave_height
"""

Q_HOTSPOTS_MATRIX = """
    SELECT 
        dl.latitude,
        dl.longitude,
        dl.ocean,
        dl.marine_setting,
        AVG(fm.measurement) AS microplasticos,
        COUNT(DISTINCT ds.species_id) AS biodiversidad,
        AVG(dc.wave_height_max) AS oleaje,
        AVG(dc.swell_wave_height_max) AS swell,
        COUNT(DISTINCT fm.observation_id) AS observaciones
    FROM fact_microplastics fm
    JOIN dim_location dl ON fm.dim_location_location_id = dl.location_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dl.latitude IS NOT NULL AND dl.longitude IS NOT NULL
    GROUP BY dl.latitude, dl.longitude, dl.ocean, dl.marine_setting
    HAVING microplasticos IS NOT NULL
"""

Q_DECADE_ANALYSIS = """
    SELECT 
        dd.decade,
        COUNT(DISTINCT fm.observation_id) AS observaciones,
        AVG(fm.measurement) AS avg_microplasticos,
        COUNT(DISTINCT ds.species_id) AS especies,
        AVG(dc.wave_height_max) AS avg_oleaje
    FROM fact_microplastics fm
    JOIN dim_date dd ON fm.dim_date_date_id = dd.date_id
    LEFT JOIN dim_climate dc ON fm.dim_climate_climate_id = dc.climate_id
    LEFT JOIN microplastics_species_bridge msb ON fm.observation_id = msb.fact_microplastics_observation_id
    LEFT JOIN dim_species ds ON msb.dim_species_species_id = ds.species_id
    WHERE dd.decade IS NOT NULL
    GROUP BY dd.decade
    ORDER BY dd.decade
"""

# ================================
# App Dash
# ================================
app = dash.Dash(
    __name__,
    title="MarineDB Analytics - ODS 14",
    suppress_callback_exceptions=True
)

# Estilos
COLORS = {
    'primary': '#0066CC',
    'secondary': '#00A86B',
    'danger': '#DC3545',
    'warning': '#FFC107',
    'info': '#17A2B8',
    'light': '#F8F9FA',
    'dark': '#343A40',
    'ocean': '#006994',
    'marine': '#1E88E5'
}

STYLE_CARD = {
    'backgroundColor': 'white',
    'borderRadius': '12px',
    'padding': '25px',
    'boxShadow': '0 4px 6px rgba(0,0,0,0.07)',
    'marginBottom': '20px',
    'border': '1px solid #E9ECEF'
}

app.layout = html.Div([
    # Header
    html.Div([
        html.Div([
            html.H1("MarineDB Analytics Dashboard", style={
                'color': COLORS['ocean'],
                'marginBottom': '10px',
                'fontWeight': '700',
                'fontSize': '2.5rem'
            }),
            html.P("Monitoreo de Microplásticos, Clima y Biodiversidad Marina | ODS 14", style={
                'color': '#6C757D',
                'fontSize': '1.1rem'
            })
        ], style={'flex': '1'}),

        html.Div([
            html.Button('Actualizar', id='update-button', n_clicks=0, style={
                'backgroundColor': COLORS['primary'],
                'color': 'white',
                'border': 'none',
                'padding': '12px 28px',
                'borderRadius': '8px',
                'cursor': 'pointer',
                'fontSize': '1rem',
                'fontWeight': '600'
            })
        ])
    ], style={
        'backgroundColor': 'white',
        'padding': '30px 40px',
        'marginBottom': '25px',
        'borderRadius': '12px',
        'boxShadow': '0 2px 8px rgba(0,0,0,0.08)',
        'display': 'flex',
        'justifyContent': 'space-between',
        'alignItems': 'center'
    }),

    # Filtros
    html.Div([
        html.H3("Filtros de Análisis", style={'marginBottom': '20px', 'color': COLORS['dark']}),
        html.Div([
            html.Div([
                html.Label("Rango Temporal:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block'}),
                dcc.DatePickerRange(
                    id='date-range',
                    start_date=datetime(2010, 1, 1),
                    end_date=datetime.now(),
                    display_format='YYYY-MM-DD'
                )
            ], style={'flex': '1', 'marginRight': '15px'}),

            html.Div([
                html.Label("Océanos:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block'}),
                dcc.Dropdown(
                    id='ocean-filter',
                    options=[],
                    multi=True,
                    placeholder="Todos los océanos..."
                )
            ], style={'flex': '1', 'marginRight': '15px'}),

            html.Div([
                html.Label("Entorno Marino:", style={'fontWeight': '600', 'marginBottom': '8px', 'display': 'block'}),
                dcc.Dropdown(
                    id='setting-filter',
                    options=[],
                    multi=True,
                    placeholder="Todos los entornos..."
                )
            ], style={'flex': '1'})
        ], style={'display': 'flex', 'gap': '15px'})
    ], style=STYLE_CARD),

    # KPIs
    html.Div(id='kpi-cards', style={'marginBottom': '25px'}),

    # Tabs
    dcc.Tabs(id='main-tabs', value='tab-overview', children=[
        dcc.Tab(label='Resumen Ejecutivo', value='tab-overview'),
        dcc.Tab(label='Zonas Críticas', value='tab-critical'),
        dcc.Tab(label='Análisis Temporal', value='tab-temporal'),
        dcc.Tab(label='Biodiversidad', value='tab-biodiversity'),
        dcc.Tab(label='Clima & Correlaciones', value='tab-climate'),
        dcc.Tab(label='Métodos Muestreo', value='tab-sampling'),
    ]),

    html.Div(id='tab-content')
], style={
    'backgroundColor': '#F5F7FA',
    'minHeight': '100vh',
    'padding': '25px',
    'fontFamily': 'Arial, sans-serif'
})

# ================================
# Callbacks
# ================================

@app.callback(
    [Output('ocean-filter', 'options'),
     Output('setting-filter', 'options')],
    Input('update-button', 'n_clicks')
)
def update_filter_options(_):
    df_oceans = execute_query("SELECT DISTINCT ocean FROM dim_location WHERE ocean IS NOT NULL ORDER BY ocean")
    df_settings = execute_query("SELECT DISTINCT marine_setting FROM dim_location WHERE marine_setting IS NOT NULL ORDER BY marine_setting")

    ocean_opts = [{'label': o, 'value': o} for o in df_oceans['ocean'].dropna().unique()] if not df_oceans.empty else []
    setting_opts = [{'label': s, 'value': s} for s in df_settings['marine_setting'].dropna().unique()] if not df_settings.empty else []

    return ocean_opts, setting_opts

@app.callback(
    Output('kpi-cards', 'children'),
    Input('update-button', 'n_clicks')
)
def update_kpis(_):
    df = execute_query(Q_KPI_SUMMARY)
    if df.empty:
        return html.Div("Sin datos disponibles", style={'textAlign': 'center'})

    s = df.iloc[0].fillna(0)

    def kpi_card(value, label, sublabel, color):
        return html.Div([
            html.H2(f"{value}", style={
                'color': color,
                'margin': '0',
                'fontSize': '2.2rem',
                'fontWeight': '700'
            }),
            html.P(label, style={
                'margin': '5px 0 0 0',
                'color': COLORS['dark'],
                'fontWeight': '600'
            }),
            html.P(sublabel, style={
                'margin': '3px 0 0 0',
                'color': '#868E96',
                'fontSize': '0.85rem'
            })
        ], style={
            **STYLE_CARD,
            'textAlign': 'center',
            'flex': '1',
            'minWidth': '200px',
            'margin': '10px'
        })

    return html.Div([
        kpi_card(f"{int(s['total_observaciones']):,}", "Observaciones", "Registros totales", COLORS['primary']),
        kpi_card(f"{int(s['total_ubicaciones']):,}", "Ubicaciones", "Puntos únicos", COLORS['ocean']),
        kpi_card(f"{int(s['total_especies']):,}", "Especies", "Biodiversidad", COLORS['secondary']),
        kpi_card(f"{int(s['total_registros_clima']):,}", "Datos Climáticos", "Registros oceánicos", COLORS['info']),
        kpi_card(f"{float(s['avg_microplasticos']):.2f}", "Microplásticos", f"Oleaje: {float(s.get('avg_wave_height', 0)):.2f} m", COLORS['danger']),
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'center'})

@app.callback(
    Output('tab-content', 'children'),
    [Input('main-tabs', 'value'), Input('update-button', 'n_clicks')],
    [State('date-range', 'start_date'),
     State('date-range', 'end_date'),
     State('ocean-filter', 'value')]
)
def render_tab(tab, _, start_date, end_date, oceans):
    try:
        if tab == 'tab-overview':
            return render_overview()
        elif tab == 'tab-critical':
            return render_critical_zones(oceans)
        elif tab == 'tab-temporal':
            return render_temporal(start_date, end_date)
        elif tab == 'tab-biodiversity':
            return render_biodiversity()
        elif tab == 'tab-climate':
            return render_climate()
        elif tab == 'tab-sampling':
            return render_sampling()
    except Exception as e:
        return html.Div(f"Error: {str(e)}", style=STYLE_CARD)

# ================================
# Funciones de renderizado
# ================================

def render_overview():
    df_ocean = execute_query(Q_OCEAN_COMPARISON)
    df_hotspots = execute_query(Q_HOTSPOTS_MATRIX)
    df_decade = execute_query(Q_DECADE_ANALYSIS)

    if df_ocean.empty:
        return html.Div("Sin datos", style=STYLE_CARD)

    # Gráfico océanos con clima
    fig_ocean = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Microplásticos por Océano', 'Condiciones de Oleaje'),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )

    fig_ocean.add_trace(
        go.Bar(x=df_ocean['ocean'], y=df_ocean['avg_microplasticos'],
               name='Microplásticos', marker_color=COLORS['danger']),
        row=1, col=1
    )

    fig_ocean.add_trace(
        go.Bar(x=df_ocean['ocean'], y=df_ocean['avg_wave_height'],
               name='Oleaje (m)', marker_color=COLORS['info']),
        row=1, col=2
    )

    fig_ocean.update_layout(height=400, showlegend=True, template='plotly_white')

    # Mapa hotspots con clima
    fig_map = go.Figure()

    if not df_hotspots.empty:
        df_hotspots['riesgo_score'] = (
            df_hotspots['microplasticos'] / df_hotspots['microplasticos'].max() * 0.6 +
            (1 - df_hotspots['biodiversidad'].fillna(0) / df_hotspots['biodiversidad'].max()) * 0.4
        )

        fig_map.add_trace(go.Scattergeo(
            lon=df_hotspots['longitude'],
            lat=df_hotspots['latitude'],
            mode='markers',
            marker=dict(
                size=df_hotspots['microplasticos']/df_hotspots['microplasticos'].max()*30 + 8,
                color=df_hotspots['riesgo_score'],
                colorscale='RdYlGn_r',
                showscale=True,
                colorbar=dict(title="Índice Riesgo"),
                line=dict(width=1, color='white')
            ),
            text=df_hotspots.apply(lambda r:
                f"{r['ocean']}<br>"
                f"Microplásticos: {r['microplasticos']:.2f}<br>"
                f"Especies: {r['biodiversidad']}<br>"
                f"Oleaje: {r['oleaje']:.2f} m<br>"
                f"Swell: {r['swell']:.2f} m", axis=1),
            hoverinfo='text'
        ))

    fig_map.update_layout(
        title='Distribución Geográfica con Datos Climáticos',
        geo=dict(scope='north america', showland=True, landcolor='rgb(243, 243, 243)'),
        height=500
    )

    # Análisis por década
    fig_decade = go.Figure()
    if not df_decade.empty:
        fig_decade.add_trace(go.Scatter(
            x=df_decade['decade'], y=df_decade['avg_microplasticos'],
            mode='lines+markers', name='Microplásticos',
            line=dict(color=COLORS['danger'], width=3)
        ))
        fig_decade.add_trace(go.Scatter(
            x=df_decade['decade'], y=df_decade['avg_oleaje'],
            mode='lines+markers', name='Oleaje Promedio (m)',
            line=dict(color=COLORS['info'], width=3), yaxis='y2'
        ))
        fig_decade.update_layout(
            title='Evolución por Década: Contaminación vs Condiciones Oceánicas',
            yaxis=dict(title='Microplásticos'),
            yaxis2=dict(title='Oleaje (m)', overlaying='y', side='right'),
            height=400, template='plotly_white'
        )

    return html.Div([
        html.Div([dcc.Graph(figure=fig_ocean)], style=STYLE_CARD),
        html.Div([dcc.Graph(figure=fig_map)], style=STYLE_CARD),
        html.Div([dcc.Graph(figure=fig_decade)], style=STYLE_CARD) if not df_decade.empty else html.Div()
    ])

def render_critical_zones(oceans):
    df = execute_query(Q_CRITICAL_ZONES)

    if oceans:
        df = df[df['ocean'].isin(oceans)]

    if df.empty:
        return html.Div("Sin zonas críticas", style=STYLE_CARD)

    # Mapa con datos climáticos
    fig = px.scatter_geo(
        df,
        lat='latitude',
        lon='longitude',
        size='avg_microplasticos',
        color='avg_wave_height',
        hover_name='marine_setting',
        hover_data={
            'ocean': True,
            'avg_microplasticos': ':.2f',
            'max_microplasticos': ':.2f',
            'num_especies': True,
            'avg_wave_height': ':.2f',
            'avg_swell_height': ':.2f',
            'avg_wave_period': ':.2f'
        },
        title='Zonas Críticas con Condiciones Oceánicas',
        labels={
            'avg_microplasticos': 'Microplásticos',
            'avg_wave_height': 'Oleaje (m)'
        },
        color_continuous_scale='Turbo',
        size_max=40
    )

    fig.update_layout(
        geo=dict(scope='north america', showland=True),
        height=600
    )

    # Note: Top 15 table was removed as requested.

    return html.Div([
        html.Div([
            html.H3("Zonas Críticas con Análisis Climático", style={'color': COLORS['danger']}),
            html.P("Integración de contaminación, biodiversidad y condiciones oceánicas"),
            dcc.Graph(figure=fig)
        ], style=STYLE_CARD)
    ])

def render_temporal(start_date, end_date):
    if not start_date or not end_date:
        start_year, end_year = 2010, datetime.now().year
    else:
        start_year = pd.to_datetime(start_date).year
        end_year = pd.to_datetime(end_date).year

    df = execute_query(Q_TEMPORAL_TRENDS, (start_year, end_year))

    if df.empty:
        return html.Div("Sin datos temporales", style=STYLE_CARD)

    # Crear fecha completa
    df['fecha'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)

    # Gráfico 1: Observaciones y Microplásticos en el tiempo (dos ejes secundarios)
    fig1 = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]]
    )

    fig1.add_trace(
        go.Scatter(
            x=df['fecha'],
            y=df['num_observaciones'],
            mode='lines+markers',
            name='Observaciones',
            line=dict(color=COLORS['primary'], width=2),
            marker=dict(size=6)
        ),
        secondary_y=False
    )

    fig1.add_trace(
        go.Scatter(
            x=df['fecha'],
            y=df['avg_microplasticos'],
            mode='lines+markers',
            name='Microplásticos',
            line=dict(color=COLORS['danger'], width=2),
            marker=dict(size=6)
        ),
        secondary_y=True
    )

    fig1.update_xaxes(title_text="Fecha")
    fig1.update_yaxes(title_text="Número Observaciones", secondary_y=False)
    fig1.update_yaxes(title_text="Concentración Microplásticos", secondary_y=True)
    fig1.update_layout(
        title='Evolución Temporal de Observaciones y Contaminación',
        height=400,
        template='plotly_white',
        hovermode='x unified'
    )

    # Gráfico 2: Condiciones climáticas en el tiempo
    fig2 = go.Figure()

    fig2.add_trace(go.Scatter(
        x=df['fecha'],
        y=df['avg_wave_height'],
        mode='lines',
        name='Altura Olas (m)',
        fill='tozeroy',
        line=dict(color=COLORS['info'], width=2)
    ))

    fig2.add_trace(go.Scatter(
        x=df['fecha'],
        y=df['avg_swell_height'],
        mode='lines',
        name='Swell (m)',
        fill='tozeroy',
        line=dict(color=COLORS['marine'], width=2)
    ))

    fig2.update_layout(
        title='Condiciones Oceánicas en el Tiempo',
        xaxis_title='Fecha',
        yaxis_title='Altura (metros)',
        height=400,
        template='plotly_white',
        hovermode='x unified'
    )

    # ----------------------------
    # Simplificado - Análisis estacional:
    # Ahora: gráfico de barras simples de microplásticos por trimestre
    # y gráfico de barras separado de oleaje promedio por trimestre (más claro)
    # ----------------------------
    # Aseguro que trimestre tenga valores 1..4
    df_quarter = df.groupby('quarter', as_index=False).agg({
        'avg_microplasticos': 'mean',
        'avg_wave_height': 'mean'
    })

    # Construyo dataframe completo de 1..4 para rellenar faltantes (sin usar categorical)
    quarters_index = pd.DataFrame({'quarter': [1, 2, 3, 4]})
    df_quarter = quarters_index.merge(df_quarter, how='left', on='quarter').fillna(0)
    df_quarter['quarter_label'] = df_quarter['quarter'].apply(lambda q: f"Q{int(q)}")

    # Barra: microplásticos por trimestre (simple)
    fig_season_mp = go.Figure()
    fig_season_mp.add_trace(go.Bar(
        x=df_quarter['quarter_label'],
        y=df_quarter['avg_microplasticos'],
        name='Microplásticos (promedio por trimestre)',
        marker_color=COLORS['danger']
    ))
    fig_season_mp.update_layout(title='Microplásticos promedio por trimestre', height=350, template='plotly_white',
                               xaxis_title='Trimestre', yaxis_title='Microplásticos (promedio)')

    # Barra: oleaje promedio por trimestre (simple)
    fig_season_wave = go.Figure()
    fig_season_wave.add_trace(go.Bar(
        x=df_quarter['quarter_label'],
        y=df_quarter['avg_wave_height'],
        name='Oleaje promedio (m) por trimestre',
        marker_color=COLORS['info']
    ))
    fig_season_wave.update_layout(title='Oleaje promedio por trimestre', height=350, template='plotly_white',
                                 xaxis_title='Trimestre', yaxis_title='Oleaje (m)')

    # Biodiversidad rápida vs clima (simple)
    fig_biod_simple = make_subplots(rows=1, cols=1, specs=[[{}]])
    fig_biod_simple.add_trace(go.Bar(x=df['fecha'], y=df['num_especies'], name='Especies', marker_color=COLORS['secondary']))
    fig_biod_simple.update_layout(title='Número de especies observadas en el tiempo', height=300, template='plotly_white')

    return html.Div([
        html.Div([dcc.Graph(figure=fig1)], style=STYLE_CARD),
        html.Div([dcc.Graph(figure=fig2)], style=STYLE_CARD),
        html.Div([dcc.Graph(figure=fig_season_mp)], style=STYLE_CARD),
        html.Div([dcc.Graph(figure=fig_season_wave)], style=STYLE_CARD),
        html.Div([dcc.Graph(figure=fig_biod_simple)], style=STYLE_CARD)
    ])

def render_biodiversity():
    df_species = execute_query(Q_SPECIES_BIODIVERSITY)
    df_climate_impact = execute_query(Q_CLIMATE_SPECIES_IMPACT)

    if df_species.empty:
        return html.Div("Sin datos de biodiversidad", style=STYLE_CARD)

    # ----------------------------
    # Simplificar: dos gráficos sencillos: especies por phylum y por familia
    # Además: gráfico Top por exposición -> cruza especies (grupos) con avg_microplasticos
    # ----------------------------
    df_phylum = df_species.groupby('phylum', dropna=True, as_index=False).agg({
        'num_especies': 'sum',
        'avg_microplasticos': 'mean'
    }).sort_values('num_especies', ascending=False)

    df_family = df_species.groupby('family', dropna=True, as_index=False).agg({
        'num_especies': 'sum',
        'avg_microplasticos': 'mean'
    }).sort_values('num_especies', ascending=False)

    fig_phylum = px.bar(df_phylum.head(20), x='phylum', y='num_especies', title='Especies por Phylum', labels={'num_especies': 'Número de especies', 'phylum': 'Phylum'})
    fig_phylum.update_layout(height=400, template='plotly_white')

    fig_family = px.bar(df_family.head(20), x='family', y='num_especies', title='Especies por Familia (top 20)', labels={'num_especies': 'Número de especies', 'family': 'Familia'})
    fig_family.update_layout(height=400, template='plotly_white')

    # Nuevo: Top grupos por exposición (avg_microplasticos) — cruza biodiversidad con contaminación
    df_exposure = df_species.groupby(['phylum', 'family'], as_index=False).agg({
        'num_especies': 'sum',
        'avg_microplasticos': 'mean'
    }).sort_values('avg_microplasticos', ascending=False)

    fig_exposure = px.bar(df_exposure.head(20), x='phylum', y='avg_microplasticos',
                         color='family', title='Top 20 Grupos (Phylum) por Exposición Promedio a Microplásticos',
                         labels={'avg_microplasticos': 'Microplásticos (promedio)', 'phylum': 'Phylum'})
    fig_exposure.update_layout(height=420, template='plotly_white', xaxis_tickangle=-45)

    # Mantener Top 20 grupos taxonómicos con mayor exposición intacto (tabla)
    df_top = df_species.head(20)[['kingdom', 'phylum', 'class', 'order_name',
                                   'num_especies', 'avg_microplasticos', 'avg_wave_height']]
    df_top.columns = ['Reino', 'Filo', 'Clase', 'Orden', 'Num Especies',
                      'Microplásticos', 'Oleaje (m)']

    return html.Div([
        html.Div([
            html.H3("Análisis de Biodiversidad Marina", style={'color': COLORS['secondary']}),
            html.P("Estructura taxonómica y exposición a contaminantes"),
            dcc.Graph(figure=fig_phylum)
        ], style=STYLE_CARD),

        html.Div([dcc.Graph(figure=fig_family)], style=STYLE_CARD),

        html.Div([dcc.Graph(figure=fig_exposure)], style=STYLE_CARD),

        html.Div([
            html.H3("Top 20 Grupos Taxonómicos con Mayor Exposición"),
            dash_table.DataTable(
                data=df_top.to_dict('records'),
                columns=[{'name': c, 'id': c} for c in df_top.columns],
                page_size=10,
                style_header={'backgroundColor': COLORS['secondary'], 'color': 'white', 'fontWeight': 'bold'},
                style_cell={'textAlign': 'left', 'padding': '10px'},
                style_data_conditional=[
                    {'if': {'column_id': 'Microplásticos', 'filter_query': '{Microplásticos} > 50'},
                     'backgroundColor': '#ffcccc', 'color': 'black'}
                ]
            )
        ], style=STYLE_CARD)
    ])

def render_climate():
    df_corr = execute_query(Q_CLIMATE_CORRELATION)

    if df_corr.empty:
        return html.Div("Sin datos climáticos suficientes", style=STYLE_CARD)

    # Eliminada la 3D - en su lugar: scatter simple oleaje vs microplásticos
    df_scatter = df_corr[['wave_height_max', 'swell_wave_height_max', 'avg_microplasticos', 'num_observaciones', 'num_especies']].dropna()
    fig_scatter = go.Figure()
    if not df_scatter.empty:
        fig_scatter.add_trace(go.Scatter(
            x=df_scatter['wave_height_max'],
            y=df_scatter['avg_microplasticos'],
            mode='markers',
            marker=dict(size=(df_scatter['num_observaciones'] / (df_scatter['num_observaciones'].max() or 1)) * 20 + 6,
                        color=df_scatter['num_especies'], colorscale='Viridis', showscale=True, colorbar=dict(title='Especies')),
            name='Microplásticos vs Oleaje'
        ))
    fig_scatter.update_layout(title='Microplásticos promedio vs Altura de las olas', xaxis_title='Altura Olas (m)', yaxis_title='Microplásticos (promedio)', height=500, template='plotly_white')

    # Heatmap de correlaciones (si hay suficientes datos)
    df_corr_clean = df_corr[['wave_height_max', 'wave_period_max', 'swell_wave_height_max',
                              'avg_microplasticos', 'num_especies']].dropna()

    if len(df_corr_clean) > 3:
        corr_matrix = df_corr_clean.corr()
        fig_heat = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=['Oleaje', 'Periodo', 'Swell', 'Microplásticos', 'Especies'],
            y=['Oleaje', 'Periodo', 'Swell', 'Microplásticos', 'Especies'],
            colorscale='RdBu',
            zmid=0,
            text=corr_matrix.values.round(2),
            texttemplate='%{text}',
            textfont={"size": 12},
            colorbar=dict(title="Correlación")
        ))
        fig_heat.update_layout(title='Matriz de Correlación: Variables Oceánicas y Ambientales', height=500)
    else:
        fig_heat = None

    # Distribución de oleaje vs contaminación (box + bar)
    # Evito crear categorías con valores nuevos; uso reindex con índice seguro
    df_corr = df_corr.copy()
    df_corr['oleaje_bin'] = pd.cut(df_corr['wave_height_max'], bins=5)
    df_grouped = df_corr.groupby('oleaje_bin', observed=True).agg({
        'avg_microplasticos': 'mean',
        'num_especies': 'sum',
        'num_observaciones': 'sum'
    }).reset_index()

    fig_box_bar = make_subplots(rows=1, cols=2, subplot_titles=('Microplásticos por Rango de Oleaje', 'Biodiversidad por Oleaje'))
    fig_box_bar.add_trace(
        go.Box(y=df_corr['avg_microplasticos'], x=df_corr['oleaje_bin'].astype(str),
               name='Microplásticos', marker_color=COLORS['danger']),
        row=1, col=1
    )
    fig_box_bar.add_trace(
        go.Bar(x=df_grouped['oleaje_bin'].astype(str), y=df_grouped['num_especies'],
               name='Especies', marker_color=COLORS['secondary']),
        row=1, col=2
    )
    fig_box_bar.update_layout(height=420, showlegend=False, template='plotly_white')

    # Dirección del oleaje -> simplificado a cardinales (sin crear categorías no existentes)
    df_dir = df_corr[df_corr['wave_direction_dominant'].notna()].copy()
    cardinal_order = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

    def angle_to_cardinal(val):
        # si es numérico (grados) convierto a cardinal; si no, retorno tal cual
        try:
            ang = float(val)
            ang = ang % 360
            dirs = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
            idx = int((ang + 22.5) // 45) % 8
            return dirs[idx]
        except Exception:
            v = str(val).strip().upper()
            if v in ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']:
                return v
            return v[:2]

    fig_polar = None
    if not df_dir.empty:
        df_dir['cardinal'] = df_dir['wave_direction_dominant'].apply(angle_to_cardinal)
        df_polar = df_dir.groupby('cardinal', as_index=False).agg({'avg_microplasticos': 'mean'})
        # Reindex according to cardinal_order to ensure consistent theta order and fill missing with 0
        df_polar = df_polar.set_index('cardinal').reindex(cardinal_order).fillna(0).reset_index()
        # If after this df_polar has no rows (unlikely), skip
        if not df_polar.empty:
            fig_polar = go.Figure(go.Scatterpolar(
                r=df_polar['avg_microplasticos'].values,
                theta=df_polar['cardinal'].astype(str).values,
                fill='toself',
                name='Microplásticos por Dirección',
            ))
            fig_polar.update_layout(polar=dict(radialaxis=dict(visible=True)), title='Concentración de Microplásticos por Dirección Cardinal', height=450, template='plotly_white')

    return html.Div([
        html.Div([
            html.H3("Análisis de Correlaciones Climáticas", style={'color': COLORS['info']}),
            html.P("Relación entre condiciones oceánicas, contaminación y biodiversidad"),
            dcc.Graph(figure=fig_scatter)
        ], style=STYLE_CARD),

        html.Div([dcc.Graph(figure=fig_heat)], style=STYLE_CARD) if fig_heat else html.Div(),
        html.Div([dcc.Graph(figure=fig_box_bar)], style=STYLE_CARD),
        html.Div([dcc.Graph(figure=fig_polar)], style=STYLE_CARD) if fig_polar else html.Div()
    ])

def render_sampling():
    df = execute_query(Q_SAMPLING_EFFECTIVENESS)

    if df.empty:
        return html.Div("Sin datos de muestreo", style=STYLE_CARD)

    # Efectividad por método: barras número observaciones
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=df['sampling_method'].head(15),
        y=df['num_observaciones'].head(15),
        name='Observaciones',
        marker_color=COLORS['primary']
    ))
    fig1.update_layout(
        title='Métodos de Muestreo más Utilizados',
        xaxis_title='Método',
        yaxis_title='Número de Observaciones',
        height=400,
        template='plotly_white'
    )

    # Simplificar: método vs oleaje promedio (scatter simple)
    df_method_avg = df.groupby('sampling_method', as_index=False).agg({'avg_wave_height': 'mean', 'avg_microplasticos': 'mean', 'num_observaciones': 'sum'})
    df_method_avg = df_method_avg.sort_values('num_observaciones', ascending=False)
    fig_method_wave = px.scatter(df_method_avg.head(30), x='sampling_method', y='avg_wave_height',
                                 size=(df_method_avg['num_observaciones']/(df_method_avg['num_observaciones'].max() or 1))*30 + 6,
                                 hover_data={'avg_microplasticos': True, 'num_observaciones': True},
                                 labels={'avg_wave_height': 'Oleaje promedio (m)', 'sampling_method': 'Método'},
                                 title='Método vs Oleaje Promedio')
    fig_method_wave.update_layout(height=450, template='plotly_white', xaxis_tickangle=-45)

    # Nota: Tabla comparativa eliminada según solicitud.

    return html.Div([
        html.Div([
            html.H3("Análisis de Métodos de Muestreo", style={'color': COLORS['marine']}),
            html.P("Efectividad de métodos bajo diferentes condiciones oceánicas"),
            dcc.Graph(figure=fig1)
        ], style=STYLE_CARD),

        html.Div([dcc.Graph(figure=fig_method_wave)], style=STYLE_CARD)
    ])

# ================================
# Run
# ================================
if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)