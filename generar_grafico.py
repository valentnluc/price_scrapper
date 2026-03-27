import pandas as pd
import numpy as np
import altair as alt
from utils import clean_price

# --- CONFIGURACIÓN DE TEMA (DARK MODE RESTORED) ---
THEME = {
    'bg_page':    '#000000', 
    'bg_surface': '#000000', 
    'text':       '#F9FAFB', 
    'text_dim':   '#9CA3AF', 
    'blue':       '#2563EB', 
    'red':        '#DC2626', 
    'pink':       '#F63366', 
    'grey':       '#9CA3AF', 
    'border':     '#333333'
}

MONOFONT = "IBM Plex Mono, Consolas, monospace"
MAINFONT = "Inter, sans-serif"

# 1. CARGA Y LIMPIEZA DE DATOS
print("1. Cargando y limpiando datos...")
df_hist = pd.read_csv('precios_historicos.csv')
df_maestros = pd.read_csv('Productos_Maestros.csv')

# clean_price importada desde utils.py

df_hist['precio_num'] = df_hist['precio_detectado'].apply(clean_price)
df_hist = df_hist.dropna(subset=['precio_num'])
df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'])
last_update = df_hist['timestamp'].max().strftime('%d/%m/%Y %H:%M')


df_full = df_hist.merge(df_maestros, on='codigo_interno', how='left')
df_full['Category'] = df_full['nombre_generico'].fillna(df_full['nombre_detectado'])

# --- CÁLCULO DE VARIACIÓN 30 DÍAS ---
def get_variation(group):
    """Calcula la variación porcentual de precio en los últimos 30 días."""
    if len(group) < 2:
        return 0.0
    group = group.sort_values('timestamp')
    latest = group.iloc[-1]
    now = latest['timestamp']
    price_now = latest['precio_num']
    target_date = now - pd.Timedelta(days=30)
    group = group.copy()
    group['diff_days'] = (group['timestamp'] - target_date).abs()
    closest = group.loc[group['diff_days'].idxmin()]
    price_old = closest['precio_num']
    if abs(closest['timestamp'] - now) < pd.Timedelta(hours=24):
        return 0.0
    if price_old == 0:
        return 0.0
    return (price_now - price_old) / price_old

variations = (
    df_full.groupby('Category')[['precio_num', 'timestamp']]
    .apply(get_variation, include_groups=False)
    .reset_index(name='pct_change')
)
stats = df_full.groupby('Category')['precio_num'].agg(['min', 'max', 'mean']).reset_index().round(0)
stats = stats.merge(variations, on='Category')
stats['zero'] = 0.0 # CRITICAL FIX: Base para las flechas

print(f"   -> Datos listos: {len(stats)} productos.")

# 2. DEFINICIÓN DEL GRÁFICO (ALTAIR)
print("2. Construyendo gráfico...")

common_sort = alt.EncodingSortField(field='mean', order='ascending')

# --- CHART PRECIOS (IZQUIERDA) ---
base_prices = alt.Chart(stats).encode(
    y=alt.Y('Category', sort=common_sort, title=None, axis=alt.Axis(
        labelColor=THEME['text'], 
        labelFont=MAINFONT,
        labelFontSize=13,
        labelAlign='left', 
        labelLimit=400,
        labelPadding=300, 
        tickSize=0, domain=False, offset=0
    ))
)

log_scale = alt.Scale(type='log', zero=False)

rail = base_prices.mark_rule(color=THEME['border'], size=3).encode(
    x=alt.X('min', axis=None, scale=log_scale), 
    x2='max'
)

point_size = 150 # Reduced from 350 as requested
p_min = base_prices.mark_circle(color=THEME['blue'], size=point_size, opacity=1).encode(
    x=alt.X('min', scale=log_scale), tooltip=['Category', 'min']
).transform_filter(alt.datum.min != alt.datum.max)

p_max = base_prices.mark_circle(color=THEME['red'], size=point_size, opacity=1).encode(
    x=alt.X('max', scale=log_scale), tooltip=['Category', 'max']
).transform_filter(alt.datum.min != alt.datum.max)

p_mean = base_prices.mark_circle(color=THEME['grey'], size=point_size, opacity=0.9).encode(
    x=alt.X('mean', scale=log_scale), tooltip=['Category', 'mean']
)

def value_text_p(field, color, align='center', dx=0, dy=0):
    return base_prices.mark_text(
        color=color, font=MAINFONT, fontSize=11, align=align, dx=dx, dy=dy
    ).encode(
        x=alt.X(field, scale=log_scale), text=alt.Text(field, format='$,.0f')
    )

t_min = value_text_p('min', THEME['blue'], align='right', dx=-15).transform_filter(alt.datum.min != alt.datum.max)
t_max = value_text_p('max', THEME['red'], align='left', dx=15).transform_filter(alt.datum.min != alt.datum.max)
t_mean = value_text_p('mean', THEME['grey'], dy=-18)

chart_prices = alt.layer(rail, p_min, p_max, p_mean, t_min, t_max, t_mean).properties(
    width=600, 
    height=len(stats)*50,
    title=alt.TitleParams(
        text="Rango de Precios (Min/Max/Promedio)",
        color=THEME['text'],
        font=MAINFONT,
        fontSize=16,
        anchor='middle',
        frame='group',
        offset=20
    )
)


# --- CHART VARIACIÓN (DERECHA - CON FLECHAS) ---
# Escala dinámica para maximizar visibilidad de flechas pequeñas
scale_trend = alt.Scale(nice=True, padding=0.2, zero=True)

trend_base = alt.Chart(stats).encode(
    y=alt.Y('Category', sort=common_sort, axis=None) 
)

print("   -> Ejemplo Variaciones:")
print(stats[['Category', 'pct_change', 'zero']].head())

# 1. Eje Central (0%) - Línea punteada
trend_zero = trend_base.mark_rule(color=THEME['border'], strokeDash=[2,2]).encode(
    x=alt.X('zero:Q', scale=scale_trend, axis=None)
)

# 2. Flecha (Shaft) - Usamos BARRA para mayor visibilidad (Grosor controlado)
trend_rule = trend_base.mark_bar(size=4).encode(
    x=alt.X('pct_change:Q', scale=scale_trend), 
    x2=alt.X2('zero:Q'),                                
    color=alt.condition(alt.datum.pct_change >= 0, alt.value(THEME['blue']), alt.value(THEME['red'])),
    opacity=alt.condition(alt.datum.pct_change == 0, alt.value(0), alt.value(1))
)

# 3. Triángulo (Cabeza) - SEPARADO para ajustar offset y evitar tapar la línea
arrow_size = 60
trend_point_pos = trend_base.mark_point(filled=True, size=arrow_size, opacity=1, shape='triangle-right', dx=4).encode(
    x=alt.X('pct_change:Q', scale=scale_trend),
    color=alt.value(THEME['blue'])
).transform_filter(alt.datum.pct_change > 0)

trend_point_neg = trend_base.mark_point(filled=True, size=arrow_size, opacity=1, shape='triangle-left', dx=-4).encode(
    x=alt.X('pct_change:Q', scale=scale_trend),
    color=alt.value(THEME['red'])
).transform_filter(alt.datum.pct_change < 0)

# 4. Texto (+12%)
trend_text_pos = trend_base.mark_text(align='left', dx=12, font=MAINFONT, fontSize=12, fontWeight=600, color=THEME['blue']).encode(
    x=alt.X('pct_change:Q', scale=scale_trend),
    text=alt.Text('pct_change:Q', format='+.1%')
).transform_filter(alt.datum.pct_change > 0)

trend_text_neg = trend_base.mark_text(align='right', dx=-12, font=MAINFONT, fontSize=12, fontWeight=600, color=THEME['red']).encode(
    x=alt.X('pct_change:Q', scale=scale_trend),
    text=alt.Text('pct_change:Q', format='+.1%')
).transform_filter(alt.datum.pct_change < 0)

# Combinar capas de variación
chart_trend = alt.layer(
    trend_zero, 
    trend_rule, 
    trend_point_pos, 
    trend_point_neg,
    trend_text_pos, 
    trend_text_neg
).properties(
    width=150, 
    height=len(stats)*50,
    title=alt.TitleParams(
        text="Variación 30d (%)",
        color=THEME['text'],
        font=MAINFONT,
        fontSize=16,
        anchor='middle',
        frame='group',
        offset=20
    )
)

# CONCATENAR (Precios | Variación)
final_chart = alt.hconcat(chart_prices, chart_trend).configure(
    background='transparent',
    view={'stroke': 'transparent'}
)

# 3. GENERACIÓN DEL HTML
print("3. Generando HTML...")
chart_json = final_chart.to_json()

html_content = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Monitor de Precios (Restored)</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
        :root {{
            --bg-page: {THEME['bg_page']};
            --bg-surface: {THEME['bg_surface']};
            --text-main: {THEME['text']};
            --border: {THEME['border']};
            --accent: {THEME['pink']};
        }}
        body {{
            background-color: var(--bg-page);
            color: var(--text-main);
            font-family: 'Inter', sans-serif;
            margin: 0;
            padding: 40px;
            display: flex;
            justify-content: center;
        }}
        .dashboard-container {{ width: 100%; max-width: 1200px; }}
        .card {{
            background-color: var(--bg-surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 32px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.5);
            overflow-x: auto; 
        }}
        header {{
            margin-bottom: 32px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 16px;
        }}
        h1 {{ margin: 0; font-size: 24px; font-weight: 600; letter-spacing: -0.5px; }}
        .subtitle {{ color: #9CA3AF; font-size: 14px; margin-top: 4px; }}
        .legend {{ display: flex; gap: 16px; margin-top: 8px; font-size: 12px; font-family: 'Inter', monospace; }}
        .dot {{ width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 4px; }}
        #vis {{ width: 100%; }}
    </style>
</head>
<body>
<div class="dashboard-container">
    <div class="card">
        <header>
            <h1>Monitor de Precios</h1>
            <div class="subtitle">Comparativa de productos en tiempo real</div>
            <div class="legend">
                <span><span class="dot" style="background: {THEME['blue']}"></span>Mínimo</span>
                <span><span class="dot" style="background: {THEME['red']}"></span>Máximo</span>
                <span><span class="dot" style="background: {THEME['grey']}"></span>Promedio (Gris)</span>
            </div>
        </header>
        <div id="vis"></div>
    </div>
    <div style="text-align: right; margin-top: 10px; font-style: italic; color: #9CA3AF; font-size: 12px;">
        Última actualización: {last_update}
    </div>
</div>
<script>
    const spec = {chart_json};
    const opts = {{ renderer: 'svg', actions: false, theme: null }};
    vegaEmbed('#vis', spec, opts);
</script>
</body>
</html>
"""

with open('grafico_precios.html', 'w', encoding='utf-8') as f:
    f.write(html_content)
    
print("¡Hecho!")
