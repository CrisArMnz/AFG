import json
import pandas as pd
import plotly.express as px
import streamlit as st

@st.cache_data
def load_geojson(file_path):
    """Carga el archivo GeoJSON para el mapa coroplético."""
    with open(file_path, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    return geojson_data

@st.cache_data
def load_data(file_path):
    """Carga el archivo de datos procesados."""
    return pd.read_parquet(file_path)

def filter_data_by_weeks(data, num_weeks):
    """Filtra los datos por el número de semanas recientes."""
    max_date = data['Fecha_Semana'].max()
    min_date = max_date - pd.Timedelta(weeks=num_weeks)
    return data[(data['Fecha_Semana'] > min_date) & (data['Fecha_Semana'] <= max_date)]

def render_choropleth_map(data_file, geojson_file):
    """Genera y muestra un mapa coroplético basado en los datos proporcionados."""
    # Cargar datos y GeoJSON
    data = load_data(data_file)
    geojson_data = load_geojson(geojson_file)

    # Seleccionar semanas
    semanas = st.slider("Número de semanas a visualizar:", min_value=1, max_value=4, value=1)
    filtered_data = filter_data_by_weeks(data, semanas)

    # Agrupar datos por región
    region_data = filtered_data.groupby('NombreRegion', as_index=False).agg({
        'Total_per_capita_2019': 'mean'
    })

    # Crear el mapa coroplético
    st.write(f"### Atenciones Per Cápita - Últimas {semanas} Semana(s)")
    fig = px.choropleth(
        region_data,
        geojson=geojson_data,
        locations='NombreRegion',
        featureidkey='properties.Region',
        color='Total_per_capita_2019',
        color_continuous_scale='Reds',
        title=f"Mapa Coroplético de Atenciones Per Cápita - Últimas {semanas} Semana(s)",
    )
    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig, use_container_width=True)

