import pandas as pd
import plotly.express as px
import streamlit as st
import json

def render_choropleth_map(data_path, geojson_path):
    """
    Renderiza un mapa coroplético con las atenciones per cápita por región.
    
    Args:
        data_path (str): Ruta al archivo de datos procesados en formato Parquet.
        geojson_path (str): Ruta al archivo GeoJSON con las geometrías de las regiones.
    """
    st.header("Mapa Coroplético de Atenciones Per Cápita")

    # Cargar datos
    @st.cache
    def load_data():
        df = pd.read_parquet(data_path)
        df['Fecha_Semana'] = pd.to_datetime(df['Fecha_Semana'])
        return df

    @st.cache
    def load_geojson():
        with open(geojson_path, "r", encoding="utf-8") as file:
            geojson = json.load(file)
        return geojson

    df = load_data()
    geojson = load_geojson()

    # Filtrar las últimas N semanas
    semanas = st.slider("Semanas (Gráfico de Barras):", min_value=1, max_value=4, value=1, step=1)
    ultima_fecha = df['Fecha_Semana'].max()
    fecha_inicio = ultima_fecha - pd.Timedelta(weeks=semanas)
    df_filtrado = df[(df['Fecha_Semana'] > fecha_inicio) & (df['Fecha_Semana'] <= ultima_fecha)]

    # Agregar los datos por región
    df_agrupado = df_filtrado.groupby("CodigoRegion").agg({"Total_per_capita_2019": "sum"}).reset_index()

    # Crear el mapa coroplético
    fig = px.choropleth(
        df_agrupado,
        geojson=geojson,
        locations="CodigoRegion",
        featureidkey="properties.codigo_region",  # Ajusta esto según el archivo GeoJSON
        color="Total_per_capita_2019",
        color_continuous_scale="YlOrRd",
        title=f"Atenciones Per Cápita - Últimas {semanas} Semana(s)",
        labels={"Total_per_capita_2019": "Atenciones Per Cápita", "CodigoRegion": "Región"}
    )

    # Ajustar visualización del mapa
    fig.update_geos(fitbounds="locations", visible=False)

    # Renderizar en Streamlit
    st.plotly_chart(fig, use_container_width=True)
