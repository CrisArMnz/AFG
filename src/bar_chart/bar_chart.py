import pandas as pd
import plotly.express as px
import streamlit as st

def render_bar_chart(data_path):
    """
    Renderiza un gráfico de barras con los datos per cápita filtrados por las últimas N semanas.
    
    Args:
        data_path (str): Ruta al archivo de datos procesados en formato Parquet.
    """
    st.header("Gráfico de barras de atenciones per cápita (últimas N semanas)")

    # Cargar datos
    @st.cache
    def load_data():
        df = pd.read_parquet(data_path)
        df['Fecha_Semana'] = pd.to_datetime(df['Fecha_Semana'])
        return df

    df = load_data()

    # Control deslizante para seleccionar rango de semanas (máximo 4 semanas)
    semanas = st.slider("Número de semanas a visualizar:", min_value=1, max_value=4, value=1, step=1)

    # Filtrar datos para las últimas N semanas
    ultima_fecha = df['Fecha_Semana'].max()
    fecha_inicio = ultima_fecha - pd.Timedelta(weeks=semanas)
    df_filtrado = df[(df['Fecha_Semana'] > fecha_inicio) & (df['Fecha_Semana'] <= ultima_fecha)]

    if df_filtrado.empty:
        st.error("No hay suficientes datos para las últimas semanas seleccionadas.")
        return

    # Agrupar datos por región
    df_barras = df_filtrado.groupby("NombreRegion").agg({"Total_per_capita_2019": "sum"}).reset_index()

    # Crear gráfico de barras
    fig = px.bar(
        df_barras,
        x="NombreRegion",
        y="Total_per_capita_2019",
        title=f"Atenciones Per Cápita - Últimas {semanas} Semana(s)",
        labels={"Total_per_capita_2019": "Atenciones Per Cápita", "NombreRegion": "Región"},
        color="NombreRegion",
    )

    # Renderizar gráfico en Streamlit
    st.plotly_chart(fig)
