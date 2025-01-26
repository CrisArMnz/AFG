import pandas as pd
import plotly.express as px
import streamlit as st

def render_bar_chart(data_path):
    """
    Renderiza un gráfico de barras con las atenciones totales por región utilizando Parquet.
    
    Args:
        data_path (str): Ruta al archivo de datos procesados en formato Parquet.
    """
    st.header("Gráfico de barras de atenciones totales por región")

    # Cargar datos
    @st.cache
    def load_data():
        df = pd.read_parquet(data_path)
        return df

    df = load_data()

    # Agrupar datos por región
    df_barras = df.groupby("NombreRegion").agg({"Total": "sum"}).reset_index()

    # Crear gráfico de barras
    fig = px.bar(
        df_barras,
        x="NombreRegion",
        y="Total",
        title="Atenciones Totales por Región",
        labels={"Total": "Atenciones Totales", "NombreRegion": "Región"},
        color="NombreRegion",
    )

    # Renderizar gráfico en Streamlit
    st.plotly_chart(fig)

# Si deseas probar este módulo de forma independiente
if __name__ == "__main__":
    st.title("Visualización de Atenciones por Región")
    render_bar_chart("../process_data/data_filtered.parquet")
