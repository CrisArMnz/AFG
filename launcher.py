import os
import yaml
import datetime
import sys
import pandas as pd
from src.data_downloader.data_downloader import DataDawnLoader
from src.data_process.data_process import DataProcess
import plotly.express as px
import geopandas as gpd

global CWD
CWD = os.path.abspath(os.path.dirname(__file__))
os.chdir(CWD)
sys.path.append(CWD)

def actualizar_base_datos():
    try:
        with open("./assets/app_settings.yaml", 'r', encoding='utf-8') as file:
            app_settings = yaml.safe_load(file)

        data = DataDawnLoader(app_settings["DATA"])
        process = DataProcess(app_settings)

        df = pd.read_parquet(app_settings["PROCESS"]["OUT_DIR"] + app_settings["PROCESS"]["OUT_FILE_NAME"])
        current_week = int(datetime.datetime.now().isocalendar().week)
        current_year = int(datetime.datetime.now().isocalendar().year)
        df = df[df["Fecha_Semana"].dt.isocalendar().year == current_year]

        if not df.empty:
            last_week = pd.to_datetime(df["Fecha_Semana"]).dt.isocalendar().week.max()
            last_year = pd.to_datetime(df["Fecha_Semana"]).dt.isocalendar().year.max()
            print(f"Ultima semana del año descargada {last_week} - semana actual {current_week}")
            if (current_week > last_week) or (last_year < current_year):
                process.get_filter_data(force_process=False)
                data.update_current_year_data()
                process.update_data()
            else:
                print("DATOS ESTAN ACTUALIZADOS")
        else:
            process.get_filter_data(force_process=False)
            data.update_current_year_data()
            process.update_data()
    except Exception as e:
        print(f"Error al actualizar la base de datos: {e}")

if __name__ == '__main__':
    with open("./assets/app_settings.yaml", 'r', encoding='utf-8') as file:
        app_settings = yaml.safe_load(file)
    data = DataDawnLoader(app_settings["DATA"])
    process = DataProcess(app_settings)

    df = pd.read_parquet("process_data/data_filtered.parquet")
    current_week = 52  # int(datetime.datetime.now().isocalendar().week)
    current_year = 2024  # int(datetime.datetime.now().isocalendar().year)
    df = df[df["Fecha_Semana"].dt.isocalendar().year == current_year]

    # Generar mapa coroplético
    geojson_file = "./assets/chile_regions.geojson"  # Archivo GeoJSON con las regiones de Chile
    gdf = gpd.read_file(geojson_file)

    if not df.empty:
        last_week = pd.to_datetime(df["Fecha_Semana"]).dt.isocalendar().week.max()
        last_year = pd.to_datetime(df["Fecha_Semana"]).dt.isocalendar().year.max()
        print(f"Ultima semana del año descargada {last_week} - semana actual {current_week}")
        if (current_week > last_week) | (last_year < current_year):
            process.get_filter_data(force_process=False)
            data.update_current_year_data()
            process.update_data()
        else:
            print("DATOS ESTAN ACTUALIZADOS")
    else:
        process.get_filter_data(force_process=False)
        data.update_current_year_data()
        process.update_data()

    # Filtrar datos para una semana específica
    selected_week = 1  # Cambiar según la semana seleccionada
    filtered_df = df[df['Fecha_Semana'].dt.isocalendar().week == selected_week]

    # Generar el mapa
    merged = gdf.merge(filtered_df, left_on="CodigoRegion", right_on="CodigoRegion", how="left")
    fig = px.choropleth_mapbox(
        merged,
        geojson=merged.geometry,
        locations=merged.index,
        color="Total_per_capita_2019",
        mapbox_style="carto-positron",
        center={"lat": -35.6751, "lon": -71.543},  # Centro aproximado de Chile
        zoom=4,
        color_continuous_scale="Viridis",
        title="Proyección de Urgencias por Región"
    )
    fig.show()
