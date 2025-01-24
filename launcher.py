import os
import yaml
import datetime
import sys
import pandas as pd
from src.data_downloader.data_downloader import DataDawnLoader
from src.data_process.data_process import DataProcess

global CWD
CWD = os.path.abspath(os.path.dirname(__file__))
os.chdir(CWD)
sys.path.append(CWD)
if __name__ == '__main__':
    with open("./assets/app_settings.yaml", 'r', encoding='utf-8') as file:
        app_settings = yaml.safe_load(file)
    data = DataDawnLoader(app_settings["DATA"])
    process = DataProcess(app_settings)

    df = pd.read_excel("process_data\data_filtered.xlsx",index_col=0)
    current_week =  52 # int(datetime.datetime.now().isocalendar().week)
    current_year =  2024 #int(datetime.datetime.now().isocalendar().year)
    df = df[df["Fecha_Semana"].dt.isocalendar().year == current_year]
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