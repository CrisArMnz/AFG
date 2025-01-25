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

    df = pd.read_parquet("process_data/data_filtered.parquet")
    current_week =  52 # int(datetime.datetime.now().isocalendar().week)
    current_year =  2024 #int(datetime.datetime.now().isocalendar().year)
    df = df[df["Fecha_Semana"].dt.isocalendar().year == current_year]
    if not df.empty:
        last_week = df["Fecha_Semana"].dt.isocalendar().week.max()
        last_year = df["Fecha_Semana"].dt.isocalendar().year.max()
        if current_week > last_week or last_year < current_year:
            process.get_filter_data(force_process=False)
            data.update_current_year_data()
            df.to_parquet("process_data/data_filtered.parquet", engine='pyarrow', compression='snappy')
    else:
        process.get_filter_data(force_process=False)
        data.update_current_year_data()
        df.to_parquet("process_data/data_filtered.parquet", engine='pyarrow', compression='snappy')
