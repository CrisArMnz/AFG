import os
import datetime
import pandas as pd
import threading
from tqdm import tqdm
import time
from multiprocessing import Pool, freeze_support, RLock
from copy import copy

def filter_data(file_path, ids):
    df = pd.read_parquet(file_path)  # Leer directamente desde Parquet
    df_filtered = df[df['IdCausa'].isin(ids)]
    return df_filtered

def drop_cols(df,columns):
    for c in columns:
        if c in list(df.columns):
            df = df.drop(columns=c)
    return df

def process_files(file_path, ids, local_info_path, region_population_path, region_maps):
    df = filter_data(file_path, ids)
    df = drop_cols(df,['CodigoRegion', 'NombreRegion','CodigoDependencia', 'NombreDependencia', 'CodigoComuna','NombreComuna'])
    local_info = pd.read_csv(local_info_path, sep=",")
    df["IdEstablecimiento"] = df["IdEstablecimiento"].astype(str)
    df['IdEstablecimiento'] = df['IdEstablecimiento'].str.strip().str.replace(" ","")
    df = df.merge(local_info,left_on='IdEstablecimiento',right_on="Code",how="left")
    df["fecha"] = pd.to_datetime(df["fecha"],dayfirst=True)
    df["Año"] = df["fecha"].dt.year
    # Crear una nueva columna 'Año-Semana' como una combinación del año y la semana
    df['Año_Semana'] = df['Año'].astype(str) + '-W' + df['semana'].astype(str)
    # Convertir 'Año-Semana' en una fecha (primer día de la semana)
    df['Fecha_Semana'] = pd.to_datetime(df['Año_Semana'] + '-1', format='%Y-W%W-%w')
    df = df.dropna(axis=0)
    df['NombreRegion'] = df['CodigoRegion'].apply(lambda x: region_maps[x])
    df = df[['Fecha_Semana', 'NombreRegion',"CodigoRegion","Total"]].groupby(['Fecha_Semana', 'NombreRegion',"CodigoRegion"]).sum()
    df[['Fecha_Semana', 'NombreRegion',"CodigoRegion"]] = [[i[0],i[1],i[2]] for i in df.index]
    df = df.reset_index(drop=True)
    df_region_population = pd.read_csv(region_population_path, sep=",", encoding="latin-1")
    df = df.merge(df_region_population[['CodigoRegion', 'Poblacion_2019', 'Poblacion_2035']], on='CodigoRegion', how='left')
    df['Total_per_capita_2019'] = df['Total'] / df['Poblacion_2019']

    return df

class DataProcess():
    def __init__(self, config):
        self.data_path = config["DATA"]["DATA_DIR"]
        self.out_dir = config["PROCESS"]["OUT_DIR"]
        os.makedirs(self.out_dir, exist_ok=True)
        self.out_file_name = config["PROCESS"]["OUT_FILE_NAME"]
        self.ids = config["PROCESS"]["ID_CAUSAS"]
        self.filtered_data = pd.DataFrame()
        self.max_processes = 5
        self.local_info_path = config["PROCESS"]["LOCAL_INFO"]
        self.region_population_path = config["PROCESS"]["REGION_POPULATION_PATH"]
        self.region_maps = config["PROCESS"]["REGION_MAPS"]

    def get_filter_data(self, force_process = False):
        if force_process:
            process = True
        elif self.out_file_name in os.listdir(self.out_dir):
            process = False
        else:
            process = True

        if process:
            print("INICIANDO PROCESAMIENTO")
            inputs = []
            files = os.listdir(self.data_path)
            files = list(filter(lambda x: "AtencionesUrgencia" in x,files))
            for file in files:
                inputs.append((self.data_path + file,copy(self.ids),self.local_info_path, self.region_population_path,self.region_maps))

            def update_progress(value):
                progress_bar.update(1)

            progress_bar = tqdm(total=len(inputs))
            freeze_support()
            if len(inputs) > self.max_processes:
                pool = Pool(processes=self.max_processes, initargs=(RLock(),), initializer=tqdm.set_lock)
            else:
                pool = Pool(processes=len(inputs), initargs=(RLock(),), initializer=tqdm.set_lock)
            jobs = [pool.apply_async(process_files, args=args_input, callback=update_progress) for args_input in inputs]
            pool.close()
            pool.join()

            df = pd.concat([job.get() for job in jobs])
            df = df.reset_index(drop=True)
            print(f"GUARDANDO EN {self.out_dir + self.out_file_name}")
            df.to_excel(self.out_dir + self.out_file_name)
        else:
            print("ARCHIVOS YA PROCESADOS")

    def update_data(self):
        if self.out_file_name in os.listdir(self.out_dir):
            current_year =  int(datetime.datetime.now().year)
            data = pd.read_excel(self.out_dir + self.out_file_name, index_col=0)
            data["year"] = pd.to_datetime(data["Fecha_Semana"]).dt.year
            data = data[data["year"] < current_year].drop(columns="year")
            current_year_file = f"AtencionesUrgencia{current_year}.csv"
            current_year_data = process_files(self.data_path + current_year_file,copy(self.ids),self.local_info_path, self.region_population_path, self.region_maps)
            df = pd.concat([data,current_year_data])
            df = df.reset_index(drop=True)
            print(f"ARCHIVO ACTUALIZADO EN {self.out_dir + self.out_file_name}")
            df.to_excel(self.out_dir + self.out_file_name)
        else:
            print("ARCHIVO NO ENCONTRADO")

    


    


    
