import os
import yaml
import datetime
import requests
import zipfile
import threading

class DataDawnLoader():
    
    def __init__(self, config: yaml):
        #Config
        self.config = config
        self.min_year = 2008
        self.current_year = 2024 #int(datetime.datetime.now().year)
        self.years = range(self.min_year,self.current_year - 1,1)
        self.download_out_dir = config["OUT_DIR"]
        self.data_out_dir = config["DATA_DIR"]
        #Check initial condictions
        self.check_dir()
        self.get_urls_list()
        self.download_zip(self.urls)
        print("Extract")
        self.extract_zip(self.urls)
    
    def check_dir(self):
        """
        Verifica si un directorio existe, y si no, lo crea.
        
        Args:
            directorio (str): Ruta del directorio a verificar o crear.
        """
        os.makedirs(self.data_out_dir, exist_ok=True)
        os.makedirs(self.download_out_dir, exist_ok=True)

    def get_urls_list(self):
        # URL del archivo ZIP
        urls = [(f"https://repositoriodeis.minsal.cl/DatosAbiertos/AtencionesDeUrgencia/AtencionesUrgencia{y}.zip",f"AtencionesUrgencia{y}.zip") for y in self.years]
        other_urls = [(f"https://repositoriodeis.minsal.cl/SistemaAtencionesUrgencia/AtencionesUrgencia{y}.zip",f"AtencionesUrgencia{y}.zip") for y in range(self.current_year-1,self.current_year+1,1)]
        urls += other_urls
        # for url,zip_name in urls:
        #     print(url,zip_name)
        self.urls = urls
    
    def download_zip(self, urls,force_download = False):
        download = False
        processed_files = os.listdir(self.download_out_dir)
        # Crear y lanzar threads para cada descarga
        threads = []
        for url, zip_filename in urls:
            if force_download:
                print(f"FORZANDO DESCARGA DE {zip_filename}")
                download = True
            elif not zip_filename in processed_files:
                download = True    
            else:
                download = False  
                print("archivo ZIP ya existente")
            if download:
                print(f"AGREGANDO {zip_filename} A LA LISTA")
                thread = threading.Thread(target=self.get_file, args=(url, zip_filename))
                threads.append(thread)
                thread.start()

        # Esperar a que todos los threads terminen
        for thread in threads:
            thread.join()
    
    def get_file(self, url, zip_filename):
        # Descargar el archivo ZIP y guardarlo localmente
        response = requests.get(url)

        # Verificar si la descarga fue exitosa
        if response.status_code == 200:
            # Guardar el archivo ZIP localmente
            with open(self.download_out_dir + zip_filename, 'wb') as f:
                f.write(response.content)
            print(f"Archivo ZIP descargado y guardado como {zip_filename}")
        else:
            print(f"Error al descargar el archivo ZIP {zip_filename}")
    
    # def extract_zip(self, urls, force_extract = False):
    #     processed_files = os.listdir(self.data_out_dir)
    #     for url, zip_filename in urls:
    #         file_path = self.download_out_dir + zip_filename
    #         if force_extract:
    #             extract = True
    #         elif not zip_filename.replace("zip","csv") in processed_files:
    #             extract = True    
    #         else:
    #             extract = False  
    #             print("archivo CSV ya existente")
    #         if extract:
    #             # Extraer el contenido del archivo ZIP
    #             with zipfile.ZipFile(file_path, 'r') as z:
    #                 # Extraer todos los archivos
    #                 z.extractall(self.data_out_dir)
    #                 print(f"Archivos extraídos en {self.data_out_dir}")

    def extract_zip(self, urls, force_extract=False):
        processed_files = os.listdir(self.data_out_dir)
        for url, zip_filename in urls:
            file_path = self.download_out_dir + zip_filename
            if force_extract:
                extract = True
            elif not zip_filename.replace(".zip", ".parquet") in processed_files:
                extract = True
            else:
                extract = False
                print("archivo Parquet ya existente")
            if extract:
                # Extraer el contenido del archivo ZIP
                with zipfile.ZipFile(file_path, 'r') as z:
                    for file in z.namelist():
                        if file.endswith('.csv'):
                            csv_path = z.extract(file, self.data_out_dir)
                            print(f"Convertiendo {csv_path} a Parquet...")
                            df = pd.read_csv(csv_path, sep=';', encoding='latin-1')
                            parquet_path = csv_path.replace('.csv', '.parquet')
                            df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
                            os.remove(csv_path)  # Elimina el CSV original

    def update_current_year_data(self):
        self.download_zip([self.urls[-1]],force_download=True)
        self.extract_zip([self.urls[-1]],force_extract=True)


