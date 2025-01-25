# -*- coding: utf-8 -*-
"""streamlit.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1pgKAUoma9-cSH3hkkNNGnxzNgEgSAZxb
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import os
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM, Dropout

import yaml
import datetime
import sys
from src.data_downloader.data_downloader import DataDawnLoader
from src.data_process.data_process import DataProcess

import sys
sys.path.append(r"./src")  # Ajusta esta ruta según la estructura de tu proyecto
from data_downloader.data_downloader import DataDawnLoader


# Función para cargar los datos
@st.cache_data
def load_data(file_path):
    if not os.path.exists(file_path):
        st.error(f"El archivo no se encuentra en la ruta: {file_path}")
        st.stop()
    df = pd.read_parquet(file_path)
    df['Fecha_Semana'] = pd.to_datetime(df['Fecha_Semana'])
    return df

# Preprocesar los datos
def preprocess_data(df, selected_regions, start_week, end_week):
    df_filtered = df[(df['NombreRegion'].isin(selected_regions)) &
                     (df['Fecha_Semana'] >= start_week) &
                     (df['Fecha_Semana'] <= end_week)]
    df_filtered = df_filtered[['Fecha_Semana', 'Total_per_capita_2019']].sort_values('Fecha_Semana')
    df_filtered.set_index('Fecha_Semana', inplace=True)
    scaler = MinMaxScaler(feature_range=(0, 1))
    data_scaled = scaler.fit_transform(df_filtered)
    return df_filtered, data_scaled, scaler

# Crear secuencias para el modelo LSTM
def create_sequences(data, seq_length):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:i + seq_length])
        y.append(data[i + seq_length])
    return np.array(X), np.array(y)

# Obtener estadísticas adicionales
def get_statistics(df_filtered, future_predictions_rescaled, future_dates):
    current_week = df_filtered.index[-1]
    last_year_week = current_week - pd.Timedelta(weeks=52)

    current_month = current_week.month
    last_year_month = current_month

    # Valores específicos
    current_week_value = df_filtered.loc[current_week, 'Total_per_capita_2019']
    last_year_week_value = df_filtered.loc[last_year_week, 'Total_per_capita_2019'] if last_year_week in df_filtered.index else None

    # Promedios mensuales
    current_month_avg = df_filtered[df_filtered.index.month == current_month]['Total_per_capita_2019'].mean()
    last_year_month_avg = df_filtered[(df_filtered.index.month == last_year_month) &
                                       (df_filtered.index.year == current_week.year - 1)]['Total_per_capita_2019'].mean()

    # Predicciones futuras
    future_values = dict(zip(future_dates.strftime("%Y-%m-%d"), future_predictions_rescaled.flatten()))

    return current_week_value, last_year_week_value, current_month_avg, last_year_month_avg, future_values

# Mostrar estadísticas en una tabla
def display_statistics(current_week_value, last_year_week_value, current_month_avg, last_year_month_avg, future_values):
    stats_data = {
        "Descripción": [
            "Valor real de la semana en curso",
            "Valor real de la misma semana el año anterior",
            "Promedio del mes en curso",
            "Promedio del mismo mes el año anterior"
        ],
        "Valor": [
            f"{current_week_value:.6f}",
            f"{last_year_week_value:.6f}" if last_year_week_value else "No disponible",
            f"{current_month_avg:.6f}",
            f"{last_year_month_avg:.6f}"
        ]
    }

    future_data = {
        "Semana": list(future_values.keys()),
        "Predicción": [f"{value:.6f}" for value in future_values.values()]
    }

    st.write("### Resumen de Estadísticas")
    st.table(pd.DataFrame(stats_data))  # Estadísticas generales
    st.write("### Predicciones para las próximas 4 semanas")
    st.table(pd.DataFrame(future_data))  # Predicciones futuras

# Define la función que actualiza la base de datos
def actualizar_base_datos():
    # Aquí incluyes el código para actualizar la base de datos
    # Por ejemplo, puedes cargar, modificar y guardar un archivo Excel
    #archivo = "base_datos.xlsx"  # Ruta al archivo Excel
    try:
        with open("./assets/app_settings.yaml", 'r', encoding='utf-8') as file:
            app_settings = yaml.safe_load(file)
        data = DataDawnLoader(app_settings["DATA"])
        process = DataProcess(app_settings)

        df = pd.read_parquet(app_settings["PROCESS"]["OUT_DIR"] + app_settings["PROCESS"]["OUT_FILE_NAME"])
        current_week =  int(datetime.datetime.now().isocalendar().week)
        current_year =  int(datetime.datetime.now().isocalendar().year)
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
    except Exception as e:
        st.error(f"Error al actualizar la base de datos: {e}")


# Función principal
def main():
    with open("./assets/app_settings.yaml", 'r', encoding='utf-8') as file:
        app_settings = yaml.safe_load(file)

    st.title("Dashboard: Predicción de Urgencias Respiratorias per Cápita")
    st.subheader("Serie de tiempo filtrada por región y rango de semanas")

    # Agregar el botón
    if st.sidebar.button("Actualizar Base de Datos"):
        actualizar_base_datos()
    # Ruta al archivo
    #file_path = "/Users/erickgarciaviveros/Library/CloudStorage/OneDrive-Personal/a_PUC_/MCD_b10_Actividad de Graduación III/Streamlit/UR/df_semanal_regional.xlsx"
    file_path = app_settings["PROCESS"]["OUT_DIR"] + app_settings["PROCESS"]["OUT_FILE_NAME"]

    df = load_data(file_path)

    # Filtros interactivos
    st.sidebar.header("Filtros")
    regiones = df['NombreRegion'].unique()
    #selected_regions = st.sidebar.multiselect("Selecciona las regiones:", regiones, default=regiones)
    selected_regions = st.sidebar.multiselect("Selecciona las regiones:", regiones, default=["Metropolitana de Santiago"])

    st.sidebar.header("Rango de Semanas")
    min_date = df['Fecha_Semana'].min()
    max_date = df['Fecha_Semana'].max()
    weeks = pd.date_range(start=min_date, end=max_date, freq='W')
    start_week, end_week = st.sidebar.select_slider(
        "Selecciona el rango de semanas:",
        options=weeks,
        value=(weeks[0], weeks[-1])
    )
    seq_length = st.sidebar.slider("Longitud de la secuencia de entrada:", 2, 10, 5)

    # Preprocesar los datos
    df_filtered, data_scaled, scaler = preprocess_data(df, selected_regions, start_week, end_week)

    if len(df_filtered) < seq_length + 1:
        st.error("No hay suficientes datos para entrenar el modelo. Ajusta los filtros o la longitud de la secuencia.")
        return

    # Crear secuencias
    X, y = create_sequences(data_scaled, seq_length)
    X = X.reshape((X.shape[0], X.shape[1], 1))

    # Dividir en entrenamiento y prueba
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    # Modelo LSTM
    model = Sequential([
        LSTM(50, activation='relu', input_shape=(seq_length, 1), return_sequences=True),
        Dropout(0.2),
        LSTM(50, activation='relu'),
        Dense(1)
    ])
    model.compile(optimizer='adam', loss='mse')

    # Entrenar el modelo
    st.write("### Entrenando el modelo...")
    with st.spinner("Por favor espera..."):
        model.fit(X_train, y_train, epochs=20, batch_size=16, validation_data=(X_test, y_test), verbose=0)
    st.success("Modelo entrenado con éxito!")

    # Predicción y proyecciones
    y_pred = model.predict(X_test)
    y_pred_rescaled = scaler.inverse_transform(y_pred)
    future_predictions = []
    input_seq = X[-1]
    for _ in range(4):
        pred = model.predict(input_seq.reshape(1, seq_length, 1))
        future_predictions.append(pred[0, 0])
        input_seq = np.append(input_seq[1:], pred[0, 0]).reshape(seq_length, 1)
    future_predictions_rescaled = scaler.inverse_transform(np.array(future_predictions).reshape(-1, 1))
    future_dates = pd.date_range(df_filtered.index[-1], periods=5, freq='W')[1:]

    # Obtener estadísticas
    current_week_value, last_year_week_value, current_month_avg, last_year_month_avg, future_values = get_statistics(
        df_filtered, future_predictions_rescaled, future_dates)

    # Mostrar estadísticas
    display_statistics(current_week_value, last_year_week_value, current_month_avg, last_year_month_avg, future_values)

    # Gráfico
    st.write("### Comparación de Valores Reales y Predicciones")
    fig = px.line(df_filtered.reset_index(), x='Fecha_Semana', y='Total_per_capita_2019', title="Valores Reales y Proyecciones")
    fig.add_scatter(x=future_dates, y=future_predictions_rescaled.flatten(), mode='lines+markers', name='Proyecciones Futuras')
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
