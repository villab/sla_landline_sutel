import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import calendar

# --- 1. CONFIGURACIÓN DE API ---
API_URL = st.secrets["api_url"]
BEARER_TOKEN = st.secrets["bearer_token"]

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

IP_NACIONAL = "138.59.18.180"
IP_INTERNACIONAL = "84.17.40.24"
METRICAS = ["Ping Nacional", "Ping Internacional", "HTTP Download", "HTTP Upload"]

st.set_page_config(page_title="SUTEL - Monitor de Clusters", layout="wide")

@st.cache_data
def load_data():
    file_path = 'Clusters_Sutel_Fijo2026.xlsx'
    try:
        # Cargamos el Excel
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Limpieza de nombres de columnas: quitamos espacios y convertimos a minúsculas
        # para que el código no falle si el Excel cambia ligeramente
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        # Mapeo interno para asegurar consistencia en el resto del script
        # Buscamos 'cluster', 'provincia', 'canton', 'operador'
        return df
    except Exception as e:
        st.error(f"Error al cargar el archivo: {e}")
        return pd.DataFrame()

def get_timestamps(year, month):
    start_dt = datetime(year, month, 1, 0, 0, 0)
    ts_start = int(start_dt.timestamp() * 1000)
    last_day = calendar.monthrange(year, month)[1]
    end_dt = datetime(year, month, last_day, 23, 59, 59, 999)
    ts_end = int(end_dt.timestamp() * 1000)
    return ts_start, ts_end

# --- INICIO APP ---
df_master = load_data()

if not df_master.empty:
    st.title("📊 Masterfile de Cumplimiento")
    
    # Sidebar
    st.sidebar.header("Configuración")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), format_func=lambda x: calendar.month_name[x].capitalize())
    ts_start, ts_end = get_timestamps(year, month)

    # Identificar operadores (columna 'operador' en minúsculas según tu Excel)
    operadores = sorted(df_master['operador'].unique())
    tabs = st.tabs([f"Hoja {op}" for op in operadores])

    for i, op in enumerate(operadores):
        with tabs[i]:
            # Filtrar por operador
            df_op = df_master[df_master['operador'] == op].copy()
            
            # AGRUPACIÓN: Usamos 'provincia', 'canton' y 'cluster' tal cual vienen en tu archivo
            # Agrupamos los clusters por cantón en una lista (por si hay varios por cantón)
            df_agrupado = df_op.groupby(['provincia', 'canton'])['cluster'].apply(list).reset_index()
            
            # Inicializar columnas de métricas
            for m in METRICAS:
                df_agrupado[m] = 0
            
            # --- BOTÓN DE ACCIÓN ---
            if st.button(f"Sincronizar Datos API - {op}", key=f"btn_{op}"):
                # Usamos la columna 'cluster' para obtener los IDs para la API
                listado_clusters = df_op['cluster'].unique().tolist()
                
                payload = {
                    "tsStart": ts_start,
                    "tsEnd": ts_end,
                    "format": "aggregate",
                    "programs": ["http-upload-burst-test", "http-down-burst-test", "ping-test"],
                    "clusters": listado_clusters,
                    "aggregate": {
                        "groupBy": {"field": "dateStart", "operation": "month"},
                        "values": [{"field": "meduxId", "operation": "count"}],
                        "breakdownBy": ["cluster", "program", "target"]
                    }
                }

                try:
                    with st.spinner("Consultando API..."):
                        response = requests.post(API_URL, json=payload, headers=HEADERS)
                        if response.status_code == 200:
                            st.success(f"Conexión exitosa para {op}")
                            # Aquí procesarías los datos reales
                        else:
                            st.error(f"Error API: {response.status_code}")
                except Exception as e:
                    st.error(f"Error de conexión: {e}")

            # --- VISUALIZACIÓN ---
            # Mostramos Provincia y Cantón con la primera letra en mayúscula para que se vea bien
            df_final = df_agrupado.copy()
            df_final['provincia'] = df_final['provincia'].str.capitalize()
            df_final['canton'] = df_final['canton'].str.capitalize()
            
            # Ponemos el índice y ocultamos la columna 'cluster'
            df_final = df_final.set_index(['provincia', 'canton'])
            
            st.dataframe(
                df_final[METRICAS].style.background_gradient(cmap='Blues', axis=None).format("{:d}"),
                use_container_width=True,
                height=600
            )

else:
    st.error("No se pudo cargar la información del archivo Excel.")
