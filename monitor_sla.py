import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import calendar

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="SUTEL - Monitor de Clusters", layout="wide")

# Mapeo de IPs para lógica de Pings
IP_NACIONAL = "138.59.18.180"
IP_INTERNACIONAL = "84.17.40.24"
METRICAS = ["Ping Nacional", "Ping Internacional", "HTTP Download", "HTTP Upload"]

@st.cache_data
def load_data():
    # Cargamos el archivo final que confirmaste
    df = pd.read_csv('Clusters_Sutel_Fijo2026.xlsx - clusters.csv')
    df.columns = [c.strip() for c in df.columns]
    return df

def get_timestamps(year, month):
    start_dt = datetime(year, month, 1, 0, 0, 0)
    ts_start = int(start_dt.timestamp() * 1000)
    last_day = calendar.monthrange(year, month)[1]
    end_dt = datetime(year, month, last_day, 23, 59, 59, 999)
    ts_end = int(end_dt.timestamp() * 1000)
    return ts_start, ts_end

# --- APP ---
df_master = load_data()

if not df_master.empty:
    st.title("📊 Masterfile de Cumplimiento")
    
    # Selector de tiempo en el sidebar
    st.sidebar.header("Configuración de Consulta")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), format_func=lambda x: calendar.month_name[x].capitalize())
    
    ts_start, ts_end = get_timestamps(year, month)

    # Definir los 4 operadores
    operadores = sorted(df_master['operador'].unique())
    tabs = st.tabs([f"Hoja {op}" for op in operadores])

    for i, op in enumerate(operadores):
        with tabs[i]:
            # 1. Filtrar datos del operador actual
            df_op = df_master[df_master['operador'] == op].copy()
            
            # 2. Agrupar por Provincia y Cantón, recolectando los StM (IDs de clusters)
            # Esto es vital porque un cantón tiene varios clusters
            df_agrupado = df_op.groupby(['Provincia', 'Canton'])['StM'].apply(list).reset_index()
            
            # 3. Añadir columnas de métricas en 0
            for m in METRICAS:
                df_agrupado[m] = 0
            
            # 4. Interfaz de Botón para API
            col_info, col_btn = st.columns([3, 1])
            with col_info:
                st.subheader(f"Operador: {op}")
            with col_btn:
                if st.button(f"Sincronizar API {op}", key=f"btn_{op}"):
                    # Aquí prepararías el payload masivo con todos los clusters del operador
                    todos_los_clusters = df_op['StM'].unique().tolist()
                    st.toast(f"Consultando {len(todos_los_clusters)} clusters...")
                    # Lógica de requests.post iría aquí
            
            # 5. Formateo de la tabla para visualización
            df_final = df_agrupado.set_index(['Provincia', 'Canton'])
            
            # Mostramos el DataFrame (excluimos la columna oculta de listas 'StM' para el usuario)
            st.dataframe(
                df_final[METRICAS].style.background_gradient(cmap='Blues', axis=None).format("{:d}"),
                use_container_width=True,
                height=650
            )
            
            st.caption(f"ID de clusters vinculados en esta hoja: {len(df_op['StM'].unique())}")

else:
    st.error("No se pudo leer el archivo de clusters.")
