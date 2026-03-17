import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import calendar

# --- 1. CONFIGURACIÓN DE API Y CREDENCIALES ---
# Puedes ponerlas aquí directamente o usar st.secrets para mayor seguridad

API_URL = st.secrets["api_url"]
BEARER_TOKEN = st.secrets["bearer_token"]

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

# IPs de referencia
IP_NACIONAL = "138.59.18.180"
IP_INTERNACIONAL = "84.17.40.24"
METRICAS = ["Ping Nacional", "Ping Internacional", "HTTP Download", "HTTP Upload"]

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="SUTEL - Monitor de Clusters", layout="wide")

@st.cache_data
def load_data():
    file_path = 'Clusters_Sutel_Fijo2026.xlsx'
    try:
        # Forzamos el uso de openpyxl para archivos .xlsx
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Limpieza de nombres de columnas
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Error al cargar el archivo: {e}")
        return pd.DataFrame()

df_master = load_data()
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
    
    # Sidebar
    st.sidebar.header("Configuración")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), format_func=lambda x: calendar.month_name[x].capitalize())
    ts_start, ts_end = get_timestamps(year, month)

    operadores = sorted(df_master['operador'].unique())
    tabs = st.tabs([f"Hoja {op}" for op in operadores])

    for i, op in enumerate(operadores):
        with tabs[i]:
            df_op = df_master[df_master['operador'] == op].copy()
            df_agrupado = df_op.groupby(['provincia', 'canton'])['StM'].apply(list).reset_index()
            
            for m in METRICAS:
                df_agrupado[m] = 0
            
            # --- BOTÓN DE ACCIÓN ---
            if st.button(f"Sincronizar Datos API - {op}", key=f"btn_{op}"):
                clusters_op = df_op['StM'].unique().tolist()
                
                # PAYLOAD según tu ejemplo de Postman
                payload = {
                    "tsStart": ts_start,
                    "tsEnd": ts_end,
                    "format": "aggregate",
                    "programs": ["http-upload-burst-test", "http-down-burst-test", "ping-test"],
                    "clusters": clusters_op,
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
                            data_api = response.json()
                            st.success(f"Conexión exitosa. Datos de {op} actualizados.")
                            # Aquí procesarías data_api para actualizar df_agrupado
                        else:
                            st.error(f"Error API: {response.status_code} - {response.text}")
                except Exception as e:
                    st.error(f"No se pudo conectar con el servidor: {e}")

            # --- TABLA ---
            df_final = df_agrupado.set_index(['Provincia', 'Canton'])
            st.dataframe(
                df_final[METRICAS].style.background_gradient(cmap='Blues', axis=None).format("{:d}"),
                use_container_width=True,
                height=600
            )

else:
    st.error("Archivo no encontrado.")
