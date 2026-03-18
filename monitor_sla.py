import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import calendar

# --- 1. CONFIGURACIÓN DE API ---
# El API_URL debe ser el endpoint fijo (ej: https://api.medux.com/v1/aggregate)
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
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = [str(c).strip().lower() for c in df.columns]
        if 'cluster' in df.columns:
            df = df.rename(columns={'cluster': 'id'})
        return df
    except Exception as e:
        st.error(f"Error al cargar el archivo: {e}")
        return pd.DataFrame()

def get_timestamps(year, month):
    # Aseguramos inicio y fin de mes exactos
    start_dt = datetime(year, month, 1, 0, 0, 0)
    ts_start = int(start_dt.timestamp() * 1000)
    last_day = calendar.monthrange(year, month)[1]
    end_dt = datetime(year, month, last_day, 23, 59, 59)
    ts_end = int(end_dt.timestamp() * 1000)
    return ts_start, ts_end

# --- INICIO APP ---
df_master = load_data()

if not df_master.empty:
    st.title("📊 Masterfile de Cumplimiento")
    
    st.sidebar.header("Configuración")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), format_func=lambda x: calendar.month_name[x].capitalize())
    ts_start, ts_end = get_timestamps(year, month)
    mes_key = f"{str(month).zfill(2)}/{year}"

    if 'operador' in df_master.columns:
        operadores = sorted(df_master['operador'].unique())
        tabs = st.tabs([f"Hoja {op}" for op in operadores])

        for i, op in enumerate(operadores):
            with tabs[i]:
                state_key = f"df_{op}_{mes_key}"
                
                if state_key not in st.session_state:
                    df_op_base = df_master[df_master['operador'] == op].copy()
                    df_grouped = df_op_base.groupby(['provincia', 'canton'])['id'].apply(list).reset_index()
                    for m in METRICAS:
                        df_grouped[m] = 0
                    df_grouped["Estado"] = "Pendiente"
                    st.session_state[state_key] = df_grouped

                df_actual = st.session_state[state_key]

                col_btn, col_info = st.columns([1, 3])
                with col_btn:
                    if st.button(f"Sincronizar API - {op}", key=f"btn_{op}"):
                        listado_clusters = df_master[df_master['operador'] == op]['id'].unique().tolist()
                        
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        clusters_403 = []

                        # Resetear datos
                        for m in METRICAS: df_actual[m] = 0

                        for idx, cid in enumerate(listado_clusters):
                            status_text.text(f"Consultando {cid}...")
                            progress_bar.progress((idx + 1) / len(listado_clusters))

                            payload = {
                                "tsStart": ts_start,
                                "tsEnd": ts_end,
                                "format": "aggregate",
                                "programs": ["http-upload-burst-test", "http-down-burst-test", "ping-test"],
                                "clusters": [cid],
                                "aggregate": {
                                    "groupBy": {"field": "dateStart", "operation": "month"},
                                    "values": [{"field": "meduxId", "operation": "count"}],
                                    "breakdownBy": ["program", "target"] # Agregamos desglose para las 4 columnas
                                }
                            }

                            try:
                                response = requests.post(API_URL, json=payload, headers=HEADERS)
                                mask = df_actual['id'].apply(lambda x: cid in x)

                                if response.status_code == 200:
                                    res_json = response.json()
                                    # Navegamos por la estructura de resultados
                                    # results -> mes -> cluster -> programa -> target
                                    data_cluster = res_json.get("results", {}).get(mes_key, {}).get(cid, {})
                                    
                                    if data_cluster:
                                        # Recorremos programas dentro del cluster
                                        for prog, targets in data_cluster.items():
                                            if isinstance(targets, dict):
                                                for tgt, values in targets.items():
                                                    count = values.get("meduxId", {}).get("count", 0)
                                                    
                                                    # Clasificación
                                                    col = None
                                                    if prog == "ping-test":
                                                        col = "Ping Nacional" if tgt == IP_NACIONAL else "Ping Internacional"
                                                    elif prog == "http-down-burst-test":
                                                        col = "HTTP Download"
                                                    elif prog == "http-upload-burst-test":
                                                        col = "HTTP Upload"
                                                    
                                                    if col:
                                                        df_actual.loc[mask, col] += count
                                        
                                        df_actual.loc[mask, "Estado"] = "✅ OK"
                                    else:
                                        if df_actual.loc[mask, "Estado"].values[0] != "✅ OK":
                                            df_actual.loc[mask, "Estado"] = "⚪ Sin Datos"
                                
                                elif response.status_code == 403:
                                    clusters_403.append(cid)
                                    df_actual.loc[mask, "Estado"] = "🚫 403"

                            except Exception as e:
                                continue

                        progress_bar.empty()
                        status_text.empty()
                        st.session_state[state_key] = df_actual
                        
                        if clusters_403:
                            with st.expander("Clusters con error de permisos (403)"):
                                st.write(clusters_403)
                        
                        st.rerun()

                with col_info:
                    st.info(f"Sincronizando {op} para {mes_key}. El endpoint es fijo: {API_URL}")

                # --- VISUALIZACIÓN ---
                df_viz = df_actual.copy()
                df_viz['provincia'] = df_viz['provincia'].str.title()
                df_viz['canton'] = df_viz['canton'].str.title()
                df_viz = df_viz.set_index(['provincia', 'canton'])

                st.dataframe(
                    df_viz[["Estado"] + METRICAS].style.applymap(
                        lambda x: 'color: red' if x == '🚫 403' else ('color: green' if x == '✅ OK' else ''),
                        subset=['Estado']
                    ).background_gradient(cmap='Blues', subset=METRICAS).format("{:d}", subset=METRICAS),
                    use_container_width=True,
                    height=600
                )
