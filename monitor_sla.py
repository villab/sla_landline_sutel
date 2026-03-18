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

st.set_page_config(page_title="SUTEL - Monitor Global", layout="wide")

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
    start_dt = datetime(year, month, 1, 0, 0, 0)
    ts_start = int(start_dt.timestamp() * 1000)
    last_day = calendar.monthrange(year, month)[1]
    end_dt = datetime(year, month, last_day, 23, 59, 59)
    ts_end = int(end_dt.timestamp() * 1000)
    return ts_start, ts_end

# --- INICIO APP ---
df_master = load_data()

if not df_master.empty:
    st.title("📊 Masterfile de Cumplimiento (Sincronización Total)")
    
    # --- SIDEBAR ---
    st.sidebar.header("Configuración")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), format_func=lambda x: calendar.month_name[x].capitalize())
    ts_start, ts_end = get_timestamps(year, month)
    mes_key = f"{str(month).zfill(2)}/{year}"

    operadores = sorted(df_master['operador'].unique())

    # --- BOTÓN GLOBAL (Fuera de las pestañas) ---
    if st.button("🚀 Sincronizar TODOS los Operadores", use_container_width=True):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Obtenemos todos los clusters únicos del archivo para el proceso
        todos_clusters = df_master['id'].unique().tolist()
        total = len(todos_clusters)

        # 1. Preparar/Limpiar session_states para todos los operadores
        for op in operadores:
            state_key = f"df_{op}_{mes_key}"
            df_op_base = df_master[df_master['operador'] == op].copy()
            df_grouped = df_op_base.groupby(['provincia', 'canton'])['id'].apply(list).reset_index()
            for m in METRICAS: df_grouped[m] = 0
            df_grouped["Estado"] = "Sincronizando..."
            st.session_state[state_key] = df_grouped

        # 2. Bucle de consulta cluster por cluster (Modo Seguro)
        for idx, cid in enumerate(todos_clusters):
            status_text.text(f"Procesando cluster {idx+1} de {total}: {cid}")
            progress_bar.progress((idx + 1) / total)

            payload = {
                "tsStart": ts_start, "tsEnd": ts_end,
                "format": "aggregate",
                "programs": ["http-upload-burst-test", "http-down-burst-test", "ping-test"],
                "clusters": [cid],
                "aggregate": {
                    "groupBy": {"field": "dateStart", "operation": "month"},
                    "values": [{"field": "meduxId", "operation": "count"}],
                    "breakdownBy": ["cluster", "test", "target"]
                }
            }

            try:
                response = requests.post(API_URL, json=payload, headers=HEADERS)
                if response.status_code == 200:
                    res_json = response.json()
                    data_mes = res_json.get("results", {}).get(mes_key, {}).get(cid, {})
                    
                    if data_mes:
                        # Identificar a qué operador pertenece este cluster para actualizar su session_state
                        op_pertenece = df_master[df_master['id'] == cid]['operador'].values[0]
                        sk = f"df_{op_pertenece}_{mes_key}"
                        df_temp = st.session_state[sk]
                        
                        mask = df_temp['id'].apply(lambda x: cid in x)
                        
                        # Procesar niveles: Test -> Target
                        for test_name, targets in data_mes.items():
                            if isinstance(targets, dict):
                                for target_addr, details in targets.items():
                                    count = details.get("meduxId", {}).get("count", 0)
                                    col = None
                                    if test_name == "ping-test":
                                        col = "Ping Nacional" if IP_NACIONAL in target_addr else "Ping Internacional"
                                    elif "down" in test_name: col = "HTTP Download"
                                    elif "upload" in test_name: col = "HTTP Upload"
                                    
                                    if col:
                                        df_temp.loc[mask, col] += count
                        
                        df_temp.loc[mask, "Estado"] = "✅ OK"
                        st.session_state[sk] = df_temp
                elif response.status_code == 403:
                    op_pertenece = df_master[df_master['id'] == cid]['operador'].values[0]
                    sk = f"df_{op_pertenece}_{mes_key}"
                    mask = st.session_state[sk]['id'].apply(lambda x: cid in x)
                    st.session_state[sk].loc[mask, "Estado"] = "🚫 403"
            except:
                continue

        progress_bar.empty()
        status_text.empty()
        st.success("¡Sincronización global completada!")
        st.rerun()

    # --- RENDERIZADO DE PESTAÑAS ---
    tabs = st.tabs([f"Hoja {op}" for op in operadores])

    for i, op in enumerate(operadores):
        with tabs[i]:
            state_key = f"df_{op}_{mes_key}"
            
            # Si no se ha sincronizado nada, inicializar vista vacía
            if state_key not in st.session_state:
                df_op_base = df_master[df_master['operador'] == op].copy()
                df_grouped = df_op_base.groupby(['provincia', 'canton'])['id'].apply(list).reset_index()
                for m in METRICAS: df_grouped[m] = 0
                df_grouped["Estado"] = "Pendiente"
                st.session_state[state_key] = df_grouped

            df_viz = st.session_state[state_key].copy()
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
