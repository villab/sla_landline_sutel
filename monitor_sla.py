import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import calendar

# --- 1. CONFIGURACIÓN DE API Y CREDENCIALES ---
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
    end_dt = datetime(year, month, last_day, 23, 59, 59, 999)
    ts_end = int(end_dt.timestamp() * 1000)
    return ts_start, ts_end

# --- INICIO APP ---
df_master = load_data()

if not df_master.empty:
    st.title("📊 Masterfile de Cumplimiento")
    
    # --- SIDEBAR ---
    st.sidebar.header("Configuración de Consulta")
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
                    df_grouped["Estado"] = "Pendiente" # Columna de control
                    st.session_state[state_key] = df_grouped

                df_actual = st.session_state[state_key]

                # --- BOTÓN DE ACCIÓN (MODO SEGURO / BARRIDO) ---
                col_btn, col_info = st.columns([1, 3])
                with col_btn:
                    if st.button(f"Sincronizar API - {op}", key=f"btn_{op}"):
                        df_op_raw = df_master[df_master['operador'] == op]
                        listado_clusters = df_op_raw['id'].unique().tolist()
                        
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        clusters_403 = []

                        # Resetear datos actuales antes de barrido
                        for m in METRICAS:
                            df_actual[m] = 0
                        df_actual["Estado"] = "Procesando..."

                        # Bucle individual por cluster para saltar errores 403
                        for idx, cid in enumerate(listado_clusters):
                            status_text.text(f"Consultando cluster {idx+1}/{len(listado_clusters)}...")
                            progress_bar.progress((idx + 1) / len(listado_clusters))

                            payload = {
                                "tsStart": ts_start, "tsEnd": ts_end,
                                "format": "aggregate",
                                "programs": ["http-upload-burst-test", "http-down-burst-test", "ping-test"],
                                "clusters": [cid], # Uno a la vez
                                "aggregate": {
                                    "groupBy": {"field": "dateStart", "operation": "month"},
                                    "values": [{"field": "meduxId", "operation": "count"}],
                                    "breakdownBy": ["cluster", "program", "target"]
                                }
                            }

                            try:
                                response = requests.post(API_URL, json=payload, headers=HEADERS)
                                mask = df_actual['id'].apply(lambda x: cid in x)

                                if response.status_code == 200:
                                    res_json = response.json()
                                    data_mes = res_json.get("results", {}).get(mes_key, {})
                                    
                                    if cid in data_mes:
                                        info = data_mes[cid]
                                        # Lógica simplificada de conteo basada en tu respuesta JSON
                                        count = info.get("meduxId", {}).get("count", 0)
                                        # Aquí podrías expandir según programa/target si el JSON es más profundo
                                        df_actual.loc[mask, "Ping Nacional"] += count
                                        df_actual.loc[mask, "Estado"] = "✅ OK"
                                    else:
                                        if df_actual.loc[mask, "Estado"].values[0] != "✅ OK":
                                            df_actual.loc[mask, "Estado"] = "⚪ Sin Datos"
                                
                                elif response.status_code == 403:
                                    clusters_403.append(cid)
                                    df_actual.loc[mask, "Estado"] = "🚫 Error 403"

                            except Exception:
                                continue

                        progress_bar.empty()
                        status_text.empty()
                        st.session_state[state_key] = df_actual

                        if clusters_403:
                            with st.expander("⚠️ Ver clusters con Error 403 (Acceso denegado)"):
                                st.write("Estos IDs fallaron y fueron omitidos de la suma:")
                                st.code("\n".join(clusters_403))
                        
                        st.success(f"Proceso completado para {op}")
                        st.rerun()

                with col_info:
                    st.info(f"Periodo: **{mes_key}**. Los clusters con error 403 se omitirán automáticamente.")

                # --- VISUALIZACIÓN DE LA TABLA ---
                df_viz = df_actual.copy()
                df_viz['provincia'] = df_viz['provincia'].str.title()
                df_viz['canton'] = df_viz['canton'].str.title()
                df_viz = df_viz.set_index(['provincia', 'canton'])

                # Estilizar columna Estado y métricas
                columnas_ver = ["Estado"] + METRICAS
                st.dataframe(
                    df_viz[columnas_ver].style.applymap(
                        lambda x: 'color: red; font-weight: bold' if x == '🚫 Error 403' else ('color: green' if x == '✅ OK' else ''),
                        subset=['Estado']
                    ).background_gradient(cmap='Blues', subset=METRICAS).format("{:d}", subset=METRICAS),
                    use_container_width=True,
                    height=600
                )
                
    else:
        st.error("Columna 'operador' no encontrada.")
else:
    st.error("Archivo Excel no cargado correctamente.")
