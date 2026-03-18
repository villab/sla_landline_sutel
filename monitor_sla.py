import streamlit as st
import pandas as pd
import requests
import os  # <-- ¡MUY IMPORTANTE!
from datetime import datetime
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed


# --- CONFIGURACIÓN DE API (Blindada para Docker) ---
try:
    # Intentamos primero con secretos de Streamlit
    API_URL = st.secrets.get("api_url")
    BEARER_TOKEN = st.secrets.get("bearer_token")
except Exception:
    # Si falla (como en Docker), forzamos la lectura desde el entorno
    API_URL = None
    BEARER_TOKEN = None

# Si no se obtuvieron de st.secrets, buscamos en las variables de sistema
if not API_URL:
    API_URL = os.getenv("api_url")
if not BEARER_TOKEN:
    BEARER_TOKEN = os.getenv("bearer_token")

# Verificación final
if not API_URL or not BEARER_TOKEN:
    st.error("❌ Error Crítico: No se encontraron las credenciales (api_url / bearer_token).")
    st.info("Revisa que el archivo .env no tenga espacios ni comillas.")
    st.stop()

# Configurar Headers para la API
HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}
IP_NACIONAL = "138.59.18.180"
IP_INTERNACIONAL = "84.17.40.24"
METRICAS = ["Ping Nacional", "Ping Internacional", "HTTP Download", "HTTP Upload"]

st.set_page_config(page_title="SUTEL - Monitor High-Speed", layout="wide")

# --- FUNCIÓN DE COLOR PERSONALIZADA (Requerimiento 3) ---
def aplicar_color_semaforo(val):
    """Aplica colores pastel según el rango del valor"""
    try:
        val = int(val)
        if val == 0:
            return 'background-color: #ffcccc'  # Rojo pastel
        elif 1 <= val <= 500:
            return 'background-color: #ffe5cc'  # Naranja pastel
        elif 501 <= val <= 999:
            return 'background-color: #ffffcc'  # Amarillo pastel
        elif val >= 1000:
            return 'background-color: #ccffcc'  # Verde pastel
        return ''
    except:
        return ''

@st.cache_data
def load_data():
    try:
        df = pd.read_excel('Clusters_Sutel_Fijo2026.xlsx', engine='openpyxl')
        df.columns = [str(c).strip().lower() for c in df.columns]
        if 'cluster' in df.columns: df = df.rename(columns={'cluster': 'id'})
        return df
    except Exception as e:
        st.error(f"Error: {e}"); return pd.DataFrame()

def fetch_cluster_data(cid, ts_start, ts_end, mes_key):
    payload = {
        "tsStart": ts_start, "tsEnd": ts_end, "format": "aggregate",
        "programs": ["http-upload-burst-test", "http-down-burst-test", "ping-test"],
        "clusters": [cid],
        "aggregate": {
            "groupBy": {"field": "dateStart", "operation": "month"},
            "values": [{"field": "meduxId", "operation": "count"}],
            "breakdownBy": ["cluster", "test", "target"]
        }
    }
    try:
        response = requests.post(API_URL, json=payload, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return cid, response.json(), 200
        return cid, None, response.status_code
    except:
        return cid, None, 500

# --- INICIO APP ---
df_master = load_data()

if not df_master.empty:
    st.sidebar.header("Configuración")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), format_func=lambda x: calendar.month_name[x].capitalize())
    
    ts_start = int(datetime(year, month, 1).timestamp() * 1000)
    ts_end = int(datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59).timestamp() * 1000)
    mes_key = f"{str(month).zfill(2)}/{year}"
    operadores = sorted(df_master['operador'].unique())

    if st.button("⚡ Sincronización PARALELA (Ultra Rápida)", use_container_width=True):
        status = st.empty()
        progress_bar = st.progress(0)
        
        for op in operadores:
            state_key = f"df_{op}_{mes_key}"
            df_op_base = df_master[df_master['operador'] == op].copy()
            df_grouped = df_op_base.groupby(['provincia', 'canton'])['id'].apply(list).reset_index()
            for m in METRICAS: df_grouped[m] = 0
            df_grouped["Estado"] = "Pendiente"
            st.session_state[state_key] = df_grouped

        todos_ids = df_master['id'].unique().tolist()
        total_clusters = len(todos_ids)
        
        results_list = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_cluster = {executor.submit(fetch_cluster_data, cid, ts_start, ts_end, mes_key): cid for cid in todos_ids}
            for i, future in enumerate(as_completed(future_to_cluster)):
                cid, data, code = future.result()
                results_list.append((cid, data, code))
                progress_bar.progress((i + 1) / total_clusters)
                status.text(f"Recibiendo datos: {i+1}/{total_clusters}")

        for cid, res_json, code in results_list:
            op_pertenece = df_master[df_master['id'] == cid]['operador'].values[0]
            df_temp = st.session_state[f"df_{op_pertenece}_{mes_key}"]
            mask = df_temp['id'].apply(lambda x: cid in x)

            if code == 200 and res_json:
                data_mes = res_json.get("results", {}).get(mes_key, {}).get(cid, {})
                for test_name, targets in data_mes.items():
                    if isinstance(targets, dict):
                        for tgt, details in targets.items():
                            count = details.get("meduxId", {}).get("count", 0)
                            col = None
                            if "ping" in test_name:
                                col = "Ping Nacional" if IP_NACIONAL in tgt else "Ping Internacional"
                            elif "down" in test_name: col = "HTTP Download"
                            elif "upload" in test_name: col = "HTTP Upload"
                            if col: df_temp.loc[mask, col] += count
                df_temp.loc[mask, "Estado"] = "✅ OK"
            elif code == 403:
                df_temp.loc[mask, "Estado"] = "🚫 403"
            else:
                df_temp.loc[mask, "Estado"] = "❌ Error"

        st.success("Sincronización en paralelo terminada.")
        st.rerun()

# --- RENDERIZADO TABS ---
    tabs = st.tabs([f"{op}" for op in operadores])
    for i, op in enumerate(operadores):
        with tabs[i]:
            state_key = f"df_{op}_{mes_key}"
            if state_key in st.session_state:
                # Trabajamos sobre una copia para no alterar los datos reales
                df_viz = st.session_state[state_key].copy()
                
                # Formateo estético de nombres
                df_viz['provincia'] = df_viz['provincia'].str.title()
                df_viz['canton'] = df_viz['canton'].str.title()
                
                # Creamos una columna de numeración real y la ponemos de primera
                df_viz.insert(0, '#', range(1, len(df_viz) + 1))
                
                # Seleccionamos el orden de las columnas
                columnas_finales = ["#", "provincia", "canton", "Estado"] + METRICAS
                df_final = df_viz[columnas_finales]

                # Aplicamos el estilo mejorado
                st.dataframe(
                    df_final.style.applymap(
                        aplicar_color_semaforo, 
                        subset=METRICAS
                    ).applymap(
                        lambda x: 'color: #d63031; font-weight: bold' if '403' in str(x) else ('color: #27ae60; font-weight: bold' if 'OK' in str(x) else ''),
                        subset=['Estado']
                    ).format("{:d}", subset=METRICAS),
                    use_container_width=True,
                    height=800,
                    hide_index=True  # <-- ESTO QUITA LA COLUMNA GRIS DE LA IZQUIERDA
                )
