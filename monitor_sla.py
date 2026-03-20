import streamlit as st
import pandas as pd
import requests
import os
from datetime import datetime
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------ Configuración de página ----------
st.set_page_config(
    page_title="SLA Control",
    page_icon="🚩",
    layout="wide"
)

# --- CONFIGURACIÓN DE API (Docker/Cloud) ---
try:
    API_URL = st.secrets.get("api_url")
    BEARER_TOKEN = st.secrets.get("bearer_token")
except Exception:
    API_URL = os.getenv("api_url")
    BEARER_TOKEN = os.getenv("bearer_token")

if not API_URL or not BEARER_TOKEN:
    st.error("❌ Error Crítico: No se encontraron las credenciales.")
    st.stop()

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

IP_NACIONAL = "138.59.18.180"
METRICAS = ["Ping Nacional", "Ping Internacional", "HTTP Download", "HTTP Upload"]

# --- FUNCIONES DE APOYO ---
def aplicar_color_semaforo(val):
    try:
        val = int(val)
        if val == 0: return 'background-color: #ffcccc'
        elif 1 <= val <= 500: return 'background-color: #ffe5cc'
        elif 501 <= val <= 999: return 'background-color: #ffffcc'
        elif val >= 1000: return 'background-color: #ccffcc'
        return ''
    except: return ''

@st.cache_data
def load_data():
    try:
        df = pd.read_excel('Clusters_Sutel_Fijo2026.xlsx', engine='openpyxl')
        df.columns = [str(c).strip().lower() for c in df.columns]
        if 'id' not in df.columns and 'cluster' in df.columns:
            df = df.rename(columns={'cluster': 'id'})
        return df
    except Exception as e:
        st.error(f"Error cargando Excel: {e}")
        return pd.DataFrame()

def fetch_cluster_data(cid, ts_start, ts_end, mes_key):
    payload = {
        "tsStart": ts_start, 
        "tsEnd": ts_end, 
        "format": "aggregate",
        "limit": 10000,
        "programs": ["http-upload-burst-test", "http-down-burst-test", "ping-test"],
        "clusters": [cid],
        "aggregate": {
            "groupBy": {"field": "dateStart", "operation": "month"},
            "values": [{"field": "meduxId", "operation": "count"}],
            "breakdownBy": ["cluster", "test", "target"]
        }
    }
    try:
        response = requests.post(API_URL, json=payload, headers=HEADERS, timeout=15)
        return cid, response.json() if response.status_code == 200 else None, response.status_code
    except:
        return cid, None, 500

# --- PROCESAMIENTO ---
df_master = load_data()

if not df_master.empty:
    st.sidebar.header("🗓️ Periodo")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), index=datetime.now().month-1, format_func=lambda x: calendar.month_name[x].capitalize())
    
    st.sidebar.header("📊 Visualización")
    tipo_vista = st.sidebar.radio("Nivel de detalle:", ["Por Cantón (Resumen)", "Por Cluster (Detalle)"])
    busqueda = st.sidebar.text_input("🔍 Buscar Cluster (por nombre):", "")

    ts_start = int(datetime(year, month, 1).timestamp() * 1000)
    ts_end = int(datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59).timestamp() * 1000)
    mes_key = f"{str(month).zfill(2)}/{year}"
    operadores = sorted(df_master['operador'].unique())

    if st.button("🔄 Sincronizar Datos Medux", use_container_width=True):
        status = st.empty()
        progress_bar = st.progress(0)
        
        # 1. Inicializar
        for op in operadores:
            state_key = f"df_{op}_{mes_key}"
            df_op_init = df_master[df_master['operador'] == op].copy()
            for m in METRICAS: 
                df_op_init[m] = 0
            df_op_init["estado"] = "Pendiente"
            st.session_state[state_key] = df_op_init

        todos_ids = df_master['id'].unique().tolist()
        results_list = []
        
        # 2. Descargar
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_cluster_data, cid, ts_start, ts_end, mes_key): cid for cid in todos_ids}
            for i, future in enumerate(as_completed(futures)):
                results_list.append(future.result())
                progress_bar.progress((i + 1) / len(todos_ids))
                status.text(f"Descargando clusters: {i+1}/{len(todos_ids)}")

        # 3. PROCESAR (Asegurarse de que esto ocurra ANTES del rerun)
# 3. PROCESAR (Ajustado a la estructura real de Medux)
        for cid, res_json, code in results_list:
            row_master = df_master[df_master['id'] == cid]
            if row_master.empty: continue
            
            op_pertenece = row_master['operador'].values[0]
            df_state = st.session_state[f"df_{op_pertenece}_{mes_key}"]
            
            idx_list = df_state[df_state['id'] == cid].index
            if len(idx_list) == 0: continue
            idx = idx_list[0]

            if code == 200 and res_json:
                # Accedemos a results -> mes_key -> cluster_id
                # Usamos .get() en cascada para evitar errores si una llave falta
                data_cluster = res_json.get("results", {}).get(mes_key, {}).get(cid, {})
                
                if data_cluster:
                    # Iteramos sobre los tests (ej: http-down-burst-test)
                    for test_name, targets in data_cluster.items():
                        if isinstance(targets, dict):
                            # Iteramos sobre los targets (la URL o IP)
                            for target_key, details in targets.items():
                                # El JSON muestra que 'details' es el objeto con 'meduxId'
                                count = details.get("meduxId", {}).get("count", 0)
                                
                                col = None
                                if "ping" in test_name:
                                    # Comparamos el target_key directamente
                                    col = "Ping Nacional" if IP_NACIONAL in str(target_key) else "Ping Internacional"
                                elif "down" in test_name: 
                                    col = "HTTP Download"
                                elif "upload" in test_name: 
                                    col = "HTTP Upload"
                                
                                if col:
                                    # Sumamos al valor existente en la tabla
                                    valor_actual = df_state.at[idx, col]
                                    df_state.at[idx, col] = int(valor_actual) + int(count)
                    
                    df_state.at[idx, "estado"] = "✅ OK"
                else:
                    # Esto pasa si el Cluster ID no viene en el JSON de ese mes
                    df_state.at[idx, "estado"] = "⚠️ No encontrado"
            else:
                df_state.at[idx, "estado"] = f"❌ Error {code}"

        st.success("Sincronización finalizada.")
        st.rerun()

    # --- RENDERIZADO ---
    tabs = st.tabs([f"OPERADOR: {op}" for op in operadores])
    for i, op in enumerate(operadores):
        with tabs[i]:
            state_key = f"df_{op}_{mes_key}"
            if state_key in st.session_state:
                df_viz = st.session_state[state_key].copy()
                
                if busqueda:
                    df_viz = df_viz[df_viz['name'].str.contains(busqueda, case=False, na=False)]

                if tipo_vista == "Por Cantón (Resumen)":
                    df_final = df_viz.groupby(['provincia', 'canton'])[METRICAS].sum().reset_index()
                    df_final["Estado"] = "📊 Resumen"
                    columnas_finales = ["provincia", "canton", "Estado"] + METRICAS
                else:
                    df_final = df_viz.rename(columns={'estado': 'Estado'})
                    columnas_finales = ["provincia", "canton", "name", "Estado"] + METRICAS

                df_final['provincia'] = df_final['provincia'].str.title()
                df_final['canton'] = df_final['canton'].str.title()
                df_final.insert(0, '#', range(1, len(df_final) + 1))

                st.dataframe(
                    df_final[["#"] + columnas_finales].style.applymap(
                        aplicar_color_semaforo, subset=METRICAS
                    ).applymap(
                        lambda x: 'color: #d63031; font-weight: bold' if 'Error' in str(x) else ('color: #27ae60; font-weight: bold' if 'OK' in str(x) else ''),
                        subset=['Estado']
                    ).format("{:,.0f}", subset=METRICAS),
                    use_container_width=True,
                    height=600,
                    hide_index=True
                )
else:
    st.warning("No se encontró el archivo Clusters_Sutel_Fijo2026.xlsx")
