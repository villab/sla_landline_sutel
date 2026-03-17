import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import calendar

# --- 1. CONFIGURACIÓN DE API Y CREDENCIALES ---
# Se recomienda usar st.secrets en Streamlit Cloud
API_URL = st.secrets["api_url"]
BEARER_TOKEN = st.secrets["bearer_token"]

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

# IPs de referencia para clasificar Pings
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
        # Limpieza: Todo a minúsculas y sin espacios
        df.columns = [str(c).strip().lower() for c in df.columns]
        # Renombrar 'cluster' a 'id' si fuera necesario para consistencia
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
    
    # --- SIDEBAR: CONFIGURACIÓN ---
    st.sidebar.header("Configuración de Consulta")
    year = st.sidebar.selectbox("Año", [2025, 2026], index=1)
    month = st.sidebar.selectbox("Mes", range(1, 13), format_func=lambda x: calendar.month_name[x].capitalize())
    ts_start, ts_end = get_timestamps(year, month)
    
    # Generar la llave que usa la API para los resultados (ej: "03/2026")
    mes_key = f"{str(month).zfill(2)}/{year}"

    if 'operador' in df_master.columns:
        operadores = sorted(df_master['operador'].unique())
        tabs = st.tabs([f"Hoja {op}" for op in operadores])

        for i, op in enumerate(operadores):
            with tabs[i]:
                # 1. Inicializar el DataFrame del operador en st.session_state si no existe
                # Esto permite que los datos persistan al cambiar de pestaña
                state_key = f"df_{op}_{mes_key}"
                
                if state_key not in st.session_state:
                    df_op_base = df_master[df_master['operador'] == op].copy()
                    # Agrupamos por provincia y cantón guardando la lista de IDs de clusters
                    df_grouped = df_op_base.groupby(['provincia', 'canton'])['id'].apply(list).reset_index()
                    # Inicializamos métricas en 0
                    for m in METRICAS:
                        df_grouped[m] = 0
                    st.session_state[state_key] = df_grouped

                # Recuperamos el dataframe del estado actual
                df_actual = st.session_state[state_key]

                # --- BOTÓN DE ACCIÓN ---
                col_btn, col_txt = st.columns([1, 3])
                with col_btn:
                    if st.button(f"Sincronizar API - {op}", key=f"btn_{op}"):
                        # Extraer todos los IDs de clusters para este operador
                        df_op_raw = df_master[df_master['operador'] == op]
                        listado_clusters = df_op_raw['id'].unique().tolist()
                        
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
                            with st.spinner(f"Consultando Medux para {op}..."):
                                response = requests.post(API_URL, json=payload, headers=HEADERS)
                                if response.status_code == 200:
                                    res_json = response.json()
                                    # Navegamos en el JSON: results -> "MM/YYYY"
                                    data_api = res_json.get("results", {}).get(mes_key, {})
                                    
                                    if not data_api:
                                        st.warning(f"No se devolvieron resultados para {mes_key}")
                                    else:
                                        # Resetear a 0 antes de la nueva carga para evitar duplicados si se pulsa 2 veces
                                        for m in METRICAS:
                                            df_actual[m] = 0

                                        # Procesar cada cluster en la respuesta
                                        # Nota: Si el breakdownBy devuelve una lista, iteramos sobre ella.
                                        # Si devuelve un diccionario por cluster (como tu ejemplo), iteramos items.
                                        for cluster_id, info in data_api.items():
                                            # Buscamos qué cantón contiene este cluster_id
                                            mask = df_actual['id'].apply(lambda x: cluster_id in x)
                                            
                                            # Intentamos obtener el conteo
                                            count = info.get("meduxId", {}).get("count", 0)
                                            prog = info.get("program", "") # Depende de si la API lo anida o lo pone al nivel
                                            tgt = info.get("target", "")

                                            # Lógica de clasificación (ajustar según niveles del JSON real)
                                            # Como tu ejemplo es simplificado, aquí un mapeo de ejemplo:
                                            if "ping" in prog:
                                                col = "Ping Nacional" if tgt == IP_NACIONAL else "Ping Internacional"
                                            elif "down" in prog:
                                                col = "HTTP Download"
                                            elif "upload" in prog:
                                                col = "HTTP Upload"
                                            else:
                                                # Si la API no separa por programa en este nivel, 
                                                # sumamos a la primera columna por defecto para pruebas
                                                col = "Ping Nacional"

                                            df_actual.loc[mask, col] += count
                                        
                                        st.session_state[state_key] = df_actual
                                        st.success(f"Datos sincronizados para {op}")
                                        st.rerun() # Refrescar para ver cambios
                                else:
                                    st.error(f"Error API {response.status_code}: {response.text}")
                        except Exception as e:
                            st.error(f"Error de procesamiento: {e}")

                with col_txt:
                    st.info(f"Mostrando periodo: **{mes_key}**. Pulsa sincronizar para actualizar los datos desde la API.")

                # --- VISUALIZACIÓN DE LA TABLA ---
                df_viz = df_actual.copy()
                # Formatear nombres para la tabla
                df_viz['provincia'] = df_viz['provincia'].str.title()
                df_viz['canton'] = df_viz['canton'].str.title()
                df_viz = df_viz.set_index(['provincia', 'canton'])

                # Mostrar solo las columnas de métricas (ocultamos la lista de 'id')
                st.dataframe(
                    df_viz[METRICAS].style.background_gradient(cmap='Blues', axis=None).format("{:d}"),
                    use_container_width=True,
                    height=600
                )
                
    else:
        st.error("No se encontró la columna 'operador' en el archivo.")
else:
    st.error("Asegúrate de que el archivo 'Clusters_Sutel_Fijo2026.xlsx' esté en la carpeta raíz.")
