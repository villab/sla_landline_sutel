import streamlit as st
import pandas as pd
import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------ Configuración de página ----------
st.set_page_config(
    page_title="SLA Control",
    page_icon="🚩",
    layout="wide"
)

# --- CONFIGURACIÓN DE API (Docker/Cloud) ---

API_URL = os.getenv("api_url")
BEARER_TOKEN = os.getenv("bearer_token")

# --- VERIFICACIÓN CRÍTICA ---
if not API_URL or not BEARER_TOKEN:
    st.error("❌ Error: No se detectan las variables de entorno.")
    st.write("Variables actuales detectadas:", list(os.environ.keys()))  # Esto te ayudará a debuguear
    st.stop()

HEADERS = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

# Zona horaria de referencia del proyecto (Costa Rica, UTC-6, sin horario de verano).
# Se usa explícitamente para que el rango del mes no dependa de la TZ del contenedor (normalmente UTC).
TZ_CR = ZoneInfo("America/Costa_Rica")

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


def color_estado(x):
    if 'Error' in str(x):
        return 'color: #d63031; font-weight: bold'
    if 'OK' in str(x):
        return 'color: #27ae60; font-weight: bold'
    return ''


def rango_mes_cr(year, month):
    """Devuelve (ts_start, ts_end) en ms epoch para el mes completo en hora de Costa Rica."""
    inicio = datetime(year, month, 1, tzinfo=TZ_CR)
    if month == 12:
        siguiente = datetime(year + 1, 1, 1, tzinfo=TZ_CR)
    else:
        siguiente = datetime(year, month + 1, 1, tzinfo=TZ_CR)
    ts_start = int(inicio.timestamp() * 1000)
    ts_end = int(siguiente.timestamp() * 1000) - 1  # último ms del mes en hora CR
    return ts_start, ts_end


@st.cache_data
def load_data():
    try:
        df = pd.read_excel('Clusters_Sutel_Fijo2026.xlsx', engine='openpyxl')
        df.columns = [str(c).strip().lower() for c in df.columns]
        if 'id' not in df.columns and 'cluster' in df.columns:
            df = df.rename(columns={'cluster': 'id'})

        # Normalizar texto para que el groupby por cantón no se rompa por espacios/casing
        for col in ['provincia', 'canton', 'name']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        return df
    except Exception as e:
        st.error(f"Error cargando Excel: {e}")
        return pd.DataFrame()


def fetch_cluster_data(cid, ts_start, ts_end, mes_key):
    payload = {
        "tsStart": ts_start,
        "tsEnd": ts_end,
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
        response = requests.post(API_URL, json=payload, headers=HEADERS, timeout=15)
        return cid, response.json() if response.status_code == 200 else None, response.status_code
    except:
        return cid, None, 500


def construir_pendientes(operadores, mes_key, meta, metricas_eval, busqueda):
    """Une los datos sincronizados de todos los operadores y calcula lo que falta para la meta."""
    frames = [
        st.session_state[f"df_{op}_{mes_key}"]
        for op in operadores
        if f"df_{op}_{mes_key}" in st.session_state
    ]
    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    if busqueda:
        df = df[df['name'].str.contains(busqueda, case=False, na=False)]

    for m in METRICAS:
        df[m] = pd.to_numeric(df[m], errors='coerce').fillna(0).astype(int)

    faltan = (meta - df[metricas_eval]).clip(lower=0)
    df["Métricas pendientes"] = (faltan > 0).sum(axis=1)
    df["Muestras faltantes"] = faltan.sum(axis=1)
    df["Avance mín. %"] = (df[metricas_eval].clip(upper=meta) / meta).min(axis=1) * 100
    df["Pendiente en"] = faltan.apply(
        lambda r: ", ".join(c for c in metricas_eval if r[c] > 0), axis=1
    )
    return df


# --- PROCESAMIENTO ---
df_master = load_data()

if not df_master.empty:
    ahora_cr = datetime.now(TZ_CR)
    anios = [2025, 2026]

    st.sidebar.header("🗓️ Periodo")
    year = st.sidebar.selectbox(
        "Año", anios,
        index=anios.index(ahora_cr.year) if ahora_cr.year in anios else len(anios) - 1
    )
    month = st.sidebar.selectbox(
        "Mes", range(1, 13), index=ahora_cr.month - 1,
        format_func=lambda x: calendar.month_name[x].capitalize()
    )

    st.sidebar.header("📊 Visualización")
    tipo_vista = st.sidebar.radio("Nivel de detalle:", ["Por Cantón (Resumen)", "Por Cluster (Detalle)"])
    busqueda = st.sidebar.text_input("🔍 Buscar Cluster (por nombre):", "")

    st.sidebar.header("🎯 Cumplimiento")
    meta = st.sidebar.number_input("Meta de muestras por métrica", min_value=1, value=1000, step=100)
    metricas_eval = st.sidebar.multiselect("Métricas a evaluar", METRICAS, default=METRICAS)
    if not metricas_eval:
        metricas_eval = METRICAS

    ts_start, ts_end = rango_mes_cr(year, month)
    mes_key = f"{str(month).zfill(2)}/{year}"
    operadores = sorted(df_master['operador'].unique())

    st.caption(
        f"Rango consultado (hora Costa Rica): "
        f"{datetime.fromtimestamp(ts_start / 1000, TZ_CR):%Y-%m-%d %H:%M} → "
        f"{datetime.fromtimestamp(ts_end / 1000, TZ_CR):%Y-%m-%d %H:%M:%S}"
    )

    # --- BOTÓN DE SINCRONIZACIÓN ---
    if st.button("🔄 Sincronizar Datos Medux", use_container_width=True):
        status = st.empty()
        progress_bar = st.progress(0)

        for op in operadores:
            state_key = f"df_{op}_{mes_key}"
            df_op_init = df_master[df_master['operador'] == op].copy()
            for m in METRICAS:
                df_op_init[m] = 0
            df_op_init["estado"] = "Pendiente"
            st.session_state[state_key] = df_op_init

        todos_ids = df_master['id'].unique().tolist()
        results_list = []

        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(fetch_cluster_data, cid, ts_start, ts_end, mes_key): cid for cid in todos_ids}
            for i, future in enumerate(as_completed(futures)):
                results_list.append(future.result())
                progress_bar.progress((i + 1) / len(todos_ids))
                status.text(f"Descargando clusters: {i+1}/{len(todos_ids)}")

        status.text("Procesando datos descargados...")
        for cid, res_json, code in results_list:
            row_master = df_master[df_master['id'] == cid]
            if row_master.empty: continue

            op_pertenece = row_master['operador'].values[0]
            state_key = f"df_{op_pertenece}_{mes_key}"
            if state_key not in st.session_state: continue

            df_state = st.session_state[state_key]
            idx_list = df_state[df_state['id'] == cid].index
            if len(idx_list) == 0: continue
            idx = idx_list[0]

            if code == 200 and res_json is not None:
                # --- Validación de estructura ---
                actual_data = {}
                if isinstance(res_json, list):
                    actual_data = res_json[0] if len(res_json) > 0 else {}
                elif isinstance(res_json, dict):
                    actual_data = res_json

                results = actual_data.get("results", {}) if isinstance(actual_data, dict) else {}
                if not isinstance(results, dict):
                    results = {}

                # Como el rango ya está acotado al mes en hora CR, se suman TODAS las llaves de mes
                # que devuelva la API. Si la API agrupa en UTC, las últimas ~6 h del mes CR caen en la
                # llave del mes siguiente; así no se pierden.
                hubo_datos = False
                for _mes, data_mes in results.items():
                    if not isinstance(data_mes, dict):
                        continue
                    data_cluster = data_mes.get(cid, {})
                    if not (data_cluster and isinstance(data_cluster, dict)):
                        continue
                    hubo_datos = True

                    for test_name, targets in data_cluster.items():
                        if not isinstance(targets, dict):
                            continue
                        for target_key, details in targets.items():
                            if not isinstance(details, dict):
                                continue
                            medux_data = details.get("meduxId", {})
                            count = medux_data.get("count", 0) if isinstance(medux_data, dict) else 0

                            col = None
                            if "ping" in test_name:
                                col = "Ping Nacional" if IP_NACIONAL in str(target_key) else "Ping Internacional"
                            elif "down" in test_name:
                                col = "HTTP Download"
                            elif "upload" in test_name:
                                col = "HTTP Upload"

                            if col:
                                try:
                                    valor_previo = int(df_state.at[idx, col])
                                except:
                                    valor_previo = 0
                                df_state.at[idx, col] = valor_previo + int(count)

                df_state.at[idx, "estado"] = "✅ OK" if hubo_datos else "⚠️ Sin datos"
            else:
                df_state.at[idx, "estado"] = f"❌ Error {code}"

        status.text("Sincronización finalizada.")
        st.rerun()

    # --- RENDERIZADO DE TABS ---
    nombres_tabs = [f"OPERADOR: {op}" for op in operadores] + ["📋 PENDIENTES DE MUESTRAS"]
    tabs = st.tabs(nombres_tabs)

    for i, op in enumerate(operadores):
        with tabs[i]:
            state_key = f"df_{op}_{mes_key}"
            if state_key in st.session_state:
                df_actual = st.session_state[state_key]
                df_viz = df_actual[df_actual['name'].str.contains(busqueda, case=False, na=False)].copy() if busqueda else df_actual.copy()

                if tipo_vista == "Por Cantón (Resumen)":
                    for m in METRICAS:
                        df_viz[m] = pd.to_numeric(df_viz[m], errors='coerce').fillna(0)
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
                    df_final[["#"] + columnas_finales].style
                        .map(aplicar_color_semaforo, subset=METRICAS)
                        .map(color_estado, subset=['Estado'])
                        .format("{:,.0f}", subset=METRICAS),
                    use_container_width=True,
                    height=600,
                    hide_index=True
                )

    # --- TAB: PENDIENTES DE MUESTRAS ---
    with tabs[-1]:
        df_eval = construir_pendientes(operadores, mes_key, meta, metricas_eval, busqueda)

        if df_eval is None:
            st.info("Sincroniza los datos del mes para ver los sitios pendientes.")
        else:
            df_pend = df_eval[df_eval["Métricas pendientes"] > 0].copy()

            # KPIs generales
            total = len(df_eval)
            n_pend = len(df_pend)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Clusters evaluados", f"{total:,}")
            c2.metric("Pendientes", f"{n_pend:,}")
            c3.metric("Cumplen", f"{total - n_pend:,}",
                      f"{(total - n_pend) / total * 100:.1f}%" if total else None)
            c4.metric("Muestras faltantes", f"{int(df_pend['Muestras faltantes'].sum()):,}")
            st.caption(f"Meta: {meta:,} muestras en cada métrica evaluada ({', '.join(metricas_eval)}).")

            # Resumen por operador
            st.subheader("Resumen por operador")
            resumen = (
                df_eval.assign(Pendiente=df_eval["Métricas pendientes"] > 0)
                .groupby("operador")
                .agg(
                    Clusters=("id", "count"),
                    Pendientes=("Pendiente", "sum"),
                    **{"Muestras faltantes": ("Muestras faltantes", "sum")},
                )
                .reset_index()
                .rename(columns={"operador": "Operador"})
            )
            resumen["% Cumplimiento"] = (1 - resumen["Pendientes"] / resumen["Clusters"]) * 100
            for m in metricas_eval:
                resumen[f"Pend. {m}"] = (
                    df_eval.assign(_p=df_eval[m] < meta).groupby("operador")["_p"].sum().values
                )
            st.dataframe(
                resumen.style.format(
                    {"Muestras faltantes": "{:,.0f}", "% Cumplimiento": "{:.1f}%"}
                ),
                use_container_width=True,
                hide_index=True
            )

            # Detalle de clusters pendientes
            st.subheader("Detalle de sitios pendientes")
            if df_pend.empty:
                st.success("🎉 Todos los clusters cumplen la meta para este mes.")
            else:
                f1, f2 = st.columns(2)
                op_sel = f1.multiselect("Operador", operadores, default=operadores, key="pend_op")
                prov_opts = sorted(df_pend["provincia"].str.title().unique())
                prov_sel = f2.multiselect("Provincia", prov_opts, default=prov_opts, key="pend_prov")

                df_det = df_pend[
                    df_pend["operador"].isin(op_sel)
                    & df_pend["provincia"].str.title().isin(prov_sel)
                ].copy()
                df_det = df_det.sort_values(["Avance mín. %", "Muestras faltantes"], ascending=[True, False])
                df_det["provincia"] = df_det["provincia"].str.title()
                df_det["canton"] = df_det["canton"].str.title()
                df_det = df_det.rename(columns={"estado": "Estado", "operador": "Operador"})
                df_det.insert(0, "#", range(1, len(df_det) + 1))

                cols_det = (
                    ["#", "Operador", "provincia", "canton", "name", "Estado"]
                    + METRICAS
                    + ["Pendiente en", "Muestras faltantes", "Avance mín. %"]
                )
                st.dataframe(
                    df_det[cols_det].style
                        .map(aplicar_color_semaforo, subset=METRICAS)
                        .map(color_estado, subset=["Estado"])
                        .format("{:,.0f}", subset=METRICAS + ["Muestras faltantes"])
                        .format("{:.1f}%", subset=["Avance mín. %"]),
                    use_container_width=True,
                    height=600,
                    hide_index=True
                )

                st.download_button(
                    "⬇️ Descargar pendientes (CSV)",
                    df_det[cols_det].to_csv(index=False).encode("utf-8-sig"),
                    file_name=f"pendientes_{year}_{str(month).zfill(2)}.csv",
                    mime="text/csv",
                )
else:
    st.warning("No se encontró el archivo Clusters_Sutel_Fijo2026.xlsx")
