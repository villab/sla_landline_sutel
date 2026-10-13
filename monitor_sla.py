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
TZ_NAME = "America/Costa_Rica"
TZ_CR = ZoneInfo(TZ_NAME)

IP_NACIONAL = "138.59.18.180"
METRICAS = ["Ping Nacional", "Ping Internacional", "HTTP Download", "HTTP Upload"]
PROGRAMAS = ["http-upload-burst-test", "http-down-burst-test", "ping-test"]

# Base de la API (p. ej. https://ids.medux.com) derivada de api_url (…/api/results),
# para consultar nombres de sondas en /api/profile/probes/status.
BASE_URL = API_URL.split("/api/")[0] if "/api/" in API_URL else API_URL.rstrip("/")

# Una sonda está ON si reportó al menos un test dentro de esta ventana antes de la consulta.
VENTANA_ON_MS = 60 * 60 * 1000  # 1 hora
CLUSTERS_POR_LOTE = 50

# Masterfile de fijo: se lee directo de SharePoint (la versión vigente que guarda la app
# "Masterfile Entorno de Medición"). Columnas usadas: "Sonda" (serial/MAC) y "Panelista".
# Usa las mismas credenciales de Azure AD (app registration) que la app del masterfile.
SP_TENANT_ID = os.getenv("tenant_id")
SP_CLIENT_ID = os.getenv("client_id")
SP_CLIENT_SECRET = os.getenv("client_secret")
SP_SITE_NAME = os.getenv("sp_site_name", "Sutel")
SP_MASTERFILE = os.getenv(
    "sp_masterfile_path",
    "01. Documentos MedUX/Automatizacion/Masterfile/MasterfileSutel.xlsx"
)
# Respaldo opcional: archivo local (solo se usa si no hay credenciales de SharePoint o falla la descarga).
MASTERFILE_PATH = os.getenv("masterfile_path", "MasterfileSutel.xlsx")
MASTERFILE_TTL = 15 * 60  # segundos que se reutiliza la copia descargada


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


def normalizar_serial(s):
    """001E:06:3A:7F:03 / 001e-063a-7f03 / 001e063a7f03 → 001e063a7f03"""
    return "".join(ch for ch in str(s).lower() if ch.isalnum())


def _graph_token():
    """Token de aplicación (client credentials) para Microsoft Graph, sin depender de msal."""
    r = requests.post(
        f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": SP_CLIENT_ID,
            "client_secret": SP_CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
        },
        timeout=20,
    )
    data = r.json()
    if "access_token" not in data:
        raise Exception(f"Token Graph: {data.get('error')} {data.get('error_description', '')[:200]}")
    return data["access_token"]


def descargar_masterfile_sharepoint():
    """Descarga los bytes del masterfile vigente desde SharePoint (misma lógica de sitio que la app del masterfile)."""
    token = _graph_token()
    h = {"Authorization": f"Bearer {token}"}
    sites = requests.get(f"https://graph.microsoft.com/v1.0/sites?search={SP_SITE_NAME}",
                         headers=h, timeout=20).json().get("value", [])
    # Coincidencia EXACTA: "Sutel" es substring de otros sitios (ProyectoSUTEL…)
    site = next((s for s in sites if s.get("name", "").strip().lower() == SP_SITE_NAME.lower()), None)
    if site is None:
        raise Exception(f"Sitio '{SP_SITE_NAME}' no encontrado. Candidatos: {[s.get('name') for s in sites]}")
    drives = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site['id']}/drives",
                          headers=h, timeout=20).json().get("value", [])
    if not drives:
        raise Exception("El sitio no tiene bibliotecas de documentos.")
    drive = next((d for d in drives if d.get("name", "").lower() in ("documents", "documentos")), drives[0])
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site['id']}/drives/{drive['id']}/root:/{SP_MASTERFILE}:/content",
        headers=h, timeout=60,
    )
    if r.status_code != 200:
        raise Exception(f"Descarga masterfile HTTP {r.status_code}")
    return r.content


def _panelistas_desde_excel(fuente):
    df = pd.read_excel(fuente, engine="openpyxl", dtype=str)
    cols = {str(c).strip().lower(): c for c in df.columns}
    col_sonda = cols.get("sonda") or cols.get("id sonda")
    col_pan = cols.get("panelista")
    if col_sonda is None or col_pan is None:
        raise Exception(f"El masterfile no tiene columnas 'Sonda' y 'Panelista' (tiene: {list(df.columns)[:15]})")
    df = df[[col_sonda, col_pan]].dropna(subset=[col_sonda])
    return {
        normalizar_serial(s): str(p).strip()
        for s, p in zip(df[col_sonda], df[col_pan])
        if str(p).strip() and str(p).strip().lower() != "nan"
    }


@st.cache_data(ttl=MASTERFILE_TTL, show_spinner="Leyendo masterfile de panelistas...")
def load_panelistas():
    """Devuelve (panelistas, origen, hora_carga, error).
    1º SharePoint (versión vigente); si no hay credenciales o falla, archivo local."""
    from io import BytesIO
    hora = datetime.now(TZ_CR)
    error_sp = None
    if SP_TENANT_ID and SP_CLIENT_ID and SP_CLIENT_SECRET:
        try:
            datos = descargar_masterfile_sharepoint()
            return _panelistas_desde_excel(BytesIO(datos)), "SharePoint", hora, None
        except Exception as e:
            error_sp = str(e)
    else:
        error_sp = "Sin credenciales de SharePoint (tenant_id / client_id / client_secret)"
    try:
        return _panelistas_desde_excel(MASTERFILE_PATH), f"archivo local {MASTERFILE_PATH}", hora, error_sp
    except Exception as e:
        return {}, None, hora, f"{error_sp} | Local: {e}"


def fetch_cluster_data(cid, ts_start, ts_end, mes_key):
    payload = {
        "tsStart": ts_start,
        "tsEnd": ts_end,
        "format": "aggregate",
        "timezone": TZ_NAME,
        "programs": PROGRAMAS,
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
    return df


# --- SONDAS POR CLUSTER (ON / OFF) ---
def _sondas_por_cluster(res_json):
    """Parsea un agregado con breakdownBy [cluster, probeId] → {cluster: {probeId: muestras}}.
    Suma todas las llaves de periodo que devuelva la API."""
    out = {}
    data = res_json[0] if isinstance(res_json, list) and res_json else res_json
    results = data.get("results", {}) if isinstance(data, dict) else {}
    if not isinstance(results, dict):
        return out
    for _periodo, por_cluster in results.items():
        if not isinstance(por_cluster, dict):
            continue
        for cid, por_sonda in por_cluster.items():
            if not isinstance(por_sonda, dict):
                continue
            for pid, det in por_sonda.items():
                cnt = 1
                if isinstance(det, dict):
                    medux = det.get("meduxId", {})
                    cnt = medux.get("count", det.get("samples", 1)) if isinstance(medux, dict) else det.get("samples", 1)
                d = out.setdefault(str(cid), {})
                d[str(pid)] = d.get(str(pid), 0) + int(cnt or 0)
    return out


def fetch_sondas_lote(cids, ts_start, ts_end):
    """Una sola petición para un lote de clusters: qué sondas reportaron tests en el rango."""
    payload = {
        "tsStart": int(ts_start),
        "tsEnd": int(ts_end),
        "format": "aggregate",
        "timezone": TZ_NAME,
        "programs": PROGRAMAS,
        "clusters": list(cids),
        "aggregate": {
            "groupBy": {"field": "dateStart", "operation": "month"},
            "values": [{"field": "meduxId", "operation": "count"}],
            "breakdownBy": ["cluster", "probeId"]
        }
    }
    try:
        r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return {}, f"HTTP {r.status_code}"
        return _sondas_por_cluster(r.json()), None
    except Exception as e:
        return {}, type(e).__name__


def fetch_nombres_sondas(probe_ids):
    """Nombres descriptivos desde Probe Status. Si falla, se muestran solo los IDs."""
    nombres = {}
    ids = sorted(probe_ids)
    for i in range(0, len(ids), 100):
        try:
            r = requests.post(
                f"{BASE_URL}/api/profile/probes/status",
                json={"probeIds": ids[i:i + 100]}, headers=HEADERS, timeout=30
            )
            if r.status_code != 200:
                continue
            data = r.json()
            filas = data if isinstance(data, list) else data.get("results", data.get("probes", [])) if isinstance(data, dict) else []
            for p in filas:
                if isinstance(p, dict) and p.get("_id"):
                    nombres[str(p["_id"])] = p.get("name") or ""
        except Exception:
            continue
    return nombres


def consultar_sondas(cids, ts_start_mes, ts_end_mes):
    """Sondas que reportan a cada cluster en el mes + estado ON/OFF según tests en la última hora.
    Guarda el resultado en session_state con la hora de la consulta."""
    ahora = datetime.now(TZ_CR)
    ts_ahora = int(ahora.timestamp() * 1000)
    ts_hace_1h = ts_ahora - VENTANA_ON_MS

    lotes = [cids[i:i + CLUSTERS_POR_LOTE] for i in range(0, len(cids), CLUSTERS_POR_LOTE)]
    mes_fin = min(ts_end_mes, ts_ahora)
    tareas = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for lote in lotes:
            if ts_start_mes < mes_fin:
                tareas.append(("mes", ex.submit(fetch_sondas_lote, lote, ts_start_mes, mes_fin)))
            tareas.append(("hora", ex.submit(fetch_sondas_lote, lote, ts_hace_1h, ts_ahora)))
        en_mes, en_hora, errores = {}, {}, []
        for tipo, fut in tareas:
            data, err = fut.result()
            if err:
                errores.append(err)
            destino = en_mes if tipo == "mes" else en_hora
            for cid, sondas in data.items():
                destino.setdefault(cid, {}).update(sondas)

    por_cluster = {}
    for cid in set(en_mes) | set(en_hora):
        todas = set(en_mes.get(cid, {})) | set(en_hora.get(cid, {}))
        por_cluster[cid] = {pid: pid in en_hora.get(cid, {}) for pid in todas}

    todas_sondas = {pid for s in por_cluster.values() for pid in s}
    nombres = fetch_nombres_sondas(todas_sondas) if todas_sondas else {}

    return {
        "consulta": ahora,
        "por_cluster": por_cluster,
        "nombres": nombres,
        "errores": errores,
    }


def formatear_sondas(cid, info, panelistas):
    """Textos de las columnas 'Sondas' y 'Panelista' (mismo orden) y conteo ON/total."""
    if info is None:
        return "Sin consultar", "", ""
    sondas = info["por_cluster"].get(str(cid), {})
    if not sondas:
        return "Sin sondas reportando", "", "0/0"
    nombres = info["nombres"]
    orden = sorted(sondas.items(), key=lambda kv: (not kv[1], kv[0]))  # ON primero
    partes, pans = [], []
    for pid, on in orden:
        etiqueta = f"{nombres[pid]} ({pid})" if nombres.get(pid) else pid
        partes.append(f"{'🟢 ON' if on else '🔴 OFF'} {etiqueta}")
        pans.append(panelistas.get(normalizar_serial(pid), "Sin registro en masterfile"))
    n_on = sum(sondas.values())
    return " | ".join(partes), " | ".join(pans), f"{n_on}/{len(sondas)}"


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

        status.text("Consultando sondas por cluster (ON/OFF)...")
        st.session_state[f"sondas_{mes_key}"] = consultar_sondas(
            [str(c) for c in todos_ids], ts_start, ts_end
        )

        status.text("Sincronización finalizada.")
        st.rerun()

    # --- RENDERIZADO DE TABS (Resumen primero → es la que abre por defecto) ---
    nombres_tabs = ["📋 RESUMEN DE PENDIENTES"] + [f"OPERADOR: {op}" for op in operadores]
    tabs = st.tabs(nombres_tabs)
    tab_resumen, tabs_ops = tabs[0], tabs[1:]

    for i, op in enumerate(operadores):
        with tabs_ops[i]:
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

    # --- TAB: RESUMEN DE PENDIENTES ---
    with tab_resumen:
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

            sondas_key = f"sondas_{mes_key}"
            s1, s2 = st.columns([3, 1])
            if s2.button("🛰️ Actualizar estado de sondas", use_container_width=True):
                with st.spinner("Consultando sondas..."):
                    st.session_state[sondas_key] = consultar_sondas(
                        [str(c) for c in df_master['id'].unique()], ts_start, ts_end
                    )
            info_sondas = st.session_state.get(sondas_key)
            if info_sondas:
                s1.caption(
                    f"Estado de sondas consultado: {info_sondas['consulta']:%Y-%m-%d %H:%M} (hora CR). "
                    f"🟢 ON = reportó al menos un test en la hora previa a la consulta; 🔴 OFF = sin tests en esa hora."
                )
                if info_sondas["errores"]:
                    s1.warning(f"{len(info_sondas['errores'])} lote(s) de la consulta de sondas fallaron "
                               f"({', '.join(sorted(set(info_sondas['errores'])))}). Puede faltar información.")
            else:
                s1.caption("Estado de sondas sin consultar. Usa el botón para obtenerlo.")

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

                m1, m2 = st.columns([3, 1])
                if m2.button("📋 Recargar masterfile", use_container_width=True):
                    load_panelistas.clear()
                panelistas, origen_mf, hora_mf, error_mf = load_panelistas()
                if panelistas:
                    m1.caption(f"Panelistas: {origen_mf} · {len(panelistas)} sondas · leído {hora_mf:%H:%M} (hora CR)")
                    if error_mf:
                        m1.warning(f"No se pudo leer de SharePoint, se usó el respaldo local: {error_mf}")
                else:
                    m1.warning(f"No se pudo leer el masterfile de panelistas: {error_mf}")
                sondas_fmt = df_det["id"].apply(lambda c: formatear_sondas(c, info_sondas, panelistas))
                df_det["Sondas"] = sondas_fmt.str[0]
                df_det["Panelista"] = sondas_fmt.str[1]
                df_det["ON / Total"] = sondas_fmt.str[2]

                def color_on_total(x):
                    try:
                        on, tot = (int(v) for v in str(x).split("/"))
                    except Exception:
                        return ""
                    if tot == 0 or on == 0:
                        return "color: #d63031; font-weight: bold"
                    if on < tot:
                        return "color: #e67e22; font-weight: bold"
                    return "color: #27ae60; font-weight: bold"

                cols_det = (
                    ["#", "Operador", "provincia", "canton", "name", "Panelista", "Estado"]
                    + METRICAS
                    + ["ON / Total", "Sondas", "Muestras faltantes", "Avance mín. %"]
                )
                st.dataframe(
                    df_det[cols_det].style
                        .map(aplicar_color_semaforo, subset=METRICAS)
                        .map(color_estado, subset=["Estado"])
                        .map(color_on_total, subset=["ON / Total"])
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
