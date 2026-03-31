import datetime
import re
import io
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor

import streamlit as st
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================================================
# CONFIGURAÇÕES
# =========================================================

SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive.file",
]

IDS = {
    # ── Estado-Maior / Comando ──────────────────────────────
    "s3":        "s3.24bis03@gmail.com",
    "cmt":       "comando24bis@gmail.com",
    "cmdo":      "cmdo24bis@gmail.com",
    "sub_cmt":   "subcomandante.24bis@gmail.com",
    "adj_cmdo":  "gleysonsmelo141214@gmail.com",
    "sec_1":     "primeira1secao@gmail.com",
    "sec_4":     "4secao24bis@gmail.com",
    # ── Subunidades ────────────────────────────────────────
    "cia_1":     "1cia.24bis@gmail.com",
    "cia_1b":    "gurupi1cia@gmail.com",
    "cia_1_sgt": "sargenteacaogurupi@gmail.com",
    "cia_2":     "jfsa2017@gmail.com",
    "cia_2b":    "timbira2cia@gmail.com",
    "cia_2_sgt": "sgtetimbira@gmail.com",
    "b_mus":     "bmus24bis@gmail.com",
    "npor":      "npor.24bis.instrutor@gmail.com",
    "npor_ste":  "allissonfeitosa1985@gmail.com",
    # ── Órgãos de apoio ────────────────────────────────────
    "ass_jur":   "assjur.24bis@gmail.com",
    "brigada":   "brigada24bis@gmail.com",
    "fisc_adm":  "capmarcusvinicius.40bi@gmail.com",
    "chales":    "cmslchales@gmail.com",
    "com_soc":   "comsoc24bis@gmail.com",
    "fiscal":    "fiscal160105@gmail.com",
    "prm":       "prmsaoluisma@gmail.com",
    "sfpc":      "sfpc24bis@gmail.com",
    # ── Agendas de grupo (Google Calendar IDs) ─────────────
    "pgi":       "915a351ec7e277234d1da0e597fb14c7455f6f1a5a05eea8de837095a6e70c9e@group.calendar.google.com",
    "cursos":    "38d1be36abd6b1e2545500964d51074f66d24c36530a3ff677ef21b6b332f003@group.calendar.google.com",
    "datas":     "c9905256a40d19cc4d9954f633783c1ee96f6ad70165b5b7800b63e31ceeef1f@group.calendar.google.com",
    "si":        "d140cd6bbf50cb6e5754222732d27f20e9ee833aca680475c0f1f34e0df74fa0@group.calendar.google.com",
    "fase":      "ac05541df4fd8c2dff7eeebe910442a84fd43a9ade0a8699b1d96cf6e2986d1e@group.calendar.google.com",
    "operacoes": "a253be647f9dd8c1b044f0e89643a569d95cbd9054f4eb8401c373a4cb2dd667@group.calendar.google.com",
}

# Mapa email → rótulo da coluna AG (usado em construir_tabela_semana)
RESP_MAP = {
    IDS["cmt"]:      "Cmt",
    IDS["cmdo"]:     "Cmdo",
    IDS["sub_cmt"]:  "Sub Cmt",
    IDS["adj_cmdo"]: "Adj Cmdo",
    IDS["sec_1"]:    "1ª Seção",
    IDS["sec_4"]:    "4ª Seção",
    IDS["cia_1"]:    "1ª Cia",
    IDS["cia_1b"]:   "1ª Cia",
    IDS["cia_1_sgt"]:"1ª Cia Sgt",
    IDS["cia_2"]:    "2ª Cia",
    IDS["cia_2b"]:   "2ª Cia",
    IDS["cia_2_sgt"]:"2ª Cia Sgt",
    IDS["b_mus"]:    "B Mus",
    IDS["npor"]:     "NPOR",
    IDS["npor_ste"]: "NPOR (STE)",
    IDS["ass_jur"]:  "Ass Jur",
    IDS["brigada"]:  "Brigada",
    IDS["fisc_adm"]: "Fisc Adm",
    IDS["chales"]:   "Chales",
    IDS["com_soc"]:  "Com Soc",
    IDS["fiscal"]:   "Fiscal",
    IDS["prm"]:      "PRM",
    IDS["sfpc"]:     "SFPC",
    IDS["pgi"]:      "PGI",
    IDS["s3"]:       "S3",
}

MEU_EMAIL = IDS["s3"]

# =========================================================
# RETRY COM BACKOFF EXPONENCIAL — resolve HTTP 429
# =========================================================

def batch_update_com_retry(docs_service, doc_id, requests_list, max_tentativas=6, tamanho_lote=50):
    if not requests_list:
        return

    DELAY_ENTRE_LOTES = 1.2

    for i in range(0, len(requests_list), tamanho_lote):
        lote = requests_list[i:i + tamanho_lote]
        for tentativa in range(max_tentativas):
            try:
                docs_service.documents().batchUpdate(
                    documentId=doc_id, body={"requests": lote}
                ).execute()
                time.sleep(DELAY_ENTRE_LOTES)
                break
            except HttpError as e:
                if e.resp.status == 429:
                    espera = (2 ** tentativa) + random.uniform(0, 1)
                    print(f"[429] Rate limit — aguardando {espera:.1f}s (tentativa {tentativa + 1}/{max_tentativas})")
                    time.sleep(espera)
                    if tentativa == max_tentativas - 1:
                        raise
                else:
                    raise

# =========================================================
# FUNÇÕES DE TRATAMENTO
# =========================================================

def limpar_texto(val) -> str:
    if val is None:
        return ""
    s = str(val)
    s = re.sub(r"[\U00010000-\U0010FFFF]", "", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("\n", " ").replace("\t", " ")
    s = re.sub(r"[^0-9A-Za-zÁÉÍÓÚÀÂÊÔÃÕÇáéíóúàâêôãõçºª \-–—.,;:()\/@]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def formatar_dia_semana(dt_date: datetime.date):
    dias = ["SEG", "TER", "QUA", "QUI", "SEX", "SÁB", "DOM"]
    return dias[dt_date.weekday()]

def formatar_mes_abreviado(dt_date: datetime.date):
    meses = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"]
    return meses[dt_date.month - 1]

def fmt_data_coluna(dt_date: datetime.date) -> str:
    return f"{dt_date.day:02d} {formatar_mes_abreviado(dt_date)} ({formatar_dia_semana(dt_date)})"

def monday_of(d: datetime.date) -> datetime.date:
    return d - datetime.timedelta(days=d.weekday())

def week_range(ref_date: datetime.date):
    ini = monday_of(ref_date)
    fim = ini + datetime.timedelta(days=6)
    return ini, fim

def fmt_periodo_titulo(ini: datetime.date, fim: datetime.date) -> str:
    ini_txt = f"{ini.day:02d} {formatar_mes_abreviado(ini)} {str(ini.year)[-2:]}"
    fim_txt = f"{fim.day:02d} {formatar_mes_abreviado(fim)} {str(fim.year)[-2:]}"
    return f"{ini_txt} a {fim_txt}"

def to_dt_utc_start(d: datetime.date) -> datetime.datetime:
    return datetime.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=datetime.timezone.utc)

def to_dt_utc_end_exclusive(d: datetime.date) -> datetime.datetime:
    next_day = d + datetime.timedelta(days=1)
    return datetime.datetime(next_day.year, next_day.month, next_day.day, 0, 0, 0, tzinfo=datetime.timezone.utc)

def eh_fim_de_semana(dt_date: datetime.date) -> bool:
    return dt_date.weekday() in [5, 6]

def validar_datas(ini: datetime.date, fim: datetime.date) -> bool:
    if fim < ini:
        st.error("❌ Data final não pode ser anterior à data inicial")
        return False
    if (fim - ini).days > 14:
        st.warning("⚠️ Período maior que 2 semanas pode gerar documento muito grande")
    return True

def registrar_log(acao: str, detalhes: str = ""):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {acao} - {detalhes}")

# =========================================================
# AUTH
# =========================================================

def _first_param_value(x):
    if isinstance(x, list):
        return x[0] if x else ""
    return x

def get_credentials():
    creds = None

    if "token_data" in st.session_state:
        try:
            creds = Credentials.from_authorized_user_info(
                st.session_state.token_data, SCOPES
            )
        except Exception:
            creds = None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            st.session_state.token_data = json.loads(creds.to_json())
            return creds
        except Exception:
            creds = None

    if creds and creds.valid:
        return creds

    try:
        client_config = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
        redirect_uri = st.secrets["REDIRECT_URI"]
    except Exception:
        st.error("❌ Secrets do Google não configurados.")
        st.stop()

    params = st.query_params
    if "code" in params:
        try:
            code = _first_param_value(params["code"])
            state_param = _first_param_value(params.get("state", ""))

            if "||" in state_param:
                original_state, verifier = state_param.split("||", 1)
            else:
                st.error("❌ Parâmetro state inválido. Tente logar novamente.")
                st.query_params.clear()
                st.stop()

            flow = Flow.from_client_config(
                client_config, scopes=SCOPES,
                redirect_uri=redirect_uri, state=original_state,
            )
            flow.code_verifier = verifier
            flow.fetch_token(code=code)

            creds = flow.credentials
            st.session_state.token_data = json.loads(creds.to_json())
            st.query_params.clear()
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro ao processar login: {e}")
            st.query_params.clear()
            st.stop()

    flow = Flow.from_client_config(
        client_config, scopes=SCOPES, redirect_uri=redirect_uri,
    )
    auth_url, state = flow.authorization_url(
        prompt="consent", access_type="offline", include_granted_scopes="true",
    )
    verifier = flow.code_verifier
    combined_state = f"{state}||{verifier}"
    auth_url = auth_url.replace(f"state={state}", f"state={combined_state}")

    st.markdown("## 🔐 Autenticação necessária")
    st.markdown(f"### [🔑 Clique aqui para Entrar com Google]({auth_url})")
    st.info("💡 Se o link não abrir, copie e cole no navegador:")
    st.code(auth_url)
    st.stop()

# =========================================================
# CALENDAR – LISTAR EVENTOS
# =========================================================

def list_events(service, calendar_id: str, d_ini: datetime.date, d_fim: datetime.date):
    time_min = to_dt_utc_start(d_ini).isoformat()
    time_max = to_dt_utc_end_exclusive(d_fim).isoformat()

    nome_cal = next((k for k, v in IDS.items() if v == calendar_id), calendar_id[:20])

    items = []
    page_token = None
    try:
        while True:
            res = service.events().list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
                maxResults=250,
                pageToken=page_token
            ).execute()

            batch = res.get("items", [])
            for e in batch:
                e["_src_calendar_id"] = calendar_id
            items.extend(batch)
            page_token = res.get("nextPageToken")
            if not page_token:
                break
    except Exception as e:
        erro = str(e)
        if "404" in erro or "notFound" in erro:
            print(f"Agenda {nome_cal} não encontrada: {erro[:80]}")
        elif "403" in erro or "forbidden" in erro.lower():
            print(f"Sem permissão: {nome_cal}")
        else:
            print(f"Erro agenda {nome_cal}: {erro[:80]}")
        return []

    return items

def carregar_todos_eventos_paralelo(srv, d_ini, d_fim):
    calendarios = [
        "s3", "cmt", "cmdo", "sub_cmt", "adj_cmdo",
        "sec_1", "sec_4",
        "cia_1", "cia_1b", "cia_1_sgt",
        "cia_2", "cia_2b", "cia_2_sgt",
        "b_mus", "npor", "npor_ste",
        "ass_jur", "brigada", "fisc_adm", "chales",
        "com_soc", "fiscal", "prm", "sfpc",
        "pgi", "cursos", "datas", "si", "fase", "operacoes"
    ]
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(list_events, srv, IDS[cal], d_ini, d_fim): cal
            for cal in calendarios
        }
        resultados = {}
        for future in futures:
            cal = futures[future]
            try:
                resultados[cal] = future.result()
            except Exception:
                resultados[cal] = []
    return resultados

def dedup_by_event_id(events):
    vistos = set()
    out = []
    for e in events:
        eid = e.get("id")
        if not eid or eid in vistos:
            continue
        vistos.add(eid)
        out.append(e)
    return out

def parse_start_end(ev):
    start = ev.get("start", {})
    end   = ev.get("end",   {})

    if "date" in start:
        s = datetime.date.fromisoformat(start["date"])
        e = datetime.date.fromisoformat(end["date"])
        return s, e, True, "D"

    sdt = start.get("dateTime")
    edt = end.get("dateTime")
    if sdt and edt:
        s_date = datetime.date.fromisoformat(sdt[:10])
        e_date = datetime.date.fromisoformat(edt[:10])
        hora   = sdt[11:16] if "T" in sdt else ""
        return s_date, e_date, False, hora

    return None, None, False, ""

def event_intersects_day(ev, day: datetime.date) -> bool:
    s_date, e_date, is_all_day, _ = parse_start_end(ev)
    if s_date is None or e_date is None:
        return False
    if is_all_day:
        return (s_date <= day) and (day < e_date)

    start = ev.get("start", {})
    end   = ev.get("end",   {})
    edt   = end.get("dateTime", "")
    if edt and edt[11:16] == "00:00" and e_date > s_date:
        e_date = e_date - datetime.timedelta(days=1)

    return (s_date <= day) and (day <= e_date)

# =========================================================
# SI / FASE
# =========================================================

def extrair_si_texto(texto: str):
    if not texto:
        return None
    texto_limpo = re.sub(r'[^\w\s\-]', ' ', texto)

    if re.search(r'\bSN\b', texto_limpo, flags=re.IGNORECASE):
        return "SN"

    m = re.search(r'\bSI\s*-\s*(\d{1,2})', texto_limpo, flags=re.IGNORECASE)
    if m:
        return f"-{m.group(1)}"

    m = re.search(r'\bSI\s+(\d{1,2})', texto_limpo, flags=re.IGNORECASE)
    if m:
        num = int(m.group(1))
        return f"{num:02d}" if num > 0 else "SN"

    m = re.search(r'\bS(\d{1,2})\s*/\s*EB\b', texto_limpo, flags=re.IGNORECASE)
    if m:
        num = int(m.group(1))
        return f"{num:02d}" if num > 0 else "SN"

    m = re.search(r'\bSEMANA\s+DE\s+INSTRU[cç][aã]O\s*(\d{1,2})\b', texto_limpo, flags=re.IGNORECASE)
    if m:
        num = int(m.group(1))
        return f"{num:02d}" if num > 0 else "SN"

    return None

def extrair_fase_texto(texto: str):
    if not texto:
        return None
    texto_limpo = re.sub(r'[^\w\s]', '', texto).upper()
    fases = ["IIB", "IIQ", "ADST", "IIA", "IIC", "ADM", "MDD ADM"]
    for fase in fases:
        if re.search(rf'\b{re.escape(fase)}\b', texto_limpo):
            return fase
        if fase in texto_limpo:
            return fase
    return None

def buscar_si_duplo(service, d_ini_s, d_fim_s, d_ini_s1, d_fim_s1):
    d_antes_s  = d_ini_s  - datetime.timedelta(days=3)
    d_depois_s = d_fim_s  + datetime.timedelta(days=3)
    evs_s = list_events(service, IDS["si"], d_antes_s, d_depois_s)

    si_s = None
    melhor_overlap_s = 0

    for ev in evs_s:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if s_date and e_date:
            e_date_inc = e_date - datetime.timedelta(days=1) if is_all_day else e_date
            overlap_start = max(s_date, d_ini_s)
            overlap_end   = min(e_date_inc, d_fim_s)
            if overlap_start <= overlap_end:
                dias_overlap  = (overlap_end - overlap_start).days + 1
                texto_completo = f"{ev.get('summary','')} {ev.get('description','')} {ev.get('location','')}"
                si = extrair_si_texto(texto_completo)
                if si and dias_overlap > melhor_overlap_s:
                    melhor_overlap_s = dias_overlap
                    si_s = si

    d_antes_s1  = d_ini_s1 - datetime.timedelta(days=3)
    d_depois_s1 = d_fim_s1 + datetime.timedelta(days=3)
    evs_s1 = list_events(service, IDS["si"], d_antes_s1, d_depois_s1)

    si_s1 = None
    melhor_overlap_s1 = 0

    for ev in evs_s1:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if s_date and e_date:
            e_date_inc = e_date - datetime.timedelta(days=1) if is_all_day else e_date
            overlap_start = max(s_date, d_ini_s1)
            overlap_end   = min(e_date_inc, d_fim_s1)
            if overlap_start <= overlap_end:
                dias_overlap   = (overlap_end - overlap_start).days + 1
                texto_completo = f"{ev.get('summary','')} {ev.get('description','')} {ev.get('location','')}"
                si = extrair_si_texto(texto_completo)
                if si and dias_overlap > melhor_overlap_s1:
                    melhor_overlap_s1 = dias_overlap
                    si_s1 = si

    if si_s and si_s1:   return f"{si_s}/{si_s1}"
    elif si_s:           return f"{si_s}/-1"
    elif si_s1:          return f"-2/{si_s1}"
    else:                return "-2/-1"

def buscar_fase(service, d_ini_s, d_fim_s1):
    d_antes  = d_ini_s  - datetime.timedelta(days=3)
    d_depois = d_fim_s1 + datetime.timedelta(days=3)
    evs = list_events(service, IDS["fase"], d_antes, d_depois)

    melhor_fase    = None
    melhor_overlap = 0

    for ev in evs:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if s_date and e_date:
            e_date_inc    = e_date - datetime.timedelta(days=1) if is_all_day else e_date
            overlap_start = max(s_date, d_ini_s)
            overlap_end   = min(e_date_inc, d_fim_s1)
            if overlap_start <= overlap_end:
                dias_overlap   = (overlap_end - overlap_start).days + 1
                texto_completo = f"{ev.get('summary','')} {ev.get('description','')} {ev.get('location','')}"
                fase = extrair_fase_texto(texto_completo)
                if fase and dias_overlap > melhor_overlap:
                    melhor_overlap = dias_overlap
                    melhor_fase    = fase

    return melhor_fase

# =========================================================
# OPERAÇÕES
# =========================================================

def buscar_operacoes(service, d_ini_s, d_fim_s1):
    d_busca_ini = d_ini_s  - datetime.timedelta(days=365)
    d_busca_fim = d_fim_s1 + datetime.timedelta(days=30)

    evs = list_events(service, IDS["operacoes"], d_busca_ini, d_busca_fim)
    operacoes_ativas = []

    for ev in evs:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if not s_date:
            continue
        if is_all_day and e_date:
            e_date = e_date - datetime.timedelta(days=1)

        if (s_date <= d_fim_s1) and (e_date >= d_ini_s):
            summary = limpar_texto(ev.get("summary", "")).strip()
            if summary:
                tipo_match    = re.search(r'\(([^)]+)\)', summary)
                tipo          = tipo_match.group(1).strip().upper() if tipo_match else ""
                nome_operacao = re.sub(r'\s*\([^)]+\)', '', summary).strip() if tipo_match else summary
                operacoes_ativas.append({'nome': nome_operacao, 'tipo': tipo, 'data_inicio': s_date})

    operacoes_unicas = {}
    for op in operacoes_ativas:
        chave = f"{op['nome']}_{op['tipo']}"
        if chave not in operacoes_unicas:
            operacoes_unicas[chave] = op

    operacoes_ordenadas = sorted(operacoes_unicas.values(), key=lambda x: x['data_inicio'])

    linhas_formatadas = []
    for op in operacoes_ordenadas:
        # ✅ CORREÇÃO 1: removidos numeração ({i}) e traços (__ Militares / ___)
        if op['tipo']:
            linha = f" {op['nome']} ({op['tipo']})"
        else:
            linha = f" {op['nome']}"
        linhas_formatadas.append(linha)

    return linhas_formatadas

# =========================================================
# BULLETS CURSOS/ESTÁGIOS — com Smn, Local e Militares
# =========================================================

def bullets_periodo(service, calendar_id: str, d_ini: datetime.date, d_fim: datetime.date, incluir_responsavel: bool = False):
    d_busca_ini = d_ini - datetime.timedelta(days=365)
    d_busca_fim = d_fim + datetime.timedelta(days=30)

    evs  = list_events(service, calendar_id, d_busca_ini, d_busca_fim)
    hoje = datetime.date.today()
    linhas = []

    for ev in evs:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if not s_date:
            continue
        if is_all_day and e_date:
            e_date = e_date - datetime.timedelta(days=1)

        if not ((s_date <= d_fim) and (e_date >= d_ini)):
            continue

        s = limpar_texto(ev.get("summary", "")).strip()
        if not s:
            continue

        ano_inicio  = str(s_date.year)[-2:]
        data_fmt    = f"{s_date.day:02d} {formatar_mes_abreviado(s_date)} {ano_inicio}"
        if e_date and e_date != s_date:
            ano_fim      = str(e_date.year)[-2:]
            data_fim_fmt = f"{e_date.day:02d} {formatar_mes_abreviado(e_date)} {ano_fim}"
            periodo_fmt  = f"{data_fmt} a {data_fim_fmt}"
        else:
            periodo_fmt = data_fmt

        total_dias = (e_date - s_date).days + 1
        total_sem  = round(total_dias / 7)

        if hoje < s_date:
            smn_txt = f"{total_sem} sem"
        elif hoje > e_date:
            smn_txt = f"{total_sem} sem (concluído)"
        else:
            dias_decorridos = (hoje - s_date).days
            sem_atual       = min((dias_decorridos // 7) + 1, total_sem)
            smn_txt         = f"Smn {sem_atual}/{total_sem}"

        local     = limpar_texto(ev.get("location",    "")).strip()
        militares = limpar_texto(ev.get("description", "")).strip()

        ja_tem_smn = bool(re.search(r'Smn\s+\d+/\d+', s, re.IGNORECASE))
        if ja_tem_smn:
            texto = f"{periodo_fmt} - {s}"
        else:
            texto = f"{periodo_fmt} - {s} | {smn_txt}"

        if local:
            texto += f" - {local}"
        if militares:
            lista_mil = [m.strip() for m in re.split(r'[,;\n]', militares) if m.strip()]
            if lista_mil:
                texto += " - " + ", ".join(lista_mil)

        # ✅ CORREÇÃO 2: removido bloco "if incluir_responsavel: texto += ' - ___'"

        linhas.append((s_date, texto))

    linhas.sort(key=lambda x: x[0])

    seen = set()
    out  = []
    for _, texto in linhas:
        if texto not in seen:
            out.append(texto)
            seen.add(texto)
    return out

# =========================================================
# FERIADOS
# =========================================================

def buscar_feriados(service, d_ini: datetime.date, d_fim: datetime.date):
    evs      = list_events(service, IDS["datas"], d_ini, d_fim)
    feriados = set()
    for ev in evs:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if s_date:
            cur = s_date
            while cur < e_date if is_all_day else cur <= e_date:
                feriados.add(cur)
                cur += datetime.timedelta(days=1)
    return feriados

# =========================================================
# ATIVIDADES FUTURAS — automático, 45 dias após fim S+1
# =========================================================

def buscar_atividades_futuras(service, fim_s1: datetime.date) -> list:
    d_ini_fut = fim_s1 + datetime.timedelta(days=1)
    d_fim_fut = fim_s1 + datetime.timedelta(days=45)

    agendas_futuras = {
        "pgi":       IDS["pgi"],
        "s3":        IDS["s3"],
        "cmt":       IDS["cmt"],
        "adj_cmdo":  IDS["adj_cmdo"],
        "b_mus":     IDS["b_mus"],
        "cia_2":     IDS["cia_2"],
        "npor":      IDS["npor"],
        "datas":     IDS["datas"],
        "operacoes": IDS["operacoes"],
    }

    todos_eventos = []

    for nome_cal, cal_id in agendas_futuras.items():
        try:
            if nome_cal == "operacoes":
                evs = list_events(service, cal_id,
                                  d_ini_fut - datetime.timedelta(days=365),
                                  d_fim_fut)
            else:
                evs = list_events(service, cal_id, d_ini_fut, d_fim_fut)

            for ev in evs:
                ev["_cal_nome"] = nome_cal
            todos_eventos.extend(evs)
        except Exception as e:
            print(f"Erro atividades futuras ({nome_cal}): {e}")

    eventos_validos = []
    for ev in todos_eventos:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if not s_date:
            continue

        if is_all_day and e_date:
            e_date_inc = e_date - datetime.timedelta(days=1)
        else:
            e_date_inc = e_date if e_date else s_date

        ativo_no_periodo = (s_date <= d_fim_fut) and (e_date_inc >= d_ini_fut)
        if not ativo_no_periodo:
            continue

        summary = limpar_texto(ev.get("summary", "")).strip()
        if not summary:
            continue

        eventos_validos.append({
            "summary": summary,
            "s_date":  s_date,
            "e_date":  e_date_inc,
        })

    eventos_validos.sort(key=lambda x: (x["summary"], x["s_date"]))

    aglutinados = []
    for ev in eventos_validos:
        if aglutinados and \
           aglutinados[-1]["summary"] == ev["summary"] and \
           ev["s_date"] <= aglutinados[-1]["e_date"] + datetime.timedelta(days=1):
            aglutinados[-1]["e_date"] = max(aglutinados[-1]["e_date"], ev["e_date"])
        else:
            aglutinados.append({"summary": ev["summary"], "s_date": ev["s_date"], "e_date": ev["e_date"]})

    vistos = set()
    unicos = []
    for ev in aglutinados:
        chave = f"{ev['summary']}_{ev['s_date']}_{ev['e_date']}"
        if chave not in vistos:
            vistos.add(chave)
            unicos.append(ev)

    unicos.sort(key=lambda x: x["s_date"])

    linhas = []
    for i, ev in enumerate(unicos, 1):
        s_date  = ev["s_date"]
        e_date  = ev["e_date"]
        summary = ev["summary"]

        dia_fmt = f"{s_date.day:02d} {formatar_mes_abreviado(s_date)}"

        if e_date and e_date != s_date:
            ano_fim      = str(e_date.year)[-2:]
            data_fim_fmt = f"{e_date.day:02d} {formatar_mes_abreviado(e_date)} {ano_fim}"
            data_exib    = f"{dia_fmt} a {data_fim_fmt}"
        else:
            data_exib = dia_fmt

        linhas.append(f" {i}) {data_exib} - {summary}")

    return linhas

# =========================================================
# TABELAS
# Prioridade: número menor = agenda "dona" do evento quando
# há duplicata pelo mesmo event_id em múltiplas agendas.
# =========================================================

# Prioridade por calendar_id — menor = mais prioritário
PRIORIDADE_RESP = {
    IDS["npor"]:     1,
    IDS["npor_ste"]: 2,
    IDS["b_mus"]:    3,
    IDS["cia_1"]:    4,
    IDS["cia_1b"]:   4,
    IDS["cia_1_sgt"]:5,
    IDS["cia_2"]:    6,
    IDS["cia_2b"]:   6,
    IDS["cia_2_sgt"]:7,
    IDS["sec_1"]:    8,
    IDS["sec_4"]:    9,
    IDS["sfpc"]:     10,
    IDS["fisc_adm"]: 11,
    IDS["fiscal"]:   12,
    IDS["ass_jur"]:  13,
    IDS["com_soc"]:  14,
    IDS["prm"]:      15,
    IDS["brigada"]:  16,
    IDS["chales"]:   17,
    IDS["adj_cmdo"]: 18,
    IDS["sub_cmt"]:  19,
    IDS["cmdo"]:     20,
    IDS["cmt"]:      21,
    IDS["pgi"]:      22,
    IDS["s3"]:       99,
}

# Lista de todas as agendas carregadas na tabela
AGENDAS_TABELA = [
    "s3", "cmt", "cmdo", "sub_cmt", "adj_cmdo",
    "sec_1", "sec_4",
    "cia_1", "cia_1b", "cia_1_sgt",
    "cia_2", "cia_2b", "cia_2_sgt",
    "b_mus", "npor", "npor_ste",
    "ass_jur", "brigada", "fisc_adm", "chales",
    "com_soc", "fiscal", "prm", "sfpc",
]

def construir_tabela_semana(service, d_ini, d_fim, incluir_cmt, incluir_pgi, feriados, semana_tipo="s"):
    # semana_tipo: "sm1" | "s" | "s1"
    todos = []

    for chave in AGENDAS_TABELA:
        if chave == "cmt" and not incluir_cmt:
            continue
        evs = list_events(service, IDS[chave], d_ini, d_fim)
        todos.extend(evs)

    if incluir_pgi:
        evs_pgi = list_events(service, IDS["pgi"], d_ini, d_fim)
        todos.extend(evs_pgi)

    # Desduplicação com prioridade
    mapa_titulo = {}
    for e in todos:
        s_date, e_date, is_all_day, hora = parse_start_end(e)
        titulo    = limpar_texto(e.get("summary", "")).strip()
        hora_norm = "D" if is_all_day else hora
        chave_titulo = f"{titulo}_{s_date}_{hora_norm}"
        src       = e.get("_src_calendar_id", "")
        prioridade = PRIORIDADE_RESP.get(src, 50)

        if chave_titulo not in mapa_titulo:
            mapa_titulo[chave_titulo] = (prioridade, e)
        else:
            prioridade_atual, _ = mapa_titulo[chave_titulo]
            if prioridade < prioridade_atual:
                mapa_titulo[chave_titulo] = (prioridade, e)

    evs = [e for _, e in mapa_titulo.values()]

    rows = []
    cur  = d_ini
    while cur <= d_fim:
        evs_dia = [e for e in evs if event_intersects_day(e, cur)]
        evs_dia.sort(key=lambda x: x.get("start", {}).get("dateTime",
                     x.get("start", {}).get("date", "")))

        eh_especial = eh_fim_de_semana(cur) or cur in feriados

        if not evs_dia:
            rows.append({
                "DATA":      fmt_data_coluna(cur),
                "HORA":      "", "ATIVIDADE": "", "ATIV_DESC": "",
                "LOCAL":     "", "UNIF":      "", "AGENDA":    "",
                "OBS":       "", "STATUS":    "☐ Realizado\n☐ Histórico\n☐ Reagendado",
                "_especial": eh_especial, "_tem_desc": False,
            })
        else:
            for i, e in enumerate(evs_dia):
                start    = e.get("start", {})
                data_iso = start.get("dateTime", start.get("date", ""))
                hora     = data_iso[11:16] if "T" in data_iso else "D"

                atividade    = limpar_texto(e.get("summary",  "S/T"))
                local        = limpar_texto(e.get("location", ""))
                src          = e.get("_src_calendar_id", "")
                resp         = RESP_MAP.get(src, "S3")
                descricao    = limpar_texto(e.get("description", "")).strip()

                # Description em azul entre parênteses — apenas primeira linha
                if descricao:
                    primeira_linha_desc = descricao.split("  ")[0].split("\n")[0].strip()[:120]
                    atividade_exib = f"{atividade} ({primeira_linha_desc})"
                else:
                    atividade_exib = atividade

                rows.append({
                    "DATA":        fmt_data_coluna(cur) if i == 0 else "",
                    "HORA":        hora,
                    "ATIVIDADE":   atividade,
                    "ATIV_DESC":   atividade_exib,   # com description em azul
                    "LOCAL":       local,
                    "UNIF":        "",
                    "AGENDA":      resp,
                    "OBS":         "",
                    "STATUS":      "☐ Realizado\n☐ Histórico\n☐ Reagendado",
                    "_especial":   eh_especial if i == 0 else False,
                    "_tem_desc":   bool(descricao),
                })

        cur += datetime.timedelta(days=1)

    return rows

# =========================================================
# EXPORTAÇÃO EXCEL
# =========================================================

def exportar_excel(rows_sm1, rows_s, rows_s1, num_fmt, si, fase, operacoes_linhas, ativ_futuras_linhas):
    try:
        output = io.BytesIO()
        operacoes_texto    = "\n".join(operacoes_linhas)    if operacoes_linhas    else "-"
        ativ_futuras_texto = "\n".join(ativ_futuras_linhas) if ativ_futuras_linhas else "-"

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_sm1 = pd.DataFrame(rows_sm1).drop(columns=['_especial'], errors='ignore')
            df_s   = pd.DataFrame(rows_s).drop(columns=['_especial'],   errors='ignore')
            df_s1  = pd.DataFrame(rows_s1).drop(columns=['_especial'],  errors='ignore')
            df_sm1.to_excel(writer, sheet_name='Semana S-1', index=False)
            df_s.to_excel(writer,   sheet_name='Semana S',   index=False)
            df_s1.to_excel(writer,  sheet_name='Semana S+1', index=False)

            info_df = pd.DataFrame({
                'Campo': ['Número DSI', 'SI', 'FASE', 'Operações', 'Atividades Futuras'],
                'Valor': [num_fmt, si, fase, operacoes_texto, ativ_futuras_texto]
            })
            info_df.to_excel(writer, sheet_name='Info', index=False)

        return output.getvalue()
    except ImportError:
        output = io.StringIO()
        df_sm1 = pd.DataFrame(rows_sm1).drop(columns=['_especial'], errors='ignore')
        df_s   = pd.DataFrame(rows_s).drop(columns=['_especial'],   errors='ignore')
        df_s1  = pd.DataFrame(rows_s1).drop(columns=['_especial'],  errors='ignore')
        output.write(f"DSI Nº {num_fmt} - SI {si} - FASE {fase}\n")
        output.write(f"Operações:\n{operacoes_texto}\n\n")
        output.write(f"Atividades Futuras:\n{ativ_futuras_texto}\n\n")
        output.write("=== SEMANA S-1 ===\n")
        output.write(df_sm1.to_csv(index=False))
        output.write("\n=== SEMANA S ===\n")
        output.write(df_s.to_csv(index=False))
        output.write("\n=== SEMANA S+1 ===\n")
        output.write(df_s1.to_csv(index=False))
        return output.getvalue().encode('utf-8')

def salvar_historico(num_dsi: int, periodo: str, doc_id: str):
    try:
        if "historico" not in st.session_state:
            st.session_state.historico = []
        st.session_state.historico.append({
            "numero":       num_dsi,
            "periodo":      periodo,
            "doc_id":       doc_id,
            "data_criacao": datetime.datetime.now().isoformat()
        })
        registrar_log("HISTORICO_SALVO", f"DSI {num_dsi}")
    except Exception as e:
        st.warning(f"⚠️ Não foi possível salvar histórico: {e}")

# =========================================================
# GOOGLE DOCS
# =========================================================

def criar_google_doc(creds, titulo_doc, num_fmt, ref_date,
                     ini_sm1, fim_sm1, ini_s, fim_s, ini_s1, fim_s1,
                     si, fase, operacoes_linhas, bullets_cursos, bullets_datas,
                     rows_sm1, rows_s, rows_s1, ativ_futuras_linhas,
                     fg=None, su="", ativ_nao_exec=""):
    if fg is None:
        fg = {"finalidade": "", "dia": "", "dobrado": "", "cancao": "", "gs": "", "armado": ""}

    docs_service = build('docs', 'v1', credentials=creds)
    doc    = docs_service.documents().create(body={'title': titulo_doc}).execute()
    doc_id = doc['documentId']
    hoje   = datetime.date.today()

    conteudo = []
    conteudo.append(f"DSI Nº {num_fmt} - S3/24º BIS")
    conteudo.append(f"{hoje.day} {formatar_mes_abreviado(hoje)} {str(hoje.year)[-2:]}")
    conteudo.append("Visto S3:")
    conteudo.append("_____________")
    conteudo.append("Cap PIERROTI")
    conteudo.append("")
    conteudo.append("MINISTÉRIO DA DEFESA")
    conteudo.append("EXÉRCITO BRASILEIRO")
    conteudo.append("24º BATALHÃO DE INFANTARIA DE SELVA")
    conteudo.append("(9º Batalhão de Caçadores / 1839)")
    conteudo.append("BATALHÃO BARÃO DE CAXIAS")
    conteudo.append("")

    periodo_s1 = fmt_periodo_titulo(ini_s1, fim_s1)
    conteudo.append(f"DIRETRIZ SEMANAL DE INSTRUÇÃO {num_fmt} ({periodo_s1})")
    conteudo.append("")
    conteudo.append(f"(QTS nº {num_fmt} - SI: {si} - FASE: {fase})")
    conteudo.append("")

    conteudo.append("1. OPERAÇÕES:")
    for linha in (operacoes_linhas or ["-"]):
        conteudo.append(linha)
    conteudo.append("")

    conteudo.append("2. CURSOS E ESTÁGIOS")
    if bullets_cursos:
        # ✅ CORREÇÃO 3: removida numeração automática ({i}) dos cursos
        for b in bullets_cursos:
            conteudo.append(f" {b}")
    else:
        conteudo.append("-")
    conteudo.append("")

    conteudo.append("3. DATAS COMEMORATIVAS E FERIADOS")
    if bullets_datas:
        for b in bullets_datas:
            conteudo.append(f" {b}")
    else:
        conteudo.append("-")
    conteudo.append("")

    conteudo.append("4. INSTRUÇÃO")
    conteudo.append("")
    conteudo.append(f" a. Semana (S-1) - {fmt_periodo_titulo(ini_sm1, fim_sm1)} - CONFIRMAR OU REAGENDAR")
    conteudo.append(" (Realizado - Realizado/Histórico - Reagendado)")
    conteudo.append("")

    texto_completo = "\n".join(conteudo)

    # --- Inserção do texto inicial ---
    batch_update_com_retry(docs_service, doc_id, [
        {'insertText': {'location': {'index': 1}, 'text': texto_completo}}
    ])

    # --- Tabela Semana S-1 ---
    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual['body']['content'][-1]['endIndex']
    inserir_e_preencher_tabela(docs_service, doc_id, rows_sm1, end_index - 1, semana_tipo="sm1")

    # --- Cabeçalho Semana S ---
    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual['body']['content'][-1]['endIndex']
    texto_s   = f"\n b. Semana (S) - {fmt_periodo_titulo(ini_s, fim_s)} - EXECUTAR OU REAGENDAR\n"
    batch_update_com_retry(docs_service, doc_id, [
        {'insertText': {'location': {'index': end_index - 1}, 'text': texto_s}}
    ])

    # --- Tabela Semana S ---
    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual['body']['content'][-1]['endIndex']
    inserir_e_preencher_tabela(docs_service, doc_id, rows_s, end_index - 1, semana_tipo="s")

    # --- Cabeçalho Semana S+1 ---
    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual['body']['content'][-1]['endIndex']
    texto_s1  = f"\n c. Semana (S+1) - {fmt_periodo_titulo(ini_s1, fim_s1)} - PLANEJAR\n"
    batch_update_com_retry(docs_service, doc_id, [
        {'insertText': {'location': {'index': end_index - 1}, 'text': texto_s1}}
    ])

    # --- Tabela Semana S+1 ---
    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual['body']['content'][-1]['endIndex']
    inserir_e_preencher_tabela(docs_service, doc_id, rows_s1, end_index - 1, semana_tipo="s1")

    # --- Conteúdo final (seções 5–8 + assinatura) ---
    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual['body']['content'][-1]['endIndex']

    conteudo_final = []

    conteudo_final.append("\n5. FORMATURA GERAL")
    conteudo_final.append(f" 1) Finalidade: {fg.get('finalidade', '')}")
    conteudo_final.append(f" 2) Dia: {fg.get('dia', '')}")
    conteudo_final.append(f" 3) Dobrado: {fg.get('dobrado', '')}")
    conteudo_final.append(f" 4) Canção: {fg.get('cancao', '')}")
    conteudo_final.append(f" 5) GS: {fg.get('gs', '')}")
    conteudo_final.append(f" 6) Armado e Equipado: {fg.get('armado', '')}")
    conteudo_final.append("")

    conteudo_final.append("6. ATIVIDADES FUTURAS")
    if ativ_futuras_linhas:
        for linha in ativ_futuras_linhas:
            conteudo_final.append(linha)
    else:
        conteudo_final.append(" ________________________________________________")
    conteudo_final.append("")

    conteudo_final.append("7. SU")
    if su.strip():
        for i, linha in enumerate([l for l in su.strip().split("\n") if l.strip()], 1):
            conteudo_final.append(f" {i}. {linha.strip()}")
    else:
        conteudo_final.append(" 1. ______________________________________")
    conteudo_final.append("")

    conteudo_final.append("8. ATIVIDADES PLANEJADAS E NÃO EXECUTADAS")
    if ativ_nao_exec.strip():
        for i, linha in enumerate([l for l in ativ_nao_exec.strip().split("\n") if l.strip()], 1):
            conteudo_final.append(f" {i}. {linha.strip()}")
    else:
        conteudo_final.append(" ________________________________________________")
    conteudo_final.append("")

    meses_completos = ["janeiro","fevereiro","março","abril","maio","junho",
                       "julho","agosto","setembro","outubro","novembro","dezembro"]
    data_assinatura = f"São Luís, MA, {hoje.day} de {meses_completos[hoje.month-1]} de {hoje.year}"
    conteudo_final.append(f"{data_assinatura}\n\n\n\nJOÃO CARLOS DUQUE – Ten Cel\nComandante do 24º Batalhão de Infantaria de Selva\n")

    batch_update_com_retry(docs_service, doc_id, [
        {'insertText': {'location': {'index': end_index - 1}, 'text': "\n".join(conteudo_final)}}
    ])

    # --- Formatação global ---
    time.sleep(3)
    formatar_documento_completo(docs_service, doc_id, rows_sm1, rows_s, rows_s1)
    return doc_id

# =========================================================
# GOOGLE DOCS – TABELA
# =========================================================

def inserir_e_preencher_tabela(docs_service, doc_id, rows, insert_index, semana_tipo="s"):
    num_cols = 6 if semana_tipo == "sm1" else 7
    batch_update_com_retry(docs_service, doc_id, [
        {'insertTable': {'rows': len(rows) + 1, 'columns': num_cols, 'location': {'index': insert_index}}}
    ])
    time.sleep(2)

    def get_ultima_tabela():
        doc     = docs_service.documents().get(documentId=doc_id).execute()
        content = doc['body']['content']
        for element in reversed(content):
            if 'table' in element:
                return element['table']
        return None

    tabela = get_ultima_tabela()
    if not tabela:
        return

    if semana_tipo == "sm1":
        larguras_pt = [80, 38, 190, 120, 35, 65]   # 6 cols: DATA HORA ATIV LOCAL AG STATUS
    else:
        larguras_pt = [95, 38, 185, 125, 28, 28, 28]   # 7 cols

    doc_temp = docs_service.documents().get(documentId=doc_id).execute()
    for el in reversed(doc_temp['body']['content']):
        if 'table' in el:
            tbl_start = el['startIndex']
            col_reqs  = [
                {'updateTableColumnProperties': {
                    'tableStartLocation': {'index': tbl_start},
                    'columnIndices': [ci],
                    'tableColumnProperties': {'widthType': 'FIXED_WIDTH', 'width': {'magnitude': larg, 'unit': 'PT'}},
                    'fields': 'widthType,width'
                }} for ci, larg in enumerate(larguras_pt)
            ]
            try:
                batch_update_com_retry(docs_service, doc_id, col_reqs)
            except Exception as e:
                print(f"Erro larguras: {e}")
            break

    grupos_data = {}
    data_atual  = None
    for idx, row_data in enumerate(rows):
        data = row_data.get("DATA", "")
        if data:
            data_atual = data
            grupos_data[data_atual] = []
        if data_atual:
            grupos_data[data_atual].append(idx)

    all_requests = []
    if semana_tipo == "sm1":
        cols    = ["DATA", "HORA", "ATIV_DESC", "LOCAL", "AGENDA", "STATUS"]
        headers = ["DATA", "HORA", "ATIVIDADE", "LOCAL", "AG",     "STATUS"]
    else:
        cols    = ["DATA", "HORA", "ATIV_DESC", "LOCAL", "UNIF", "AGENDA", "OBS"]
        headers = ["DATA", "HORA", "ATIVIDADE", "LOCAL", "UNIF", "AG",     "OBS"]
    n_cols = len(cols)

    for row_idx in range(len(rows) - 1, -1, -1):
        if row_idx + 1 >= len(tabela['tableRows']):
            continue
        linha_tabela = tabela['tableRows'][row_idx + 1]
        row_data     = rows[row_idx]
        for col_idx in range(n_cols - 1, -1, -1):
            if col_idx < len(linha_tabela['tableCells']):
                celula       = linha_tabela['tableCells'][col_idx]
                cell_content = celula.get('content')
                if cell_content:
                    start_idx = cell_content[0].get('startIndex')
                    if start_idx is not None:
                        valor = row_data.get(cols[col_idx], "")
                        texto = "" if (pd.isna(valor) if isinstance(valor, float) else (valor is None or str(valor).strip() == '')) else str(valor).strip()
                        if texto:
                            # STATUS: inserir só a primeira linha aqui;
                            # as outras linhas são inseridas depois via parágrafos
                            if cols[col_idx] == "STATUS":
                                primeira = texto.split("\n")[0]
                                all_requests.append({'insertText': {'location': {'index': start_idx}, 'text': primeira}})
                            else:
                                all_requests.append({'insertText': {'location': {'index': start_idx}, 'text': texto}})

    primeira_linha = tabela['tableRows'][0]
    for i in range(n_cols - 1, -1, -1):
        if i < len(primeira_linha['tableCells']):
            cell_content = primeira_linha['tableCells'][i].get('content')
            if cell_content:
                start_idx = cell_content[0].get('startIndex')
                if start_idx is not None:
                    all_requests.append({'insertText': {'location': {'index': start_idx}, 'text': headers[i]}})

    if all_requests:
        try:
            batch_update_com_retry(docs_service, doc_id, all_requests)
        except Exception as e:
            print(f"Erro preencher tabela: {e}")

    # --- Inserir linhas 2 e 3 do STATUS como parágrafos separados ---
    if semana_tipo == "sm1":
        time.sleep(1)
        doc_status = docs_service.documents().get(documentId=doc_id).execute()
        tabela_st  = None
        for el in reversed(doc_status['body']['content']):
            if 'table' in el:
                tabela_st = el['table']
                break
        if tabela_st:
            col_status_idx = cols.index("STATUS")
            reqs_status = []
            # Percorre de trás para frente para não deslocar índices
            for row_idx in range(len(rows) - 1, -1, -1):
                if row_idx + 1 >= len(tabela_st['tableRows']):
                    continue
                tbl_row = tabela_st['tableRows'][row_idx + 1]
                if col_status_idx >= len(tbl_row['tableCells']):
                    continue
                cell_st  = tbl_row['tableCells'][col_status_idx]
                cnt_st   = cell_st.get('content', [])
                if not cnt_st:
                    continue
                # endIndex do último parágrafo da célula = onde inserir
                insert_at = cnt_st[-1].get('endIndex', 0) - 1
                if insert_at <= 0:
                    continue
                # Inserir "\n☐ Reagendado\n☐ Histórico" de trás pra frente
                # Ordem reversa: primeiro Reagendado, depois Histórico
                # pois inserimos antes do 
 final da célula
                reqs_status.append({'insertText': {
                    'location': {'index': insert_at},
                    'text': "\n☐ Reagendado\n☐ Histórico"
                }})
            if reqs_status:
                batch_update_com_retry(docs_service, doc_id, reqs_status)

    time.sleep(0.5)
    aplicar_formatacao_tabela(docs_service, doc_id, rows, grupos_data, semana_tipo=semana_tipo)


def aplicar_formatacao_tabela(docs_service, doc_id, rows, grupos_data, semana_tipo="s"):
    doc     = docs_service.documents().get(documentId=doc_id).execute()
    content = doc['body']['content']

    tabela_element = None
    for element in reversed(content):
        if 'table' in element:
            tabela_element = element
            break
    if not tabela_element:
        return

    tabela      = tabela_element['table']
    table_start = tabela_element['startIndex']
    requests    = []

    n_cols_tab = 6 if semana_tipo == "sm1" else 7
    for row_idx in range(len(tabela.get('tableRows', []))):
        for col_idx in range(n_cols_tab):
            borda = {'color': {'color': {'rgbColor': {'red': 0, 'green': 0, 'blue': 0}}}, 'width': {'magnitude': 1, 'unit': 'PT'}, 'dashStyle': 'SOLID'}
            requests.append({'updateTableCellStyle': {
                'tableRange': {'tableCellLocation': {'tableStartLocation': {'index': table_start}, 'rowIndex': row_idx, 'columnIndex': col_idx}, 'rowSpan': 1, 'columnSpan': 1},
                'tableCellStyle': {'borderTop': borda, 'borderBottom': borda, 'borderLeft': borda, 'borderRight': borda},
                'fields': 'borderTop,borderBottom,borderLeft,borderRight'
            }})

    for col_idx in range(n_cols_tab):
        requests.append({'updateTableCellStyle': {
            'tableRange': {'tableCellLocation': {'tableStartLocation': {'index': table_start}, 'rowIndex': 0, 'columnIndex': col_idx}, 'rowSpan': 1, 'columnSpan': 1},
            'tableCellStyle': {'backgroundColor': {'color': {'rgbColor': {'red': 0.4, 'green': 0.4, 'blue': 0.4}}}},
            'fields': 'backgroundColor'
        }})

    for data, indices in grupos_data.items():
        if len(indices) > 1:
            requests.append({'mergeTableCells': {
                'tableRange': {'tableCellLocation': {'tableStartLocation': {'index': table_start}, 'rowIndex': indices[0] + 1, 'columnIndex': 0}, 'rowSpan': len(indices), 'columnSpan': 1}
            }})

    cor_alternada = True
    for data, indices in grupos_data.items():
        eh_dia_especial = any(rows[idx].get('_especial', False) for idx in indices)
        if eh_dia_especial:
            cor = {'red': 1.0, 'green': 0.8, 'blue': 0.8}
        else:
            cor = {'red': 0.85, 'green': 0.85, 'blue': 0.85} if cor_alternada else {'red': 1.0, 'green': 1.0, 'blue': 1.0}
        for idx in indices:
            for col_idx in range(n_cols_tab):
                requests.append({'updateTableCellStyle': {
                    'tableRange': {'tableCellLocation': {'tableStartLocation': {'index': table_start}, 'rowIndex': idx + 1, 'columnIndex': col_idx}, 'rowSpan': 1, 'columnSpan': 1},
                    'tableCellStyle': {'backgroundColor': {'color': {'rgbColor': cor}}},
                    'fields': 'backgroundColor'
                }})
        if not eh_dia_especial:
            cor_alternada = not cor_alternada

    for row_idx in range(len(rows) + 1):
        if row_idx < len(tabela.get('tableRows', [])):
            row_cells = tabela['tableRows'][row_idx].get('tableCells', [])
            for col_idx, cell in enumerate(row_cells):
                cell_content = cell.get('content', [])
                if cell_content:
                    requests.append({'updateParagraphStyle': {
                        'paragraphStyle': {'alignment': 'CENTER'}, 'fields': 'alignment',
                        'range': {'startIndex': cell_content[0]['startIndex'], 'endIndex': cell_content[0]['endIndex'] - 1}
                    }})
                    requests.append({'updateTableCellStyle': {
                        'tableRange': {'tableCellLocation': {'tableStartLocation': {'index': table_start}, 'rowIndex': row_idx, 'columnIndex': col_idx}, 'rowSpan': 1, 'columnSpan': 1},
                        'tableCellStyle': {'contentAlignment': 'MIDDLE', 'paddingTop': {'magnitude': 2, 'unit': 'PT'}, 'paddingBottom': {'magnitude': 2, 'unit': 'PT'}, 'paddingLeft': {'magnitude': 3, 'unit': 'PT'}, 'paddingRight': {'magnitude': 3, 'unit': 'PT'}},
                        'fields': 'contentAlignment,paddingTop,paddingBottom,paddingLeft,paddingRight'
                    }})

    if requests:
        batch_update_com_retry(docs_service, doc_id, requests)

    time.sleep(0.5)
    doc = docs_service.documents().get(documentId=doc_id).execute()
    tabela_element = None
    for element in reversed(doc['body']['content']):
        if 'table' in element:
            tabela_element = element
            break
    if tabela_element:
        tabela         = tabela_element['table']
        primeira_linha = tabela['tableRows'][0]
        reqs_negrito   = []
        for col_idx in range(n_cols_tab):
            if col_idx < len(primeira_linha['tableCells']):
                cell_content = primeira_linha['tableCells'][col_idx].get('content', [])
                if cell_content:
                    s = cell_content[0].get('startIndex')
                    e = cell_content[0].get('endIndex')
                    if s and e and e > s:
                        reqs_negrito.append({'updateTextStyle': {
                            'range': {'startIndex': s, 'endIndex': e - 1},
                            'textStyle': {'bold': True, 'foregroundColor': {'color': {'rgbColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}}}},
                            'fields': 'bold,foregroundColor'
                        }})
        if reqs_negrito:
            try:
                batch_update_com_retry(docs_service, doc_id, reqs_negrito)
            except Exception as e:
                print(f"Erro negrito: {e}")

    # --- Colorir description (em azul) nas células de ATIVIDADE ---
    # e colorir STATUS nas células correspondentes
    try:
        doc_refresco = docs_service.documents().get(documentId=doc_id).execute()
        tabela_el2   = None
        for el in reversed(doc_refresco['body']['content']):
            if 'table' in el:
                tabela_el2 = el
                break
        if tabela_el2:
            tabela2     = tabela_el2['table']
            tbl_start2  = tabela_el2['startIndex']
            col_ativ    = 2  # coluna ATIVIDADE (índice 2)
            col_status  = 5  # coluna STATUS (índice 5 em sm1 sem UNIF)
            reqs_cor    = []
            azul        = {'red': 0.07, 'green': 0.36, 'blue': 0.68}
            CORES_STATUS = {
                'Realizado':           {'red': 0.0,  'green': 0.39, 'blue': 0.0},
                'Realizado/Histórico': {'red': 0.0,  'green': 0.55, 'blue': 0.27},
                'Reagendado':          {'red': 0.85, 'green': 0.33, 'blue': 0.1},
            }
            for row_idx_t, (tbl_row, row_data) in enumerate(
                    zip(tabela2['tableRows'][1:], rows), 1):
                # Colorir description entre parênteses na col ATIVIDADE
                if row_data.get('_tem_desc') and col_ativ < len(tbl_row['tableCells']):
                    cell  = tbl_row['tableCells'][col_ativ]
                    cnt   = cell.get('content', [])
                    if cnt:
                        ativ_txt  = row_data.get('ATIV_DESC', '')
                        abre_par  = ativ_txt.find('(')
                        if abre_par >= 0:
                            # Calcula offset do '(' no texto da célula
                            s_cell = cnt[0].get('startIndex', 0)
                            e_cell = cnt[-1].get('endIndex', s_cell)
                            # Colorir do '(' até o final do texto (parênteses)
                            par_start = s_cell + abre_par
                            par_end   = min(e_cell - 1, s_cell + len(ativ_txt))
                            if par_end > par_start:
                                reqs_cor.append({'updateTextStyle': {
                                    'range': {'startIndex': par_start, 'endIndex': par_end},
                                    'textStyle': {'foregroundColor': {'color': {'rgbColor': azul}}},
                                    'fields': 'foregroundColor'
                                }})
                # Colorir STATUS (3 parágrafos) apenas para tabela sm1
                # Ordem de inserção foi: ☐ Realizado / ☐ Histórico / ☐ Reagendado
                if semana_tipo == "sm1" and col_status < len(tbl_row['tableCells']):
                    cell_s = tbl_row['tableCells'][col_status]
                    cnt_s  = cell_s.get('content', [])
                    COR_STATUS_LINES = [
                        # (cor_rgb, strikethrough)
                        ({'red': 0.07, 'green': 0.36, 'blue': 0.68}, False),  # ☐ Realizado — azul
                        ({'red': 0.0,  'green': 0.50, 'blue': 0.13}, False),  # ☐ Histórico — verde
                        ({'red': 0.78, 'green': 0.08, 'blue': 0.08}, False),  # ☐ Reagendado — vermelho
                    ]
                    for par_idx, paragrafo in enumerate(cnt_s):
                        if par_idx >= len(COR_STATUS_LINES):
                            break
                        p_start = paragrafo.get('startIndex')
                        p_end   = paragrafo.get('endIndex')
                        if p_start is None or p_end is None or p_end <= p_start + 1:
                            continue
                        cor_linha, strike = COR_STATUS_LINES[par_idx]
                        reqs_cor.append({'updateTextStyle': {
                            'range': {'startIndex': p_start, 'endIndex': p_end - 1},
                            'textStyle': {
                                'foregroundColor': {'color': {'rgbColor': cor_linha}},
                                'fontSize': {'magnitude': 10, 'unit': 'PT'},
                                'strikethrough': strike,
                            },
                            'fields': 'foregroundColor,fontSize,strikethrough'
                        }})
            if reqs_cor:
                batch_update_com_retry(docs_service, doc_id, reqs_cor)
    except Exception as e:
        print(f"Erro coloração description/status: {e}")


def formatar_documento_completo(docs_service, doc_id, rows_sm1, rows_s, rows_s1):
    doc       = docs_service.documents().get(documentId=doc_id).execute()
    content   = doc['body']['content']
    end_index = content[-1]['endIndex']
    requests  = []

    requests.append({'updateTextStyle': {
        'range': {'startIndex': 1, 'endIndex': end_index - 1},
        'textStyle': {'fontSize': {'magnitude': 12, 'unit': 'PT'}, 'weightedFontFamily': {'fontFamily': 'Calibri'}},
        'fields': 'fontSize,weightedFontFamily'
    }})

    requests.append({'updateDocumentStyle': {
        'documentStyle': {
            'marginTop':    {'magnitude': 28.35, 'unit': 'PT'},
            'marginBottom': {'magnitude': 28.35, 'unit': 'PT'},
            'marginLeft':   {'magnitude': 28.35, 'unit': 'PT'},
            'marginRight':  {'magnitude': 28.35, 'unit': 'PT'}
        },
        'fields': 'marginTop,marginBottom,marginLeft,marginRight'
    }})

    if requests:
        batch_update_com_retry(docs_service, doc_id, requests)
        requests = []

    # --- Título DSI: caixa cinza com borda + negrito ---
    # --- QTS: centralizado e negrito ---
    titulo_pattern = re.compile(r'DIRETRIZ SEMANAL DE INSTRUÇÃO \d+', re.IGNORECASE)
    qts_pattern    = re.compile(r'\(QTS nº', re.IGNORECASE)
    conf_pattern   = re.compile(r'CONFIRMAR OU REAGENDAR|EXECUTAR OU REAGENDAR|PLANEJAR', re.IGNORECASE)
    cinza_claro    = {'red': 0.85, 'green': 0.85, 'blue': 0.85}
    laranja        = {'red': 0.85, 'green': 0.33, 'blue': 0.1}

    doc2    = docs_service.documents().get(documentId=doc_id).execute()
    cont2   = doc2['body']['content']
    reqs2   = []
    for element in cont2:
        if 'paragraph' not in element:
            continue
        para      = element['paragraph']
        para_els  = para.get('elements', [])
        full_text = ''.join(pe.get('textRun', {}).get('content', '') for pe in para_els).strip()
        p_start   = element.get('startIndex', 0)
        p_end     = element.get('endIndex', 0)
        if not full_text or p_end <= p_start:
            continue

        if titulo_pattern.search(full_text):
            # Centralizar + negrito + fundo cinza
            reqs2.append({'updateParagraphStyle': {
                'range': {'startIndex': p_start, 'endIndex': p_end},
                'paragraphStyle': {'alignment': 'CENTER',
                    'shading': {'backgroundColor': {'color': {'rgbColor': cinza_claro}}}},
                'fields': 'alignment,shading'
            }})
            reqs2.append({'updateTextStyle': {
                'range': {'startIndex': p_start, 'endIndex': p_end - 1},
                'textStyle': {'bold': True},
                'fields': 'bold'
            }})

        elif qts_pattern.search(full_text):
            # Centralizar + negrito
            reqs2.append({'updateParagraphStyle': {
                'range': {'startIndex': p_start, 'endIndex': p_end},
                'paragraphStyle': {'alignment': 'CENTER'},
                'fields': 'alignment'
            }})
            reqs2.append({'updateTextStyle': {
                'range': {'startIndex': p_start, 'endIndex': p_end - 1},
                'textStyle': {'bold': True},
                'fields': 'bold'
            }})

        elif conf_pattern.search(full_text):
            # Colorir laranja (CONFIRMAR OU REAGENDAR / EXECUTAR / PLANEJAR)
            reqs2.append({'updateTextStyle': {
                'range': {'startIndex': p_start, 'endIndex': p_end - 1},
                'textStyle': {'foregroundColor': {'color': {'rgbColor': laranja}}, 'bold': True},
                'fields': 'foregroundColor,bold'
            }})

    if reqs2:
        batch_update_com_retry(docs_service, doc_id, reqs2)
        requests = []  # reset para evitar duplicação

    padroes_negrito = [
        r"1\.\s+OPERA[ÇC][ÕO]ES[:\s]?",
        r"2\.\s+CURSOS E EST[ÁA]GIOS",
        r"3\.\s+DATAS COMEMORATIVAS",
        r"4\.\s+INSTRU[ÇC][ÃA]O",
        r"5\.\s+FORMATURA GERAL",
        r"6\.\s+ATIVIDADES FUTURAS",
        r"7\.\s+SU\b",
        r"8\.\s+ATIVIDADES PLANEJADAS",
    ]

    for element in content:
        if 'paragraph' not in element:
            continue
        para = element['paragraph']
        para_elements = para.get('elements', [])
        for pe in para_elements:
            if 'textRun' not in pe:
                continue
            texto_run = pe['textRun'].get('content', '').strip()
            run_start = pe.get('startIndex')
            run_end   = pe.get('endIndex')
            if run_start is None or run_end is None or run_end <= run_start:
                continue
            for padrao in padroes_negrito:
                if re.search(padrao, texto_run, re.IGNORECASE):
                    safe_end = min(run_end, end_index - 1)
                    if safe_end > run_start:
                        requests.append({'updateTextStyle': {
                            'range': {'startIndex': run_start, 'endIndex': safe_end},
                            'textStyle': {'bold': True},
                            'fields': 'bold'
                        }})
                    break

    if requests:
        batch_update_com_retry(docs_service, doc_id, requests)


def criar_google_doc_safe(creds, *args, **kwargs):
    for tentativa in range(3):
        try:
            return criar_google_doc(creds, *args, **kwargs)
        except Exception as e:
            if tentativa < 2:
                st.warning(f"⚠️ Tentativa {tentativa + 1} falhou. Tentando novamente em 5s...")
                time.sleep(5)
            else:
                st.error(f"❌ Erro após 3 tentativas: {e}")
                raise

# =========================================================
# INTERFACE STREAMLIT
# =========================================================

st.set_page_config(page_title="DSI 24º BIS", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #f0f2f6; }
    .stMarkdown h1 { color: #1f4788; font-weight: bold; }
    div[data-testid="stDataFrame"] { border: 2px solid #1f4788; border-radius: 5px; }
    .success-box { padding: 1rem; background-color: #d4edda; border: 1px solid #c3e6cb; border-radius: 5px; color: #155724; }
</style>
""", unsafe_allow_html=True)

st.title("📋 Diretriz Semanal de Instrução - 24º BIS")

for key in ['exportar', 'doc_criado', 'historico']:
    if key not in st.session_state:
        st.session_state[key] = False if key == 'exportar' else (None if key == 'doc_criado' else [])

try:
    creds = get_credentials()
    srv   = build("calendar", "v3", credentials=creds)

    with st.sidebar:
        st.header("⚙️ Parâmetros da DSI")

        num_doc  = st.number_input("Nº da DSI / QTS", min_value=1, max_value=999, value=6, step=1)
        num_fmt  = f"{int(num_doc):03d}"
        ref_date = st.date_input("Data de referência (para calcular S)", value=datetime.date.today())

        incluir_cmt = st.checkbox("Incluir agenda do Cmt",   value=True)
        incluir_pgi = st.checkbox("Incluir agenda PGI 2026", value=True)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 ATUALIZAR", type="primary", use_container_width=True):
                st.cache_data.clear()
                st.session_state.exportar   = False
                st.session_state.doc_criado = None
                st.rerun()
        with col2:
            if st.button("📄 EXPORTAR DOCS", type="secondary", use_container_width=True):
                st.session_state.exportar = True

        st.markdown("---")
        with st.expander("📚 Histórico de DSIs"):
            historico = st.session_state.get("historico", [])
            if historico:
                for item in reversed(historico[-10:]):
                    st.markdown(f"**DSI {item['numero']:03d}** - {item['periodo']}")
                    st.markdown(f"[📄 Abrir](https://docs.google.com/document/d/{item['doc_id']}/edit)")
                    st.markdown("---")
            else:
                st.info("Nenhuma DSI gerada ainda")

        st.markdown("---")
        st.info("💡 **Dica:** Use Ctrl+F para buscar no documento")

    ini_s, fim_s = week_range(ref_date)
    ini_sm1      = ini_s - datetime.timedelta(days=7)
    fim_sm1      = fim_s - datetime.timedelta(days=7)
    ini_s1       = ini_s + datetime.timedelta(days=7)
    fim_s1       = fim_s + datetime.timedelta(days=7)

    if not validar_datas(ini_s, fim_s1):
        st.stop()

    periodo_titulo = fmt_periodo_titulo(ini_s1, fim_s1)
    titulo_dsi     = f"DIRETRIZ SEMANAL DE INSTRUÇÃO {num_fmt} ({periodo_titulo})"

    with st.spinner("🔍 Buscando informações dos calendários..."):
        si                  = buscar_si_duplo(srv, ini_s, fim_s, ini_s1, fim_s1)
        fase                = buscar_fase(srv, ini_s, fim_s1) or "Mdd Adm"
        operacoes_linhas    = buscar_operacoes(srv, ini_s, fim_s1)
        ativ_futuras_linhas = buscar_atividades_futuras(srv, fim_s1)

    linha_qts = f"(QTS nº {num_fmt} - SI: {si} - FASE: {fase})"

    with st.expander("🔍 Debug - SI, FASE, OPERAÇÕES e ATIVIDADES FUTURAS"):
        st.write(f"**Período S-1:** {ini_sm1.strftime('%d/%m/%Y')} a {fim_sm1.strftime('%d/%m/%Y')}")
        st.write(f"**Período S:** {ini_s.strftime('%d/%m/%Y')} a {fim_s.strftime('%d/%m/%Y')}")
        st.write(f"**Período S+1:** {ini_s1.strftime('%d/%m/%Y')} a {fim_s1.strftime('%d/%m/%Y')}")
        st.write(f"**Ativ. Futuras:** {fim_s1 + datetime.timedelta(days=1)} a {fim_s1 + datetime.timedelta(days=45)}")
        st.write(f"**SI:** {si} | **FASE:** {fase}")
        st.write(f"**OPERAÇÕES:** {len(operacoes_linhas)}")
        st.write(f"**ATIVIDADES FUTURAS:** {len(ativ_futuras_linhas)}")
        for linha in ativ_futuras_linhas:
            st.write(f"  {linha}")

    bullets_cursos = bullets_periodo(srv, IDS["cursos"], ini_s, fim_s1, incluir_responsavel=True)
    bullets_datas  = bullets_periodo(srv, IDS["datas"],  ini_s, fim_s1)
    feriados       = buscar_feriados(srv, ini_sm1, fim_s1)

    rows_sm1 = construir_tabela_semana(srv, ini_sm1, fim_sm1, incluir_cmt, incluir_pgi, feriados)
    rows_s   = construir_tabela_semana(srv, ini_s,   fim_s,   incluir_cmt, incluir_pgi, feriados)
    rows_s1  = construir_tabela_semana(srv, ini_s1,  fim_s1,  incluir_cmt, incluir_pgi, feriados)

    if st.session_state.exportar and st.session_state.doc_criado is None:
        fg = {k: st.session_state.get(f"fg_{k}", "")
              for k in ["finalidade", "dia", "dobrado", "cancao", "gs", "armado"]}

        with st.spinner("📝 Criando documento no Google Docs... (pode levar 1-2 minutos)"):
            try:
                doc_id = criar_google_doc_safe(
                    creds, titulo_dsi, num_fmt, ref_date,
                    ini_sm1, fim_sm1, ini_s, fim_s, ini_s1, fim_s1,
                    si, fase, operacoes_linhas, bullets_cursos, bullets_datas,
                    rows_sm1, rows_s, rows_s1,
                    ativ_futuras_linhas=ativ_futuras_linhas,
                    fg=fg,
                    su=st.session_state.get("su_texto", ""),
                    ativ_nao_exec=st.session_state.get("ativ_nao_exec", "")
                )
                salvar_historico(int(num_doc), periodo_titulo, doc_id)
                st.session_state.doc_criado = doc_id
                st.session_state.exportar   = False
            except Exception as e:
                st.error(f"❌ Erro ao criar documento: {e}")
                st.session_state.exportar   = False
                st.session_state.doc_criado = None

    if st.session_state.doc_criado:
        st.markdown(f"""
        <div class="success-box">
            <h3>✅ Documento criado com sucesso!</h3>
            <p><a href="https://docs.google.com/document/d/{st.session_state.doc_criado}/edit" target="_blank">
                📄 Abrir documento no Google Docs
            </a></p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("🔄 Criar Novo Documento"):
            st.session_state.doc_criado = None
            st.rerun()

    try:
        excel_data = exportar_excel(rows_sm1, rows_s, rows_s1, num_fmt, si, fase, operacoes_linhas, ativ_futuras_linhas)
        file_ext   = "xlsx" if isinstance(excel_data, bytes) and excel_data[:2] == b'PK' else "csv"
        mime_type  = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if file_ext == "xlsx" else "text/csv"
        st.download_button(
            label=f"📊 Baixar {'Excel' if file_ext == 'xlsx' else 'CSV'} (Backup)",
            data=excel_data,
            file_name=f"DSI_{num_fmt}_{datetime.date.today()}.{file_ext}",
            mime=mime_type
        )
    except Exception as e:
        st.warning(f"⚠️ Não foi possível gerar arquivo de backup: {e}")

    st.markdown("---")
    st.markdown("### 📄 Preview do Documento")

    hoje = datetime.date.today()
    st.markdown(f"""
    <div style='font-size:10px; text-align:left;'>
    DSI Nº {num_fmt} - S3/24º BIS<br>
    {hoje.day} {formatar_mes_abreviado(hoje)} {str(hoje.year)[-2:]}<br>
    Visto S3: _____________<br>Cap PIERROTI
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style='text-align:center; font-weight:bold; font-family:Calibri;'>
    MINISTÉRIO DA DEFESA<br>EXÉRCITO BRASILEIRO<br>
    24º BATALHÃO DE INFANTARIA DE SELVA<br>
    (9º Batalhão de Caçadores / 1839)<br>BATALHÃO BARÃO DE CAXIAS
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"<h3 style='text-align:center; font-family:Calibri;'>{titulo_dsi}</h3>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center; font-family:Calibri;'><strong>{linha_qts}</strong></p>", unsafe_allow_html=True)

    st.markdown("**1. OPERAÇÕES:**")
    for linha in (operacoes_linhas or ["-"]):
        st.markdown(linha)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**2. CURSOS E ESTÁGIOS**")
        for item in (bullets_cursos or ["-"]):
            st.markdown(f" {item}")
    with col2:
        st.markdown("**3. DATAS COMEMORATIVAS E FERIADOS**")
        for item in (bullets_datas or ["-"]):
            st.markdown(f" {item}")

    def render_tabela_html(rows, especial_list, table_id="dsi", semana_tipo="s"):
        if semana_tipo == "sm1":
            cols   = ["DATA", "HORA", "ATIV_DESC", "LOCAL", "UNIF", "AGENDA", "STATUS"]
            hdrs   = ["DATA", "HORA", "ATIVIDADE", "LOCAL", "UNIF", "AG",     "STATUS"]
            widths = {"DATA":"10%","HORA":"5%","ATIV_DESC":"30%","LOCAL":"20%","UNIF":"5%","AGENDA":"5%","STATUS":"10%"}
        else:
            cols   = ["DATA", "HORA", "ATIV_DESC", "LOCAL", "UNIF", "AGENDA", "OBS"]
            hdrs   = ["DATA", "HORA", "ATIVIDADE", "LOCAL", "UNIF", "AG",     "OBS"]
            widths = {"DATA":"11%","HORA":"5%","ATIV_DESC":"33%","LOCAL":"22%","UNIF":"5%","AGENDA":"6%","OBS":"8%"}
        html   = f"""
        <style>
        #{table_id} {{width:100%;border-collapse:collapse;font-size:12px;font-family:Calibri,Arial,sans-serif;}}
        #{table_id} th {{background:#555;color:white;text-align:center;vertical-align:middle;padding:4px 3px;border:1px solid #999;font-weight:bold;}}
        #{table_id} td {{text-align:center;vertical-align:middle;padding:3px 3px;border:1px solid #ccc;line-height:1.2;}}
        #{table_id} tr.alt {{background:#ddd;}} #{table_id} tr.normal {{background:#fff;}}
        #{table_id} tr.especial td {{color:red;background:#fdd;}}
        .desc-azul {{color:#1258ae;}}
        </style><table id="{table_id}"><thead><tr>"""
        for c, h in zip(cols, hdrs):
            html += f'<th style="width:{widths[c]}">{h}</th>'
        html += "</tr></thead><tbody>"
        alt = True
        for idx, row in enumerate(rows):
            eh_esp = especial_list[idx] if idx < len(especial_list) else False
            if row.get("DATA"):
                alt = not alt
            cls  = "especial" if eh_esp else ("alt" if alt else "normal")
            html += f'<tr class="{cls}">'
            for c in cols:
                val = row.get(c, "") or ""
                if c == "ATIV_DESC" and row.get("_tem_desc"):
                    abre = val.find("(")
                    if abre >= 0:
                        val = val[:abre] + f'<span class="desc-azul">' + val[abre:] + "</span>"
                elif c == "STATUS":
                    if val:
                        linhas_s = val.split("\n")
                        cores_s  = ["#1258ae", "#008021", "#c71414"]
                        val = "<br>".join(
                            f'<span style="color:{c};font-size:10px">{l}</span>'
                            for l, c in zip(linhas_s, cores_s)
                        )
                html += f"<td>{val}</td>"
            html += "</tr>"
        html += "</tbody></table>"
        return html

    st.markdown("**4. INSTRUÇÃO**")

    st.markdown(f"**a. Semana (S-1) - {fmt_periodo_titulo(ini_sm1, fim_sm1)}** — :orange[CONFIRMAR OU REAGENDAR]")
    st.markdown(render_tabela_html(rows_sm1, [r.get('_especial', False) for r in rows_sm1], table_id="tabela_sm1", semana_tipo="sm1"), unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(f"**b. Semana (S) - {fmt_periodo_titulo(ini_s, fim_s)}** — :orange[EXECUTAR OU REAGENDAR]")
    st.markdown(render_tabela_html(rows_s,  [r.get('_especial', False) for r in rows_s],  table_id="tabela_s",  semana_tipo="s"),  unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(f"**c. Semana (S+1) - {fmt_periodo_titulo(ini_s1, fim_s1)}** — :orange[PLANEJAR]")
    st.markdown(render_tabela_html(rows_s1, [r.get('_especial', False) for r in rows_s1], table_id="tabela_s1", semana_tipo="s1"), unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    with st.expander("5. FORMATURA GERAL", expanded=False):
        st.text_input("1) Finalidade:", key="fg_finalidade")
        st.text_input("2) Dia:", placeholder="ex: 25/02/2026", key="fg_dia")
        st.text_input("3) Dobrado:", key="fg_dobrado")
        st.text_input("4) Canção:", key="fg_cancao")
        st.text_input("5) GS:", key="fg_gs")
        st.text_input("6) Armado e Equipado:", key="fg_armado")

    with st.expander("6. ATIVIDADES FUTURAS", expanded=True):
        d_ini_fut = fim_s1 + datetime.timedelta(days=1)
        d_fim_fut = fim_s1 + datetime.timedelta(days=45)
        st.caption(f"📅 Período: {d_ini_fut.strftime('%d/%m/%Y')} a {d_fim_fut.strftime('%d/%m/%Y')} — preenchido automaticamente")
        if ativ_futuras_linhas:
            for linha in ativ_futuras_linhas:
                st.markdown(linha)
        else:
            st.info("Nenhuma atividade encontrada no período.")

    with st.expander("7. SU", expanded=False):
        st.caption("Digite um item por linha — a numeração será automática")
        su_raw = st.text_area("SU:", placeholder="Ex:\nS/A\nS/A", height=120,
                              key="su_texto", label_visibility="collapsed")
        if su_raw.strip():
            st.markdown("**Preview:**")
            for i, linha in enumerate(su_raw.strip().split("\n"), 1):
                if linha.strip():
                    st.markdown(f"{i}. {linha.strip()}")

    with st.expander("8. ATIVIDADES PLANEJADAS E NÃO EXECUTADAS", exposed=False):
        st.caption("Digite uma atividade por linha — a numeração será automática")
        ativ_nao_exec_raw = st.text_area("Atividades não executadas:",
                                          placeholder="Ex:\nReu componentes ASA",
                                          height=150, key="ativ_nao_exec",
                                          label_visibility="collapsed")
        if ativ_nao_exec_raw.strip():
            st.markdown("**Preview:**")
            for i, linha in enumerate(ativ_nao_exec_raw.strip().split("\n"), 1):
                if linha.strip():
                    st.markdown(f"{i}. {linha.strip()}")

except Exception as e:
    st.error(f"❌ Erro no sistema: {e}")
    registrar_log("ERRO_SISTEMA", str(e))
    import traceback
    with st.expander("Ver detalhes do erro"):
        st.code(traceback.format_exc())
