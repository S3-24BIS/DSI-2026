import datetime
import re
import io
import json
import time
from concurrent.futures import ThreadPoolExecutor

import streamlit as st
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

# =========================================================
# CONFIGURAÇÕES
# =========================================================

SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive.file",
]

IDS = {
    "s3": "s3.24bis03@gmail.com",
    "cmt": "comando24bis@gmail.com",
    "pgi": "915a351ec7e277234d1da0e597fb14c7455f6f1a5a05eea8de837095a6e70c9e@group.calendar.google.com",
    "cursos": "38d1be36abd6b1e2545500964d51074f66d24c36530a3ff677ef21b6b332f003@group.calendar.google.com",
    "datas":  "c9905256a40d19cc4d9954f633783c1ee96f6ad70165b5b7800b63e31ceeef1f@group.calendar.google.com",
    "si":   "d140cd6bbf50cb6e5754222732d27f20e9ee833aca680475c0f1f34e0df74fa0@group.calendar.google.com",
    "fase": "ac05541df4fd8c2dff7eeebe910442a84fd43a9ade0a8699b1d96cf6e2986d1e@group.calendar.google.com",
    "operacoes": "a253be647f9dd8c1b044f0e89643a569d95cbd9054f4eb8401c373a4cb2dd667@group.calendar.google.com",
}

MEU_EMAIL = IDS["s3"]

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
    """Registra ações no console (sem arquivo local na nuvem)"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {acao} - {detalhes}")

# =========================================================
# AUTH - ADAPTADA PARA NUVEM (OAuth via redirect)
# =========================================================

def get_credentials():
    """
    Autenticação OAuth adaptada para Streamlit Cloud.
    Usa redirect_uri em vez de servidor local.
    Credenciais ficam nos Secrets do Streamlit.
    """
    creds = None

    # 1. Tenta usar token salvo na sessão
    if "token_data" in st.session_state:
        try:
            creds = Credentials.from_authorized_user_info(
                st.session_state.token_data, SCOPES
            )
        except Exception:
            creds = None

    # 2. Renova token expirado se tiver refresh_token
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            st.session_state.token_data = json.loads(creds.to_json())
            return creds
        except Exception:
            creds = None

    # 3. Token válido — usa diretamente
    if creds and creds.valid:
        return creds

    # 4. Carrega configurações do OAuth dos Secrets
    try:
        client_config = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
        redirect_uri = st.secrets["REDIRECT_URI"]
    except Exception as e:
        st.error("❌ Secrets do Google não configurados. Veja as instruções de deploy.")
        st.info("Configure GOOGLE_CREDENTIALS e REDIRECT_URI nos Secrets do Streamlit Cloud.")
        st.stop()

    # 5. Verifica se voltou do login Google com o código de autorização
    params = st.query_params
    if "code" in params:
        try:
            flow = Flow.from_client_config(
                client_config,
                scopes=SCOPES,
                redirect_uri=redirect_uri
            )
            flow.fetch_token(code=params["code"])
            creds = flow.credentials
            st.session_state.token_data = json.loads(creds.to_json())
            st.query_params.clear()
            st.rerun()
        except Exception as e:
            st.error(f"❌ Erro ao processar login: {e}")
            st.stop()

    # 6. Redireciona para página de login do Google
    flow = Flow.from_client_config(
        client_config,
        scopes=SCOPES,
        redirect_uri=redirect_uri
    )
    auth_url, _ = flow.authorization_url(
        prompt="consent",
        access_type="offline"
    )

    st.markdown("## 🔐 Autenticação necessária")
    st.markdown("Clique no link abaixo para fazer login com sua conta Google:")
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

    items = []
    page_token = None
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
    return items

@st.cache_data(ttl=300)
def buscar_eventos_cached(_service, calendar_id: str, d_ini: datetime.date, d_fim: datetime.date):
    return list_events(_service, calendar_id, d_ini, d_fim)

def carregar_todos_eventos_paralelo(srv, d_ini, d_fim):
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(list_events, srv, IDS[cal], d_ini, d_fim): cal
            for cal in ["s3", "cmt", "pgi", "cursos", "datas", "si", "fase", "operacoes"]
        }

        resultados = {}
        for future in futures:
            cal = futures[future]
            try:
                resultados[cal] = future.result()
            except Exception as e:
                st.warning(f"⚠️ Erro ao carregar calendário {cal}: {e}")
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
    end = ev.get("end", {})

    if "date" in start:
        s = datetime.date.fromisoformat(start["date"])
        e = datetime.date.fromisoformat(end["date"])
        return s, e, True, "D"

    sdt = start.get("dateTime")
    edt = end.get("dateTime")
    if sdt and edt:
        s_date = datetime.date.fromisoformat(sdt[:10])
        e_date = datetime.date.fromisoformat(edt[:10])
        hora = sdt[11:16] if "T" in sdt else ""
        return s_date, e_date, False, hora

    return None, None, False, ""

def event_intersects_day(ev, day: datetime.date) -> bool:
    s_date, e_date, is_all_day, _ = parse_start_end(ev)
    if s_date is None or e_date is None:
        return False

    if is_all_day:
        return (s_date <= day) and (day < e_date)

    return (s_date <= day) and (day <= e_date)

# =========================================================
# SI / FASE
# =========================================================

def extrair_si_texto(texto: str):
    if not texto:
        return None
    texto_limpo = re.sub(r'[^\w\s-]', ' ', texto)

    if re.search(r'\bSN\b', texto_limpo, flags=re.IGNORECASE):
        return "SN"

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
    d_antes_s = d_ini_s - datetime.timedelta(days=3)
    d_depois_s = d_fim_s + datetime.timedelta(days=3)
    evs_s = list_events(service, IDS["si"], d_antes_s, d_depois_s)

    si_s = None
    melhor_overlap_s = 0

    for ev in evs_s:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if s_date and e_date:
            if is_all_day:
                e_date_inclusivo = e_date - datetime.timedelta(days=1)
            else:
                e_date_inclusivo = e_date

            overlap_start = max(s_date, d_ini_s)
            overlap_end = min(e_date_inclusivo, d_fim_s)

            if overlap_start <= overlap_end:
                dias_overlap = (overlap_end - overlap_start).days + 1

                summary = ev.get("summary", "") or ""
                desc = ev.get("description", "") or ""
                location = ev.get("location", "") or ""
                texto_completo = f"{summary} {desc} {location}"
                si = extrair_si_texto(texto_completo)

                if si and dias_overlap > melhor_overlap_s:
                    melhor_overlap_s = dias_overlap
                    si_s = si

    d_antes_s1 = d_ini_s1 - datetime.timedelta(days=3)
    d_depois_s1 = d_fim_s1 + datetime.timedelta(days=3)
    evs_s1 = list_events(service, IDS["si"], d_antes_s1, d_depois_s1)

    si_s1 = None
    melhor_overlap_s1 = 0

    for ev in evs_s1:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if s_date and e_date:
            if is_all_day:
                e_date_inclusivo = e_date - datetime.timedelta(days=1)
            else:
                e_date_inclusivo = e_date

            overlap_start = max(s_date, d_ini_s1)
            overlap_end = min(e_date_inclusivo, d_fim_s1)

            if overlap_start <= overlap_end:
                dias_overlap = (overlap_end - overlap_start).days + 1

                summary = ev.get("summary", "") or ""
                desc = ev.get("description", "") or ""
                location = ev.get("location", "") or ""
                texto_completo = f"{summary} {desc} {location}"
                si = extrair_si_texto(texto_completo)

                if si and dias_overlap > melhor_overlap_s1:
                    melhor_overlap_s1 = dias_overlap
                    si_s1 = si

    if si_s and si_s1:
        return f"{si_s}/{si_s1}"
    elif si_s:
        return f"{si_s}/-1"
    elif si_s1:
        return f"-2/{si_s1}"
    else:
        return "-2/-1"

def buscar_fase(service, d_ini_s, d_fim_s1):
    d_antes = d_ini_s - datetime.timedelta(days=3)
    d_depois = d_fim_s1 + datetime.timedelta(days=3)

    evs = list_events(service, IDS["fase"], d_antes, d_depois)

    melhor_fase = None
    melhor_overlap = 0

    for ev in evs:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)
        if s_date and e_date:
            if is_all_day:
                e_date_inclusivo = e_date - datetime.timedelta(days=1)
            else:
                e_date_inclusivo = e_date

            overlap_start = max(s_date, d_ini_s)
            overlap_end = min(e_date_inclusivo, d_fim_s1)

            if overlap_start <= overlap_end:
                dias_overlap = (overlap_end - overlap_start).days + 1

                summary = ev.get("summary", "") or ""
                desc = ev.get("description", "") or ""
                location = ev.get("location", "") or ""
                texto_completo = f"{summary} {desc} {location}"
                fase = extrair_fase_texto(texto_completo)

                if fase and dias_overlap > melhor_overlap:
                    melhor_overlap = dias_overlap
                    melhor_fase = fase

    return melhor_fase

# =========================================================
# FUNÇÃO: BUSCAR OPERAÇÕES
# =========================================================

def buscar_operacoes(service, d_ini_s, d_fim_s1):
    d_busca_ini = d_ini_s - datetime.timedelta(days=365)
    d_busca_fim = d_fim_s1 + datetime.timedelta(days=30)

    evs = list_events(service, IDS["operacoes"], d_busca_ini, d_busca_fim)

    operacoes_ativas = []

    for ev in evs:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)

        if not s_date:
            continue

        if is_all_day and e_date:
            e_date = e_date - datetime.timedelta(days=1)

        evento_ativo = (s_date <= d_fim_s1) and (e_date >= d_ini_s)

        if evento_ativo:
            summary = limpar_texto(ev.get("summary", "")).strip()

            if summary:
                tipo_match = re.search(r'\(([^)]+)\)', summary)
                tipo = ""
                nome_operacao = summary

                if tipo_match:
                    tipo = tipo_match.group(1).strip().upper()
                    nome_operacao = re.sub(r'\s*\([^)]+\)', '', summary).strip()

                operacoes_ativas.append({
                    'nome': nome_operacao,
                    'tipo': tipo,
                    'data_inicio': s_date
                })

    operacoes_unicas = {}
    for op in operacoes_ativas:
        chave = f"{op['nome']}_{op['tipo']}"
        if chave not in operacoes_unicas:
            operacoes_unicas[chave] = op

    operacoes_ordenadas = sorted(operacoes_unicas.values(), key=lambda x: x['data_inicio'])

    linhas_formatadas = []
    for i, op in enumerate(operacoes_ordenadas, 1):
        if op['tipo']:
            linha = f" {i}) {op['nome']} ({op['tipo']}) - __ Militares - _____________"
        else:
            linha = f" {i}) {op['nome']} - __ Militares - _____________"
        linhas_formatadas.append(linha)

    return linhas_formatadas

# =========================================================
# BULLETS E FERIADOS
# =========================================================

def bullets_periodo(service, calendar_id: str, d_ini: datetime.date, d_fim: datetime.date, incluir_responsavel: bool = False):
    d_busca_ini = d_ini - datetime.timedelta(days=365)
    d_busca_fim = d_fim + datetime.timedelta(days=30)

    evs = list_events(service, calendar_id, d_busca_ini, d_busca_fim)
    linhas = []

    for ev in evs:
        s_date, e_date, is_all_day, _ = parse_start_end(ev)

        if not s_date:
            continue

        if is_all_day and e_date:
            e_date = e_date - datetime.timedelta(days=1)

        evento_ativo = (s_date <= d_fim) and (e_date >= d_ini)

        if evento_ativo:
            s = limpar_texto(ev.get("summary", "")).strip()
            if s:
                ano_inicio = str(s_date.year)[-2:]
                data_fmt = f"{s_date.day:02d} {formatar_mes_abreviado(s_date)} {ano_inicio}"

                if e_date and e_date != s_date:
                    ano_fim = str(e_date.year)[-2:]
                    data_fim_fmt = f"{e_date.day:02d} {formatar_mes_abreviado(e_date)} {ano_fim}"
                    texto = f"{data_fmt} a {data_fim_fmt} - {s}"
                else:
                    texto = f"{data_fmt} - {s}"

                if incluir_responsavel:
                    texto += " - _____________"

                linhas.append((s_date, texto))

    linhas.sort(key=lambda x: x[0])

    seen = set()
    out = []
    for _, texto in linhas:
        if texto not in seen:
            out.append(texto)
            seen.add(texto)
    return out

def buscar_feriados(service, d_ini: datetime.date, d_fim: datetime.date):
    evs = list_events(service, IDS["datas"], d_ini, d_fim)
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
# TABELAS
# =========================================================

def construir_tabela_semana(service, d_ini, d_fim, incluir_cmt, incluir_pgi, feriados):
    ev_s3 = list_events(service, IDS["s3"], d_ini, d_fim)

    ev_cmd = []
    if incluir_cmt:
        ev_cmd = list_events(service, IDS["cmt"], d_ini, d_fim)

    ev_pgi = []
    if incluir_pgi:
        ev_pgi = list_events(service, IDS["pgi"], d_ini, d_fim)

    evs = dedup_by_event_id(ev_s3 + ev_cmd + ev_pgi)

    rows = []
    cur = d_ini
    while cur <= d_fim:
        evs_dia = [e for e in evs if event_intersects_day(e, cur)]
        evs_dia.sort(key=lambda x: x.get("start", {}).get("dateTime", x.get("start", {}).get("date", "")))

        eh_especial = eh_fim_de_semana(cur) or cur in feriados

        if not evs_dia:
            rows.append({
                "DATA": fmt_data_coluna(cur),
                "HORA": "",
                "ATIVIDADE": "",
                "LOCAL": "",
                "UNIF": "",
                "RESP": "",
                "OBS": "",
                "_especial": eh_especial
            })
        else:
            for i, e in enumerate(evs_dia):
                start = e.get("start", {})
                data_iso = start.get("dateTime", start.get("date", ""))
                hora = data_iso[11:16] if "T" in data_iso else "D"

                atividade = limpar_texto(e.get("summary", "S/T"))
                local = limpar_texto(e.get("location", ""))

                resp = "S3"
                if e.get("_src_calendar_id") == IDS["cmt"]:
                    resp = "Cmdo"
                elif e.get("_src_calendar_id") == IDS["pgi"]:
                    resp = "PGI"

                rows.append({
                    "DATA": fmt_data_coluna(cur) if i == 0 else "",
                    "HORA": hora,
                    "ATIVIDADE": atividade,
                    "LOCAL": local,
                    "UNIF": "",
                    "RESP": resp,
                    "OBS": "",
                    "_especial": eh_especial if i == 0 else False
                })

        cur += datetime.timedelta(days=1)

    return rows

# =========================================================
# EXPORTAÇÃO
# =========================================================

def exportar_excel(rows_s, rows_s1, num_fmt, si, fase, operacoes_linhas):
    try:
        output = io.BytesIO()

        operacoes_texto = "\n".join(operacoes_linhas) if operacoes_linhas else "-"

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_s = pd.DataFrame(rows_s).drop(columns=['_especial'], errors='ignore')
            df_s1 = pd.DataFrame(rows_s1).drop(columns=['_especial'], errors='ignore')

            df_s.to_excel(writer, sheet_name='Semana S', index=False)
            df_s1.to_excel(writer, sheet_name='Semana S+1', index=False)

            info_df = pd.DataFrame({
                'Campo': ['Número DSI', 'SI', 'FASE', 'Operações'],
                'Valor': [num_fmt, si, fase, operacoes_texto]
            })
            info_df.to_excel(writer, sheet_name='Info', index=False)

        return output.getvalue()
    except ImportError:
        output = io.StringIO()
        df_s = pd.DataFrame(rows_s).drop(columns=['_especial'], errors='ignore')
        df_s1 = pd.DataFrame(rows_s1).drop(columns=['_especial'], errors='ignore')
        operacoes_texto = "\n".join(operacoes_linhas) if operacoes_linhas else "-"
        output.write(f"DSI Nº {num_fmt} - SI {si} - FASE {fase}\n")
        output.write(f"Operações:\n{operacoes_texto}\n\n")
        output.write("=== SEMANA S ===\n")
        output.write(df_s.to_csv(index=False))
        output.write("\n=== SEMANA S+1 ===\n")
        output.write(df_s1.to_csv(index=False))
        return output.getvalue().encode('utf-8')

def salvar_historico(num_dsi: int, periodo: str, doc_id: str):
    """Salva histórico na session_state (sem arquivo local na nuvem)"""
    try:
        if "historico" not in st.session_state:
            st.session_state.historico = []

        st.session_state.historico.append({
            "numero": num_dsi,
            "periodo": periodo,
            "doc_id": doc_id,
            "data_criacao": datetime.datetime.now().isoformat()
        })
        registrar_log("HISTORICO_SALVO", f"DSI {num_dsi}")
    except Exception as e:
        st.warning(f"⚠️ Não foi possível salvar histórico: {e}")

# =========================================================
# EXPORTAR PARA GOOGLE DOCS
# =========================================================

def criar_google_doc(creds, titulo_doc, num_fmt, ref_date, ini_s, fim_s, ini_s1, fim_s1,
                      si, fase, operacoes_linhas, bullets_cursos, bullets_datas, rows_s, rows_s1,
                      fg=None, ativ_futuras="", su="", ativ_nao_exec=""):
    if fg is None:
        fg = {"finalidade": "", "dia": "", "dobrado": "", "cancao": "", "gs": "", "armado": ""}
    docs_service = build('docs', 'v1', credentials=creds)

    doc = docs_service.documents().create(body={'title': titulo_doc}).execute()
    doc_id = doc['documentId']

    hoje = datetime.date.today()

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
    if operacoes_linhas:
        for linha in operacoes_linhas:
            conteudo.append(linha)
    else:
        conteudo.append("-")
    conteudo.append("")

    conteudo.append("2. CURSOS E ESTÁGIOS")
    if bullets_cursos:
        for i, b in enumerate(bullets_cursos, 1):
            conteudo.append(f" {i}) {b}")
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

    conteudo.append("4. PERÍODO")
    conteudo.append("")
    conteudo.append(f" a. Semana (S) - {fmt_periodo_titulo(ini_s, fim_s)}")
    conteudo.append("")

    texto_completo = "\n".join(conteudo)

    requests = [{
        'insertText': {
            'location': {'index': 1},
            'text': texto_completo
        }
    }]

    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={'requests': requests}
    ).execute()

    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual.get('body').get('content')[-1].get('endIndex')

    inserir_e_preencher_tabela(docs_service, doc_id, rows_s, end_index - 1)

    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual.get('body').get('content')[-1].get('endIndex')

    texto_s1 = f"\n b. 2. Semana (S+1) - {fmt_periodo_titulo(ini_s1, fim_s1)}\n"

    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={'requests': [{'insertText': {'location': {'index': end_index - 1}, 'text': texto_s1}}]}
    ).execute()

    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual.get('body').get('content')[-1].get('endIndex')

    inserir_e_preencher_tabela(docs_service, doc_id, rows_s1, end_index - 1)

    doc_atual = docs_service.documents().get(documentId=doc_id).execute()
    end_index = doc_atual.get('body').get('content')[-1].get('endIndex')

    conteudo_futuras = []

    # Item 5: FORMATURA GERAL
    conteudo_futuras.append("\n5. FORMATURA GERAL")
    conteudo_futuras.append(f" 1) Finalidade: {fg.get('finalidade', '')}")
    conteudo_futuras.append(f" 2) Dia: {fg.get('dia', '')}")
    conteudo_futuras.append(f" 3) Dobrado: {fg.get('dobrado', '')}")
    conteudo_futuras.append(f" 4) Canção: {fg.get('cancao', '')}")
    conteudo_futuras.append(f" 5) GS: {fg.get('gs', '')}")
    conteudo_futuras.append(f" 6) Armado e Equipado: {fg.get('armado', '')}")
    conteudo_futuras.append("")

    # Item 6: ATIVIDADES FUTURAS (autonumerado)
    conteudo_futuras.append("6. ATIVIDADES FUTURAS")
    if ativ_futuras.strip():
        for i, linha in enumerate([l for l in ativ_futuras.strip().split("\n") if l.strip()], 1):
            conteudo_futuras.append(f" {i}. {linha.strip()}")
    else:
        conteudo_futuras.append(" ________________________________________________")
    conteudo_futuras.append("")

    # Item 7: SU (autonumerado)
    conteudo_futuras.append("7. SU")
    if su.strip():
        for i, linha in enumerate([l for l in su.strip().split("\n") if l.strip()], 1):
            conteudo_futuras.append(f" {i}. {linha.strip()}")
    else:
        conteudo_futuras.append(" 1. ______________________________________")
    conteudo_futuras.append("")

    # Item 8: ATIVIDADES PLANEJADAS E NÃO EXECUTADAS (autonumerado)
    conteudo_futuras.append("8. ATIVIDADES PLANEJADAS E NÃO EXECUTADAS")
    if ativ_nao_exec.strip():
        for i, linha in enumerate([l for l in ativ_nao_exec.strip().split("\n") if l.strip()], 1):
            conteudo_futuras.append(f" {i}. {linha.strip()}")
    else:
        conteudo_futuras.append(" ________________________________________________")
    conteudo_futuras.append("")

    meses_completos = [
        "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
    ]
    mes_completo = meses_completos[hoje.month - 1]
    data_assinatura = f"São Luís, MA, {hoje.day} de {mes_completo} de {hoje.year}"

    conteudo_futuras.append(f"{data_assinatura}\n\n\n\nJOÃO CARLOS DUQUE – Ten Cel\nComandante do 24º Batalhão de Infantaria de Selva\n")

    texto_futuras = "\n".join(conteudo_futuras)

    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={'requests': [{'insertText': {'location': {'index': end_index - 1}, 'text': texto_futuras}}]}
    ).execute()

    formatar_documento_completo(docs_service, doc_id, rows_s, rows_s1)

    return doc_id


def inserir_e_preencher_tabela(docs_service, doc_id, rows, insert_index):
    requests_tabela = [{
        'insertTable': {
            'rows': len(rows) + 1,
            'columns': 7,
            'location': {'index': insert_index}
        }
    }]

    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={'requests': requests_tabela}
    ).execute()

    time.sleep(0.8)

    def get_ultima_tabela():
        doc = docs_service.documents().get(documentId=doc_id).execute()
        content = doc.get('body').get('content')
        for element in reversed(content):
            if 'table' in element:
                return element.get('table')
        return None

    tabela = get_ultima_tabela()
    if not tabela:
        print("Erro: Tabela não encontrada")
        return

    grupos_data = {}
    data_atual = None
    for idx, row_data in enumerate(rows):
        data = row_data.get("DATA", "")
        if data:
            data_atual = data
            grupos_data[data_atual] = []
        if data_atual:
            grupos_data[data_atual].append(idx)

    all_requests = []

    for row_idx in range(len(rows) - 1, -1, -1):
        if row_idx + 1 >= len(tabela.get('tableRows')):
            continue

        linha_tabela = tabela.get('tableRows')[row_idx + 1]
        row_data = rows[row_idx]

        cols = ["DATA", "HORA", "ATIVIDADE", "LOCAL", "UNIF", "RESP", "OBS"]

        for col_idx in range(6, -1, -1):
            if col_idx < len(linha_tabela.get('tableCells')):
                celula = linha_tabela.get('tableCells')[col_idx]
                cell_content = celula.get('content')

                if cell_content and len(cell_content) > 0:
                    start_idx = cell_content[0].get('startIndex')

                    if start_idx is not None:
                        valor = row_data.get(cols[col_idx], "")

                        if pd.isna(valor) if isinstance(valor, float) else (valor is None or str(valor).strip() == ''):
                            texto = ""
                        else:
                            texto = str(valor).strip()

                        if texto:
                            all_requests.append({
                                'insertText': {
                                    'location': {'index': start_idx},
                                    'text': texto
                                }
                            })

    headers = ["DATA", "HORA", "ATIVIDADE", "LOCAL", "UNIF", "RESP", "OBS"]
    primeira_linha = tabela.get('tableRows')[0]

    for i in range(6, -1, -1):
        if i < len(primeira_linha.get('tableCells')):
            celula = primeira_linha.get('tableCells')[i]
            cell_content = celula.get('content')
            if cell_content and len(cell_content) > 0:
                start_idx = cell_content[0].get('startIndex')
                if start_idx is not None:
                    all_requests.append({
                        'insertText': {
                            'location': {'index': start_idx},
                            'text': headers[i]
                        }
                    })

    if all_requests:
        try:
            batch_size = 100
            for i in range(0, len(all_requests), batch_size):
                batch = all_requests[i:i+batch_size]
                docs_service.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': batch}
                ).execute()
                time.sleep(0.2)
        except Exception as e:
            print(f"Erro ao preencher tabela: {e}")

    time.sleep(0.5)
    aplicar_formatacao_tabela(docs_service, doc_id, rows, grupos_data)


def aplicar_formatacao_tabela(docs_service, doc_id, rows, grupos_data):
    doc = docs_service.documents().get(documentId=doc_id).execute()
    content = doc.get('body').get('content')

    tabela_element = None
    for element in reversed(content):
        if 'table' in element:
            tabela_element = element
            break

    if not tabela_element:
        return

    tabela = tabela_element['table']
    table_start = tabela_element['startIndex']

    requests = []

    for row_idx in range(len(tabela.get('tableRows', []))):
        for col_idx in range(7):
            requests.append({
                'updateTableCellStyle': {
                    'tableRange': {
                        'tableCellLocation': {
                            'tableStartLocation': {'index': table_start},
                            'rowIndex': row_idx,
                            'columnIndex': col_idx
                        },
                        'rowSpan': 1,
                        'columnSpan': 1
                    },
                    'tableCellStyle': {
                        'borderTop': {'color': {'color': {'rgbColor': {'red': 0, 'green': 0, 'blue': 0}}}, 'width': {'magnitude': 1, 'unit': 'PT'}, 'dashStyle': 'SOLID'},
                        'borderBottom': {'color': {'color': {'rgbColor': {'red': 0, 'green': 0, 'blue': 0}}}, 'width': {'magnitude': 1, 'unit': 'PT'}, 'dashStyle': 'SOLID'},
                        'borderLeft': {'color': {'color': {'rgbColor': {'red': 0, 'green': 0, 'blue': 0}}}, 'width': {'magnitude': 1, 'unit': 'PT'}, 'dashStyle': 'SOLID'},
                        'borderRight': {'color': {'color': {'rgbColor': {'red': 0, 'green': 0, 'blue': 0}}}, 'width': {'magnitude': 1, 'unit': 'PT'}, 'dashStyle': 'SOLID'}
                    },
                    'fields': 'borderTop,borderBottom,borderLeft,borderRight'
                }
            })

    for col_idx in range(7):
        requests.append({
            'updateTableCellStyle': {
                'tableRange': {
                    'tableCellLocation': {
                        'tableStartLocation': {'index': table_start},
                        'rowIndex': 0,
                        'columnIndex': col_idx
                    },
                    'rowSpan': 1,
                    'columnSpan': 1
                },
                'tableCellStyle': {
                    'backgroundColor': {'color': {'rgbColor': {'red': 0.4, 'green': 0.4, 'blue': 0.4}}}
                },
                'fields': 'backgroundColor'
            }
        })

    for data, indices in grupos_data.items():
        if len(indices) > 1:
            start_row = indices[0] + 1
            requests.append({
                'mergeTableCells': {
                    'tableRange': {
                        'tableCellLocation': {
                            'tableStartLocation': {'index': table_start},
                            'rowIndex': start_row,
                            'columnIndex': 0
                        },
                        'rowSpan': len(indices),
                        'columnSpan': 1
                    }
                }
            })

    cor_alternada = True
    for data, indices in grupos_data.items():
        eh_dia_especial = any(rows[idx].get('_especial', False) for idx in indices)

        if eh_dia_especial:
            cor = {'red': 1.0, 'green': 0.8, 'blue': 0.8}
        else:
            cor = {'red': 0.85, 'green': 0.85, 'blue': 0.85} if cor_alternada else {'red': 1.0, 'green': 1.0, 'blue': 1.0}

        for idx in indices:
            row_idx = idx + 1
            for col_idx in range(7):
                requests.append({
                    'updateTableCellStyle': {
                        'tableRange': {
                            'tableCellLocation': {
                                'tableStartLocation': {'index': table_start},
                                'rowIndex': row_idx,
                                'columnIndex': col_idx
                            },
                            'rowSpan': 1,
                            'columnSpan': 1
                        },
                        'tableCellStyle': {
                            'backgroundColor': {'color': {'rgbColor': cor}}
                        },
                        'fields': 'backgroundColor'
                    }
                })

        if not eh_dia_especial:
            cor_alternada = not cor_alternada

    for row_idx in range(0, len(rows) + 1):
        if row_idx < len(tabela.get('tableRows', [])):
            row_cells = tabela.get('tableRows')[row_idx].get('tableCells', [])
            for col_idx in range(len(row_cells)):
                cell_content = row_cells[col_idx].get('content', [])
                if cell_content:
                    requests.append({
                        'updateParagraphStyle': {
                            'paragraphStyle': {'alignment': 'CENTER'},
                            'fields': 'alignment',
                            'range': {
                                'startIndex': cell_content[0].get('startIndex'),
                                'endIndex': cell_content[0].get('endIndex') - 1
                            }
                        }
                    })

    if requests:
        try:
            batch_size = 50
            for i in range(0, len(requests), batch_size):
                batch = requests[i:i+batch_size]
                docs_service.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': batch}
                ).execute()
                time.sleep(0.3)
        except Exception as e:
            print(f"Erro ao aplicar formatação: {e}")

    time.sleep(0.5)
    doc = docs_service.documents().get(documentId=doc_id).execute()
    content = doc.get('body').get('content')

    tabela_element = None
    for element in reversed(content):
        if 'table' in element:
            tabela_element = element
            break

    if tabela_element:
        tabela = tabela_element['table']
        primeira_linha = tabela.get('tableRows')[0]

        requests_negrito = []
        for col_idx in range(7):
            if col_idx < len(primeira_linha.get('tableCells')):
                cell_content = primeira_linha.get('tableCells')[col_idx].get('content', [])
                if cell_content and len(cell_content) > 0:
                    start_idx = cell_content[0].get('startIndex')
                    end_idx = cell_content[0].get('endIndex')
                    if start_idx and end_idx and end_idx > start_idx:
                        requests_negrito.append({
                            'updateTextStyle': {
                                'range': {'startIndex': start_idx, 'endIndex': end_idx - 1},
                                'textStyle': {
                                    'bold': True,
                                    'foregroundColor': {'color': {'rgbColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}}}
                                },
                                'fields': 'bold,foregroundColor'
                            }
                        })

        if requests_negrito:
            try:
                docs_service.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': requests_negrito}
                ).execute()
            except Exception as e:
                print(f"Erro ao aplicar negrito: {e}")


def formatar_documento_completo(docs_service, doc_id, rows_s, rows_s1):
    time.sleep(1)

    doc = docs_service.documents().get(documentId=doc_id).execute()
    content = doc.get('body').get('content', [])
    end_index = content[-1].get('endIndex')

    requests = []

    requests.append({
        'updateTextStyle': {
            'range': {'startIndex': 1, 'endIndex': end_index - 1},
            'textStyle': {
                'fontSize': {'magnitude': 12, 'unit': 'PT'},
                'weightedFontFamily': {'fontFamily': 'Calibri'}
            },
            'fields': 'fontSize,weightedFontFamily'
        }
    })

    requests.append({
        'updateDocumentStyle': {
            'documentStyle': {
                'marginTop': {'magnitude': 28.35, 'unit': 'PT'},
                'marginBottom': {'magnitude': 28.35, 'unit': 'PT'},
                'marginLeft': {'magnitude': 28.35, 'unit': 'PT'},
                'marginRight': {'magnitude': 28.35, 'unit': 'PT'}
            },
            'fields': 'marginTop,marginBottom,marginLeft,marginRight'
        }
    })

    texto_completo = ""
    for element in content:
        if 'paragraph' in element:
            paragraph = element['paragraph']
            for elem in paragraph.get('elements', []):
                if 'textRun' in elem:
                    texto_completo += elem['textRun'].get('content', '')

    padroes_negrito = [
        r"1\.\s+OPERAÇÕES:",
        r"2\.\s+CURSOS E ESTÁGIOS",
        r"3\.\s+DATAS COMEMORATIVAS E FERIADOS",
        r"4\.\s+PERÍODO",
        r"5\.\s+FORMATURA GERAL",
        r"6\.\s+ATIVIDADES FUTURAS",
        r"7\.\s+SU",
        r"8\.\s+ATIVIDADES PLANEJADAS E NÃO EXECUTADAS"
    ]

    for padrao in padroes_negrito:
        match = re.search(padrao, texto_completo)
        if match:
            start_pos = len(texto_completo[:match.start()]) + 1
            end_pos = start_pos + len(match.group())

            requests.append({
                'updateTextStyle': {
                    'range': {'startIndex': start_pos, 'endIndex': end_pos},
                    'textStyle': {'bold': True},
                    'fields': 'bold'
                }
            })

    for element in content:
        if 'table' in element:
            table = element['table']
            for row in table.get('tableRows', [])[1:]:
                for cell in row.get('tableCells', []):
                    cell_content = cell.get('content', [])
                    if cell_content:
                        for paragraph in cell_content:
                            if 'paragraph' in paragraph:
                                para = paragraph['paragraph']
                                for elem in para.get('elements', []):
                                    if 'textRun' in elem:
                                        text = elem['textRun'].get('content', '')
                                        if any(dia in text for dia in ["SÁB", "DOM"]):
                                            start = elem.get('startIndex')
                                            end = elem.get('endIndex')
                                            if start and end:
                                                requests.append({
                                                    'updateTextStyle': {
                                                        'range': {'startIndex': start, 'endIndex': end},
                                                        'textStyle': {
                                                            'foregroundColor': {'color': {'rgbColor': {'red': 1.0, 'green': 0.0, 'blue': 0.0}}}
                                                        },
                                                        'fields': 'foregroundColor'
                                                    }
                                                })

    if requests:
        batch_size = 50
        for i in range(0, len(requests), batch_size):
            batch = requests[i:i+batch_size]
            docs_service.documents().batchUpdate(
                documentId=doc_id,
                body={'requests': batch}
            ).execute()
            time.sleep(0.3)


def criar_google_doc_safe(creds, *args, **kwargs):
    max_tentativas = 3

    for tentativa in range(max_tentativas):
        try:
            return criar_google_doc(creds, *args, **kwargs)
        except Exception as e:
            if tentativa < max_tentativas - 1:
                st.warning(f"⚠️ Tentativa {tentativa + 1} falhou. Tentando novamente...")
                registrar_log("ERRO_CRIACAO_DOC", f"Tentativa {tentativa + 1}: {e}")
                time.sleep(2)
            else:
                st.error(f"❌ Erro após {max_tentativas} tentativas: {e}")
                registrar_log("ERRO_CRIACAO_DOC_FINAL", str(e))
                raise

# =========================================================
# INTERFACE STREAMLIT
# =========================================================

st.set_page_config(page_title="DSI 24º BIS", layout="wide")

st.markdown("""
<style>
    .stApp {
        background-color: #f0f2f6;
    }
    .stMarkdown h1 {
        color: #1f4788;
        font-weight: bold;
    }
    div[data-testid="stDataFrame"] {
        border: 2px solid #1f4788;
        border-radius: 5px;
    }
    .success-box {
        padding: 1rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        color: #155724;
    }
</style>
""", unsafe_allow_html=True)

st.title("📋 Diretriz Semanal de Instrução - 24º BIS")

if 'exportar' not in st.session_state:
    st.session_state.exportar = False
if 'doc_criado' not in st.session_state:
    st.session_state.doc_criado = None
if 'historico' not in st.session_state:
    st.session_state.historico = []

try:
    creds = get_credentials()
    srv = build("calendar", "v3", credentials=creds)

    with st.sidebar:
        st.header("⚙️ Parâmetros da DSI")

        num_doc = st.number_input("Nº da DSI / QTS", min_value=1, max_value=999, value=6, step=1)
        num_fmt = f"{int(num_doc):03d}"

        ref_date = st.date_input("Data de referência (para calcular S)", value=datetime.date.today())

        incluir_cmt = st.checkbox("Incluir agenda do Cmt", value=True)
        incluir_pgi = st.checkbox("Incluir agenda PGI 2026", value=True)

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🔄 ATUALIZAR", type="primary", use_container_width=True):
                st.cache_data.clear()
                st.session_state.exportar = False
                st.session_state.doc_criado = None
                registrar_log("ATUALIZACAO", f"DSI {num_fmt}")
                st.rerun()

        with col2:
            exportar_btn = st.button("📄 EXPORTAR DOCS", type="secondary", use_container_width=True)
            if exportar_btn:
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
    ini_s1 = ini_s + datetime.timedelta(days=7)
    fim_s1 = fim_s + datetime.timedelta(days=7)

    if not validar_datas(ini_s, fim_s1):
        st.stop()

    periodo_titulo = fmt_periodo_titulo(ini_s1, fim_s1)
    titulo_dsi = f"DIRETRIZ SEMANAL DE INSTRUÇÃO {num_fmt} ({periodo_titulo})"

    with st.spinner("🔍 Buscando informações dos calendários..."):
        si = buscar_si_duplo(srv, ini_s, fim_s, ini_s1, fim_s1)
        fase = buscar_fase(srv, ini_s, fim_s1) or "Mdd Adm"
        operacoes_linhas = buscar_operacoes(srv, ini_s, fim_s1)

    linha_qts = f"(QTS nº {num_fmt} - SI: {si} - FASE: {fase})"

    with st.expander("🔍 Debug - SI, FASE e OPERAÇÕES"):
        st.write(f"**Período S:** {ini_s.strftime('%d/%m/%Y')} a {fim_s.strftime('%d/%m/%Y')}")
        st.write(f"**Período S+1:** {ini_s1.strftime('%d/%m/%Y')} a {fim_s1.strftime('%d/%m/%Y')}")
        st.write(f"**SI:** {si}")
        st.write(f"**FASE:** {fase}")
        st.write(f"**OPERAÇÕES:** {len(operacoes_linhas)}")
        for op in operacoes_linhas:
            st.write(f"  - {op}")

    bullets_cursos = bullets_periodo(srv, IDS["cursos"], ini_s, fim_s1, incluir_responsavel=True)
    bullets_datas  = bullets_periodo(srv, IDS["datas"],  ini_s, fim_s1)
    feriados = buscar_feriados(srv, ini_s, fim_s1)

    rows_s  = construir_tabela_semana(srv, ini_s,  fim_s,  incluir_cmt, incluir_pgi, feriados)
    rows_s1 = construir_tabela_semana(srv, ini_s1, fim_s1, incluir_cmt, incluir_pgi, feriados)

    if st.session_state.exportar and st.session_state.doc_criado is None:
        fg = {
            "finalidade": st.session_state.get("fg_finalidade", ""),
            "dia": st.session_state.get("fg_dia", ""),
            "dobrado": st.session_state.get("fg_dobrado", ""),
            "cancao": st.session_state.get("fg_cancao", ""),
            "gs": st.session_state.get("fg_gs", ""),
            "armado": st.session_state.get("fg_armado", ""),
        }
        ativ_futuras_txt = st.session_state.get("ativ_futuras", "")
        su_txt = st.session_state.get("su_texto", "")
        ativ_nao_exec_txt = st.session_state.get("ativ_nao_exec", "")

        with st.spinner("📝 Criando documento no Google Docs..."):
            try:
                registrar_log("EXPORTACAO_INICIADA", f"DSI {num_fmt}")
                doc_id = criar_google_doc_safe(
                    creds, titulo_dsi, num_fmt, ref_date,
                    ini_s, fim_s, ini_s1, fim_s1,
                    si, fase, operacoes_linhas, bullets_cursos, bullets_datas,
                    rows_s, rows_s1,
                    fg=fg,
                    ativ_futuras=ativ_futuras_txt,
                    su=su_txt,
                    ativ_nao_exec=ativ_nao_exec_txt
                )

                salvar_historico(int(num_doc), periodo_titulo, doc_id)
                st.session_state.doc_criado = doc_id
                st.session_state.exportar = False
                registrar_log("EXPORTACAO_SUCESSO", f"DSI {num_fmt} - Doc ID: {doc_id}")

            except Exception as e:
                st.error(f"❌ Erro ao criar documento: {e}")
                registrar_log("EXPORTACAO_ERRO", str(e))
                st.session_state.exportar = False
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
        excel_data = exportar_excel(rows_s, rows_s1, num_fmt, si, fase, operacoes_linhas)

        if isinstance(excel_data, bytes) and excel_data[:2] == b'PK':
            file_ext = "xlsx"
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            label = "📊 Baixar Excel (Backup)"
        else:
            file_ext = "csv"
            mime_type = "text/csv"
            label = "📊 Baixar CSV (Backup)"

        st.download_button(
            label=label,
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
    <div style='font-size: 10px; text-align: left;'>
    DSI Nº {num_fmt} - S3/24º BIS<br>
    {hoje.day} {formatar_mes_abreviado(hoje)} {str(hoje.year)[-2:]}<br>
    Visto S3:<br>
    _____________<br>
    Cap PIERROTI
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(f"""
    <div style='text-align: center; font-weight: bold; font-family: Calibri;'>
    MINISTÉRIO DA DEFESA<br>
    EXÉRCITO BRASILEIRO<br>
    24º BATALHÃO DE INFANTARIA DE SELVA<br>
    (9º Batalhão de Caçadores / 1839)<br>
    BATALHÃO BARÃO DE CAXIAS
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(f"<h3 style='text-align: center; font-family: Calibri;'>{titulo_dsi}</h3>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center; font-family: Calibri;'><strong>{linha_qts}</strong></p>", unsafe_allow_html=True)

    st.markdown("**1. OPERAÇÕES:**")
    if operacoes_linhas:
        for linha in operacoes_linhas:
            st.markdown(linha)
    else:
        st.markdown("-")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**2. CURSOS E ESTÁGIOS**")
        if bullets_cursos:
            for i, item in enumerate(bullets_cursos, 1):
                st.markdown(f" {i}) {item}")
        else:
            st.markdown("-")

    with col2:
        st.markdown("**3. DATAS COMEMORATIVAS E FERIADOS**")
        if bullets_datas:
            for item in bullets_datas:
                st.markdown(f" {item}")
        else:
            st.markdown("-")

    # ── ITEM 4: PERÍODO ──
    st.markdown("**4. PERÍODO**")
    st.markdown(f"**a. Semana (S) - {fmt_periodo_titulo(ini_s, fim_s)}**")

    df_s_display = pd.DataFrame(rows_s).drop(columns=['_especial'], errors='ignore')

    def highlight_especial(row):
        idx = row.name
        if idx < len(rows_s) and rows_s[idx].get('_especial', False):
            return ['color: red; text-align: center'] * len(row)
        return ['text-align: center'] * len(row)

    st.dataframe(
        df_s_display.style.apply(highlight_especial, axis=1).set_properties(**{'text-align': 'center'}),
        use_container_width=True,
        height=300
    )

    st.markdown(f"**b. Semana (S+1) - {fmt_periodo_titulo(ini_s1, fim_s1)}**")

    df_s1_display = pd.DataFrame(rows_s1).drop(columns=['_especial'], errors='ignore')

    def highlight_especial_s1(row):
        idx = row.name
        if idx < len(rows_s1) and rows_s1[idx].get('_especial', False):
            return ['color: red; text-align: center'] * len(row)
        return ['text-align: center'] * len(row)

    st.dataframe(
        df_s1_display.style.apply(highlight_especial_s1, axis=1).set_properties(**{'text-align': 'center'}),
        use_container_width=True,
        height=300
    )

    # ── ITEM 5: FORMATURA GERAL (dropdown editável) ──
    with st.expander("5. FORMATURA GERAL", expanded=False):
        fg_finalidade = st.text_input("1) Finalidade:", key="fg_finalidade")
        fg_dia        = st.text_input("2) Dia:", placeholder="ex: 25/02/2026", key="fg_dia")
        fg_dobrado    = st.text_input("3) Dobrado:", key="fg_dobrado")
        fg_cancao     = st.text_input("4) Canção:", key="fg_cancao")
        fg_gs         = st.text_input("5) GS:", key="fg_gs")
        fg_armado     = st.text_input("6) Armado e Equipado:", key="fg_armado")

    # ── ITEM 6: ATIVIDADES FUTURAS (dropdown com autonumeração) ──
    with st.expander("6. ATIVIDADES FUTURAS", expanded=False):
        st.caption("Digite uma atividade por linha — a numeração será automática")
        ativ_futuras_raw = st.text_area(
            "Atividades futuras:",
            placeholder="Ex:\nEAVS - 23 a 27 fev 26\nEstg Plj Op Selva - 23 a 28 fev 26",
            height=200,
            key="ativ_futuras",
            label_visibility="collapsed"
        )
        if ativ_futuras_raw.strip():
            st.markdown("**Preview:**")
            for i, linha in enumerate(ativ_futuras_raw.strip().split("\n"), 1):
                if linha.strip():
                    st.markdown(f"{i}. {linha.strip()}")

    # ── ITEM 7: SU (dropdown com autonumeração) ──
    with st.expander("7. SU", expanded=False):
        st.caption("Digite um item por linha — a numeração será automática")
        su_raw = st.text_area(
            "SU:",
            placeholder="Ex:\nS/A\nS/A\nS/A",
            height=120,
            key="su_texto",
            label_visibility="collapsed"
        )
        if su_raw.strip():
            st.markdown("**Preview:**")
            for i, linha in enumerate(su_raw.strip().split("\n"), 1):
                if linha.strip():
                    st.markdown(f"{i}. {linha.strip()}")

    # ── ITEM 8: ATIVIDADES PLANEJADAS E NÃO EXECUTADAS (dropdown com autonumeração) ──
    with st.expander("8. ATIVIDADES PLANEJADAS E NÃO EXECUTADAS", expanded=False):
        st.caption("Digite uma atividade por linha — a numeração será automática")
        ativ_nao_exec_raw = st.text_area(
            "Atividades não executadas:",
            placeholder="Ex:\nReu componentes ASA\nSimpósio Gerenciamento de Crises",
            height=150,
            key="ativ_nao_exec",
            label_visibility="collapsed"
        )
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
