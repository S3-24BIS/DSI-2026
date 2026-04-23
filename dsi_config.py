"""
Configuração centralizada para DSI-2026
Remove hardcodes e permite diferentes ambientes
"""

import os
from typing import Dict

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

import streamlit as st

# Estados-Maiores
ESTADO_MAIOR = {
    "s3": os.getenv("DSI_S3_EMAIL", "s3.24bis03@gmail.com"),
    "cmt": os.getenv("DSI_CMT_EMAIL", "comando24bis@gmail.com"),
    "cmdo": os.getenv("DSI_CMDO_EMAIL", "cmdo24bis@gmail.com"),
    "sub_cmt": os.getenv("DSI_SUB_CMT_EMAIL", "subcomandante.24bis@gmail.com"),
    "adj_cmdo": os.getenv("DSI_ADJ_CMDO_EMAIL", "gleysonsmelo141214@gmail.com"),
}

# Seções
SECOES = {
    "sec_1": os.getenv("DSI_SEC1_EMAIL", "primeira1secao@gmail.com"),
    "sec_4": os.getenv("DSI_SEC4_EMAIL", "4secao24bis@gmail.com"),
}

# Companhias
COMPANHIAS = {
    "cia_1": os.getenv("DSI_CIA1_EMAIL", "1cia.24bis@gmail.com"),
    "cia_1b": os.getenv("DSI_CIA1B_EMAIL", "gurupi1cia@gmail.com"),
    "cia_1_sgt": os.getenv("DSI_CIA1_SGT_EMAIL", "sargenteacaogurupi@gmail.com"),
    "cia_2": os.getenv("DSI_CIA2_EMAIL", "jfsa2017@gmail.com"),
    "cia_2b": os.getenv("DSI_CIA2B_EMAIL", "timbira2cia@gmail.com"),
    "cia_2_sgt": os.getenv("DSI_CIA2_SGT_EMAIL", "sgtetimbira@gmail.com"),
}

# Órgãos de Apoio
ORGAOS_APOIO = {
    "b_mus": os.getenv("DSI_BMUS_EMAIL", "bmus24bis@gmail.com"),
    "npor": os.getenv("DSI_NPOR_EMAIL", "npor.24bis.instrutor@gmail.com"),
    "npor_ste": os.getenv("DSI_NPOR_STE_EMAIL", "allissonfeitosa1985@gmail.com"),
    "ass_jur": os.getenv("DSI_ASSJUR_EMAIL", "assjur.24bis@gmail.com"),
    "brigada": os.getenv("DSI_BRIGADA_EMAIL", "brigada24bis@gmail.com"),
    "fisc_adm": os.getenv("DSI_FISC_ADM_EMAIL", "capmarcusvinicius.40bi@gmail.com"),
    "chales": os.getenv("DSI_CHALES_EMAIL", "cmslchales@gmail.com"),
    "com_soc": os.getenv("DSI_COM_SOC_EMAIL", "comsoc24bis@gmail.com"),
    "fiscal": os.getenv("DSI_FISCAL_EMAIL", "fiscal160105@gmail.com"),
    "prm": os.getenv("DSI_PRM_EMAIL", "prmsaoluisma@gmail.com"),
    "sfpc": os.getenv("DSI_SFPC_EMAIL", "sfpc24bis@gmail.com"),
}

# Calendários de Grupo - PADRÕES
CALENDARIOS_PADRAO = {
    "pgi": "915a351ec7e277234d1da0e597fb14c7455f6f1a5a05eea8de837095a6e70c9e@group.calendar.google.com",
    "cursos": "38d1be36abd6b1e2545500964d51074f66d24c36530a3ff677ef21b6b332f003@group.calendar.google.com",
    "datas": "c9905256a40d19cc4d9954f633783c1ee96f6ad70165b5b7800b63e31ceeef1f@group.calendar.google.com",
    "si": "d140cd6bbf50cb6e5754222732d27f20e9ee833aca680475c0f1f34e0df74fa0@group.calendar.google.com",
    "fase": "ac05541df4fd8c2dff7eeebe910442a84fd43a9ade0a8699b1d96cf6e2986d1e@group.calendar.google.com",
    "operacoes": "a253be647f9dd8c1b044f0e89643a569d95cbd9054f4eb8401c373a4cb2dd667@group.calendar.google.com",
    "proj_obras": "ff0f90677f41394c1caebe925fdebda1e69ab47b7d114cbae6a7c8feccaeeef3@group.calendar.google.com",
}

# Carregar IDS do Streamlit Secrets ou usar padrões
try:
    IDS = st.secrets.get("IDS", CALENDARIOS_PADRAO)
    if not IDS:
        IDS = CALENDARIOS_PADRAO
except:
    IDS = CALENDARIOS_PADRAO

# Mapeamento de emails para rótulos
RESP_MAP = {
    **{v: "Cmt" for k, v in ESTADO_MAIOR.items()},
    **{v: "Seção" for k, v in SECOES.items()},
    **{v: "Cia" for k, v in COMPANHIAS.items()},
    **{v: "Apoio" for k, v in ORGAOS_APOIO.items()},
}

# Configurações de app
APP_CONFIG = {
    "page_title": "DSI-2026",
    "page_icon": "🎖️",
    "layout": "wide",
    "initial_sidebar_state": "expanded",
    "menu_items": {
        "Get help": "https://github.com/S3-24BIS/DSI-2026",
        "Report a bug": "https://github.com/S3-24BIS/DSI-2026/issues",
        "About": "# DSI-2026\nSistema de Documentos Integrados do 24º BIS",
    },
}

# Configurações de cache
CACHE_CONFIG = {
    "calendar_ttl_minutes": 5,
    "documents_ttl_minutes": 15,
    "max_age_days": 30,
}

# Configurações de Google APIs
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive.file",
]

def get_all_users() -> Dict[str, str]:
    """Retorna mapa completo de todos os usuários"""
    return {
        **ESTADO_MAIOR,
        **SECOES,
        **COMPANHIAS,
        **ORGAOS_APOIO,
    }

def get_user_email(user_key: str) -> str:
    """Obtém email de um usuário por chave"""
    all_users = get_all_users()
    return all_users.get(user_key, "")
