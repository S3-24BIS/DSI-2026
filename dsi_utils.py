"""
Utilitários melhorados para DSI-2026
- Logging estruturado
- Cache decorator
- Validações
- Rate limiting
"""

import logging
import time
from functools import wraps
from typing import Optional, Callable, Any
from datetime import datetime, timedelta

class DSILogger:
    def __init__(self, name: str = "DSI-2026"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            handler = logging.FileHandler("dsi.log")
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, msg: str, **kwargs):
        self.logger.info(f"[ℹ️] {msg}", extra=kwargs)

    def success(self, msg: str, **kwargs):
        self.logger.info(f"[✅] {msg}", extra=kwargs)

    def warning(self, msg: str, **kwargs):
        self.logger.warning(f"[⚠️] {msg}", extra=kwargs)

    def error(self, msg: str, exc: Optional[Exception] = None, **kwargs):
        if exc:
            self.logger.error(f"[❌] {msg}: {str(exc)}", extra=kwargs, exc_info=True)
        else:
            self.logger.error(f"[❌] {msg}", extra=kwargs)

logger = DSILogger()

def cache_with_ttl(ttl_minutes: int = 5):
    """Decorator para cachear resultado de função com TTL"""
    def decorator(func: Callable) -> Callable:
        cache_key = f"dsi_cache_{func.__name__}"

        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                import streamlit as st
                if cache_key in st.session_state:
                    cached_data, timestamp = st.session_state[cache_key]
                    age = (datetime.now() - timestamp).total_seconds() / 60

                    if age < ttl_minutes:
                        logger.info(f"Cache hit para {func.__name__} ({age:.1f}min old)")
                        return cached_data

                result = func(*args, **kwargs)
                st.session_state[cache_key] = (result, datetime.now())
                logger.success(f"Cache miss - executou {func.__name__}")
                return result
            except Exception as e:
                logger.error(f"Erro ao executar {func.__name__}", exc=e)
                raise

        return wrapper
    return decorator

def validate_email(email: str) -> bool:
    """Valida formato de email"""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_date_range(start_date, end_date) -> bool:
    """Valida intervalo de datas"""
    if start_date >= end_date:
        logger.warning(f"Data inválida: {start_date} >= {end_date}")
        return False
    return True

def validate_calendar_id(calendar_id: str) -> bool:
    """Valida formato de ID de calendário"""
    return len(calendar_id) > 20 and all(c in '0123456789abcdef@.' for c in calendar_id.lower())

class RateLimiter:
    """Limita taxa de requisições à API"""
    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []

    def allow(self) -> bool:
        """Verifica se requisição é permitida"""
        now = time.time()
        cutoff = now - self.window_seconds

        self.requests = [ts for ts in self.requests if ts > cutoff]

        if len(self.requests) >= self.max_requests:
            logger.warning(f"Rate limit atingido: {self.max_requests} req/{self.window_seconds}s")
            return False

        self.requests.append(now)
        return True

    def wait_if_needed(self):
        """Aguarda se necessário"""
        if not self.allow():
            sleep_time = self.window_seconds / self.max_requests
            logger.warning(f"Aguardando {sleep_time:.2f}s...")
            time.sleep(sleep_time)

api_limiter = RateLimiter(max_requests=100, window_seconds=60)

def handle_api_error(error_type: str):
    """Decorator para tratar erros de API"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Optional[Any]:
            try:
                api_limiter.wait_if_needed()
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Erro na API ({error_type})", exc=e)
                try:
                    import streamlit as st
                    st.error(f"❌ Erro ao acessar {error_type}: {str(e)}")
                except:
                    print(f"❌ Erro: {str(e)}")
                return None
        return wrapper
    return decorator

def format_timestamp(dt: datetime) -> str:
    """Formata timestamp para exibição"""
    return dt.strftime("%d/%m/%Y %H:%M")

def format_duration(seconds: float) -> str:
    """Formata duração em segundos para string legível"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"
