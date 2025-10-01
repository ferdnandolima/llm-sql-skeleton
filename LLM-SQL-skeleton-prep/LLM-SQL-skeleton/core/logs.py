# core/logs.py
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import sys
import uuid
from typing import Any, Dict

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": _dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Campos extras (passados via extra={"extra_fields": {...}})
        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            # Evita objetos não serializáveis
            safe = {}
            for k, v in extra_fields.items():
                try:
                    json.dumps(v)
                    safe[k] = v
                except Exception:
                    safe[k] = str(v)
            payload.update(safe)
        return json.dumps(payload, ensure_ascii=False)

def get_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(stream=sys.stdout)
    h.setFormatter(_JsonFormatter())
    logger.addHandler(h)
    logger.propagate = False
    return logger

def new_corr_id() -> str:
    return uuid.uuid4().hex

def sql_digest(sql: str) -> str:
    """Hash curto do SQL para log (evita logar o SQL inteiro)."""
    return hashlib.sha1(sql.encode("utf-8", errors="ignore")).hexdigest()[:12]

def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """
    Logger unificado: msg = event, atributos em JSON.
    Uso:
      log_event(logger, "consulta_ok", corr_id=..., tenant=..., intent=..., ...)
    """
    logger.info(event, extra={"extra_fields": fields})
