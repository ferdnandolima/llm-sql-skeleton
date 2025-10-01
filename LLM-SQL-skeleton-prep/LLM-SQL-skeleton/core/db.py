# core/db.py
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.settings import get_settings

_ENGINE: Engine | None = None

# ---------- DSN / Engine ----------

def _build_mysql_dsn() -> str:
    s = get_settings()
    # mysql+pymysql://user:pass@host:port/db?charset=utf8mb4
    from urllib.parse import quote_plus
    return (
        "mysql+pymysql://"
        f"{quote_plus(s.DB_USER)}:{quote_plus(s.DB_PASS)}@{s.DB_HOST}:{s.DB_PORT}/"
        f"{s.DB_NAME}?charset={quote_plus(s.DB_CHARSET)}"
    )

def get_engine() -> Engine:
    """Engine singleton com pool/timeout configuráveis."""
    global _ENGINE
    if _ENGINE is None:
        s = get_settings()
        dsn = _build_mysql_dsn()
        _ENGINE = create_engine(
            dsn,
            pool_pre_ping=bool(s.DB_POOL_PRE_PING),
            pool_recycle=int(s.DB_POOL_RECYCLE),
            pool_size=int(s.DB_POOL_SIZE),
            max_overflow=int(s.DB_MAX_OVERFLOW),
            connect_args={
                "connect_timeout": int(s.DB_CONNECT_TIMEOUT),
                "read_timeout": int(s.DB_READ_TIMEOUT),
                "write_timeout": int(s.DB_WRITE_TIMEOUT),
            },
            echo=bool(s.DB_ECHO),
            future=True,
        )
    return _ENGINE

# ---------- Helpers de SQL ----------

_param_pat = re.compile(r"%s", re.IGNORECASE)
_select_pat = re.compile(r"^\s*select\b", re.IGNORECASE)

def _params_to_named(sql: str, params: Any) -> Tuple[str, Dict[str, Any]]:
    """
    Converte placeholders '%s' para binds nomeados ':p0', ':p1', ...
    Aceita params list/tuple (posicional) ou dict (mantém).
    """
    if params is None:
        return sql, {}

    if isinstance(params, dict):
        if _param_pat.search(sql):
            keys = list(params.keys())
            def repl(_m, _idx=[0]):
                k = f"p{_idx[0]}"; _idx[0] += 1
                return f":{k}"
            sql_named = _param_pat.sub(repl, sql)
            new_params = {f"p{i}": params[k] for i, k in enumerate(keys)}
            return sql_named, new_params
        return sql, params

    if not isinstance(params, (list, tuple)):
        return sql, {"p0": params}

    idx = 0
    def repl(_m):
        nonlocal idx
        token = f":p{idx}"; idx += 1
        return token

    sql_named = _param_pat.sub(repl, sql)
    named = {f"p{i}": v for i, v in enumerate(params)}
    return sql_named, named

def _inject_max_exec_hint(sql: str, ms: int) -> str:
    """
    Injeta /*+ MAX_EXECUTION_TIME(ms) */ logo após SELECT (ou SELECT DISTINCT).
    Só para SELECT.
    """
    if ms <= 0 or not _select_pat.match(sql or ""):
        return sql

    # SELECT [DISTINCT] ...
    m = re.match(r"^(\s*select\s*)(distinct\s+)?", sql, flags=re.IGNORECASE)
    if not m:
        return sql
    prefix = m.group(0)
    rest = sql[len(prefix):]
    hint = f"/*+ MAX_EXECUTION_TIME({int(ms)}) */ "
    return prefix + hint + rest

def _cap_select_limit(sql: str, cap: int) -> str:
    """
    Garante LIMIT <= cap para SELECT.
    - Se não tiver LIMIT, adiciona LIMIT cap.
    - Se tiver LIMIT numérico > cap, reduz para cap (respeitando OFFSET,LIMIT).
    **Apenas** quando LIMIT é numérico literal (não faz bind-aware).
    """
    if cap <= 0 or not _select_pat.match(sql or ""):
        return sql

    # LIMIT <n>  | LIMIT <offset>, <n>
    lim_re = re.compile(r"\blimit\s+(\d+)(?:\s*,\s*(\d+))?\b", re.IGNORECASE)
    m = lim_re.search(sql)
    if not m:
        return f"{sql.rstrip()} LIMIT {cap}"

    if m and m.group(2):
        offset = int(m.group(1))
        n = int(m.group(2))
        if n > cap:
            repl = f"LIMIT {offset}, {cap}"
            return sql[:m.start()] + repl + sql[m.end():]
        return sql

    if m:
        n = int(m.group(1))
        if n > cap:
            repl = f"LIMIT {cap}"
            return sql[:m.start()] + repl + sql[m.end():]
    return sql

# ---------- Execução ----------

def run_query(sql: str, params: Any | None = None) -> Tuple[List[str], List[List[Any]], List[Dict[str, Any]], int]:
    """
    Executa o SQL via SQLAlchemy, com binds nomeados e pooling.
    Aplica:
      - MAX_EXECUTION_TIME para SELECT (hint).
      - LIMIT cap global (GLOBAL_LIMIT_CAP) para SELECT.
      - Truncamento do payload em memória (MAX_ROWS_PAYLOAD).
    Retorna (cols, rows_2d, rows_dict, rowcount_truncado).
    """
    s = get_settings()
    engine = get_engine()

    # 1) Parametrização
    sql_named, bind = _params_to_named(sql, params)

    # 2) Proteções de SELECT
    sql_exec = sql_named
    if _select_pat.match(sql_exec):
        sql_exec = _inject_max_exec_hint(sql_exec, int(s.DB_STATEMENT_TIMEOUT_MS))
        sql_exec = _cap_select_limit(sql_exec, int(s.GLOBAL_LIMIT_CAP))

    # 3) Execução
    with engine.connect() as conn:
        res = conn.execute(text(sql_exec), bind)
        if res.returns_rows:
            cols = list(res.keys())
            rows_dict = [dict(r._mapping) for r in res.fetchall()]
            # Truncamento em memória por segurança
            max_rows = int(s.MAX_ROWS_PAYLOAD)
            if max_rows > 0 and len(rows_dict) > max_rows:
                rows_dict = rows_dict[:max_rows]
            rows_2d = [[row.get(c) for c in cols] for row in rows_dict]
            return cols, rows_2d, rows_dict, len(rows_dict)
        return [], [], [], res.rowcount
