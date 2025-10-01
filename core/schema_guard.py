# core/schema_guard.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional, Set
import re

class SchemaMismatch(Exception):
    """Lançada quando há divergências entre intents e o schema do banco."""
    def __init__(self, errors: List[str], warnings: Optional[List[str]] = None) -> None:
        super().__init__("\n".join(errors))
        self.errors = errors
        self.warnings = warnings or []


def _fetch_db_name(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE()")
        row = cur.fetchone()
        return list(row.values())[0] if isinstance(row, dict) else row[0]


def load_schema_snapshot(conn, db_name: Optional[str] = None) -> Dict[str, Set[str]]:
    """
    Lê INFORMATION_SCHEMA e retorna { 'tabela': {'COL_A','COL_B',...}, ... } (upper-normalized).
    """
    if not db_name:
        db_name = _fetch_db_name(conn)
    sql = """
        SELECT TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
    """
    snapshot: Dict[str, Set[str]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (db_name,))
        for row in cur.fetchall():
            t = (row["TABLE_NAME"] if isinstance(row, dict) else row[0]).upper()
            c = (row["COLUMN_NAME"] if isinstance(row, dict) else row[1]).upper()
            snapshot.setdefault(t, set()).add(c)
    return snapshot


def _alias_map(spec: dict) -> Dict[str, str]:
    """
    Monta um mapa alias->tabela a partir da intent (inclui o alias principal).
    """
    amap: Dict[str, str] = {}
    alias_principal = spec.get("alias_principal") or "t"
    tabela_principal = spec.get("tabela_principal")
    if tabela_principal:
        amap[alias_principal] = tabela_principal
    for j in (spec.get("joins") or []):
        ja = j.get("alias")
        jt = j.get("tabela")
        if ja and jt:
            amap[ja] = jt
    return amap


def _split_table_col(expr: str) -> Tuple[Optional[str], str]:
    """
    Divide 'a.COL' -> ('a','COL'), 'TABELA.COL' -> ('TABELA','COL'), 'COL' -> (None,'COL').
    Remove crases/aspas.
    """
    expr = expr.strip().strip("`").strip('"')
    if "." in expr:
        left, right = expr.split(".", 1)
        return left.strip().strip("`").strip('"'), right.strip().strip("`").strip('"')
    return None, expr


def _resolve_table_for(expr: str, aliasmap: Dict[str, str], default_table: str) -> Tuple[str, str, bool]:
    """
    Resolve para qual tabela devemos validar a coluna.
    Retorna (tabela_resolvida, coluna, resolved) onde resolved indica se foi possível
    mapear *com confiança* (alias conhecido ou tabela explícita).
    """
    left, col = _split_table_col(expr)
    if left is None:
        return default_table, col, True  # sem prefixo → tabela principal
    # Se for alias conhecido, troca por tabela
    if left in aliasmap:
        return aliasmap[left], col, True
    # Pode ser o nome real da tabela (não alias)
    return left, col, True  # deixamos o caller decidir se existe no snapshot


def _is_simple_col(term: str) -> bool:
    """Heurística: é uma coluna simples (com ou sem alias)?"""
    return bool(re.fullmatch(r"[A-Za-z0-9_\.`]+", term.strip()))


def check_intent_against_schema(intent_key: str, spec: dict, snapshot: Dict[str, Set[str]]) -> Tuple[List[str], List[str]]:
    """
    Verifica:
      - tabela_principal existe
      - joins.tabela existem
      - cada 'colunas' aponta para coluna existente (considerando alias/tabela)
      - filtros.periodo_em existe (se declarado)
      - ordenacao.por: valida apenas termos simples (col/alias.col). Expressões são ignoradas.
    Retorna (errors, warnings)
    """
    errors: List[str] = []
    warnings: List[str] = []

    tabela_principal = spec.get("tabela_principal")
    alias_principal = spec.get("alias_principal") or "t"

    if not tabela_principal:
        errors.append(f"[{intent_key}] falta 'tabela_principal'.")
        return errors, warnings

    amap = _alias_map(spec)

    # --- Tabela principal ---
    t_upper = tabela_principal.upper()
    if t_upper not in snapshot:
        errors.append(f"[{intent_key}] tabela_principal '{tabela_principal}' não existe no schema.")
        # sem tabela principal não dá para validar colunas
        return errors, warnings

    # --- Joins ---
    for j in (spec.get("joins") or []):
        jt = (j.get("tabela") or "").upper()
        ja = j.get("alias")
        if not jt:
            warnings.append(f"[{intent_key}] join sem 'tabela' definido (alias={ja}).")
            continue
        if jt not in snapshot:
            errors.append(f"[{intent_key}] tabela do join '{j.get('tabela')}' não existe no schema.")

    # --- Colunas expostas ---
    cols_map: Dict[str, str] = spec.get("colunas") or {}
    if not cols_map:
        errors.append(f"[{intent_key}] não define 'colunas' (SELECT * é proibido).")
    else:
        for logical, physical in cols_map.items():
            table_name, col, _ = _resolve_table_for(physical, amap, tabela_principal)
            t_up = table_name.upper()
            if t_up not in snapshot:
                errors.append(f"[{intent_key}] coluna '{physical}' referencia tabela inexistente '{table_name}'.")
                continue
            if col.upper() not in snapshot[t_up]:
                errors.append(f"[{intent_key}] coluna '{physical}' não encontrada em '{table_name}'.")

    # --- Filtros: periodo_em ---
    periodo_em = (spec.get("filtros") or {}).get("periodo_em")
    if periodo_em:
        table_name, col, _ = _resolve_table_for(periodo_em, amap, tabela_principal)
        t_up = table_name.upper()
        if t_up not in snapshot or col.upper() not in snapshot[t_up]:
            errors.append(f"[{intent_key}] filtros.periodo_em='{periodo_em}' não encontrado em '{table_name}'.")

    # --- Ordenação ---
    orden = (spec.get("ordenacao") or {}).get("por") or []
    for term in orden:
        if not isinstance(term, str):
            continue
        base = term.strip().split()[0]  # remove ASC/DESC
        if not _is_simple_col(base):
            # expressão/func. Ignora checagem rígida, só aviso leve.
            continue
        table_name, col, _ = _resolve_table_for(base, amap, tabela_principal)
        t_up = table_name.upper()
        if t_up not in snapshot or col.upper() not in snapshot[t_up]:
            warnings.append(f"[{intent_key}] ordenacao '{base}' não encontrada em '{table_name}' — verifique alias/expressão.")

    return errors, warnings


def check_registry_against_schema(intents_registry: Dict[str, dict], conn, db_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Executa a checagem para todas as intents.
    Retorna um resumo. Lança SchemaMismatch se houver errors.
    """
    snapshot = load_schema_snapshot(conn, db_name=db_name)
    all_errors: List[str] = []
    all_warnings: List[str] = []

    for key, spec in intents_registry.items():
        e, w = check_intent_against_schema(key, spec, snapshot)
        all_errors.extend(e)
        all_warnings.extend(w)

    result = {
        "tables": len(snapshot),
        "intents_checked": len(intents_registry),
        "errors": all_errors,
        "warnings": all_warnings,
    }
    if all_errors:
        raise SchemaMismatch(all_errors, all_warnings)
    return result
