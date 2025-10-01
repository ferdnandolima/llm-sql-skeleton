# api/routes_llm.py
from fastapi import APIRouter, Body, HTTPException, Query, Request
from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime, date, timedelta
import os
import re
import time
import pymysql

from core.nlu import route_and_fill

# Plano JSON estruturado + validação
from pydantic import ValidationError
from core.schemas import QueryPlan
from core.validators import validate_plan_vs_intent, PlanValidationError
from core.llm_provider import request_plan_via_llm

# Execução via SQLAlchemy (pool, timeouts, binds)
from core.db import run_query

# Logs estruturados
from core.logs import get_logger, log_event, sql_digest

# Settings (caps globais, etc.)
from core.settings import get_settings

# Cache
from core.cache import get_cache, make_cache_key, is_select

router = APIRouter(prefix="/llm", tags=["llm"])
APP_LOG = get_logger("llm-sql")

# INTENTS é injetado pelo main.py (ver api/main.py)
INTENTS_REGISTRY: Dict[str, dict] = {}

# -------------------------
# Helpers
# -------------------------

_NUMERIC_HINTS = ("valor", "vlr", "qtde", "quant", "tempo", "minut")


def _set_limit(sql: str, lim: int | None) -> str:
    if not lim or lim <= 0:
        return sql
    if not re.search(r"^\s*select\b", sql, flags=re.I):
        return sql
    # se já tem LIMIT, substitui; se não tem, acrescenta
    if re.search(r"\blimit\b\s+\d+", sql, flags=re.I):
        return re.sub(r"\blimit\b\s+\d+", f"LIMIT {int(lim)}", sql, flags=re.I)
    return f"{sql} LIMIT {int(lim)}"


def _qual(alias: str, col: str) -> str:
    """Qualifica a coluna com alias se não tiver ponto."""
    return f"{alias}.{col}" if "." not in col else col


def _col(spec: dict, logical_name: str, default: Optional[str] = None) -> Optional[str]:
    cols = spec.get("colunas") or {}
    val = cols.get(logical_name)
    if not val and default:
        return default
    return val


def _guess_numeric_cols(spec: dict) -> List[str]:
    cols = spec.get("colunas") or {}
    outs = []
    for k, v in cols.items():
        lk = k.lower()
        if any(h in lk for h in _NUMERIC_HINTS):
            outs.append(v)
    return outs


def _ensure_limit(sql: str, limit_override: Optional[int], default_limit: int) -> str:
    # Se já houver LIMIT, respeita. Senão, aplica limit (ou default)
    if re.search(r"\blimit\b\s+\d+", sql, flags=re.I):
        return sql
    lim = int(limit_override) if (limit_override and limit_override > 0) else default_limit
    return f"{sql} LIMIT {lim}"


def _date_from_iso(s: str) -> datetime:
    # aceita 'YYYY-MM-DD' e 'YYYY-MM-DD HH:MM:SS'
    try:
        if len(s) > 10:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        # fallback: hoje
        return datetime.today()


def _get_db_conn() -> pymysql.connections.Connection:
    # Mantido porque api/main.py importa isso (cheque de schema e admin)
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASS", "")
    database = os.getenv("DB_NAME", "")
    charset = os.getenv("DB_CHARSET", "utf8mb4")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        cursorclass=pymysql.cursors.DictCursor,  # rows como dicts
        charset=charset,
        autocommit=True,
    )
    return conn

# ---------- resolver período e mapear plano → slots ----------

def _today() -> date:
    return datetime.now().date()

def _monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())

def _first_day_of_month(d: date) -> date:
    return d.replace(day=1)

def _last_day_of_month(d: date) -> date:
    if d.month == 12:
        nxt = d.replace(year=d.year + 1, month=1, day=1)
    else:
        nxt = d.replace(month=d.month + 1, day=1)
    return nxt - timedelta(days=1)

def _resolve_periodo(plan_periodo: Optional[QueryPlan.model_fields['periodo'].annotation]) -> Tuple[str, str]:
    if not plan_periodo:
        td = _today()
        return (f"{td:%Y-%m-%d} 00:00:00", f"{td:%Y-%m-%d} 23:59:59")

    rel = getattr(plan_periodo, "relativo", None)
    if rel:
        today = _today()
        if rel == "hoje":
            di, df = today, today
        elif rel == "ontem":
            y = today - timedelta(days=1)
            di, df = y, y
        elif rel == "esta_semana":
            di = _monday_of(today); df = di + timedelta(days=6)
        elif rel == "semana_passada":
            fim = _monday_of(today) - timedelta(days=1)
            di = fim - timedelta(days=6); df = fim
        elif rel == "este_mes":
            di = _first_day_of_month(today); df = _last_day_of_month(today)
        elif rel == "mes_passado":
            first_this = _first_day_of_month(today)
            last_prev = first_this - timedelta(days=1)
            di = _first_day_of_month(last_prev); df = _last_day_of_month(last_prev)
        elif rel == "este_ano":
            di = date(today.year, 1, 1); df = date(today.year, 12, 31)
        else:
            di, df = today, today
        return (f"{di:%Y-%m-%d} 00:00:00", f"{df:%Y-%m-%d} 23:59:59")

    ini = getattr(plan_periodo, "inicio", None)
    fim = getattr(plan_periodo, "fim", None)
    if not (ini and fim):
        td = _today()
        return (f"{td:%Y-%m-%d} 00:00:00", f"{td:%Y-%m-%d} 23:59:59")

    return (f"{ini:%Y-%m-%d} 00:00:00", f"{fim:%Y-%m-%d} 23:59:59")

def _slots_from_plan(plan: QueryPlan, spec: dict) -> Dict[str, Any]:
    slots: Dict[str, Any] = {}
    di, df = _resolve_periodo(plan.periodo)
    slots["data_ini"] = di
    slots["data_fim"] = df

    for k, v in (plan.filtros or {}).items():
        k_l = str(k).lower()
        if k_l in ("status", "id_status", "id_stat_lancto"):
            slots["status"] = v
        elif k_l in ("cliente", "nu_cli"):
            slots["cliente"] = v
        elif k_l in ("numero_pedido", "nu_pve"):
            slots["nu_pve"] = v
        else:
            slots[k] = v

    if plan.order_by:
        try:
            direcao = (plan.order_by[0].direcao or "desc").upper()
            if direcao in ("ASC", "DESC"):
                slots["ORD_DIR"] = direcao
        except Exception:
            pass

    if plan.limit:
        slots["N"] = int(plan.limit)

    return slots

# ---------- Cap global de LIMIT (cosmético p/ SQL mostrado/executado) ----------

_lim_pat = re.compile(r"\blimit\s+(\d+)(?:\s*,\s*(\d+))?\b", re.IGNORECASE)
_select_pat = re.compile(r"^\s*select\b", re.IGNORECASE)

def _cap_limit_in_sql_string(sql: str, cap: int) -> str:
    """
    Aplica o teto GLOBAL_LIMIT_CAP no texto do SQL (e é esse SQL que executamos).
    - Se não houver LIMIT → adiciona LIMIT cap.
    - Se houver LIMIT n  e n>cap → reduz p/ cap.
    - Se houver LIMIT o,n e n>cap → reduz n p/ cap.
    """
    if cap <= 0 or not _select_pat.match(sql or ""):
        return sql
    m = _lim_pat.search(sql)
    if not m:
        return f"{sql.rstrip()} LIMIT {cap}"
    if m.group(2):
        offset = int(m.group(1)); n = int(m.group(2))
        if n > cap:
            return sql[:m.start()] + f"LIMIT {offset}, {cap}" + sql[m.end():]
        return sql
    else:
        n = int(m.group(1))
        if n > cap:
            return sql[:m.start()] + f"LIMIT {cap}" + sql[m.end():]
        return sql

# ---------- Mascaramento PII por intent ----------

def _mask_value(v: Any, strategy: str) -> Any:
    if v is None:
        return None
    s = str(v)
    if strategy == "all":
        return "***"
    if strategy in ("cpf", "phone"):
        # mascara todos os dígitos exceto os 2 últimos
        digits = re.sub(r"\D", "", s)
        if not digits:
            return s
        def repl(m, _count=[0], total=len(digits)-2):
            ch = m.group(0)
            if _count[0] < total:
                _count[0] += 1
                return "*"
            return ch
        masked = re.sub(r"\d", repl, s, count=len(digits)-2 if len(digits) > 2 else 0)
        return masked
    if strategy == "email":
        m = re.match(r"([^@])([^@]*)(@.*)", s)
        if not m:
            return s
        return f"{m.group(1)}***{m.group(3)}"
    if strategy == "last4":
        vis = 4
        hide = max(0, len(s) - vis)
        return ("*" * hide) + s[-vis:]
    # fallback
    return "***"

def _apply_mask(rows: List[Dict[str, Any]], cols: List[str], mask_cfg: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    mask_cfg: { alias_coluna: 'cpf'|'email'|'phone'|'last4'|'all' }
    """
    if not rows or not mask_cfg:
        return rows
    out: List[Dict[str, Any]] = []
    for row in rows:
        r = dict(row)
        for col, strat in mask_cfg.items():
            if col in r:
                r[col] = _mask_value(r[col], (strat or "all").lower())
        out.append(r)
    return out

# ---------- Normalizadores para filtros_plus ----------

_SN_TRUE = {"s", "sim", "y", "yes", "true", "1"}
_SN_FALSE = {"n", "nao", "não", "no", "false", "0"}

def _to_sn(val):
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in _SN_TRUE:
        return "S"
    if s in _SN_FALSE:
        return "N"
    if s.upper() in ("S", "N"):
        return s.upper()
    return val

def _to_tipo(val):
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in {"1", "pf", "pessoa fisica", "pessoa física", "fisica", "física"}:
        return 1
    if s in {"2", "pj", "pessoa juridica", "pessoa jurídica", "juridica", "jurídica"}:
        return 2
    try:
        return int(val)
    except:
        return val

def _to_sexo(val):
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in {"1", "m", "masc", "masculino"}:
        return 1
    if s in {"2", "f", "fem", "feminino"}:
        return 2
    if s in {"3", "i", "indef", "indefinido", "nao informado", "não informado"}:
        return 3
    try:
        return int(val)
    except:
        return val

# slots numéricos típicos (para coerção automática e IN)
_NUM_SLOTS = {
    "cliente",
    "classificação", "classificacao",
    "ramo",
    "ocupacao",
    "rota",
    "vendedor",
    "motivo_bloqueio",
}

def _coerce_num(val):
    if isinstance(val, (list, tuple, set)):
        out = []
        for v in val:
            try:
                out.append(int(str(v).strip()))
            except:
                out.append(v)
        return out
    try:
        return int(str(val).strip())
    except:
        return val

def _split_listish(val):
    if val is None:
        return []
    if isinstance(val, (list, tuple, set)):
        return list(val)
    s = str(val).strip()
    if not s:
        return []
    parts = re.split(r"[,\s]+", s)
    return [p for p in parts if p]

# ---------- NEW: helper para extrair alternativas do roteamento ----------

def _alts_from_routed(r: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extrai alternativas do resultado do route_and_fill, independente do formato:
    - lista de tuplas (intent, score)
    - lista de dicts {"intent": "...", "score": ...}
    - lista simples de strings
    Retorna no máx. 5 itens: [{"intent": str, "score": float|None}, ...]
    """
    alts_raw = r.get("alternatives") or r.get("candidates") or r.get("top_k") or []
    out: List[Dict[str, Any]] = []
    for item in alts_raw:
        intent = None
        score = None
        if isinstance(item, (list, tuple)) and len(item) >= 1:
            intent = item[0]
            if len(item) > 1:
                try:
                    score = float(item[1])
                except Exception:
                    score = None
        elif isinstance(item, dict):
            intent = item.get("intent") or item.get("key") or item.get("name")
            sc = item.get("score")
            try:
                score = float(sc) if sc is not None else None
            except Exception:
                score = None
        else:
            intent = str(item)
        if intent:
            out.append({"intent": str(intent), "score": score})
        if len(out) >= 5:
            break
    return out

# ---------- NEW: detectar quando o usuário pediu "todos" ----------

def _detect_quer_todos(user_text: str, slots: Dict[str, Any]) -> bool:
    """
    Retorna True se o texto/slots indicam que o usuário quer 'todos' (sem LIMIT).
    Heurísticas conservadoras:
      - texto contém a palavra isolada 'todos' ou 'tudo'
      - slot N com valores especiais (0, -1, 'all', 'todos', 'tudo')
    """
    try:
        t = f" {str(user_text or '').lower()} "
    except Exception:
        t = " "
    if " todos " in t or " tudo " in t or " sem limite " in t or " sem limit " in t:
        return True

    N = slots.get("N")
    if N is None:
        return False
    try:
        if int(N) <= 0:
            return True
    except Exception:
        pass
    if str(N).strip().lower() in {"all", "todos", "tudo"}:
        return True
    return False

# -------------------------
# 1) Apenas rotear
# -------------------------
@router.post("/route")
def llm_route(
    request: Request,
    payload: Dict = Body(...),
    use_llm: bool = Query(False, description="Se true, usa LLM (Ollama/OpenAI) para desempate/slots"),
    threshold: float = Query(0.55, description="Score mínimo antes de chamar o LLM"),
):
    text = (payload or {}).get("text") or ""
    if not text:
        raise HTTPException(400, "Campo 'text' é obrigatório.")
    if not INTENTS_REGISTRY:
        raise HTTPException(500, "Registro de intents vazio. Verifique o loader.")

    tenant = getattr(request.state, "tenant", "default")
    corr_id = getattr(request.state, "corr_id", "")

    t0 = time.perf_counter()
    result = route_and_fill(text, INTENTS_REGISTRY, use_llm=use_llm, threshold=threshold)
    llm_ms = int((time.perf_counter() - t0) * 1000)

    # ---------- GATE POR SCORE (abaixo do threshold = 422) ----------
    score = float(result.get("score") or 0.0)
    if score < float(threshold):
        alts = _alts_from_routed(result)
        log_event(
            APP_LOG,
            "llm_route_low_score",
            corr_id=corr_id,
            tenant=tenant,
            intent=result.get("intent"),
            score=score,
            threshold=threshold,
            alts=alts,
        )
        raise HTTPException(
            422,
            detail={
                "erro": "Não entendi a intenção da pergunta (score abaixo do threshold).",
                "score": score,
                "threshold": threshold,
                "intent_sugerida": result.get("intent"),
                "alternativas": alts,
            },
        )

    log_event(
        APP_LOG,
        "llm_route",
        corr_id=corr_id,
        tenant=tenant,
        intent=result.get("intent"),
        llm_ms=llm_ms,
        slots=len(result.get("slots") or {}),
    )
    return result


# -------------------------
# 2) Builder de SQL (sem SELECT *)
# -------------------------
def build_sql(
    intent_key: str,
    spec: dict,
    slots: Dict[str, Any],
    campos_selecionados: Optional[List[str]] = None,
) -> Tuple[str, List[Any]]:
    alias = spec.get("alias_principal") or "t"
    tabela = spec.get("tabela_principal")
    if not tabela:
        raise HTTPException(500, f"Intent '{intent_key}' sem 'tabela_principal'.")

    retorna = (spec.get("retorna") or "linhas").lower()
    cols_map: Dict[str, str] = spec.get("colunas") or {}

    # --- SELECT ---
    select_parts: List[str] = []
    group_by: List[str] = []

    if retorna == "linhas":
        if not cols_map:
            raise HTTPException(500, f"Intent '{intent_key}' não define 'colunas' (SELECT * é proibido).")

        if not campos_selecionados:
            campos_usados = list(cols_map.keys())
        else:
            invalidos = sorted(set(campos_selecionados) - set(cols_map.keys()))
            if invalidos:
                raise HTTPException(
                    400,
                    detail={
                        "erro": "Campos fora da intent",
                        "campos_invalidos": invalidos,
                        "campos_permitidos": sorted(cols_map.keys()),
                        "intent": intent_key,
                    },
                )
            campos_usados = list(campos_selecionados)

        # aliases de saída com crases (suporta acentos/cedilha)
        select_parts = [f"{_qual(alias, cols_map[k])} AS `{k}`" for k in campos_usados]

    elif retorna == "agregado_tabela":
        agr = (spec.get("agrupamento") or {}).get("por") or []
        for g in agr:
            group_by.append(g)
            # Alias do grupo igual ao texto do g (já com crase)
            select_parts.append(f"{g} AS `{g}`")
        num_cols = _guess_numeric_cols(spec)
        if num_cols:
            for nc in num_cols:
                select_parts.append(f"SUM({_qual(alias, nc)}) AS `sum_{nc}`")
        select_parts.append("COUNT(*) AS total_linhas")

    elif retorna == "agregado_unico":
        num_cols = _guess_numeric_cols(spec)
        if num_cols:
            for nc in num_cols:
                select_parts.append(f"SUM({_qual(alias, nc)}) AS `sum_{nc}`")
        select_parts.append("COUNT(*) AS total_linhas")

    else:
        if not cols_map:
            raise HTTPException(500, f"Intent '{intent_key}' não define 'colunas' (SELECT * é proibido).")
        select_parts = [f"{_qual(alias, v)} AS `{k}`" for k, v in cols_map.items()]
        retorna = "linhas"

    select_sql = ", ".join(select_parts)

    # --- FROM + JOINs ---
    joins = spec.get("joins") or []
    join_sql = ""

    def _get_join_on(j: dict) -> Optional[str]:
        # YAML 1.1: 'on' pode virar booleano True se não vier com aspas
        return j.get("on") or j.get(True) or j.get("cond")

    for j in joins:
        jt = j.get("tabela")
        ja = j.get("alias")
        jon = _get_join_on(j)
        jtipo = (j.get("tipo") or "LEFT").upper()  # suporte a tipo com default LEFT
        if jt and jon:
            if ja:
                join_sql += f" {jtipo} JOIN {jt} {ja} ON {jon}"
            else:
                join_sql += f" {jtipo} JOIN {jt} ON {jon}"
        elif ja and not jon:
            # Evita passar pela validação de alias sem realmente ter JOIN
            raise HTTPException(500, f"JOIN de '{ja}' na intent '{intent_key}' sem cláusula ON.")

    # >>> Validação de aliases usados x aliases declarados (anti-1054 do MySQL)
    def _aliases_from_expr(expr: str) -> set:
        return {m.group(1) for m in re.finditer(r"\b([A-Za-z_]\w*)\.", expr or "")}

    declared_aliases = {alias} | {
        j.get("alias")
        for j in (joins or [])
        if j.get("alias") and j.get("tabela") and _get_join_on(j)
    }
    used_aliases = set()

    # aliases usados nas colunas
    for expr in (cols_map or {}).values():
        used_aliases |= _aliases_from_expr(expr)

    # aliases usados nos filtros_plus (equals/like/in)
    filtros_plus_spec = spec.get("filtros_plus") or {}
    for expr in (filtros_plus_spec.get("equals") or {}).values():
        used_aliases |= _aliases_from_expr(expr)
    for expr in (filtros_plus_spec.get("like") or {}).values():
        used_aliases |= _aliases_from_expr(expr)
    for expr in (filtros_plus_spec.get("in") or {}).values():
        used_aliases |= _aliases_from_expr(expr)

    # alias usado em periodo_em e ordenação padrão
    periodo_col_spec = (spec.get("filtros") or {}).get("periodo_em")
    if periodo_col_spec:
        used_aliases |= _aliases_from_expr(periodo_col_spec)
    for ob in (spec.get("ordenacao") or {}).get("por") or []:
        used_aliases |= _aliases_from_expr(ob)

    missing_aliases = used_aliases - declared_aliases
    if missing_aliases:
        raise HTTPException(
            500,
            detail=f"Intent '{intent_key}' referencia aliases {sorted(missing_aliases)} sem JOIN correspondente."
        )

    # --- WHERE ---
    where: List[str] = []
    params: List[Any] = []

    filtros = spec.get("filtros") or {}

    periodo_col = filtros.get("periodo_em")
    di = slots.get("data_ini")
    df = slots.get("data_fim")
    if periodo_col and di and df:
        di_dt = _date_from_iso(str(di))
        df_dt = _date_from_iso(str(df))
        where.append(f"{_qual(alias, periodo_col)} BETWEEN %s AND %s")
        params.extend([di_dt.strftime("%Y-%m-%d 00:00:00"), df_dt.strftime("%Y-%m-%d 23:59:59")])

    status_col = _col(spec, "status") or ("ID_STATUS" if "ID_STATUS" in (cols_map.values()) else None)
    if slots.get("status") and status_col:
        where.append(f"{_qual(alias, status_col)} = %s")
        params.append(slots["status"])

    cli_col = _col(spec, "cliente") or ("NU_CLI" if "NU_CLI" in (cols_map.values()) else None)
    if slots.get("cliente") and cli_col:
        where.append(f"{_qual(alias, cli_col)} = %s")
        params.append(slots["cliente"])

    np_col = _col(spec, "numero_pedido") or ("NU_PVE" if "NU_PVE" in (cols_map.values()) else None)
    if slots.get("nu_pve") and np_col:
        where.append(f"{_qual(alias, np_col)} = %s")
        params.append(slots["nu_pve"])

    if filtros.get("somente_pendentes"):
        vt = _col(spec, "valor_titulo")
        vp = _col(spec, "valor_pago")
        if vt and vp:
            where.append(f"COALESCE({_qual(alias, vp)},0) < COALESCE({_qual(alias, vt)},0)")
        elif status_col:
            where.append(f"{_qual(alias, status_col)} <> %s")
            params.append("PAGO")
    if filtros.get("somente_quitados"):
        vt = _col(spec, "valor_titulo")
        vp = _col(spec, "valor_pago")
        if vt and vp:
            where.append(f"COALESCE({_qual(alias, vp)},0) >= COALESCE({_qual(alias, vt)},0)")
        elif status_col:
            where.append(f"{_qual(alias, status_col)} = %s")
            params.append("PAGO")

    # ----- Filtros genéricos declarativos (filtros_plus) -----
    filtros_plus = spec.get("filtros_plus") or {}

    # equals: slot == coluna
    eq_map = (filtros_plus.get("equals") or {})
    for slot_name, colname in eq_map.items():
        if slot_name in slots and slots[slot_name] not in (None, ""):
            val = slots[slot_name]
            # normalizações úteis
            if slot_name in {"ativo", "bloqueado", "consumidor", "simples", "estrangeiro"}:
                val = _to_sn(val)
            elif slot_name == "tipo":
                val = _to_tipo(val)
            elif slot_name == "sexo":
                val = _to_sexo(val)
            elif slot_name in _NUM_SLOTS:
                val = _coerce_num(val)

            where.append(f"{_qual(alias, colname)} = %s")
            params.append(val)

    # like: coluna LIKE %valor%
    like_map = (filtros_plus.get("like") or {})
    for slot_name, colname in like_map.items():
        val = slots.get(slot_name)
        if val not in (None, ""):
            sval = str(val).strip()
            if sval != "":
                where.append(f"{_qual(alias, colname)} LIKE %s")
                params.append(f"%{sval}%")

    # in: coluna IN (...). Aceita CSV e/ou espaço; coerção numérica automática
    in_map = (filtros_plus.get("in") or {})
    for slot_name, colname in in_map.items():
        raw = slots.get(slot_name)
        vals = _split_listish(raw)
        if not vals:
            continue
        if slot_name in _NUM_SLOTS:
            vals = _coerce_num(vals)
        placeholders = ", ".join(["%s"] * len(vals))
        where.append(f"{_qual(alias, colname)} IN ({placeholders})")
        params.extend(list(vals))

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    # --- GROUP BY (apenas para agregado_tabela)
    group_sql = ""
    if retorna == "agregado_tabela" and group_by:
        gb_cols = [_qual(alias, g) for g in group_by]
        group_sql = " GROUP BY " + ", ".join(gb_cols)

    # --- ORDER BY ---
    orden = (spec.get("ordenacao") or {}).get("por") or []
    order_sql = ""
    dir_override = (str(slots.get("ORD_DIR") or "")).upper()

    if orden:
        if dir_override in ("ASC", "DESC"):
            norm_orden: List[str] = []
            for item in orden:
                if re.search(r"\bASC\b|\bDESC\b", item, flags=re.I):
                    norm_orden.append(re.sub(r"\bASC\b|\bDESC\b", dir_override, item, flags=re.I))
                else:
                    norm_orden.append(f"{item} {dir_override}")
            order_sql = " ORDER BY " + ", ".join(norm_orden)
        else:
            order_sql = " ORDER BY " + ", ".join(orden)
    else:
        if dir_override in ("ASC", "DESC"):
            base_col = (spec.get("filtros") or {}).get("periodo_em")
            if not base_col and "DT_PVE" in (cols_map.values() or []):
                base_col = "DT_PVE"
            if base_col:
                order_sql = " ORDER BY " + _qual(alias, base_col) + f" {dir_override}"

    # --- LIMIT ---
    regras = spec.get("regras") or {}
    obrigar_limit_em_listas = bool(regras.get("obrigar_limit_em_listas", retorna == "linhas"))
    limit_padrao = int(regras.get("limit_padrao") or (200 if retorna == "linhas" else 0))

    # >>> NEW: pular LIMIT padrão quando usuário pediu "todos"
    quer_todos_flag = bool(slots.get("__quer_todos__", False))

    # monta SQL final
    base_sql = f"SELECT {select_sql} FROM {tabela} {alias}{join_sql}{where_sql}{group_sql}{order_sql}"
    if retorna == "linhas" and obrigar_limit_em_listas and not quer_todos_flag:
        base_sql = _ensure_limit(base_sql, slots.get("N"), limit_padrao)

    # (opcional) debug do SQL final
    try:
        APP_LOG.debug("SQL FINAL [%s]:\n%s\nPARAMS: %s", intent_key, base_sql, params)
    except Exception:
        pass

    return base_sql, params


# -------------------------
# 3) /llm/run
# -------------------------
@router.post("/run")
def llm_run(
    request: Request,
    payload: Dict = Body(...),
    execute: bool = Query(True, description="Se true, executa no banco; senão, apenas retorna o SQL."),
    limit: Optional[int] = Query(None, description="Override de LIMIT quando retornar 'linhas'."),
    use_llm: bool = Query(False, description="Se true, usa LLM (Ollama/OpenAI) para desempate/slots"),
    threshold: float = Query(0.55, description="Score mínimo antes de chamar o LLM"),
):
    text = (payload or {}).get("text") or ""
    if not text:
        raise HTTPException(400, "Campo 'text' é obrigatório.")
    if not INTENTS_REGISTRY:
        raise HTTPException(500, "Registro de intents vazio. Verifique o loader.")

    tenant = getattr(request.state, "tenant", "default")
    corr_id = getattr(request.state, "corr_id", "")

    s = get_settings()
    cache_ttl = int(getattr(s, "CACHE_SELECT_TTL", 15))
    cache_max = int(getattr(s, "CACHE_MAX_ITEMS", 256))
    max_rows_payload = int(getattr(s, "MAX_ROWS_PAYLOAD", 5000))

    # 1) Roteia
    t_llm0 = time.perf_counter()
    routed = route_and_fill(text, INTENTS_REGISTRY, use_llm=use_llm, threshold=threshold)
    t_llm_ms = int((time.perf_counter() - t_llm0) * 1000)

    # ---------- GATE POR SCORE (abaixo do threshold = 422) ----------
    score = float(routed.get("score") or 0.0)
    if score < float(threshold):
        alts = _alts_from_routed(routed)
        log_event(
            APP_LOG,
            "llm_run_low_score",
            corr_id=corr_id,
            tenant=tenant,
            intent=routed.get("intent"),
            score=score,
            threshold=threshold,
            alts=alts,
        )
        raise HTTPException(
            422,
            detail={
                "erro": "Não entendi a intenção da pergunta (score abaixo do threshold).",
                "score": score,
                "threshold": threshold,
                "intent_sugerida": routed.get("intent"),
                "alternativas": alts,
            },
        )

    key = routed["intent"]
    spec = INTENTS_REGISTRY.get(key) or {}
    slots = dict(routed.get("slots") or {})

    # 2) Overrides opcionais
    overrides = (payload or {}).get("override_slots") or {}
    for k, v in overrides.items():
        slots[k] = v

    # >>> detectar pedido de "todos" e marcar flag de controle
    regras = spec.get("regras") or {}
    quer_todos = False
    if regras.get("reconhecer_todos_sem_limit"):
        if _detect_quer_todos(text, slots):
            slots["__quer_todos__"] = True
            quer_todos = True

    # 3) SQL
    sql, params = build_sql(key, spec, slots, campos_selecionados=None)

    # LIMIT override (do usuário)
    limit_override = None
    if not quer_todos:
        if limit and int(limit) > 0:
            limit_override = int(limit)
        elif "N" in slots:
            try:
                limit_override = int(slots["N"])
            except Exception:
                limit_override = None

    if limit_override and re.search(r"^\s*select\b", sql, flags=re.I) and not re.search(r"\blimit\b\s+\d+", sql, flags=re.I):
        sql = f"{sql} LIMIT {limit_override}"

    # >>> CAP global condicional (não capar quando "todos")
    cap = int(getattr(s, "GLOBAL_LIMIT_CAP", 0) or 0)
    if quer_todos:
        cap = int(getattr(s, "GLOBAL_LIMIT_CAP_TODOS", 0) or 0)  # 0 = sem cap
    sql = _cap_limit_in_sql_string(sql, cap)

    # 3.1) Cache (somente SELECT e se execute=True)
    cache = get_cache(max_items=cache_max)
    cache_key = None
    if execute and is_select(sql):
        cache_key = make_cache_key(sql, params, tenant)
        cached = cache.get(cache_key)
        if cached is not None:
            log_event(
                APP_LOG,
                "llm_run_cache_hit",
                corr_id=corr_id,
                tenant=tenant,
                intent=key,
                llm_ms=t_llm_ms,
                rowcount=cached.get("rowcount"),
                sql_digest=sql_digest(sql),
            )
            return {
                "route": routed,
                "sql": sql,
                "params": params,
                "executed": True,
                "cols": cached["cols"],
                "rows": cached["rows_2d"],
                "rows_dict": cached["rows_dict"],
                "rowcount": cached["rowcount"],
                "truncated": cached.get("truncated", False),
                "total_rows": cached.get("total_rows", cached.get("rowcount")),
                "cache": True,
            }

    # Dry-run?
    if not execute:
        log_event(
            APP_LOG,
            "llm_run_dryrun",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            limit=limit_override,
            sql_digest=sql_digest(sql),
        )
        return {
            "route": routed,
            "sql": sql,
            "params": params,
            "executed": False,
            "cols": [],
            "rows": [],
            "rowcount": 0,
        }

    # 4) Execução
    t_db0 = time.perf_counter()
    try:
        cols, rows_2d, rows_dict, rowcount = run_query(sql, params)

        # Mascaramento PII por intent (se houver politicas.mask)
        mask_cfg = ((spec.get("politicas") or {}).get("mask") or {})
        if mask_cfg:
            rows_dict = _apply_mask(rows_dict, cols, mask_cfg)
            rows_2d = [[row.get(c) for c in cols] for row in rows_dict]

        # Truncamento de payload
        total_rows = rowcount
        truncated = False
        if max_rows_payload and rowcount and rowcount > max_rows_payload:
            rows_dict = rows_dict[:max_rows_payload]
            rows_2d = rows_2d[:max_rows_payload]
            truncated = True

        # Save no cache (somente SELECT)
        if cache_key and is_select(sql):
            cache.set(
                cache_key,
                {
                    "cols": cols,
                    "rows_2d": rows_2d,
                    "rows_dict": rows_dict,
                    "rowcount": len(rows_dict),
                    "truncated": truncated,
                    "total_rows": total_rows,
                },
                ttl=cache_ttl,
            )

        db_ms = int((time.perf_counter() - t_db0) * 1000)
        log_event(
            APP_LOG,
            "llm_run_ok",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            db_ms=db_ms,
            rowcount=total_rows,
            limit=limit_override,
            sql_digest=sql_digest(sql),
        )
        return {
            "route": routed,
            "sql": sql,
            "params": params,
            "executed": True,
            "cols": cols,
            "rows": rows_2d,
            "rows_dict": rows_dict,
            "rowcount": len(rows_dict),
            "truncated": truncated,
            "total_rows": total_rows,
            "cache": False,
        }
    except Exception as e:
        db_ms = int((time.perf_counter() - t_db0) * 1000)
        log_event(
            APP_LOG,
            "llm_run_error",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            db_ms=db_ms,
            err=str(e),
            sql_digest=sql_digest(sql),
        )
        raise HTTPException(500, f"Falha ao executar SQL: {e}")


# -------------------------
# 4) /llm/consulta — (IA → JSON) + validação antes do SQL
# -------------------------
@router.post("/consulta")
def llm_consulta(
    request: Request,
    payload: Dict = Body(...),
    execute: bool = Query(True, description="Se true, executa no banco; senão, apenas retorna o SQL."),
):
    tenant = getattr(request.state, "tenant", "default")
    corr_id = getattr(request.state, "corr_id", "")

    s = get_settings()
    cache_ttl = int(getattr(s, "CACHE_SELECT_TTL", 15))
    cache_max = int(getattr(s, "CACHE_MAX_ITEMS", 256))
    max_rows_payload = int(getattr(s, "MAX_ROWS_PAYLOAD", 5000))

    pergunta = (payload or {}).get("pergunta") or (payload or {}).get("text") or ""
    if not pergunta:
        raise HTTPException(400, "Campo 'pergunta' (ou 'text') é obrigatório.")
    if not INTENTS_REGISTRY:
        raise HTTPException(500, "Registro de intents vazio. Verifique o loader.")

    # 1) IA → plano JSON
    t_llm0 = time.perf_counter()
    try:
        plan: QueryPlan = request_plan_via_llm(pergunta=pergunta, intents_catalogo=INTENTS_REGISTRY)
    except ValidationError as ve:
        log_event(APP_LOG, "llm_consulta_plan_error", corr_id=corr_id, tenant=tenant, err="validation", details=str(ve))
        raise HTTPException(422, detail={"erro": "JSON inválido do modelo", "detalhes": ve.errors()})
    except Exception as e:
        log_event(APP_LOG, "llm_consulta_plan_error", corr_id=corr_id, tenant=tenant, err=str(e))
        raise HTTPException(500, detail=f"Falha ao obter plano estruturado: {e}")
    t_llm_ms = int((time.perf_counter() - t_llm0) * 1000)

    # 2) Validar plano x intent
    intent_def = INTENTS_REGISTRY.get(plan.intent)
    try:
        validate_plan_vs_intent(plan, intent_def)
    except PlanValidationError as e:
        log_event(APP_LOG, "llm_consulta_validation_error", corr_id=corr_id, tenant=tenant, intent=plan.intent, err=str(e))
        raise HTTPException(400, detail=str(e))

    # 3) Converter plano → slots esperados
    slots = _slots_from_plan(plan, intent_def or {})
    key = plan.intent
    spec = intent_def or {}

    # >>> NEW: detectar "todos" e marcar flag (só se habilitado na intent)
    regras = (spec.get("regras") or {})
    quer_todos = False
    if regras.get("reconhecer_todos_sem_limit"):
        if _detect_quer_todos(pergunta, slots):
            slots["__quer_todos__"] = True
            quer_todos = True

    # 4) Montar SQL
    try:
        sql, params = build_sql(key, spec, slots, campos_selecionados=(plan.campos or []))
        # >>> NEW: não aplicar plan.limit quando "todos"
        if not quer_todos and plan.limit and re.search(r"^\s*select\b", sql, flags=re.I) and not re.search(r"\blimit\b\s+\d+", sql, flags=re.I):
            sql = f"{sql} LIMIT {int(plan.limit)}"
    except HTTPException:
        raise
    except Exception as e:
        log_event(APP_LOG, "llm_consulta_build_error", corr_id=corr_id, tenant=tenant, intent=key, err=str(e))
        raise HTTPException(400, detail=f"Falha ao montar SQL: {e}")

    # >>> PATCH: CAP global refletido no SQL mostrado/executado (não capar quando "todos")
    cap = int(getattr(s, "GLOBAL_LIMIT_CAP", 0) or 0)
    if quer_todos:
        cap = int(getattr(s, "GLOBAL_LIMIT_CAP_TODOS", 0) or 0)  # 0 = sem cap
    sql = _cap_limit_in_sql_string(sql, cap)

    # 4.1) Cache (somente SELECT e se execute=True)
    cache = get_cache(max_items=cache_max)
    cache_key = None
    if execute and is_select(sql):
        cache_key = make_cache_key(sql, params, tenant)
        cached = cache.get(cache_key)
        if cached is not None:
            log_event(
                APP_LOG,
                "llm_consulta_cache_hit",
                corr_id=corr_id,
                tenant=tenant,
                intent=key,
                llm_ms=t_llm_ms,
                rowcount=cached.get("rowcount"),
                sql_digest=sql_digest(sql),
            )
            return {
                "plan": plan.model_dump(),
                "sql": sql,
                "params": params,
                "executed": True,
                "cols": cached["cols"],
                "rows": cached["rows_2d"],
                "rows_dict": cached["rows_dict"],
                "rowcount": cached["rowcount"],
                "truncated": cached.get("truncated", False),
                "total_rows": cached.get("total_rows", cached.get("rowcount")),
                "cache": True,
            }

    # 5) Dry-run?
    if not execute:
        log_event(
            APP_LOG,
            "llm_consulta_dryrun",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            sql_digest=sql_digest(sql),
            limit=getattr(plan, "limit", None),
        )
        return {
            "plan": plan.model_dump(),
            "sql": sql,
            "params": params,
            "executed": False,
            "cols": [],
            "rows": [],
            "rowcount": 0,
        }

    # 6) Executar
    t_db0 = time.perf_counter()
    try:
        cols, rows_2d, rows_dict, rowcount = run_query(sql, params)

        # Mascaramento PII por intent (se houver politicas.mask)
        mask_cfg = ((spec.get("politicas") or {}).get("mask") or {})
        if mask_cfg:
            rows_dict = _apply_mask(rows_dict, cols, mask_cfg)
            rows_2d = [[row.get(c) for c in cols] for row in rows_dict]

        # Truncamento de payload
        total_rows = rowcount
        truncated = False
        if max_rows_payload and rowcount and rowcount > max_rows_payload:
            rows_dict = rows_dict[:max_rows_payload]
            rows_2d = rows_2d[:max_rows_payload]
            truncated = True

        # Save no cache
        if cache_key and is_select(sql):
            cache.set(
                cache_key,
                {
                    "cols": cols,
                    "rows_2d": rows_2d,
                    "rows_dict": rows_dict,
                    "rowcount": len(rows_dict),
                    "truncated": truncated,
                    "total_rows": total_rows,
                },
                ttl=cache_ttl,
            )

        db_ms = int((time.perf_counter() - t_db0) * 1000)
        log_event(
            APP_LOG,
            "llm_consulta_ok",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            db_ms=db_ms,
            rowcount=total_rows,
            sql_digest=sql_digest(sql),
        )
        return {
            "plan": plan.model_dump(),
            "sql": sql,
            "params": params,
            "executed": True,
            "cols": cols,
            "rows": rows_2d,
            "rows_dict": rows_dict,
            "rowcount": len(rows_dict),
            "truncated": truncated,
            "total_rows": total_rows,
            "cache": False,
        }
    except Exception as e:
        db_ms = int((time.perf_counter() - t_db0) * 1000)
        log_event(
            APP_LOG,
            "llm_consulta_error",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            db_ms=db_ms,
            err=str(e),
            sql_digest=sql_digest(sql),
        )
        raise HTTPException(500, f"Falha ao executar SQL: {e}")

