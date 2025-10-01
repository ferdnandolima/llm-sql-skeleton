# core/validators.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Set

from core.schemas import QueryPlan


class PlanValidationError(Exception):
    """Erro de validação do plano (IA → JSON) contra a intent declarada."""
    pass


def _extract_allowed_order_columns(spec: dict) -> Set[str]:
    """
    Colunas permitidas para ORDER BY:
    - todas as chaves de spec['colunas']
    - e os termos listados em spec['ordenacao']['por'] (removendo alias e ASC/DESC)
    """
    allowed: Set[str] = set((spec.get("colunas") or {}).keys())
    orden = (spec.get("ordenacao") or {}).get("por") or []
    for term in orden:
        if not isinstance(term, str):
            continue
        base = term.strip().split()[0]        # remove ASC/DESC se houver
        if "." in base:
            base = base.split(".")[-1]        # tira alias (t.col → col)
        base = base.replace("`", "")
        if base:
            allowed.add(base)
    return allowed


def validate_plan_vs_intent(plan: QueryPlan, intent_def: Optional[dict]) -> None:
    """
    Regras principais:
      - intent deve existir
      - intent deve declarar 'colunas' (proibido SELECT *)
      - campos[] ⊆ spec['colunas'].keys()
      - filtros apenas os permitidos
      - order_by apenas colunas permitidas
      - limit > 0 e ≤ limit_max (default 1000 se não especificado)
    Lança PlanValidationError com detalhes quando inválido.
    """
    if not intent_def:
        raise PlanValidationError(f"Intent '{plan.intent}' não encontrada no catálogo.")

    spec = intent_def
    cols_map: Dict[str, str] = spec.get("colunas") or {}
    if not cols_map:
        raise PlanValidationError(f"Intent '{plan.intent}' não define 'colunas' (SELECT * é proibido).")

    # --- CAMPOS ---
    if plan.campos:
        allowed_fields = set(cols_map.keys())
        invalid = sorted(set(plan.campos) - allowed_fields)
        if invalid:
            raise PlanValidationError({
                "erro": "Campos fora da intent",
                "intent": plan.intent,
                "campos_invalidos": invalid,
                "campos_permitidos": sorted(allowed_fields),
            })

    # --- FILTROS ---
    # Permitimos:
    # - chaves declaradas em spec['filtros'] (além de 'periodo_em', que é config interna)
    # - alguns nomes comuns normalizados (status, cliente/nu_cli, numero_pedido/nu_pve, pendências)
    allowed_filters = set((spec.get("filtros") or {}).keys()) | {
        "status", "id_status", "id_stat_lancto",
        "cliente", "nu_cli",
        "numero_pedido", "nu_pve",
        "somente_pendentes", "somente_quitados",
    }
    if plan.filtros:
        invalid_f = sorted(set(plan.filtros.keys()) - allowed_filters)
        if invalid_f:
            raise PlanValidationError({
                "erro": "Filtros não permitidos pela intent",
                "intent": plan.intent,
                "filtros_invalidos": invalid_f,
                "filtros_permitidos": sorted(allowed_filters),
            })

    # --- ORDER BY ---
    if getattr(plan, "order_by", None):
        allowed_order_cols = _extract_allowed_order_columns(spec)
        bad_order: List[str] = []
        for item in plan.order_by:
            # item pode ser string ("coluna" ou "t.coluna") OU dict {"coluna": "...", "direcao": "..."}
            if isinstance(item, dict):
                col = item.get("coluna") or item.get("campo") or item.get("col")
            else:
                col = str(item)
            if not col:
                continue
            base = col.strip().split()[0]
            if "." in base:
                base = base.split(".")[-1]
            base = base.replace("`", "")
            if base not in allowed_order_cols:
                bad_order.append(col)
        if bad_order:
            raise PlanValidationError({
                "erro": "Colunas de ordenação não permitidas",
                "intent": plan.intent,
                "order_by_invalidos": bad_order,
                "order_by_permitidos": sorted(allowed_order_cols),
            })

    # --- LIMIT ---
    if getattr(plan, "limit", None) is not None:
        try:
            lim = int(plan.limit)
        except Exception:
            raise PlanValidationError({"erro": "limit inválido", "valor": plan.limit})
        if lim <= 0:
            raise PlanValidationError({"erro": "limit deve ser > 0", "valor": lim})
        regras = spec.get("regras") or {}
        lim_max = int(regras.get("limit_max") or 1000)
        if lim > lim_max:
            raise PlanValidationError({
                "erro": "limit excede o máximo permitido pela intent",
                "intent": plan.intent,
                "limit_solicitado": lim,
                "limit_maximo": lim_max,
            })
