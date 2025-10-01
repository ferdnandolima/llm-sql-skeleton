# core/schemas.py
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Literal

from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    conint,
    model_validator,
)

class OrderBy(BaseModel):
    campo: str
    direcao: Literal["asc", "desc"] = "desc"

    model_config = ConfigDict(extra="forbid")


class Periodo(BaseModel):
    # Use um OU outro: relativo  OU  (inicio & fim)
    relativo: Optional[
        Literal[
            "hoje",
            "ontem",
            "esta_semana",
            "semana_passada",
            "este_mes",
            "mes_passado",
            "este_ano",
        ]
    ] = None
    inicio: Optional[date] = None
    fim: Optional[date] = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def _check_interval(self) -> "Periodo":
        # Se não houver 'relativo', exija 'inicio' E 'fim' juntos
        if self.relativo is None:
            if (self.inicio is None) != (self.fim is None):
                raise ValueError("Informe 'relativo' OU 'inicio' e 'fim' (ambos).")
        return self


class QueryPlan(BaseModel):
    """Plano estruturado que a IA deve retornar para montarmos o SQL de forma determinística."""

    intent: str
    campos: List[str] = Field(default_factory=list, description="Campos a exibir (lógicos/permitidos pela intent)")
    filtros: Dict[str, Any] = Field(default_factory=dict, description="Filtros por chave=valor")
    periodo: Optional[Periodo] = None
    order_by: List[OrderBy] = Field(default_factory=list)
    limit: Optional[conint(gt=0, le=1000)] = 200
    formato: Optional[Literal["tabela", "resumo"]] = "tabela"

    model_config = ConfigDict(extra="forbid")


__all__ = ["OrderBy", "Periodo", "QueryPlan"]
