# api/schemas.py
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError, conint, field_validator


class Periodo(BaseModel):
    ini: Optional[str] = None  # "YYYY-MM-DD"
    fim: Optional[str] = None  # "YYYY-MM-DD"

    @field_validator("ini", "fim")
    @classmethod
    def _valida_iso(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError("deve estar no formato YYYY-MM-DD")
        return v


class Consulta(BaseModel):
    """
    Saída esperada da IA (strict JSON):
    {
      "intent": "nome_da_intent",
      "campos": ["campo1", "campo2"],
      "filtros": {"coluna":"valor", "...":"..."},
      "periodo": {"ini":"YYYY-MM-DD","fim":"YYYY-MM-DD"},
      "order_by": ["coluna1", "-coluna2"],
      "limit": 200
    }
    """
    intent: str
    campos: List[str] = Field(default_factory=list)
    filtros: Dict[str, Any] = Field(default_factory=dict)
    periodo: Optional[Periodo] = None
    order_by: Optional[List[str]] = None
    limit: conint(gt=0, le=1000) = 200  # limite duro; refinamos depois por intent


class ConsultaInvalida(Exception):
    def __init__(self, message: str, details: Any | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


def parse_and_validate_consulta(
    raw: str | Dict[str, Any],
    allowed_fields: Optional[set[str]] = None,
) -> Consulta:
    """
    - Converte string JSON -> dict (se necessário)
    - Valida contra o modelo Consulta
    - Se allowed_fields for fornecido, rejeita 'campos' fora da intent
    """
    data = raw if isinstance(raw, dict) else json.loads(raw)
    consulta = Consulta.model_validate(data)

    if allowed_fields is not None:
        extras = set(consulta.campos) - allowed_fields
        if extras:
            raise ConsultaInvalida(
                "Campos fora da intent",
                {
                    "campos_invalidos": sorted(extras),
                    "campos_permitidos": sorted(allowed_fields),
                },
            )

    return consulta


def format_validation_error(e: ValidationError) -> Dict[str, Any]:
    """
    Transforma o ValidationError em payload amigável para o cliente/UI.
    """
    return {
        "erro": "JSON inválido",
        "detalhes": e.errors(),  # lista pydantic-style
        "exemplo": {
            "intent": "nome_intent",
            "campos": ["coluna1", "coluna2"],
            "filtros": {"coluna": "valor"},
            "periodo": {"ini": "2025-01-01", "fim": "2025-01-31"},
            "order_by": ["coluna1", "-coluna2"],
            "limit": 200,
        },
    }
