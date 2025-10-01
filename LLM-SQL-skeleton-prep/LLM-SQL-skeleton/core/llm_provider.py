# core/llm_provider.py
import os, json, re
from typing import List, Tuple, Optional, Dict, Any
import requests  # <- novo: usamos HTTP do Ollama (sem precisar do pacote 'ollama')
from pydantic import ValidationError  # para propagar erros de validação do plano
from core.schemas import QueryPlan     # modelo Pydantic do plano estruturado

# ---------- Utils ----------
def _clean_json(text: str) -> str:
    """Extrai o primeiro objeto JSON válido de uma string (remove ```json ... ``` se vier)."""
    if not text:
        return "{}"
    # remove cercas ```json ... ```
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.I | re.M)
    # heurística: pega o primeiro {...} balanceado
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text

def _ollama_client():
    """
    Mantido por compatibilidade, mas não usamos mais o pacote 'ollama'.
    Agora chamamos o servidor via HTTP (requests).
    """
    return None

def _provider() -> str:
    return (os.getenv("LLM_PROVIDER") or "ollama").lower()

def _base_url() -> str:
    # aceita os dois nomes para compatibilidade
    return (
        os.getenv("OLLAMA_BASE_URL")
        or os.getenv("OLLAMA_HOST")
        or "http://127.0.0.1:11434"
    )

def _model_name() -> str:
    # aceita os dois nomes para compatibilidade
    return (
        os.getenv("MODEL_NAME")
        or os.getenv("OLLAMA_MODEL")
        or "qwen2.5:32b-instruct"
    )

_TIMEOUT = int(os.getenv("LLM_TIMEOUT_SECONDS", "120"))
_DEF_TEMP = float(os.getenv("LLM_TEMPERATURE", "0.2"))
_DEF_CTX = int(os.getenv("LLM_NUM_CTX", "8192"))

def _chat_ollama(
    messages: List[Dict[str, str]],
    temperature: float = None,
    max_tokens: int = 1024,
    num_ctx: int = None,
    stream: bool = False,
) -> str:
    temperature = _DEF_TEMP if temperature is None else float(temperature)
    num_ctx = _DEF_CTX if num_ctx is None else int(num_ctx)
    payload = {
        "model": _model_name(),
        "messages": messages,
        "options": {"temperature": temperature, "num_ctx": num_ctx, "num_predict": max_tokens},
        "stream": stream,
    }
    r = requests.post(f"{_base_url()}/api/chat", json=payload, timeout=_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return (data.get("message") or {}).get("content") or data.get("content", "")

def _chat_managed(
    messages: List[Dict[str, str]],
    temperature: float = None,
    max_tokens: int = 1024,
) -> str:
    """Compatível com provedores estilo OpenAI (opcional)."""
    base = os.getenv("MANAGED_BASE_URL")
    key = os.getenv("MANAGED_API_KEY")
    if not base or not key:
        raise RuntimeError("MANAGED_BASE_URL / MANAGED_API_KEY não configurados.")
    temperature = _DEF_TEMP if temperature is None else float(temperature)
    r = requests.post(
        f"{base}/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}"},
        json={
            "model": _model_name(),
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        },
        timeout=_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]

def _chat(
    messages: List[Dict[str, str]],
    temperature: float = None,
    max_tokens: int = 1024,
    num_ctx: int = None,
    stream: bool = False,
) -> str:
    if _provider() == "ollama":
        return _chat_ollama(messages, temperature, max_tokens, num_ctx, stream)
    # fallback/alternativa gerenciada
    return _chat_managed(messages, temperature, max_tokens)

# ---------- Intent picking ----------
def pick_intent_with_llm(utterance: str, candidates: List[Tuple[str, float]]) -> Optional[str]:
    """
    Usa o provedor ativo (Ollama por default) para escolher UMA intent entre candidates.
    Retorna a key escolhida ou None (mantém heurística).
    """
    prov = _provider()
    if prov not in ("ollama", "managed"):
        # só implementado ollama/managed neste passo
        pass

    if not candidates:
        return None

    labels = [k for k, _ in candidates]
    lista = "\n".join(f"- {k}" for k in labels)

    messages = [
        {
            "role": "system",
            "content": (
                "Você é um roteador de intenções. Escolha exatamente UMA chave da lista informada. "
                "Responda SOMENTE com a chave exata, sem aspas, sem comentários."
            ),
        },
        {
            "role": "user",
            "content": f"Texto do usuário:\n{utterance}\n\nChaves possíveis:\n{lista}\n\nResponda apenas com UMA chave exata.",
        },
    ]

    try:
        raw = _chat(messages, temperature=0.0, max_tokens=16, num_ctx=1024).strip()
        # normalizações leves
        choice = raw.strip().strip("`'\" ").splitlines()[0]
        if choice in labels:
            return choice
    except Exception:
        return None
    return None

# ---------- Slot extraction ----------
def extract_slots_with_llm(utterance: str, slot_names: List[str]) -> Dict[str, str]:
    """
    Pede ao LLM para extrair slots e retornar APENAS JSON.
    Retorna dict (pode ser parcial).
    """
    if not slot_names:
        return {}

    # Exemplo de esqueleto para orientar o modelo
    # {"slot1": null, "slot2": null, ...}
    skeleton = "{" + ", ".join([f"\"{k}\": null" for k in slot_names]) + "}"

    prompt_rules = (
        "Extraia os campos solicitados do texto abaixo.\n"
        "- Responda APENAS com um JSON válido, sem comentários.\n"
        "- Use as CHAVES exatamente como fornecidas.\n"
        "- Datas no formato YYYY-MM-DD.\n"
        "- Se não souber o valor de algum campo, deixe null (não invente).\n"
        f"- Esqueleto esperado: {skeleton}\n"
    )

    messages = [
        {"role": "system", "content": prompt_rules},
        {"role": "user", "content": f"Campos: {', '.join(slot_names)}\n\nTexto: {utterance}"},
    ]

    try:
        raw = _chat(messages, temperature=0.0, max_tokens=512, num_ctx=2048).strip()
        raw_json = _clean_json(raw)
        data = json.loads(raw_json)
        if isinstance(data, dict):
            # mantém somente os slots solicitados e filtra vazios
            return {k: v for k, v in data.items() if k in slot_names and v not in (None, "", [])}
    except Exception:
        return {}
    return {}

# ---------- QueryPlan (JSON) ----------
_JSON_INSTRUCOES = """
Você é um planejador de consultas para um sistema NL→SQL com intents.
TAREFA: Dado um pedido do usuário, retorne APENAS um JSON válido, sem explicações, obedecendo o seguinte schema:

{
  "intent": "<nome_da_intent>",
  "campos": ["lista", "de", "campos"],
  "filtros": { "chave": "valor", "outra": 123, "booleano": true },
  "periodo": {
    "relativo": "hoje|ontem|esta_semana|semana_passada|este_mes|mes_passado|este_ano"
    // OU use "inicio" e "fim" no formato YYYY-MM-DD
  },
  "order_by": [ { "campo": "nome_campo", "direcao": "asc|desc" } ],
  "limit": 200,
  "formato": "tabela|resumo"
}

Regras:
- Use nomes de intents e campos do domínio do ERP (português, snake_case).
- Não invente campos; se não tiver certeza, deixe "campos" vazio.
- Nunca envolva o JSON em markdown, texto antes/depois, ou comentários.
""".strip()

def _build_system_prompt_for_plan(intents_catalogo: Dict[str, Any]) -> str:
    nomes = sorted(list(intents_catalogo.keys()))
    lista = ", ".join(nomes) if nomes else "(nenhuma intent listada)"
    return _JSON_INSTRUCOES + "\n\nIntents disponíveis: " + lista + "\n"

def request_plan_via_llm(pergunta: str, intents_catalogo: Dict[str, Any]) -> QueryPlan:
    """
    Chama o LLM pedindo um QueryPlan em JSON e valida com Pydantic.
    - pergunta: texto do usuário
    - intents_catalogo: dict retornado pelo loader de intents
    Retorna: QueryPlan
    Lança: json.JSONDecodeError ou pydantic.ValidationError em caso de formato inválido.
    """
    system_prompt = _build_system_prompt_for_plan(intents_catalogo)

    # 1) Primeira tentativa: pedir JSON puro conforme schema
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": pergunta},
    ]

    texto = _chat(messages, temperature=0.1, max_tokens=900, num_ctx=4096).strip()

    # 2) Tentar carregar JSON cru
    try:
        data = json.loads(_clean_json(texto))
    except json.JSONDecodeError:
        # 2b) Reparar JSON via segunda chamada simples
        repair_messages = [
            {
                "role": "system",
                "content": (
                    "Você recebeu um JSON inválido. Corrija para JSON válido, SEM COMENTÁRIOS/markdown, "
                    "mantendo o mesmo conteúdo e chaves."
                ),
            },
            {"role": "user", "content": texto},
        ]
        reparo = _chat(repair_messages, temperature=0.0, max_tokens=900, num_ctx=2048).strip()
        data = json.loads(_clean_json(reparo))  # deixa levantar JSONDecodeError se ainda vier inválido

    # 3) Validar com Pydantic → QueryPlan
    plan = QueryPlan.model_validate(data)  # pode levantar ValidationError; deixe subir
    return plan
