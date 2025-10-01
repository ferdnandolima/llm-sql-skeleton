# core/nlu.py
import re
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Dict, Tuple, Optional, List, Any

# LLM opcional (Ollama/OpenAI) — se o módulo não existir, caímos em no-ops
try:
    from core.llm_provider import pick_intent_with_llm, extract_slots_with_llm  # type: ignore
except Exception:  # fallback seguro quando o provedor não está configurado
    def pick_intent_with_llm(utterance, candidates):
        return None
    def extract_slots_with_llm(utterance, slot_names):
        return {}

# =========================
# Normalização & Scoring
# =========================
def _normalize(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^\w\s/:-]", " ", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()

def _score(a: str, b: str) -> float:
    """Mix de Similaridade (difflib) + Jaccard simples."""
    a_n, b_n = _normalize(a), _normalize(b)
    sm = SequenceMatcher(None, a_n, b_n).ratio()
    set_a, set_b = set(a_n.split()), set(b_n.split())
    jacc = len(set_a & set_b) / max(1, len(set_a | set_b))
    return 0.6 * sm + 0.4 * jacc

# --- boosts de domínio/palavra-chave para ajudar o ranqueamento ---
DOMAIN_HINTS = {
    "vendas":     ["pedido", "pedidos", "pve", "venda", "vendas"],
    "financeiro": ["título", "titulo", "titulos", "boleto", "duplicata", "contas a receber", "financeiro"],
    "producao":   ["produção", "producao", "pcp", "ordem", "op"],
    "clientes":   ["cliente", "clientes", "classificação", "classificacao", "rota", "vendedor"],  # NEW
}

def _domain_bonus(utt: str, intent_key: str) -> float:
    dom = intent_key.split(".", 1)[0].lower()
    hints = DOMAIN_HINTS.get(dom, [])
    t = _normalize(utt)
    return 0.18 if any(h in t for h in hints) else 0.0

def _keyword_bonus(utt: str, intent_key: str) -> float:
    t = _normalize(utt)
    ik = intent_key.lower()
    b = 0.0
    if "pedido" in t and ("vendas" in ik or "pedido" in ik):
        b += 0.12
    if ("titulo" in t or "título" in t or "boleto" in t) and ("financeiro" in ik or "titulo" in ik):
        b += 0.12
    if any(w in t for w in ["cliente","clientes"]) and ("cliente" in ik or "clientes" in ik):  # NEW
        b += 0.10
    return b

def rank_intents(utterance: str, intents_registry: Dict[str, dict], topk: int = 5) -> List[Tuple[str, float]]:
    """Ranqueia intents pelo match com descrição/exemplos + bônus de domínio/palavra-chave."""
    cand: List[Tuple[str, float]] = []
    for key, spec in intents_registry.items():
        nome = key.split(".", 1)[-1]
        desc = spec.get("descricao") or spec.get("description") or ""
        exemplos = spec.get("exemplos") or spec.get("examples") or []
        base = f"{nome} {desc} " + " ".join(exemplos if isinstance(exemplos, list) else [])
        score = _score(utterance, base)
        score += _domain_bonus(utterance, key)
        score += _keyword_bonus(utterance, key)
        cand.append((key, score))
    cand.sort(key=lambda x: x[1], reverse=True)
    return cand[:topk]

# =========================
# Extração de slots
# =========================
_MESES = {
    "janeiro":1,"fevereiro":2,"março":3,"marco":3,"abril":4,"maio":5,"junho":6,
    "julho":7,"agosto":8,"setembro":9,"outubro":10,"novembro":11,"dezembro":12
}
_STATUS = {"pago":"PAGO","pendente":"PENDENTE","cancelado":"CANCELADO"}

def _parse_data_br(txt: str) -> Optional[datetime]:
    # 01/09/2025 | 01-09-25
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b", txt)
    if m:
        d, mth, y = map(int, m.groups())
        if y < 100: y += 2000
        return datetime(y, mth, d)
    # 1 de setembro (opcional "de 2025")
    m = re.search(r"\b(\d{1,2})\s+de\s+([a-zç]+)(?:\s+de\s+(\d{4}))?\b", txt, flags=re.I)
    if m:
        d = int(m.group(1)); mes = _MESES.get(_normalize(m.group(2)), None)
        y = int(m.group(3)) if m.group(3) else datetime.now().year
        if mes: return datetime(y, mes, d)
    return None

def _periodo_natural(txt: str, now: Optional[datetime]=None) -> Optional[Tuple[datetime, datetime]]:
    now = now or datetime.now()
    t = _normalize(txt)

    # dias específicos
    if "hoje" in t:
        di = now.replace(hour=0,minute=0,second=0,microsecond=0)
        df = di + timedelta(days=1) - timedelta(seconds=1)
        return di, df
    if "ontem" in t:
        di = (now - timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
        df = di + timedelta(days=1) - timedelta(seconds=1)
        return di, df
    if "anteontem" in t:
        di = (now - timedelta(days=2)).replace(hour=0,minute=0,second=0,microsecond=0)
        df = di + timedelta(days=1) - timedelta(seconds=1)
        return di, df

    # semana atual
    if any(x in t for x in ["esta semana","essa semana","semana atual"]):
        di = now - timedelta(days=now.weekday())  # segunda
        di = di.replace(hour=0,minute=0,second=0,microsecond=0)
        df = di + timedelta(days=7) - timedelta(seconds=1)
        return di, df

    # semana passada (segunda a domingo anterior)
    if "semana passada" in t:
        start_this = (now - timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
        di = start_this - timedelta(days=7)
        df = start_this - timedelta(seconds=1)
        return di, df

    # mês atual
    if any(x in t for x in ["este mês","esse mes","mês atual","mes atual"]):
        di = now.replace(day=1, hour=0,minute=0,second=0,microsecond=0)
        if di.month == 12:
            df = di.replace(year=di.year+1, month=1) - timedelta(seconds=1)
        else:
            df = di.replace(month=di.month+1) - timedelta(seconds=1)
        return di, df

    # mês passado
    if "mês passado" in t or "mes passado" in t:
        first_this = now.replace(day=1, hour=0,minute=0,second=0,microsecond=0)
        last_prev = first_this - timedelta(seconds=1)
        di = last_prev.replace(day=1, hour=0,minute=0,second=0,microsecond=0)
        df = last_prev
        return di, df

    # últimos N dias
    m = re.search(r"últim[oa]s?\s+(\d+)\s+dias", t)
    if m:
        n = int(m.group(1))
        di = (now - timedelta(days=n)).replace(hour=0,minute=0,second=0,microsecond=0)
        df = now
        return di, df

    return None

# --------- Helpers de extração (NEW) ---------

_NUM_RE = re.compile(r"\d+")
def _pull_numbers(s: str) -> List[int]:
    return [int(x) for x in _NUM_RE.findall(s or "")]

def _pull_text_quoted(s: str) -> Optional[str]:
    m = re.search(r"[\"']([^\"']+)[\"']", s)
    if m:
        return m.group(1).strip()
    return None

def _maybe_bool(val: str) -> Optional[str]:
    t = _normalize(val)
    if t in {"sim","s","yes","y","true","1"}:
        return "sim"
    if t in {"nao","não","n","no","false","0"}:
        return "não"
    return None

# =========================
# Extração de slots PT-BR (expandida)
# =========================
def extract_slots_pt(texto: str) -> Dict[str, Any]:
    """Extrai slots comuns e específicos de clientes (data_ini, data_fim, N, status, nu_pve, cliente, classificacao, rota, vendedor, sexo, tipo, flags e *likes*)."""
    out: Dict[str, Any] = {}

    # --- Período: explícito (duas datas) ou natural
    datas = re.findall(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", texto)
    if len(datas) >= 2:
        d1 = _parse_data_br(datas[0]); d2 = _parse_data_br(datas[1])
        if d1 and d2:
            out["data_ini"] = d1.strftime("%Y-%m-%d")
            out["data_fim"] = d2.strftime("%Y-%m-%d")
    else:
        p = _periodo_natural(texto)
        if p:
            di, df = p
            out["data_ini"] = di.strftime("%Y-%m-%d")
            out["data_fim"] = df.strftime("%Y-%m-%d")

    # --- N (top / últimos / primeiros)
    m = re.search(r"\b(?:top|últim[oa]s?|ultim[oa]s?|primeir[oa]s?)\s+(\d+)\b", texto, flags=re.I)
    if m:
        try:
            out["N"] = int(m.group(1))
        except Exception:
            pass

    # --- Status
    tnorm = _normalize(texto)
    for k, v in _STATUS.items():
        if k in tnorm:
            out["status"] = v
            break

    # --- Pedido & Cliente (números diretos)
    m = re.search(r"\bpedido\s*(\d+)\b", tnorm)
    if m: out["nu_pve"] = int(m.group(1))
    m = re.search(r"\bcliente\s*(\d+)\b", tnorm)
    if m: out["cliente"] = int(m.group(1))

    # =============================
    # Slots específicos de clientes
    # =============================

    # Classificação (único)
    m = re.search(r"classifica[cç][aã]o\s*(\d+)\b", tnorm)
    if m:
        out["classificacao"] = int(m.group(1))
    # Classificações (lista)
    m = re.search(r"classifica[cç][aã]o(?:es|s)?\s+((?:\d+[,\s]*)+)", tnorm)
    if m:
        nums = _pull_numbers(m.group(1))
        if nums:
            out["classificacoes"] = nums

    # Rota
    m = re.search(r"\brota\s*(\d+)\b", tnorm)
    if m:
        out["rota"] = int(m.group(1))

    # Vendedor (único)
    m = re.search(r"\bvendedor(?:a)?\s*(\d+)\b", tnorm)
    if m:
        out["vendedor"] = int(m.group(1))
    # Vendedores (lista)
    m = re.search(r"\bvendedores?\s+((?:\d+[,\s]*)+)", tnorm)
    if m:
        nums = _pull_numbers(m.group(1))
        if nums:
            out["vendedores"] = nums

    # Sexo
    if any(w in tnorm for w in ["sexo masculino", "masculino"]):
        out["sexo"] = "masculino"
    elif any(w in tnorm for w in ["sexo feminino", "feminino"]):
        out["sexo"] = "feminino"
    elif any(w in tnorm for w in ["indefinido", "não informado", "nao informado"]):
        out["sexo"] = "indefinido"

    # Tipo (PF/PJ)
    if re.search(r"\bpf\b|pessoa f[ií]sica", tnorm):
        out["tipo"] = "pf"
    elif re.search(r"\bpj\b|pessoa j[uú]r[ií]dica", tnorm):
        out["tipo"] = "pj"

    # Flags comuns (sim/não)
    if "ativos" in tnorm or "somente ativos" in tnorm:
        out["ativo"] = "sim"
    if "inativos" in tnorm or "somente inativos" in tnorm:
        out["ativo"] = "não"
    if "bloqueados" in tnorm or "somente bloqueados" in tnorm:
        out["bloqueado"] = "sim"
    if "desbloqueados" in tnorm or "sem bloqueio" in tnorm:
        out["bloqueado"] = "não"
    if "consumidor final" in tnorm:
        out["consumidor"] = "sim"
    if "simples nacional" in tnorm or "do simples" in tnorm:
        out["simples"] = "sim"
    if "estrangeiro" in tnorm or "estrangeiros" in tnorm:
        out["estrangeiro"] = "sim"

    # Documento (CPF/CNPJ)
    m = re.search(r"\b(?:cpf|cnpj|documento)\s*([0-9.\-\/]+)\b", texto, flags=re.I)
    if m:
        out["documento"] = m.group(1).strip()

    # LIKEs: nome, fantasia, email
    # nome parecido/contendo "xxx"
    m = re.search(r"nome\s+(?:parecido\s+com|contendo|cont[eé]m|que\s+contenha)\s+(.+)", texto, flags=re.I)
    if m:
        quoted = _pull_text_quoted(m.group(1)) or m.group(1).strip()
        if quoted:
            out["nome_like"] = quoted

    # fantasia contendo "xxx"
    m = re.search(r"fanta(?:sia)?\s+(?:contendo|cont[eé]m|que\s+contenha)\s+(.+)", texto, flags=re.I)
    if m:
        quoted = _pull_text_quoted(m.group(1)) or m.group(1).strip()
        if quoted:
            out["fantasia_like"] = quoted

    # email terminando/contendo
    m = re.search(r"e-?mail|email\s+(?:terminando\s+em|contendo|cont[eé]m|que\s+contenha)\s+(.+)", texto, flags=re.I)
    if m:
        quoted = _pull_text_quoted(m.group(1)) or m.group(1).strip()
        if quoted:
            # "terminando em gmail" vira um like simples "gmail" (o builder já envolve com %)
            out["email_like"] = quoted

    return out

# =========================
# API principal (rotear + slots)
# =========================
def route_and_fill(
    utterance: str,
    intents_registry: Dict[str, dict],
    use_llm: bool = False,
    threshold: float = 0.55,
    llm_slots: bool = True
) -> Dict:
    """
    - Ranqueia intents por heurística + boosts.
    - Se use_llm=True e (score baixo/empate), pede desempate ao LLM (entre top-K).
    - Extrai slots por heurística; se faltar e llm_slots=True, tenta complementar com LLM.
    """
    if not intents_registry:
        return {"intent": None, "score": 0.0, "slots": {}, "alternatives": []}

    ranked = rank_intents(utterance, intents_registry, topk=5)
    if not ranked:
        return {"intent": None, "score": 0.0, "slots": {}, "alternatives": []}

    best_key, best_score = ranked[0]

    # Desempate com LLM (opcional)
    if use_llm and (best_score < threshold or (len(ranked) > 1 and abs(ranked[0][1] - ranked[1][1]) < 0.05)):
        llm_choice = pick_intent_with_llm(utterance, ranked)
        if llm_choice and llm_choice in intents_registry:
            # Mantemos o score heurístico (serve como "confiança" para o gate externo)
            best_key = llm_choice

    # Slots por heurística (agora bem mais rica)
    slots = extract_slots_pt(utterance)

    # Complementar slots via LLM (opcional)
    if use_llm and llm_slots and best_key:
        spec = intents_registry.get(best_key) or {}
        obrig = spec.get("slots_obrigatorios") or []
        opcs  = spec.get("slots_opcionais") or []
        wanted = list(dict.fromkeys((obrig or []) + (opcs or [])))  # únicos, ordem estável
        missing = [s for s in wanted if s not in slots]
        if missing:
            llm_slots_dict = extract_slots_with_llm(utterance, wanted)
            for k, v in (llm_slots_dict or {}).items():
                if k not in slots and v not in (None, "", []):
                    slots[k] = v

    return {
        "intent": best_key,
        "score": round(best_score, 3),
        "slots": slots,
        "alternatives": ranked[1:]
    }
