# util/domains_loader.py
from pathlib import Path
import yaml

def load_domains(root: str) -> dict:
    """
    Carrega todos os arquivos *.yml/*.yaml em config/ que contenham a chave top-level 'dominios'.
    Também mescla com config/dominios/*.y*ml se essa pasta existir (opcional).
    """
    base = Path(root) / "config"
    out = {}

    candidates = list(base.glob("*.y*ml"))
    domdir = base / "dominios"
    if domdir.exists():
        candidates += list(domdir.glob("*.y*ml"))

    for f in candidates:
        try:
            data = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        doms = data.get("dominios")
        if isinstance(doms, dict):
            for name, items in doms.items():
                # última definição vence em caso de duplicata
                out[name] = items
    return out

def coerce_enum(value, domain_items):
    """Aceita código (int/str) ou rótulo (case-insensitive) e devolve o código."""
    if value is None:
        return None
    s = str(value).strip().lower()
    for item in domain_items or []:
        if str(item.get("codigo")).lower() == s or str(item.get("rotulo","")).strip().lower() == s:
            return item.get("codigo")
    raise ValueError(f"Valor '{value}' não encontrado no domínio.")
