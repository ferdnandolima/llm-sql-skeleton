# core/cache.py
from __future__ import annotations
import json, time, threading, hashlib, re
from typing import Any, Optional, Tuple, Dict

_select_pat = re.compile(r"^\s*select\b", re.IGNORECASE)

def is_select(sql: str) -> bool:
    return bool(_select_pat.match(sql or ""))

def _stable_params_repr(params: Any) -> str:
    try:
        if isinstance(params, (list, tuple)):
            return json.dumps(list(params), ensure_ascii=False, separators=(",", ":"), default=str)
        if isinstance(params, dict):
            return json.dumps(params, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str)
        return json.dumps(params, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        return str(params)

def make_cache_key(sql: str, params: Any, tenant: str = "default") -> str:
    base = (sql or "").strip()
    p = _stable_params_repr(params)
    raw = f"{tenant}||{base}||{p}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

class TTLCache:
    def __init__(self, max_items: int = 256):
        self.max_items = max_items
        self._data: Dict[str, Tuple[float, Any]] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        now = time.monotonic()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            exp, val = item
            if exp < now:
                self._data.pop(key, None)
                return None
            return val

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            return
        now = time.monotonic()
        exp = now + ttl_seconds
        with self._lock:
            # limpeza simples se estourar
            if len(self._data) >= self.max_items:
                # remove os 10 mais antigos (heurística simples)
                oldest = sorted(self._data.items(), key=lambda kv: kv[1][0])[:10]
                for k, _ in oldest:
                    self._data.pop(k, None)
            self._data[key] = (exp, value)

# instância global e “construtor” configurável
_cache: TTLCache | None = None

def get_cache(max_items: int = 256) -> TTLCache:
    global _cache
    if _cache is None:
        _cache = TTLCache(max_items=max_items)
    return _cache
