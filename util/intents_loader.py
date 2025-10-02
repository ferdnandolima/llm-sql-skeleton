import os, glob, yaml, re, json
from typing import Dict, Any

RESERVED_KEYS = {
    "namespace", "versao", "timezone", "locale", "comentarios", "_meta", "tabelas",
}

_macro_pat = re.compile(r"\{\{\s*([a-zA-Z0-9_\.]+)\s*\}\}")

def _resolve_macros(obj, ctx):
    # Resolve apenas macros conhecidas no contexto.
    # Placeholders de runtime (ex.: {{cliente}}) ficam INTACTOS.
    if isinstance(obj, dict):
        return {k: _resolve_macros(v, ctx) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_macros(v, ctx) for v in obj]
    if isinstance(obj, str):
        def repl(m):
            key = m.group(1).strip()
            if key in ctx:
                return str(ctx[key])
            # macro desconhecida => mantém como está (não quebra intents)
            return m.group(0)
        return _macro_pat.sub(repl, obj)
    return obj

def _is_enabled(spec: Dict[str, Any]) -> bool:
    if not isinstance(spec, dict):
        return False
    if spec.get("enabled") is False:
        return False
    if spec.get("habilitado") is False:
        return False
    return True

def _seems_intent(spec: Dict[str, Any]) -> bool:
    # Só registra como intent se tiver tabela_principal (proibimos SELECT *)
    return isinstance(spec, dict) and "tabela_principal" in spec

def load_intents(intents_dir="config/intents"):
    registry: Dict[str, Dict[str, Any]] = {}
    skipped: Dict[str, str] = {}

    for path in glob.glob(os.path.join(intents_dir, "*.yaml")):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        ns = data.get("namespace") or os.path.splitext(os.path.basename(path))[0]
        ctx = dict(data)

        # Modo lista
        if isinstance(data.get("intents"), list):
            for intent in data["intents"]:
                name = intent.get("name")
                if not name or not isinstance(intent, dict):
                    continue
                intent_resolved = _resolve_macros(intent, ctx)
                key = f"{ns}.{name}"
                if not _is_enabled(intent_resolved):
                    skipped[key] = "disabled"
                    continue
                if not _seems_intent(intent_resolved):
                    skipped[key] = "faltando 'tabela_principal'"
                    continue
                if key in registry:
                    raise ValueError(f"Intent duplicada: {key}")
                registry[key] = intent_resolved
            continue

        # Modo dict plano
        for name, spec in data.items():
            if name in RESERVED_KEYS:
                continue
            if not isinstance(spec, dict):
                continue
            spec_resolved = _resolve_macros(spec, ctx)
            key = f"{ns}.{name}"
            if not _is_enabled(spec_resolved):
                skipped[key] = "disabled"
                continue
            if not _seems_intent(spec_resolved):
                skipped[key] = "faltando 'tabela_principal'"
                continue
            if key in registry:
                raise ValueError(f"Intent duplicada: {key}")
            registry[key] = spec_resolved

    if skipped:
        print("[intent-loader] Ignorados (não-intents ou desativados):")
        for k, motivo in skipped.items():
            print(f"  - {k} — {motivo}")

    return registry

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="config/intents")
    args = p.parse_args()
    intents = load_intents(args.dir)
    print(json.dumps({"count": len(intents), "keys": sorted(list(intents.keys()))[:50]}, ensure_ascii=False, indent=2))
