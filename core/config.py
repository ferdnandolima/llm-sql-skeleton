import os, yaml

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def _load_yaml(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_all_configs() -> dict:
    cfg_dir = os.path.join(BASE_DIR, "config")
    data_dir = os.path.join(BASE_DIR, "data")
    app = _load_yaml(os.path.join(cfg_dir, "app.yaml"))
    tenants = _load_yaml(os.path.join(cfg_dir, "tenants.yaml"))
    intencoes = _load_yaml(os.path.join(cfg_dir, "intencoes.yaml"))
    regras_sql = _load_yaml(os.path.join(cfg_dir, "regras_sql.yaml"))
    datas = _load_yaml(os.path.join(cfg_dir, "datas.yaml"))
    return {
        "app": app,
        "tenants": tenants,
        "intencoes": intencoes,
        "regras_sql": regras_sql,
        "datas": datas,
        "dirs": {"config": cfg_dir, "data": data_dir},
    }
