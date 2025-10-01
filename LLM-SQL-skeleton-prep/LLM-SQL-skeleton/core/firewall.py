import re

class SQLFirewall:
    def __init__(self, configs: dict):
        self.cfg = configs.get("regras_sql", {})
        self.permitir = set(self.cfg.get("comandos", {}).get("permitir", ["SELECT"]))
        self.proibir = set(self.cfg.get("comandos", {}).get("proibir", []))
        self.proibicoes_especificas = self.cfg.get("proibicoes_especificas", {})

    def validate(self, sql: str, meta: dict):
        s = sql.strip().upper()
        if not s.startswith("SELECT "):
            raise PermissionError("Apenas SELECT é permitido.")
        for cmd in self.proibir:
            if re.search(rf"\b{cmd}\b", s):
                raise PermissionError(f"Comando proibido detectado: {cmd}")
        if self.proibicoes_especificas.get("union", False) is False and " UNION " in s:
            raise PermissionError("UNION não permitido.")
        if self.proibicoes_especificas.get("select_into", False) is False and " INTO " in s:
            raise PermissionError("SELECT INTO não permitido.")
        if self.proibicoes_especificas.get("order_by_rand", False) is False and "ORDER BY RAND()" in s:
            raise PermissionError("ORDER BY RAND() proibido.")
        if self.proibicoes_especificas.get("star_select", False) is False and re.search(r"SELECT\s+\*", s):
            raise PermissionError("SELECT * proibido.")
        if meta.get("retorna") == "linhas":
            limits = self.cfg.get("limites", {})
            if limits.get("obrigar_limit_em_listas", True) and " LIMIT " not in s:
                raise PermissionError("Listagens devem ter LIMIT.")
        return True
