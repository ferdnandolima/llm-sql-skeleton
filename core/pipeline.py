import time
from core.router import Router
from core.templates import TemplateEngine
from core.firewall import SQLFirewall
from core.executor import SQLExecutor
from util.text import normalize_question
from util.dates import resolve_period

class Pipeline:
    def __init__(self, configs: dict):
        self.configs = configs
        self.router = Router(configs)
        self.templates = TemplateEngine(configs)
        self.firewall = SQLFirewall(configs)
        self.executor = SQLExecutor(configs)

    def handle_request(self, pergunta: str, tenant_id: str, idioma: str, params: dict) -> dict:
        t0 = time.time()
        app_cfg = self.configs.get("app", {})
        tz = app_cfg.get("servidor", {}).get("timezone", "America/Sao_Paulo")

        pergunta_norm = normalize_question(pergunta, idioma=idioma)

        # 1) Intenção + slots
        intent, slots = self.router.classify_and_extract(pergunta_norm, params)

        # 2) Resolver período (se aplicável)
        if "periodo" in slots and isinstance(slots["periodo"], str):
            slots["periodo_resolvido"] = resolve_period(slots["periodo"], tz, self.configs.get("datas", {}), slots.get("parametros_periodo"))
        elif "periodo" in slots and isinstance(slots["periodo"], (list, tuple)):
            slots["periodo_resolvido"] = slots["periodo"]

        # 3) SQL pelo template
        sql, bind_params, meta = self.templates.build_sql(intent, slots)
        origem = "template"

        # 4) Firewall sintático
        self.firewall.validate(sql, meta)

        # 5) Execução (com EXPLAIN gate no executor)
        rows, agg = self.executor.execute(tenant_id, sql, bind_params, meta)

        t1 = time.time()
        return {
            "resposta_texto": self._format_answer(intent, rows, agg),
            "dados": agg if agg is not None else {"rows": rows},
            "interpretacao": {"intencao": intent, "slots": slots},
            "sql_executado": sql,
            "origem": origem,
            "latencia_ms": int((t1 - t0) * 1000),
            "avisos": []
        }

    def _format_answer(self, intent: str, rows, agg):
        if intent == "contagem_por_periodo" and agg is not None and "qtd_pedidos" in agg:
            return f"Foram {agg['qtd_pedidos']} pedidos no período."
        if intent == "listar_ultimos_N_pedidos" and isinstance(rows, list):
            return f"Encontrei {len(rows)} pedidos (últimos N)."
        return "Ok."
