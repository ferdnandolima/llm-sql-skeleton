import re

class Router:
    def __init__(self, configs: dict):
        self.configs = configs
        self.intencoes = configs.get("intencoes", {})

    def classify_and_extract(self, pergunta_norm: str, params: dict) -> tuple[str, dict]:
        slots = {}

        # período
        periodo = self._guess_period(pergunta_norm)
        if periodo:
            slots["periodo"] = periodo
            if periodo == "ultimos_n_dias":
                m = re.search(r"(?:ultimos|últimos)\s+(\d+)\s+dias", pergunta_norm)
                if m:
                    slots["parametros_periodo"] = {"n": int(m.group(1))}

        # status
        if "faturad" in pergunta_norm:
            slots["status"] = "faturado"
        elif "cancelad" in pergunta_norm:
            slots["status"] = "cancelado"

        # cliente id simples: "cliente 123"
        m = re.search(r"cliente\s+(\d+)", pergunta_norm)
        if m:
            slots["cliente"] = int(m.group(1))

        # 1) Contagem por período
        if ("quant" in pergunta_norm or "qtd" in pergunta_norm or "numero" in pergunta_norm) and "pedido" in pergunta_norm:
            if "contagem_por_periodo" in self.intencoes:
                return "contagem_por_periodo", slots

        # 2) Listar últimos N pedidos
        if ("ultimo" in pergunta_norm or "ultimos" in pergunta_norm or "últimos" in pergunta_norm) and "pedido" in pergunta_norm:
            mN = re.search(r"\b(\d{1,4})\b", pergunta_norm)
            if mN:
                slots["N"] = int(mN.group(1))
            if "listar_ultimos_N_pedidos" in self.intencoes:
                return "listar_ultimos_N_pedidos", slots

        raise ValueError("Não consegui identificar a intenção. Exemplos: 'Quantos pedidos ontem?' ou 'Liste os 10 últimos pedidos'.")

    def _guess_period(self, q: str) -> str | None:
        keys = [
            "ontem","anteontem","hoje","mes atual","mes anterior",
            "ultimos 7 dias","semana passada","semana atual","ano atual","ano anterior"
        ]
        for k in keys:
            if k in q:
                return k
        m = re.search(r"(?:ultimos|últimos)\s+(\d+)\s+dias", q)
        if m:
            return "ultimos_n_dias"
        return None
