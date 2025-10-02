import re
import unicodedata

class Router:
    def __init__(self, configs: dict):
        self.configs = configs
        self.intencoes = configs.get("intencoes", {})

    # -------------------------------
    # Helpers
    # -------------------------------
    @staticmethod
    def _normalize(s: str) -> str:
        """lowercase + remove acentos para regex robusta"""
        s = unicodedata.normalize("NFD", s)
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
        return s.lower()

    def _extract_N(self, text: str) -> int | None:
        """
        Extrai N somente quando houver gatilho 'últimos/últimas/top'.
        Evita capturar unidades de tempo (ex.: 'últimas 24 horas').
        """
        t = self._normalize(text)

        # Unidades de tempo a serem excluídas (não são limite N)
        units = r'(?:h|hora|horas|min|minutos|dia|dias|semana|semanas|mes|meses|ano|anos)'

        patterns = [
            # 'últimos 20 ...' / 'ultimas 20 ...' (com/sem acento após normalização)
            rf'\b(?:ultimos?|ultimas?)\s+(\d{{1,4}})(?!\s*{units})\b',
            # 'top 10' / 'top-10' / 'top10'
            rf'\btop[\s\-]*?(\d{{1,4}})\b',
            # '20 últimos' / '20 ultimas'
            rf'\b(\d{{1,4}})\s+(?:ultimos?|ultimas?)\b(?!\s*{units})',
        ]

        for pat in patterns:
            m = re.search(pat, t)
            if m:
                try:
                    n = int(m.group(1))
                    if 1 <= n <= 10000:
                        return n
                except ValueError:
                    pass
        return None

    # -------------------------------
    # Público
    # -------------------------------
    def classify_and_extract(self, pergunta_norm: str, params: dict) -> tuple[str, dict]:
        slots = {}
        tnorm = self._normalize(pergunta_norm)

        # período
        periodo = self._guess_period(pergunta_norm)
        if periodo:
            slots["periodo"] = periodo
            if periodo == "ultimos_n_dias":
                # aceita 'ultimos/ultimas N dias' (com/sem acento)
                m = re.search(r"(?:ultimos|ultimas)\s+(\d+)\s+dias", tnorm)
                if m:
                    slots["parametros_periodo"] = {"n": int(m.group(1))}

        # status
        if "faturad" in tnorm:
            slots["status"] = "faturado"
        elif "cancelad" in tnorm:
            slots["status"] = "cancelado"

        # cliente id simples: "cliente 123"
        m = re.search(r"cliente\s+(\d+)", tnorm)
        if m:
            slots["cliente"] = int(m.group(1))

        # 1) Contagem por período: "quantos pedidos ...?"
        if ("quant" in tnorm or "qtd" in tnorm or "numero" in tnorm) and "pedido" in tnorm:
            if "contagem_por_periodo" in self.intencoes:
                return "contagem_por_periodo", slots

        # 2) Listar últimos N pedidos
        gatilho_ultimos_top = ("ultimo" in tnorm or "ultimos" in tnorm or "ultimas" in tnorm or "top" in tnorm)
        if gatilho_ultimos_top and "pedido" in tnorm:
            n = self._extract_N(pergunta_norm)
            if n is not None:
                slots["N"] = n
            # Importante: NÃO fazer fallback para "primeiro número que aparecer"
            if "listar_ultimos_N_pedidos" in self.intencoes:
                return "listar_ultimos_N_pedidos", slots

        raise ValueError("Não consegui identificar a intenção. Exemplos: 'Quantos pedidos ontem?' ou 'Liste os 10 últimos pedidos'.")

    def _guess_period(self, q: str) -> str | None:
        t = self._normalize(q)
        keys = [
            "ontem","anteontem","hoje","mes atual","mes anterior",
            "ultimos 7 dias","semana passada","semana atual","ano atual","ano anterior"
        ]
        for k in keys:
            if k in t:
                return k
        # aceita 'ultimos/ultimas N dias'
        m = re.search(r"(?:ultimos|ultimas)\s+(\d+)\s+dias", t)
        if m:
            return "ultimos_n_dias"
        return None
