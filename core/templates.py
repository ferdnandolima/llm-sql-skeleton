class TemplateEngine:
    def __init__(self, configs: dict):
        self.configs = configs
        self.intencoes_cfg = configs.get("intencoes", {})

    def build_sql(self, intent: str, slots: dict):
        if intent == "contagem_por_periodo":
            return self._contagem_por_periodo(slots)
        if intent == "listar_ultimos_N_pedidos":
            return self._listar_ultimos_N_pedidos(slots)
        raise ValueError(f"Intenção não implementada: {intent}")

    def _contagem_por_periodo(self, slots: dict):
        cfg = self.intencoes_cfg.get("contagem_por_periodo", {})
        tabela = cfg.get("tabela_principal")
        col_data = cfg.get("colunas", {}).get("data")
        col_status = cfg.get("colunas", {}).get("status")
        col_cliente = cfg.get("colunas", {}).get("cliente")

        if not tabela or not col_data:
            raise ValueError("Config de 'contagem_por_periodo' incompleta (tabela/coluna data).")
        periodo = slots.get("periodo_resolvido")
        if not periodo:
            raise ValueError("Período é obrigatório para contagem_por_periodo.")

        where = [f"{col_data} BETWEEN %(ini)s AND %(fim)s"]
        params = {"ini": periodo[0], "fim": periodo[1]}

        status = slots.get("status")
        if status and col_status:
            mapa = {"faturado": [3], "cancelado": [5,6], "aberto": [1,2]}
            cods = mapa.get(status.lower())
            if not cods:
                raise ValueError(f"Status desconhecido: {status}")
            where.append(f"{col_status} IN %(status)s")
            params["status"] = tuple(cods)

        cliente = slots.get("cliente")
        if cliente is not None and col_cliente:
            where.append(f"{col_cliente} = %(cliente)s")
            params["cliente"] = int(cliente)

        sql = f"SELECT COUNT(*) AS qtd_pedidos FROM {tabela} WHERE " + " AND ".join(where)
        meta = {"retorna": "agregado_unico", "intent": "contagem_por_periodo"}
        return sql, params, meta

    def _listar_ultimos_N_pedidos(self, slots: dict):
        cfg = self.intencoes_cfg.get("listar_ultimos_N_pedidos", {})
        tabela = cfg.get("tabela_principal")
        cols = cfg.get("colunas", {})
        col_id = cols.get("id")
        col_data = cols.get("data")
        col_status = cols.get("status")
        col_cliente = cols.get("cliente")
        col_valor = cols.get("valor_total")

        if not all([tabela, col_id, col_data]):
            raise ValueError("Config de 'listar_ultimos_N_pedidos' incompleta.")

        n = slots.get("N")
        if not n:
            n = cfg.get("regras", {}).get("limit_padrao", 100)
        n = int(n)

        where = []
        params = {}

        status = slots.get("status")
        if status and col_status:
            mapa = {"faturado": [3], "cancelado": [5,6], "aberto": [1,2]}
            cods = mapa.get(status.lower())
            if not cods:
                raise ValueError(f"Status desconhecido: {status}")
            where.append(f"{col_status} IN %(status)s")
            params["status"] = tuple(cods)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        order_by = cfg.get("regras", {}).get("ordenar_por", ["DT_PVE DESC"])[0]
        select_cols = [col_id + " AS id_pedido", col_data + " AS data"]
        if col_cliente:
            select_cols.append(col_cliente + " AS id_cliente")
        if col_valor:
            select_cols.append(col_valor + " AS valor_total")
        if col_status:
            select_cols.append(col_status + " AS status")

        sql = f"SELECT {', '.join(select_cols)} FROM {tabela}{where_sql} ORDER BY {order_by} LIMIT {n}"
        meta = {"retorna": "linhas", "intent": "listar_ultimos_N_pedidos"}
        return sql, params, meta
