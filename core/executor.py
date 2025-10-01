import pymysql

class SQLExecutor:
    def __init__(self, configs: dict):
        self.configs = configs

    def _get_tenant_cfg(self, tenant_id: str):
        tenants = self.configs.get("tenants", {}).get("tenants", {})
        if tenant_id not in tenants:
            raise ValueError(f"Tenant desconhecido: {tenant_id}")
        return tenants[tenant_id]

    def _parse_dsn(self, dsn: str):
        userpass, hostpart = dsn.split("://", 1)[1].split("@", 1)
        user, pwd = userpass.split(":", 1)
        hostport, schema = hostpart.split("/", 1)
        if ":" in hostport:
            host, port = hostport.split(":", 1)
            port = int(port)
        else:
            host, port = hostport, 3306
        return user, pwd, host, port, schema

    def _connect(self, dsn: str, timeout_ms: int):
        user, pwd, host, port, schema = self._parse_dsn(dsn)
        return pymysql.connect(
            host=host, port=port, user=user, password=pwd, database=schema,
            cursorclass=pymysql.cursors.DictCursor,
            read_timeout=timeout_ms/1000.0, write_timeout=timeout_ms/1000.0
        )

    def _run_explain_gate(self, cur, sql: str, params: dict):
        cfg = self.configs.get("regras_sql", {}).get("explain_gate", {})
        if not cfg or not cfg.get("habilitado", False):
            return
        try:
            cur.execute("EXPLAIN " + sql, params)
            plan_rows = cur.fetchall() or []
        except Exception:
            return  # fail-open if EXPLAIN not possible
        bloquear = cfg.get("bloquear_se", {})
        rows_abs = bloquear.get("rows_absoluto_maior_que", 500000)
        for r in plan_rows:
            rtype = str(r.get("type", "")).lower()
            rrows = int(r.get("rows", 0) or 0)
            rkey = r.get("key")
            if rtype in ("all","index") and rrows >= rows_abs and not rkey:
                raise PermissionError("Consulta muito pesada (EXPLAIN indica varredura ampla sem índice).")

    def execute(self, tenant_id: str, sql: str, params: dict, meta: dict):
        tcfg = self._get_tenant_cfg(tenant_id)
        dsn_primary = tcfg.get("dsn")
        dsn_replica = tcfg.get("read_replica") or None
        if not dsn_primary or "mysql://" not in dsn_primary:
            raise ValueError("DSN do tenant inválido. Ex.: mysql://usuario_ro:SENHA@host:3306/schema")

        timeout_ms = self.configs.get("app", {}).get("limites", {}).get("timeout_mysql_ms", 1800)

        tried_primary = False
        for dsn in [dsn_replica, dsn_primary]:
            if not dsn:
                continue
            try:
                conn = self._connect(dsn, timeout_ms)
                try:
                    with conn.cursor() as cur:
                        self._run_explain_gate(cur, sql, params)
                        cur.execute(sql, params)
                        if meta.get("retorna") == "agregado_unico":
                            row = cur.fetchone() or {}
                            return [], row
                        else:
                            rows = cur.fetchall()
                            return rows, None
                finally:
                    conn.close()
            except Exception as e:
                if dsn == dsn_primary or tried_primary:
                    raise
                else:
                    tried_primary = True
                    continue
        raise RuntimeError("Falha ao conectar/consultar no tenant.")
