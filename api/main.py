# api/main.py
import os, re, time
from core.db import run_query
from pathlib import Path
from fastapi.staticfiles import StaticFiles
from datetime import datetime
from fastapi import FastAPI, Body, Query, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from dotenv import load_dotenv
import pymysql
import unicodedata

from util.intents_loader import load_intents
from core.nlu import route_and_fill
from api.routes_llm import router as llm_router, INTENTS_REGISTRY, build_sql, _get_db_conn
from util.domains_loader import load_domains, coerce_enum  # <<< novo

# Guardião de schema
from core.schema_guard import check_registry_against_schema, SchemaMismatch

# <<< NOVO: logger estruturado + correlação
from core.logs import get_logger, new_corr_id, log_event, sql_digest

# ==========================
# Bootstrap / Config
# ==========================
ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

INTENTS = load_intents(str(ROOT / "config" / "intents"))
DOMAINS = load_domains(str(ROOT))  # <<< novo

app = FastAPI(title="LLM SQL API", version="0.10.0")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Logger
APP_LOG = get_logger("llm-sql")

# injeta as intents no registro usado pelas rotas LLM
INTENTS_REGISTRY.clear()
INTENTS_REGISTRY.update(INTENTS)

LLM_THRESHOLD = 0.55  # pode ajustar depois

# expõe também as rotas /llm/...
app.include_router(llm_router)

# ==========================
# Middleware — correlação e access log
# ==========================
@app.middleware("http")
async def correlation_and_accesslog(request: Request, call_next):
    corr_id = request.headers.get("X-Request-ID") or new_corr_id()
    tenant = request.headers.get("X-Tenant") or "default"
    request.state.corr_id = corr_id
    request.state.tenant = tenant

    t0 = time.perf_counter()
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    except Exception as e:
        status = 500
        # log de erro genérico no middleware
        log_event(
            APP_LOG,
            "http_error",
            corr_id=corr_id,
            tenant=tenant,
            method=request.method,
            path=str(request.url.path),
            status=status,
            err=str(e),
        )
        raise
    finally:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        # access log resumido
        log_event(
            APP_LOG,
            "http_request",
            corr_id=corr_id,
            tenant=tenant,
            method=request.method,
            path=str(request.url.path),
            status=status,
            ms=elapsed_ms,
        )

# ==========================
# Conexão DB utilitária (opcional/legado)
# ==========================
DB_CFG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", ""),
    "cursorclass": pymysql.cursors.DictCursor,
    "charset": os.getenv("DB_CHARSET", "utf8mb4"),
    "autocommit": True,
}

def executar_sql(sql: str, params: tuple | list | None = None):
    try:
        conn = pymysql.connect(**DB_CFG)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro de conexão com o banco: {e}")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if cur.description:
                return {"linhas": cur.fetchall()}
            else:
                return {"ok": True, "linhas_afetadas": cur.rowcount}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao executar SQL: {e}")
    finally:
        conn.close()

# ==========================
# STARTUP — Anti–schema drift
# ==========================
@app.on_event("startup")
def _startup_schema_check():
    try:
        conn = _get_db_conn()
    except Exception as e:
        raise RuntimeError(f"[startup] Falha ao conectar ao DB para checagem de schema: {e}")

    try:
        summary = check_registry_against_schema(INTENTS_REGISTRY, conn)
        warns = summary.get("warnings") or []
        if warns:
            print("[schema-guard] Avisos:")
            for w in warns:
                print("  -", w)
        print(f"[schema-guard] OK — {summary.get('intents_checked')} intents verificadas contra {summary.get('tables')} tabelas.")
    except SchemaMismatch as sm:
        print("[schema-guard] ERROS detectados entre intents e schema:")
        for e in sm.errors:
            print("  -", e)
        if sm.warnings:
            print("[schema-guard] Avisos:")
            for w in sm.warnings:
                print("  -", w)
        raise RuntimeError("Startup abortado: divergências de schema detectadas. Corrija as intents ou o banco.")
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ==========================
# Helpers (ordenação/limite/domínios)
# ==========================
def _set_limit(sql: str, lim: int | None) -> str:
    """Substitui/adiciona LIMIT no SQL (apenas SELECT)."""
    if not lim or lim <= 0:
        return sql
    if not re.search(r"^\s*select\b", sql, flags=re.I):
        return sql
    if re.search(r"\blimit\b\s+\d+", sql, flags=re.I):
        return re.sub(r"\blimit\b\s+\d+", f"LIMIT {int(lim)}", sql, flags=re.I)
    return f"{sql} LIMIT {int(lim)}"

def _norm_txt(s: str) -> str:
    """minúsculas + sem acentos (pra facilitar match)"""
    if not s:
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _extract_order_hints(texto: str) -> dict:
    """
    Lê a frase do usuário e retorna:
      - ORD_DIR: 'ASC' | 'DESC'
      - N: inteiro
    """
    t = _norm_txt(texto)
    out = {}
    asc_words = ["primeir", "mais antigo", "antig", "do comeco", "do começo", "inicio", "iniciais", "crescente", "asc"]
    desc_words = ["ultimo", "ultim", "mais recente", "recent", "novos", "mais novo", "decrescente", "desc"]
    if any(w in t for w in asc_words):
        out["ORD_DIR"] = "ASC"
    if any(w in t for w in desc_words):
        out["ORD_DIR"] = "DESC"
    m = re.search(r"\b(?:top|primeir(?:o|a|os|as)?|ultim(?:o|a|os|as)?)\s+(\d{1,4})\b", t)
    if m:
        try:
            out["N"] = int(m.group(1))
        except Exception:
            pass
    return out

def resolve_domain_param(param_def: dict, raw_value):
    dom_key = (param_def or {}).get("domain")
    if not dom_key:
        return raw_value
    dom_name = dom_key.split(".", 1)[-1]
    return coerce_enum(raw_value, DOMAINS.get(dom_name))

def _apply_domains(spec: dict, slots: dict) -> dict:
    params_def = (spec or {}).get("params") or {}
    out = dict(slots or {})
    for pname, pdef in params_def.items():
        if not isinstance(pdef, dict):
            continue
        if "domain" in pdef and pname in out and str(out[pname]).strip() != "":
            try:
                out[pname] = resolve_domain_param(pdef, out[pname])
            except Exception as e:
                raise HTTPException(400, f"Valor inválido para '{pname}': {e}")
    return out

# ==========================
# Endpoints utilitários
# ==========================
@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"

@app.get("/intencoes")
def intencoes():
    return [
        "Listar primeiros 20 pedidos",
        "Top 10 clientes por valor no mês",
        "Pedidos por dia nos últimos 14 dias",
        "títulos pendentes desta semana",
        "títulos pagos na semana passada",
        "saldo por cliente no mês",
        "detalhes do pedido 1234",
    ]

@app.get("/admin/schema-check")
def admin_schema_check():
    try:
        conn = _get_db_conn()
    except Exception as e:
        raise HTTPException(500, f"Falha ao conectar ao DB: {e}")
    try:
        summary = check_registry_against_schema(INTENTS_REGISTRY, conn)
        return {
            "ok": True,
            "intents_checked": summary.get("intents_checked"),
            "tables": summary.get("tables"),
            "warnings": summary.get("warnings"),
        }
    except SchemaMismatch as sm:
        raise HTTPException(status_code=409, detail={"errors": sm.errors, "warnings": sm.warnings})
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.get("/dominios/{nome}")
def get_dominio(nome: str):
    items = DOMAINS.get(nome)
    if items is None:
        raise HTTPException(404, "Domínio não encontrado")
    return items

# ==========================
# /consulta (compat com front) -> motor novo
# ==========================
@app.post("/consulta")
def consulta(
    request: Request,  # <- não-optional e antes dos defaults
    payload: dict = Body(...),
    execute: bool = Query(True, description="Se true, executa no banco; senão, apenas gera o SQL."),
    use_llm: bool = Query(True, description="Usa LLM para desempate/complemento de slots."),
    limit: int | None = Query(None, description="Override de LIMIT (opcional)"),
):
    texto = (payload or {}).get("pergunta") or (payload or {}).get("query") or (payload or {}).get("text") or ""
    texto = str(texto).strip()
    if not texto:
        raise HTTPException(400, "Forneça 'pergunta', 'query' ou 'text' com conteúdo.")
    if not INTENTS_REGISTRY:
        raise HTTPException(500, "Registro de intents vazio. Verifique o loader no startup.")

    tenant = getattr(request.state, "tenant", "default")
    corr_id = getattr(request.state, "corr_id", new_corr_id())

    # 1) Roteia + extrai slots
    t_llm0 = time.perf_counter()
    try:
        routed = route_and_fill(texto, INTENTS_REGISTRY, use_llm=use_llm, threshold=LLM_THRESHOLD)
    except Exception as e:
        log_event(APP_LOG, "consulta_route_error", corr_id=corr_id, tenant=tenant, err=str(e))
        raise
    t_llm_ms = int((time.perf_counter() - t_llm0) * 1000)

    key = routed["intent"]
    spec = INTENTS_REGISTRY.get(key) or {}
    slots = dict(routed.get("slots") or {})

    # Dicas dinâmicas
    hints = _extract_order_hints(texto)
    if hints.get("ORD_DIR"):
        slots["ORD_DIR"] = hints["ORD_DIR"]
    if hints.get("N") and "N" not in slots:
        slots["N"] = hints["N"]

    # Domínios
    slots = _apply_domains(spec, slots)

    # 2) SQL
    sql, params = build_sql(key, spec, slots)

    # LIMIT override
    limit_override = int(limit) if (limit and limit > 0) else None
    if limit_override is None:
        N = slots.get("N")
        try:
            limit_override = int(N) if N else None
        except Exception:
            limit_override = None
    sql = _set_limit(sql, limit_override)

    # Dry-run?
    if not execute:
        log_event(
            APP_LOG,
            "consulta_dryrun",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            limit=limit_override,
            sql_digest=sql_digest(sql),
        )
        return {
            "route": routed,
            "sql": sql,
            "params": params,
            "executed": False,
            "linhas": [],
            "rowcount": 0,
        }

    # 3) Execução
    t_db0 = time.perf_counter()
    try:
        cols, rows_2d, rows_dict, rowcount = run_query(sql, params)
        db_ms = int((time.perf_counter() - t_db0) * 1000)
        log_event(
            APP_LOG,
            "consulta_ok",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            db_ms=db_ms,
            rowcount=rowcount,
            limit=limit_override,
            sql_digest=sql_digest(sql),
        )
        return {
            "route": routed,
            "sql": sql,
            "params": params,
            "executed": True,
            "linhas": rows_dict,  # lista de dicts para a UI
            "rowcount": rowcount,
        }
    except Exception as e:
        db_ms = int((time.perf_counter() - t_db0) * 1000)
        log_event(
            APP_LOG,
            "consulta_db_error",
            corr_id=corr_id,
            tenant=tenant,
            intent=key,
            llm_ms=t_llm_ms,
            db_ms=db_ms,
            err=str(e),
            sql_digest=sql_digest(sql),
        )
        raise HTTPException(500, f"Falha ao executar SQL: {e}")

# ==========================
# UI /app — estilo inspirado no site da MGNet
# ==========================
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/app")

@app.get("/app", response_class=HTMLResponse, include_in_schema=False)
def app_page():
    return """
<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>MGNet • Console de Consultas</title>
<link rel="icon" href="/static/mgnet-logo.png" type="image/png" />
<style>
  :root{
    --azul:#1e40af;
    --ciano:#0ea5e9;
    --azul-escuro:#0b1b3a;
    --txt:#0f172a;
    --muted:#475569;
    --borda:#e5e7eb;
    --card:#ffffff;
  }
  *{box-sizing:border-box}
  body{ margin:0; font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; color:var(--txt); background:#f7f8fb; }
  .hero{ background: linear-gradient(120deg, var(--azul) 0%, var(--ciano) 100%); color:#fff; padding:36px 16px; }
  .container{max-width:1100px; margin:0 auto; padding:0 16px;}
  .topbar { display:flex; align-items:center; gap:12px; }
  .right-tools { margin-left:auto; display:flex; align-items:center; gap:10px; }
  .logo-wrap { display:flex; align-items:center; gap:12px; }
  .logo-img { height: clamp(52px, 10vw, 96px); display:block; filter: drop-shadow(0 2px 10px rgba(255,255,255,.35)); }
  @media (max-width:520px){ .logo-img { height:52px; } }
  .brand{ font-weight:900; letter-spacing:.5px; font-size:20px; text-transform:uppercase; }
  .pill{ padding:6px 10px; border:1px solid rgba(255,255,255,.35); border-radius:999px; font-size:12px; opacity:.95}
  .hero h1{ margin:0; font-size:28px; text-transform:uppercase; letter-spacing:.5px; }
  .hero p{ margin:8px 0 0; opacity:.95; max-width:700px }
  .grid{ display:grid; gap:18px; grid-template-columns:1fr; margin-top:-28px; padding-bottom:28px;}
  /* Empilhado mesmo em telas grandes */
  @media (min-width: 980px){ .grid{ grid-template-columns: 1fr; } }
  .card{ background: var(--card); border:1px solid var(--borda); border-radius:16px; box-shadow:0 10px 24px rgba(2,6,23,.08); padding:16px; }
  .card h3{ margin:0 0 10px; font-size:14px; color:var(--muted); text-transform:uppercase; letter-spacing:.4px; }
  textarea{ width:100%; min-height:120px; padding:12px 14px; border-radius:12px; border:1px solid var(--borda); font-size:14px; }
  .chips{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin:12px 0; max-height: 200px; overflow:auto; }
  .chip{ padding:8px 12px; border-radius:999px; border:1px solid var(--borda); font-size:12px; color:var(--muted); background:#fff; cursor:pointer; }
  .actions{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  button{ padding:10px 14px; border-radius:999px; border:0; cursor:pointer; font-weight:700; letter-spacing:.2px; }
  .btn-primary{ background: linear-gradient(120deg, var(--azul) 0%, var(--ciano) 100%); color:#fff; }
  .btn-ghost{ background:#eef2ff; color:#1e293b; }
  .btn-link{ border:1px solid var(--borda); background:#fff; color:#1e293b; }
  button:disabled{ opacity:.6; cursor:not-allowed; }
  .statusline{ display:flex; align-items:center; gap:10px; margin-bottom:10px; }
  .sqlnote{ color: var(--muted); font-size:12px; margin-bottom:8px; }
  .spinner{ width:16px; height:16px; border-radius:999px; border:2px solid #e2e8f0; border-top-color: var(--azul); animation: spin .9s linear infinite; }
  @keyframes spin { to{ transform: rotate(360deg);} }
  .out{ min-height:160px; }
  .mono{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  table{ border-collapse: collapse; width: 100%; font-size:14px; }
  th, td{ border: 1px solid var(--borda); padding: 8px 10px; text-align: left; }
  th{ font-weight: 800; color:#0f172a; background:#f1f5f9; text-transform: uppercase; font-size:12px; letter-spacing:.3px; }
  tbody tr:nth-child(odd){ background:#fafcff; }
  tbody tr:hover{ background:#f1f5f9; }

  /* === VOICE BTN (novo) === */
  #voiceBtn{
    border:1px solid var(--borda);
    background:#fff;
    color:#1e293b;
    border-radius:999px;
    width:40px;
    height:40px;
    display:grid;
    place-items:center;
    cursor:pointer;
  }
  #voiceBtn.listening{
    outline:2px solid #0af;
    animation:pulse 1.2s infinite;
  }
  @keyframes pulse {
    0%   { box-shadow: 0 0 0 0 rgba(0,160,255,.55); }
    70%  { box-shadow: 0 0 0 12px rgba(0,160,255,0); }
    100% { box-shadow: 0 0 0 0 rgba(0,160,255,0); }
  }
  .sr-only {
    position:absolute; width:1px; height:1px; padding:0; margin:-1px;
    overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0;
  }

  /* ========= CARD RESPOSTA ocupa a tela; rolagem só no conteúdo ========= */
  #cardResposta{
    display:flex;
    flex-direction:column;
    min-height:340px;    /* fallback */
    overflow:hidden;     /* evita scroll no body por causa do card */
    border-radius:16px;
  }
  #cardResposta .statusline{ flex:0 0 auto; }
  #cardResposta .out{
    flex:1 1 auto;
    overflow:auto;       /* rolagem só aqui (tabela/resultado) */
  }

  /* ========= Cabeçalho fixo no scroll da resposta ========= */
  #cardResposta .out table{
    border-collapse: separate;   /* sticky funciona melhor assim */
    border-spacing: 0;
    width: 100%;
  }
  #cardResposta .out thead th{
    position: sticky;
    top: 0;
    z-index: 5;
    background: #f1f5f9;         /* mantém o fundo ao passar por linhas */
    /* opcional: linha de separação abaixo do header */
    box-shadow: 0 2px 0 0 var(--borda);
  }
</style>

</head>
<body>
  <section class="hero">
    <div class="container">
      <div class="topbar">
        <div class="logo-wrap">
          <img src="/static/mgnet-logo.png" alt="MGNet" class="logo-img" />
        </div>
        <div class="right-tools">
          <span id="health" class="pill">status: ...</span>
          <a href="/docs" target="_blank" rel="noopener" class="pill">/docs</a>
        </div>
      </div>
      <h1>Consultas para o seu ERP</h1>
      <p>Faça perguntas em linguagem natural e visualize os resultados de forma clara.</p>
    </div>
  </section>

  <main class="container">
    <div class="grid">
      <section class="card">
        <h3>Pergunta</h3>
        <textarea id="pergunta" placeholder="Ex.: Pedidos por dia nos últimos 14 dias"></textarea>
        <div class="chips" id="chips"></div>
        <div class="actions">
          <button id="btnEnviar" class="btn-primary">Consultar</button>

          <!-- === Botão de microfone (novo) === -->
          <button id="voiceBtn" class="btn-link" type="button" aria-pressed="false" aria-label="Falar para pesquisar" title="Falar">
            <svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">
              <path d="M12 14a3 3 0 0 0 3-3V6a3 3 0 1 0-6 0v5a3 3 0 0 0 3 3zm5-3a5 5 0 0 1-10 0H5a7 7 0 0 0 14 0h-2zM11 19v3h2v-3h-2z"/>
            </svg>
            <span id="voiceStatus" class="sr-only">Inativo</span>
          </button>

          <button id="btnLimpar" class="btn-link" type="button">Limpar</button>
          <span id="spinner" class="spinner" hidden></span>
        </div>
      </section>

      <!-- >>> id para controlar altura e scroll -->
      <section class="card" id="cardResposta">
        <h3>Resposta</h3>
        <div class="statusline">
          <button id="btnCopiar" class="btn-ghost" type="button">Copiar</button>
          <button id="btnCSV" class="btn-link" type="button" hidden>Baixar CSV</button>
        </div>
        <div id="saida" class="out"></div>
      </section>
    </div>
  </main>

<script>
const $ = (id) => document.getElementById(id);
const btn = $("btnEnviar");
const limpar = $("btnLimpar");
const copiar = $("btnCopiar");
const btnCSV = $("btnCSV");
const spinner = $("spinner");
const saida = $("saida");
const pergunta = $("pergunta");
const chipsEl = $("chips");
let __ultLinhas = null;

async function loadHealth() {
  try {
    const r = await fetch("/health");
    $("health").textContent = "status: " + (await r.text());
  } catch {
    $("health").textContent = "status: indisponível";
  }
}

async function loadIntencoes() {
  chipsEl.innerHTML = "";
  try {
    const r = await fetch("/intencoes");
    if (!r.ok) return;
    const lista = await r.json();
    (Array.isArray(lista) ? lista : []).forEach((texto) => {
      const b = document.createElement("button");
      b.className = "chip";
      b.type = "button";
      b.textContent = texto;
      b.title = "Clique para preencher";
      b.onclick = () => { pergunta.value = texto; pergunta.focus(); };
      chipsEl.appendChild(b);
    });
  } catch {}
}

function render(data) {
  btnCSV.hidden = true;
  __ultLinhas = null;

  if (data && Array.isArray(data.linhas)) {
    if (data.linhas.length === 0) {
      saida.innerHTML = "<div class='sqlnote mono'>SQL: " + (data.sql || '(n/d)') + "</div><div>Nenhum resultado.</div>";
      return;
    }
    const cols = Object.keys(data.linhas[0] || {});
    let html = "";
    if (data.sql) html += "<div class='sqlnote mono'>SQL: " + data.sql + "</div>";
    /* removido overflow:auto interno para evitar scrollbar duplo */
    html += "<div><table><thead><tr>" + cols.map(c => "<th>" + c + "</th>").join("") + "</tr></thead><tbody>";
    for (const row of data.linhas) {
      html += "<tr>" + cols.map(c => "<td>" + (row[c] ?? "") + "</td>").join("") + "</tr>";
    }
    html += "</tbody></table></div>";
    saida.innerHTML = html;

    __ultLinhas = data.linhas;
    btnCSV.hidden = !(__ultLinhas && __ultLinhas.length);
    return;
  }
  saida.innerHTML = "<pre class='mono' style='white-space:pre-wrap;margin:0'>" + JSON.stringify(data, null, 2) + "</pre>";
}

/* === Ajusta o card de Resposta para ocupar exatamente o viewport === */
(function fitRespostaToViewport(){
  const card = document.getElementById('cardResposta');
  if (!card) return;

  function resize(){
    const top = card.getBoundingClientRect().top;
    const vh  = Math.max(window.innerHeight || 0, document.documentElement.clientHeight || 0);
    const bottomGap = 16;  // respiro inferior
    const h = Math.max(320, vh - top - bottomGap);
    card.style.height = h + 'px';
  }

  window.addEventListener('load', resize);
  window.addEventListener('resize', resize);
  window.addEventListener('orientationchange', resize);
  window.addEventListener('scroll', resize, { passive: true });
  resize();
})();

async function consultar() {
  const q = (pergunta.value || "").trim();
  if (!q) { pergunta.focus(); return; }
  btn.disabled = true; spinner.hidden = false; saida.innerHTML = "";
  try {
    const r = await fetch("/consulta?use_llm=true", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pergunta: q })
    });
    const ct = r.headers.get("content-type") || "";
    if (!r.ok) {
      const texto = ct.includes("application/json") ? await r.json() : await r.text();
      throw new Error(typeof texto === "string" ? texto : JSON.stringify(texto));
    }
    const data = ct.includes("application/json") ? await r.json() : await r.text();
    render(typeof data === "string" ? { texto: data } : data);
  } catch (e) {
    saida.innerHTML = "<div style='color:#b91c1c'>⚠️ Erro na consulta:</div><pre class='mono' style='white-space:pre-wrap;margin:6px 0 0'>"
        + (e.message || e.toString()) + "</pre>";
  } finally {
    btn.disabled = false; spinner.hidden = true;
  }
}

btn.addEventListener("click", consultar);
pergunta.addEventListener("keydown", (ev) => { if (ev.key === "Enter" && (ev.ctrlKey || ev.metaKey)) consultar(); });
limpar.addEventListener("click", () => { pergunta.value = ""; saida.innerHTML = ""; pergunta.focus(); });

copiar.addEventListener("click", async () => {
  try {
    const text = (saida.innerText || "").trim();
    if (!text) return;
    await navigator.clipboard.writeText(text);
    copiar.textContent = "Copiado!";
    setTimeout(() => copiar.textContent = "Copiar", 900);
  } catch {}
});

btnCSV.addEventListener("click", () => {
  if (!__ultLinhas || !__ultLinhas.length) return;
  const cols = Object.keys(__ultLinhas[0] || {});
  const esc = (v) => {
    const s = (v === null || v === undefined) ? "" : String(v);
    return '"' + s.replace(/"/g, '""') + '"';
  };
  const linhas = [
    cols.map(esc).join(","),
    ...__ultLinhas.map(row => cols.map(c => esc(row[c])).join(","))
  ].join("\\r\\n");
  const blob = new Blob([linhas], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = "resultado.csv"; document.body.appendChild(a); a.click();
  document.body.removeChild(a); URL.revokeObjectURL(url);
});

loadHealth();
loadIntencoes();

/* ============ VOICE SEARCH (novo) ============ */
(() => {
  const btn = document.getElementById('voiceBtn');
  const statusEl = document.getElementById('voiceStatus');
  if (!btn) return;

  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  let recognition = null;
  let isListening = false;
  let interimBuffer = '';
  const autoSubmit = true; // chama consultar() ao terminar

  function updateUI() {
    btn.setAttribute('aria-pressed', String(isListening));
    btn.classList.toggle('listening', isListening);
    if (statusEl) statusEl.textContent = isListening ? 'Ouvindo…' : 'Inativo';
    btn.title = isListening ? 'Ouvindo… clique para parar' : 'Falar';
  }

  function start() {
    if (!recognition || isListening) return;
    interimBuffer = '';
    try { recognition.start(); } catch {}
  }
  function stop() {
    if (!recognition || !isListening) return;
    recognition.stop();
  }
  function toggle() { isListening ? stop() : start(); }

  if (SpeechRecognition) {
    recognition = new SpeechRecognition();
    recognition.lang = (navigator.language || 'pt-BR').toLowerCase().startsWith('pt') ? 'pt-BR' : 'en-US';
    recognition.interimResults = true;
    recognition.continuous = false;
    recognition.maxAlternatives = 1;

    recognition.onstart = () => { isListening = true; updateUI(); };
    recognition.onerror = (e) => {
      console.warn('[voice] erro:', e.error);
      isListening = false; updateUI();
      if (e.error === 'not-allowed' || e.error === 'service-not-allowed') {
        alert('Permita o acesso ao microfone para usar a busca por voz.');
      }
    };
    recognition.onend = () => {
      isListening = false; updateUI();
      if (interimBuffer.trim() && !pergunta.value.trim()) {
        pergunta.value = interimBuffer.trim();
      }
      interimBuffer = '';
      if (autoSubmit && pergunta.value.trim()) { consultar(); }
    };
    recognition.onresult = (event) => {
      let finalTranscript = '';
      interimBuffer = '';
      for (let i = event.resultIndex; i < event.results.length; i++) {
        const t = event.results[i][0].transcript;
        if (event.results[i].isFinal) finalTranscript += t + ' ';
        else interimBuffer += t + ' ';
      }
      const textNow = (finalTranscript || interimBuffer).trim();
      if (textNow) {
        pergunta.value = textNow;
        // move cursor pro fim
        const v = pergunta.value; pergunta.value = ''; pergunta.value = v;
      }
    };

    // Interações
    btn.addEventListener('click', toggle);
    btn.addEventListener('keydown', (e) => {
      if (e.code === 'Space' || e.code === 'Enter') { e.preventDefault(); toggle(); }
    });
    // Pressionar e segurar (opcional)
    btn.addEventListener('mousedown', () => start());
    btn.addEventListener('mouseup', () => stop());
    btn.addEventListener('mouseleave', () => stop());
  } else {
    btn.disabled = true;
    btn.title = 'Seu navegador não suporta busca por voz (Web Speech API). Use Chrome/Edge no desktop.';
  }
})();
</script>
</body>
</html>
"""
