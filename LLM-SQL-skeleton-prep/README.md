# LLM-SQL Skeleton

Projeto FastAPI para consultas em linguagem natural traduzidas para SQL (LLM → SQL).

## Rodando localmente

```bash
# 1) criar venv (Windows PowerShell)
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip

# 2) instalar dependências
pip install -r requirements.txt

# 3) configurar variáveis de ambiente
# copie .env.example para .env e ajuste as chaves

# 4) subir API
uvicorn api.main:app --reload --host 0.0.0.0 --port 8080
```

## Estrutura rápida
- `api/`: Rotas FastAPI
- `core/`: Config, logs, LLM provider, NLU, validações
- `config/`: intents/domínios/datas (YAML)
- `.env`: credenciais (IGNORADO no git)
- `.venv`: ambiente virtual (IGNORADO no git)

## Aviso
Nunca versione `.env`, credenciais, ou bases de dados locais.
