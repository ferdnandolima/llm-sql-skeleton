# LLM-SQL Skeleton (v2)

Atualizações:
- Preferência por **read_replica** no executor (fallback para DSN primário).
- **EXPLAIN gate** para bloquear planos caros (varredura ampla sem índice).
- Nova intenção implementada: **listar_ultimos_N_pedidos**.

## Rodar
```
pip install -r requirements.txt
uvicorn api.main:app --host 0.0.0.0 --port 8080 --reload
```
