import unicodedata

def normalize_question(q: str, idioma: str = "pt-BR") -> str:
    q = q.strip().lower()
    q = unicodedata.normalize("NFKD", q).encode("ASCII", "ignore").decode("ASCII")
    q = q.replace("pvs", " pedidos ")
    q = q.replace("vendas", " pedidos ")
    q = " ".join(q.split())
    return q
