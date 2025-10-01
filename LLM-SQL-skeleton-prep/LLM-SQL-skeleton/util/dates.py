from datetime import datetime, timedelta, timezone
import re

def resolve_period(label: str, tz_name: str, datas_cfg: dict, parametros: dict | None = None):
    tz = timezone(timedelta(hours=-3))  # America/Sao_Paulo (simplificado)
    now = datetime.now(tz)

    def day_bounds(d: datetime):
        ini = d.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = d.replace(hour=23, minute=59, second=59, microsecond=0)
        return ini, fim

    label = label.lower()
    if label in ("hoje",):
        i, f = day_bounds(now)
    elif label in ("ontem",):
        i, f = day_bounds(now - timedelta(days=1))
    elif label in ("anteontem",):
        i, f = day_bounds(now - timedelta(days=2))
    elif label in ("mes atual",):
        i = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        f = now.replace(hour=23, minute=59, second=59, microsecond=0)
    elif label in ("mes anterior",):
        first_this = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_prev = first_this - timedelta(seconds=1)
        i = last_prev.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        f = last_prev.replace(hour=23, minute=59, second=59, microsecond=0)
    elif label in ("ultimos 7 dias",):
        i = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        f = now.replace(hour=23, minute=59, second=59, microsecond=0)
    elif label == "ultimos_n_dias":
        n = int((parametros or {}).get("n", 7))
        i = (now - timedelta(days=n-1)).replace(hour=0, minute=0, second=0, microsecond=0)
        f = now.replace(hour=23, minute=59, second=59, microsecond=0)
    else:
        m = re.search(r"(\d{2})/(\d{2})/(\d{4}).+?(\d{2})/(\d{2})/(\d{4})", label)
        if m:
            d1 = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), tzinfo=tz).replace(hour=0, minute=0, second=0, microsecond=0)
            d2 = datetime(int(m.group(6)), int(m.group(5)), int(m.group(4)), tzinfo=tz).replace(hour=23, minute=59, second=59, microsecond=0)
            return [d1.strftime("%Y-%m-%d %H:%M:%S"), d2.strftime("%Y-%m-%d %H:%M:%S")]
        raise ValueError("Período não reconhecido. Ex.: 'ontem', 'últimos 7 dias', '01/08/2025 a 15/08/2025'.")

    return [i.strftime("%Y-%m-%d %H:%M:%S"), f.strftime("%Y-%m-%d %H:%M:%S")]
