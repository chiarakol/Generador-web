"""Reloj simulado. El tiempo interno es 't' = segundos desde SIM_START (puede ser
negativo durante el calentamiento). El timestamp del evento es siempre tiempo
simulado, nunca el instante de ingestion."""
from datetime import datetime, timedelta, timezone

DIA = 86400.0
HORA = 3600.0


class RelojSimulado:
    def __init__(self, inicio_iso: str):
        self.inicio = datetime.fromisoformat(inicio_iso.replace("Z", "+00:00")).astimezone(timezone.utc)

    def dt(self, t: float) -> datetime:
        return self.inicio + timedelta(seconds=t)

    def iso(self, t: float) -> str:
        return self.dt(t).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def hora_dia_semana(self, t: float):
        d = self.dt(t)
        return d.hour, d.weekday(), d.day


def _perfil(hora: int, dia_semana: int) -> float:
    if dia_semana >= 5:  # fin de semana
        return 0.35 if 9 <= hora <= 17 else 0.12
    if 8 <= hora <= 17:
        return 2.0
    if 6 <= hora <= 7 or 18 <= hora <= 20:
        return 0.8
    if 21 <= hora <= 23:
        return 0.3
    return 0.15


_NORMA = sum(_perfil(h, d) for d in range(7) for h in range(24)) / 168.0
INTENSIDAD_MAX = 2.0 * 1.25 / _NORMA


def intensidad_relativa(hora: int, dia_semana: int, dia_mes: int) -> float:
    """Multiplicador (media ~1) de la tasa de llegada: estacionalidad diaria,
    semanal y de cierre de mes (dias >= 26)."""
    f = _perfil(hora, dia_semana) / _NORMA
    if dia_mes >= 26:
        f *= 1.25
    return f
