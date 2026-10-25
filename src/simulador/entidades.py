"""Entidades del mundo B2B simulado: empresas, cuentas, relaciones comprador-
proveedor y estado del grafo. Todo el azar sale de un unico random.Random
(semilla), de modo que una ejecucion es reproducible bit a bit."""
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Set, Tuple

CATEGORIAS = [
    "materia_prima", "logistica", "tecnologia", "servicios_consultoria",
    "empaque", "mantenimiento", "marketing",
]
FACTOR_CATEGORIA = {
    "materia_prima": 1.6, "logistica": 0.8, "tecnologia": 1.4,
    "servicios_consultoria": 1.0, "empaque": 0.5, "mantenimiento": 0.7, "marketing": 0.9,
}
# categorias cuya prestacion es dificil de verificar fisicamente (T2, T3)
CATEGORIAS_DIFICILES = ["servicios_consultoria", "marketing", "tecnologia"]

PAISES = ["PE", "CO", "CL", "MX", "AR", "BR", "US"]
MONEDA_POR_PAIS = {"PE": "PEN", "CO": "COP", "CL": "CLP", "MX": "MXN", "AR": "ARS", "BR": "BRL", "US": "USD"}

REDES = ["corporativa", "vpn", "residencial", "movil", "anonimizada"]
CANALES = ["web", "api", "app_movil", "edi", "manual"]
METODOS = ["contado", "credito_30", "credito_60", "credito_90", "carta_credito", "transferencia_bancaria"]
PLAZO_DIAS = {"contado": 2, "credito_30": 30, "credito_60": 60, "credito_90": 90,
              "carta_credito": 15, "transferencia_bancaria": 5}
A_CREDITO = {"credito_30", "credito_60", "credito_90"}

SIGMA_COMPRADOR_ESCALA = 0.8  # heterogeneidad de tamano entre compradores


@dataclass
class Cuenta:
    id: str
    pais: str
    saldo: float
    ingreso_dia: float          # ingresos externos por dia (0 para cuentas de paso)
    t_ult: float
    flujo: Deque[Tuple[float, float]] = field(default_factory=deque)  # (t, +entrada/-salida), ultimas 24h simuladas

    def devengar(self, t: float):
        """Aplica los ingresos externos acumulados hasta t (el saldo nunca baja)."""
        if t > self.t_ult:
            self.saldo += self.ingreso_dia * (t - self.t_ult) / 86400.0
            self.t_ult = t


@dataclass
class Empresa:
    id: str
    rol: str                      # 'comprador' | 'proveedor' | 'hibrida'
    pais: str
    alta: float                   # t de alta (negativo = ya existia)
    cuenta: str
    domicilio: str
    representante: str
    grupo: Optional[int] = None
    es_intermediario: bool = False
    es_fantasma: bool = False     # (solo para uso interno del motor; NUNCA se emite)
    # --- comprador ---
    actividad: float = 1.0
    escala: float = 1.0
    limite_aprob: float = 0.0
    limite_credito: float = 0.0
    credito_usado: float = 0.0
    atraso_acum: float = 0.0
    p_dispositivo_nuevo: float = 0.08
    hab_inicio: int = 8
    hab_fin: int = 18
    habil_finde: bool = False
    mfa: bool = True
    p_atraso: float = 0.12
    favoritos: List[str] = field(default_factory=list)
    cat_pref: List[str] = field(default_factory=list)
    canal_w: List[float] = field(default_factory=list)
    red_w: List[float] = field(default_factory=list)
    metodo_w: List[float] = field(default_factory=list)
    t_cambio_contacto: Optional[float] = None
    eventos_recientes: Deque[float] = field(default_factory=deque)
    provs_30d: Deque[Tuple[float, str]] = field(default_factory=deque)
    socios_grupo: List[str] = field(default_factory=list)
    # --- proveedor ---
    categorias: List[str] = field(default_factory=list)
    t_cambio_cuenta: Optional[float] = None
    facturas: Deque[Tuple[float, str, str, float]] = field(default_factory=deque)  # (t, numero, comprador, monto)
    contador_factura: int = 0


@dataclass
class Relacion:
    n: int = 0
    primera: float = 0.0
    ultima: float = 0.0
    suma: float = 0.0
    # Fase 1 (docs/roadmap_correccion.md, B1): suma de montos al cuadrado, para poder calcular
    # la varianza real de la relacion (ver motor.py:_z_categoria). Estado interno del motor,
    # nunca se serializa al esquema de eventos.
    suma_cuadrados: float = 0.0
    recientes: Deque[Tuple[float, float]] = field(default_factory=deque)   # (t, monto) ultimas 72h
    n_inversas_cache: int = 0


class GrafoB2B:
    """Grafo dirigido comprador -> proveedor con aristas agregadas."""

    def __init__(self, max_hops: int):
        self.max_hops = max_hops
        self.rel: Dict[Tuple[str, str], Relacion] = {}
        # dict como "conjunto ordenado": iterar un set de str no es reproducible
        # entre procesos (PYTHONHASHSEED), un dict si.
        self.sal: Dict[str, Dict[str, None]] = {}
        self.ent: Dict[str, Dict[str, None]] = {}

    def grado_comprador(self, u: str) -> int:
        return len(self.sal.get(u, ()))

    def grado_proveedor(self, v: str) -> int:
        return len(self.ent.get(v, ()))

    def reciprocidad(self, u: str, v: str) -> float:
        a = self.rel.get((u, v))
        b = self.rel.get((v, u))
        na = a.n if a else 0
        nb = b.n if b else 0
        return 0.0 if nb == 0 else nb / (na + nb)

    def ciclo_que_cierra(self, u: str, v: str) -> Optional[int]:
        """Si la arista u->v cierra un ciclo dirigido (existe camino v ~> u en
        el grafo previo), devuelve la longitud del ciclo (>=2); si no, None."""
        if v == u:
            return None
        sv = self.sal.get(v)
        if not sv:
            return None
        if u in sv:
            return 2
        visitados = {v}
        frontera = [v]
        for prof in range(1, self.max_hops):
            nueva = []
            for x in frontera:
                for y in self.sal.get(x, ()):
                    if y == u:
                        return prof + 1
                    if y not in visitados:
                        visitados.add(y)
                        nueva.append(y)
                if len(visitados) > 4000:
                    return None
            frontera = nueva
            if not frontera:
                break
        return None

    def registrar(self, u: str, v: str, t: float, monto: float) -> Relacion:
        r = self.rel.get((u, v))
        if r is None:
            r = Relacion(primera=t)
            self.rel[(u, v)] = r
            self.sal.setdefault(u, {})[v] = None
            self.ent.setdefault(v, {})[u] = None
        r.n += 1
        r.ultima = t
        r.suma += monto
        r.suma_cuadrados += monto * monto
        return r
