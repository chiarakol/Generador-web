"""Configuracion del generador v2 (por tipologias). Todo se controla por
variables de entorno (ver .env.example). Ningun parametro de las secciones
de tipologias esta calibrado con datos reales: son hipotesis de diseno
(ver documento de especificacion, seccion 7) y deben reportarse con
analisis de sensibilidad."""
import os
from dataclasses import dataclass, field
from typing import Dict

from dotenv import load_dotenv

load_dotenv()

SCHEMA_VERSION = "2.0"

TIPOLOGIAS = [
    "toma_de_cuenta",              # T1
    "proveedor_fantasma",          # T2
    "anillo_colusion",             # T3
    "fraccionamiento_ordenes",     # T4
    "bust_out",                    # T5
    "factura_inflada_duplicada",   # T6
    "bec_datos_bancarios",         # T7
    "cadena_fondos_paso",          # T8
]

CODIGO_TIPOLOGIA = {
    "toma_de_cuenta": "T1", "proveedor_fantasma": "T2", "anillo_colusion": "T3",
    "fraccionamiento_ordenes": "T4", "bust_out": "T5", "factura_inflada_duplicada": "T6",
    "bec_datos_bancarios": "T7", "cadena_fondos_paso": "T8",
}

MEZCLA_DEFECTO = "T1:25,T2:10,T3:10,T4:15,T5:10,T6:15,T7:5,T8:10"


# Fase 1 (docs/roadmap_correccion.md, D2): antes, un valor presente pero malformado en .env
# (un typo, una coma en vez de punto decimal) caia al default SIN avisar y quedaba solo en el
# manifest para quien lo revisara. Ahora una variable AUSENTE sigue usando el default (eso no
# cambia); una variable PRESENTE pero no parseable hace fallar el arranque con un mensaje que
# dice cual variable y que valor tenia, en vez de correr en silencio con un valor distinto al
# que la persona creia estar configurando.
_BOOL_TRUE = {"true", "1", "si", "sí", "yes"}
_BOOL_FALSE = {"false", "0", "no"}


def _bool(name: str, fallback: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return fallback
    vv = v.strip().lower()
    if vv in _BOOL_TRUE:
        return True
    if vv in _BOOL_FALSE:
        return False
    raise ValueError(
        f"Variable de entorno {name}={v!r} no es un booleano reconocido "
        f"(usa true/false, si/no, 1/0). Revisa tu .env."
    )


def _int(name: str, fallback: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return fallback
    try:
        return int(v)
    except ValueError:
        raise ValueError(
            f"Variable de entorno {name}={v!r} no es un entero valido. Revisa tu .env."
        ) from None


def _float(name: str, fallback: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return fallback
    try:
        return float(v)
    except ValueError:
        raise ValueError(
            f"Variable de entorno {name}={v!r} no es un numero valido. Revisa tu .env."
        ) from None


def parse_mezcla(texto: str) -> Dict[str, float]:
    """'T1:25,T2:10,...' -> {'toma_de_cuenta': 0.25, ...} (normalizada a 1)."""
    por_codigo = {}
    for par in texto.split(","):
        par = par.strip()
        if not par:
            continue
        k, v = par.split(":")
        por_codigo[k.strip().upper()] = float(v)
    inv = {c: n for n, c in CODIGO_TIPOLOGIA.items()}
    mezcla = {n: 0.0 for n in TIPOLOGIAS}
    for c, v in por_codigo.items():
        if c not in inv:
            raise ValueError(f"Codigo de tipologia desconocido en MEZCLA_TIPOLOGIAS: {c}")
        mezcla[inv[c]] = v
    total = sum(mezcla.values())
    if total <= 0:
        raise ValueError("MEZCLA_TIPOLOGIAS suma 0")
    return {k: v / total for k, v in mezcla.items()}


@dataclass
class EventHubConfig:
    connection_string: str = os.getenv("EVENT_HUB_CONNECTION_STRING", "")
    fully_qualified_namespace: str = os.getenv("EVENT_HUB_NAMESPACE", "")
    use_managed_identity: bool = _bool("USE_MANAGED_IDENTITY", False)
    features_hub_name: str = os.getenv("EVENT_HUB_NAME_FEATURES", "eventos-features")
    ground_truth_hub_name: str = os.getenv("EVENT_HUB_NAME_GROUND_TRUTH", "eventos-ground-truth")


@dataclass
class RelojConfig:
    # Fecha de inicio del tiempo simulado
    inicio_iso: str = os.getenv("SIM_START", "2026-01-01T00:00:00Z")
    # Segundos simulados por segundo real. 3600 = 1 s real equivale a 1 h simulada
    # (6 meses simulados ~ 73 min reales). Los jobs de Databricks deben usar
    # el MISMO valor (ver ESCALA_RELOJ en capa1_cal.py).
    escala: float = _float("RELOJ_ESCALA", 3600.0)
    # Segundos reales entre lotes publicados (modo en vivo)
    intervalo_seg: float = _float("GENERATOR_INTERVAL_SECONDS", 2.0)
    # Horizonte simulado en dias (0 = sin limite, solo modo en vivo)
    horizonte_dias: float = _float("HORIZONTE_DIAS", 182.0)
    # Dias simulados de calentamiento previo (no se emiten eventos)
    warmup_dias: float = _float("WARMUP_DIAS", 60.0)


@dataclass
class MundoConfig:
    num_compradores: int = _int("NUM_COMPRADORES", 2000)
    num_proveedores: int = _int("NUM_PROVEEDORES", 400)
    # proporcion de entidades hibridas (comprador y proveedor a la vez)
    pct_hibridas: float = _float("PCT_HIBRIDAS", 0.05)
    # intermediarios legitimos (distribuidores/plataformas) entre las hibridas
    pct_intermediarios_de_hibridas: float = _float("PCT_INTERMEDIARIOS", 0.08)
    # fraccion de empresas que pertenecen a un grupo empresarial legitimo
    pct_grupos: float = _float("PCT_GRUPOS", 0.06)
    # sesgo de preferential attachment al elegir proveedor nuevo
    alpha_pa: float = _float("PREFERENTIAL_ATTACHMENT_ALPHA", 0.6)
    # altas de empresas nuevas por mes (fraccion de la poblacion)
    churn_mensual: float = _float("ENTITY_CHURN_MENSUAL", 0.02)
    # probabilidad de que una hibrida compre a otra hibrida (ciclos legitimos)
    p_hibrida_a_hibrida: float = _float("P_HIBRIDA_A_HIBRIDA", 0.08)
    cycle_detection_max_hops: int = _int("CYCLE_DETECTION_MAX_HOPS", 6)
    monto_mediana_base: float = _float("MONTO_MEDIANA_BASE", 4500.0)
    monto_sigma_log: float = _float("MONTO_SIGMA_LOG", 0.65)
    velocity_window_minutes: int = _int("VELOCITY_WINDOW_MINUTES", 120)
    # Fase 4 (docs/roadmap_correccion.md, 3.2(a), B4-T5): dias simulados medios entre dos
    # revisiones de limite de credito de un mismo comprador organico (proceso de Poisson
    # global, ver Simulador._revision_limite_legitima). Antes no existia: ningun comprador
    # legitimo cambiaba de limite nunca, asi que cualquier cambio observado en
    # relacion_historica.limite_credito era bust_out con certeza (medido: 4/4). HIPOTESIS DE
    # DISENO, no calibrada con datos reales (CLAUDE.md #4.7): 180 = revision semestral. 0 =
    # desactivado (reproduce la fuga, solo para comparar).
    revision_limite_dias_media: float = _float("REVISION_LIMITE_DIAS_MEDIA", 180.0)
    # Pendiente separado de 3.1 (docs/roadmap_correccion.md): fraccion de proveedores ORGANICOS
    # que cobran desde el alta a una cuenta en otro pais (importadores, tesoreria en el
    # exterior). Es el negativo duro de la senal "pais de cuenta distinto" de T2: sin el, la
    # combinacion proveedor nuevo + nunca cambio de cuenta + cuenta extranjera tenia 0 casos
    # legitimos (medido) y habria sido una marca pura de proveedor_fantasma. HIPOTESIS DE
    # DISENO, no calibrada con datos reales (CLAUDE.md #4.7).
    prob_cuenta_extranjera_proveedor: float = _float("PROB_CUENTA_EXTRANJERA_PROVEEDOR", 0.05)

    def __post_init__(self):
        if self.revision_limite_dias_media < 0:
            raise ValueError(
                f"REVISION_LIMITE_DIAS_MEDIA={self.revision_limite_dias_media} no puede ser negativo "
                f"(0 desactiva las revisiones organicas). Revisa tu .env."
            )
        if not 0.0 <= self.prob_cuenta_extranjera_proveedor <= 1.0:
            raise ValueError(
                f"PROB_CUENTA_EXTRANJERA_PROVEEDOR={self.prob_cuenta_extranjera_proveedor} debe estar "
                f"entre 0 y 1. Revisa tu .env."
            )


@dataclass
class FraudeConfig:
    # Prevalencia objetivo de EVENTOS fraudulentos (reales) sobre el total.
    prevalencia: float = _float("PREVALENCIA", 0.01)
    mezcla: Dict[str, float] = field(
        default_factory=lambda: parse_mezcla(os.getenv("MEZCLA_TIPOLOGIAS", MEZCLA_DEFECTO))
    )
    # Minimo de campanas por tipologia a lo largo del horizonte (0 = solo por prevalencia).
    # Fase 2 (docs/roadmap_correccion.md, A4): el default pasa de 0 a 6 -- es la recomendacion
    # que el propio proyecto ya documentaba (README_CAMBIOS.md, este mismo comentario antes de
    # este cambio) sin aplicarla. Con 0, T2/T3/T5 resultaban en 1-2 campanas en todo el
    # horizonte y T5 podia terminar con 0 casos evaluables en la particion de test del
    # validador. Sube el volumen total de eventos de fraude (cambia la forma del dataset,
    # no el contrato).
    min_campanas_tipologia: int = _int("MIN_CAMPANAS_TIPOLOGIA", 6)
    # Fase 2 (A4): bust_out (T5) no usa el controlador continuo de las demas tipologias (se
    # preplanifica una sola vez al arrancar, ver motor.py:_preplanificar_bust_out), asi que
    # necesita su propio minimo en vez de compartir min_campanas_tipologia -- con el mismo
    # numero para las 8 tipologias, T5 seguia dependiendo de la estratificacion por azar para
    # caer o no en la mitad de test.
    min_campanas_bust_out: int = _int("MIN_CAMPANAS_BUST_OUT", 6)
    # Etiquetas: ruido (ver seccion 6.2 del documento)
    ruido_fn: float = _float("RUIDO_ETIQUETA_FN", 0.05)    # fraude que nunca se etiqueta
    ruido_fp: float = _float("RUIDO_ETIQUETA_FP", 0.005)   # legitimo etiquetado como fraude
    # Fase 1 (docs/roadmap_correccion.md, A5): por defecto el ruido de etiqueta sigue siendo
    # MCAR (ruido_fn/ruido_fp parejo para todas las tipologias, como siempre). Si se activa,
    # ruido_fn se modula por DIFICULTAD_FN (tipologias.py): un analista real falla mas en las
    # tipologias sutiles que en las obvias, esto no esta calibrado con datos reales -- es una
    # hipotesis de diseno igual que el resto de parametros de tipologia.
    ruido_sistematico: bool = _bool("RUIDO_ETIQUETA_SISTEMATICO", False)
    # Deriva controlada: probabilidades de senal de las campañas decaen a lo largo del tiempo
    # (el defraudador se adapta). 0 = sin deriva. Reduccion total al final del horizonte.
    # Fase 1 (docs/roadmap_correccion.md, A6): el default queda en 0 DELIBERADAMENTE, no por
    # omision. Activarla por defecto mezclaria "deteccion de fraude estatico" con "adaptacion
    # del fraudador" en la misma corrida de referencia. Ademas, mientras el generador no
    # reaccione a las decisiones del detector (lazo abierto), esto es un decaimiento
    # programado, no una respuesta adversarial real -- no basta con subir este numero para
    # que OE2 (adaptacion/deriva) quede verificado. Usar un valor > 0 solo en una corrida
    # separada, declarada explicitamente como tal.
    deriva_intensidad: float = _float("DERIVA_INTENSIDAD", 0.0)
    # T1
    t1_p_dispositivo_nuevo: float = _float("T1_P_DISPOSITIVO_NUEVO", 0.85)  # variante credencial_robada
    t1_p_cambio_contacto: float = _float("T1_P_CAMBIO_CONTACTO", 0.50)
    t1_p_fuera_horario: float = _float("T1_P_FUERA_HORARIO", 0.50)
    # T2
    t2_p_complicidad: float = _float("T2_P_COMPLICIDAD", 0.35)
    t2_p_cuenta_compartida: float = _float("T2_P_CUENTA_COMPARTIDA", 0.40)
    # Senal de la especificacion ("pais de la cuenta bancaria distinto del pais del proveedor")
    # que el generador nunca implemento (0% en T2, medido). Su negativo duro es
    # MundoConfig.prob_cuenta_extranjera_proveedor. Hipotesis de diseno.
    t2_p_pais_cuenta_distinto: float = _float("T2_P_PAIS_CUENTA_DISTINTO", 0.30)
    # T3
    t3_p_atributo_compartido: float = _float("T3_P_ATRIBUTO_COMPARTIDO", 0.45)
    # T7
    t7_p_pais_distinto: float = _float("T7_P_PAIS_DISTINTO", 0.60)
    t7_p_fuera_horario: float = _float("T7_P_FUERA_HORARIO", 0.50)
    t7_p_verificacion_alterna: float = _float("T7_P_VERIFICACION", 0.20)


@dataclass
class GeneradorConfig:
    seed: int = _int("SEED", 42)
    # tasa media de eventos totales por hora simulada (compras + pagos + otros)
    # Fase 2 (docs/roadmap_correccion.md, B6): default 5.0 -> 12.0. NO es una calibracion contra
    # datos reales (no existen, ver CLAUDE.md #4.7) -- es el techo del rango que la rejilla de
    # sensibilidad (2.4, P5) ya confirmo estable (AUC temporal entre 0.944 y 0.970 para
    # EVENTOS_POR_HORA_SIM en 3..12). Se eligio el techo, no un punto intermedio, porque el
    # objetivo de B6 es densidad de historial por relacion: con 5.0 la mediana era 3 compras
    # por comprador en 6 meses (relacion_historica casi siempre vacio); con 12.0 sube a 7 y
    # los eventos con relacion nueva bajan de 20.1% a 12.5%. No se subio mas alla de 12 porque
    # seria extrapolar fuera de lo que la rejilla probo. NUM_COMPRADORES/NUM_PROVEEDORES
    # quedan sin tocar: no estaban en la rejilla de sensibilidad, cambiarlos exige una rejilla
    # propia antes (no estan en la lista de pendientes inmediatos del roadmap).
    eventos_por_hora_sim: float = _float("EVENTOS_POR_HORA_SIM", 12.0)
    # 0 = sin limite
    max_eventos: int = _int("MAX_EVENTOS", 0)
    # porcentaje (0..1) de compras legitimas que son "negativos duros" generados
    # a proposito (rafagas de reposicion, reposiciones periodicas, etc.)
    negativos_duros: float = _float("NEGATIVOS_DUROS", 0.02)
    # Fase 2 (docs/roadmap_correccion.md, B2): presupuesto SEPARADO (antes compartia mitad de
    # negativos_duros, es decir 1% de las compras organicas) para precios de lista que caen
    # cerca del limite de aprobacion por azar. T2/T4 SI dependen conceptualmente de estar cerca
    # del limite -- es la definicion de la tipologia (especificacion, seccion 4), no un
    # artefacto a eliminar. Lo que se corrige es la falta de negativos duros suficientes para
    # diluir esa correlacion: con 1% y un rango que no cubria la banda de T2, el detector podia
    # aprender "monto cerca del limite = fraude" casi sin excepciones.
    negativos_duros_limite: float = _float("NEGATIVOS_DUROS_LIMITE", 0.04)


@dataclass
class Config:
    dry_run: bool = _bool("DRY_RUN", True)
    output_dir: str = os.getenv("OUTPUT_DIR", "output")
    event_hub: EventHubConfig = field(default_factory=EventHubConfig)
    reloj: RelojConfig = field(default_factory=RelojConfig)
    mundo: MundoConfig = field(default_factory=MundoConfig)
    fraude: FraudeConfig = field(default_factory=FraudeConfig)
    generador: GeneradorConfig = field(default_factory=GeneradorConfig)


def cargar_config() -> Config:
    return Config()


config = cargar_config()


# Fase 5 (docs/roadmap_correccion.md, 5.5): reintentos del productor real (EventHubPublisher).
# Va FUERA de Config a proposito: no cambia el dataset generado (el modo offline ni lo usa),
# asi que no debe entrar en huella_config -- si entrara, cambiaria la huella de todos los
# reportes archivados sin que el dataset cambiara. Codigo nuevo: falla fuerte ante valores
# fuera de rango (CLAUDE.md #8), sin caer al default.
@dataclass
class ProductorConfig:
    # intentos TOTALES por sub-lote (1 = sin reintento). Se suman a los reintentos internos
    # del SDK de Event Hubs (retry_total, 3 por defecto), que no se tocan.
    intentos: int = _int("PRODUCTOR_INTENTOS", 4)
    # espera antes del 1er reintento; se duplica en cada uno, con tope espera_max_seg
    espera_inicial_seg: float = _float("PRODUCTOR_ESPERA_INICIAL_SEG", 1.0)
    espera_max_seg: float = _float("PRODUCTOR_ESPERA_MAX_SEG", 30.0)

    def __post_init__(self):
        if self.intentos < 1:
            raise ValueError(f"PRODUCTOR_INTENTOS={self.intentos} debe ser >= 1. Revisa tu .env.")
        if self.espera_inicial_seg < 0 or self.espera_max_seg < self.espera_inicial_seg:
            raise ValueError(
                f"PRODUCTOR_ESPERA_INICIAL_SEG={self.espera_inicial_seg} y PRODUCTOR_ESPERA_MAX_SEG="
                f"{self.espera_max_seg}: deben ser >= 0 y la maxima >= la inicial. Revisa tu .env."
            )


productor = ProductorConfig()
