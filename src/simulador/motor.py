"""Motor de simulacion de eventos B2B (generador v2 por tipologias).

Arquitectura:
  - Un unico heap de "intenciones" ordenado por tiempo simulado. Cada
    intencion se convierte en un evento COMPLETO en el momento de emitirse,
    leyendo el estado vigente (relaciones, grafo, saldos, sesion), de modo que
    todas las features son consistentes con el instante del evento y con los
    eventos previos (no hay estado "del futuro").
  - El flujo organico (compras y sus pagos) y las campanas de fraude usan el
    mismo constructor de eventos y el mismo esquema: la fuente de datos ya no
    es una variable observable.
  - La etiqueta no viaja en el evento de features. Se registra aparte con
    retraso y ruido (cola de ground truth).
"""
import heapq
import math
import random
import uuid
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from src.config import SCHEMA_VERSION, TIPOLOGIAS, Config
from src.simulador import tipologias as tip
from src.simulador.entidades import (
    A_CREDITO, CANALES, CATEGORIAS, FACTOR_CATEGORIA, METODOS, MONEDA_POR_PAIS, PAISES,
    PLAZO_DIAS, REDES, SIGMA_COMPRADOR_ESCALA, Cuenta, Empresa, GrafoB2B, Relacion,
)
from src.simulador.reloj import DIA, HORA, INTENSIDAD_MAX, RelojSimulado, intensidad_relativa

PESO_PAIS = [0.25, 0.18, 0.12, 0.17, 0.08, 0.12, 0.08]
VENTANA_RECIENTES_72H = 72 * HORA

# Fase 4 (docs/roadmap_correccion.md, 3.2(a), B4-T5): revisiones organicas de limite de credito.
# Todos los valores son HIPOTESIS DE DISENO, no calibrados con datos reales (CLAUDE.md #4.7).
# - Comprador con atraso acumulado: el area de credito puede recortarle la linea.
# - Comprador al dia: puede recibir un aumento. La probabilidad crece con la utilizacion, pero
#   tiene una BASE > 0 a proposito: un aumento grande con utilizacion baja es justo lo que hace
#   T5 (el defraudador pide mas linea a mitad de la construccion, sin necesitarla). Si lo
#   organico solo subiera limites a quien ya los usa, "aumento con utilizacion baja" seria una
#   marca pura de bust_out -- la misma fuga, un nivel mas abajo.
# - El rango de aumento contiene al de T5 (tipologias.FACTOR_SUBIDA_LIMITE_T5), para que el
#   factor del cambio no sea una segunda huella (antes T5 usaba siempre x2,0 exacto).
REVISION_ATRASO_MIN_DIAS = 20.0
REVISION_P_RECORTE = 0.6
REVISION_FACTOR_RECORTE = (0.5, 0.9)
REVISION_P_AUMENTO_BASE = 0.25
REVISION_P_AUMENTO_POR_UTILIZACION = 0.5   # se suma, escalado por min(1, utilizacion / 0.6)
REVISION_FACTOR_AUMENTO = (1.2, 2.5)


class Simulador:
    def __init__(self, cfg: Config, seed: Optional[int] = None):
        self.cfg = cfg
        self.rng = random.Random(cfg.generador.seed if seed is None else seed)
        self.reloj = RelojSimulado(cfg.reloj.inicio_iso)
        self.grafo = GrafoB2B(cfg.mundo.cycle_detection_max_hops)
        self.empresas: Dict[str, Empresa] = {}
        self.cuentas: Dict[str, Cuenta] = {}
        self.cuenta_uso: Dict[str, int] = {}
        # pools para seleccion organica
        self._comp_ids: List[str] = []
        self._comp_cum: List[float] = []
        self._comp_dirty = True
        self.prov_por_cat: Dict[str, List[str]] = {c: [] for c in CATEGORIAS}
        self.hibridas: List[str] = []
        self.intermediarios: List[str] = []
        # colas
        self._seq = 0
        self.cola: List[Tuple[float, int, Dict[str, Any]]] = []
        self.cola_gt: List[Tuple[float, int, Dict[str, Any]]] = []
        self.pagos_pendientes: Dict[int, Dict[str, Any]] = {}   # para T7
        self._ids_pagos: List[int] = []
        self._buf: List[Dict[str, Any]] = []
        # estado de reloj/control
        self.t = -cfg.reloj.warmup_dias * DIA
        self.t_inicio_emision = 0.0
        self.horizonte = cfg.reloj.horizonte_dias * DIA if cfg.reloj.horizonte_dias > 0 else None
        self.emitiendo = False
        self.fraude_activo = False
        # contadores
        self.total_emitidos = 0
        self.fraude_planeado = {n: 0 for n in TIPOLOGIAS}
        self.fraude_emitido = {n: 0 for n in TIPOLOGIAS}
        self.campanas_lanzadas = {n: 0 for n in TIPOLOGIAS}
        self._n_campana = 0
        # tasa de compras organicas (por segundo simulado, media)
        R = cfg.generador.eventos_por_hora_sim
        r_c = R * (1.0 - cfg.fraude.prevalencia) / 2.2
        self.lambda_compras = r_c / HORA
        self._construir_mundo()

    # ------------------------------------------------------------------
    # utilidades basicas
    # ------------------------------------------------------------------
    def programar(self, t: float, intent: Dict[str, Any]):
        self._seq += 1
        heapq.heappush(self.cola, (t, self._seq, intent))

    def nuevo_event_id(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def nuevo_id_empresa(self) -> str:
        while True:
            i = "EMP_%06d" % self.rng.randrange(100000, 1000000)
            if i not in self.empresas:
                return i

    def nuevo_id_cuenta(self) -> str:
        while True:
            i = "CTA_%07d" % self.rng.randrange(1000000, 10000000)
            if i not in self.cuentas:
                return i

    def nueva_campana(self, codigo: str) -> str:
        self._n_campana += 1
        return "%s-%05d" % (codigo, self._n_campana)

    def elegir_pais(self) -> str:
        return self.rng.choices(PAISES, PESO_PAIS)[0]

    def _lognorm(self, mediana: float, sigma: float) -> float:
        return self.rng.lognormvariate(math.log(max(mediana, 1e-6)), sigma)

    # ------------------------------------------------------------------
    # mundo
    # ------------------------------------------------------------------
    def crear_cuenta(self, pais: str, saldo: float, ingreso_dia: float, t: float) -> Cuenta:
        c = Cuenta(id=self.nuevo_id_cuenta(), pais=pais, saldo=saldo, ingreso_dia=ingreso_dia, t_ult=t)
        self.cuentas[c.id] = c
        self.cuenta_uso[c.id] = 1
        return c

    def crear_empresa(self, rol: str, alta: float, pais: Optional[str] = None,
                      organica: bool = True, tamano: Optional[float] = None) -> Empresa:
        """Crea una empresa. organica=False la deja fuera de los pools de seleccion
        organica (empresas controladas por una campana de fraude)."""
        rng = self.rng
        pais = pais or self.elegir_pais()
        escala = tamano if tamano is not None else self._lognorm(1.0, SIGMA_COMPRADOR_ESCALA)
        base = self.cfg.mundo.monto_mediana_base
        es_prov = rol in ("proveedor", "hibrida")
        es_comp = rol in ("comprador", "hibrida")
        # Negativo duro de la senal de pais de cuenta de T2 (ver
        # MundoConfig.prob_cuenta_extranjera_proveedor): solo proveedores organicos -- los de
        # campana deciden su cuenta en su propio lanzador.
        pais_cuenta = pais
        if es_prov and organica and rng.random() < self.cfg.mundo.prob_cuenta_extranjera_proveedor:
            pais_cuenta = rng.choice([x for x in PAISES if x != pais])
        cuenta = self.crear_cuenta(
            pais_cuenta,
            saldo=self._lognorm(base * escala * 25.0, 0.8),
            ingreso_dia=base * escala * rng.uniform(1.5, 6.0) if es_comp else base * escala * 0.3,
            t=alta if alta > self.t else self.t,
        )
        e = Empresa(
            id=self.nuevo_id_empresa(), rol=rol, pais=pais, alta=alta, cuenta=cuenta.id,
            domicilio="DOM_%07d" % rng.randrange(10 ** 7), representante="REP_%07d" % rng.randrange(10 ** 7),
            escala=escala,
        )
        self.empresas[e.id] = e
        if es_comp:
            e.actividad = self._lognorm(1.0, 1.0)
            e.limite_aprob = base * escala * rng.uniform(2.5, 8.0)
            e.limite_credito = base * escala * rng.uniform(10.0, 30.0)
            e.p_dispositivo_nuevo = min(0.3, max(0.01, rng.betavariate(1.2, 12.0)))
            e.hab_inicio = rng.choice([6, 7, 8, 8, 9])
            e.hab_fin = rng.choice([17, 18, 18, 19, 20])
            e.habil_finde = rng.random() < 0.25
            e.mfa = rng.random() < 0.60
            e.p_atraso = 0.55 if rng.random() < 0.05 else rng.uniform(0.05, 0.18)   # 5% morosos legitimos
            k = rng.choice([1, 2, 2, 3])
            e.cat_pref = rng.sample(CATEGORIAS, k)
            e.canal_w = [rng.uniform(0.3, 1.0) * w for w in (55, 15, 10, 15, 5)]
            e.red_w = [rng.uniform(0.5, 1.5) * w for w in (70, 12, 10, 6, 2)]
            e.metodo_w = [rng.uniform(0.5, 1.5) * w for w in (20, 30, 20, 10, 5, 15)]
            if rng.random() < 0.03:       # compradores legitimos en rapido crecimiento
                e.actividad *= 4.0
        if es_prov:
            e.categorias = rng.sample(CATEGORIAS, rng.choice([1, 1, 2, 3]))
            e.t_cambio_cuenta = None
        if organica:
            if es_comp:
                self._comp_ids.append(e.id)
                self._comp_dirty = True
            if es_prov:
                for c in e.categorias:
                    self.prov_por_cat[c].append(e.id)
            if rol == "hibrida":
                self.hibridas.append(e.id)
        return e

    def _construir_mundo(self):
        cfg, rng = self.cfg.mundo, self.rng
        n_total = cfg.num_compradores + cfg.num_proveedores
        n_hib = int(round(cfg.pct_hibridas * n_total))
        t_ini = self.t
        for _ in range(cfg.num_compradores):
            self.crear_empresa("comprador", alta=t_ini - rng.uniform(40, 1800) * DIA)
        for _ in range(cfg.num_proveedores):
            self.crear_empresa("proveedor", alta=t_ini - rng.uniform(40, 1800) * DIA)
        for _ in range(n_hib):
            self.crear_empresa("hibrida", alta=t_ini - rng.uniform(40, 1800) * DIA)
        n_int = max(1, int(round(cfg.pct_intermediarios_de_hibridas * len(self.hibridas))))
        for hid in rng.sample(self.hibridas, min(n_int, len(self.hibridas))):
            self.empresas[hid].es_intermediario = True
            self.intermediarios.append(hid)
        self._crear_grupos_legitimos()
        # procesos recurrentes
        self.programar(self._siguiente_llegada(self.t), {"tipo": "llegada_organica"})
        self._programar_proximo(self.t, "alta_entidad", self._tasa_altas())
        self._programar_proximo(self.t, "cambio_banco_legitimo", self._tasa_cambios_banco())
        self._programar_proximo(self.t, "revision_limite_legitima", self._tasa_revisiones_limite())

    def _crear_grupos_legitimos(self):
        """Grupos empresariales legitimos: comparten domicilio (y a veces representante o
        cuenta) y comercian entre si. Son negativos duros para 'atributos compartidos'."""
        rng = self.rng
        ids_c = [i for i in self._comp_ids]
        pool_prov = sorted({p for lst in self.prov_por_cat.values() for p in lst})
        n_grupos = int(self.cfg.mundo.pct_grupos * (len(ids_c) + len(pool_prov)) / 2.5)
        for g in range(n_grupos):
            comp = self.empresas[rng.choice(ids_c)]
            miembros = [comp] + [self.empresas[rng.choice(pool_prov)] for _ in range(rng.choice([1, 1, 2]))]
            miembros = list({m.id: m for m in miembros}.values())
            if len(miembros) < 2:
                continue
            dom = miembros[0].domicilio
            rep = miembros[0].representante if rng.random() < 0.5 else None
            for m in miembros:
                m.grupo = g
                m.domicilio = dom
                if rep:
                    m.representante = rep
            if rng.random() < 0.25 and len(miembros) > 2:   # tesoreria centralizada entre proveedores del grupo
                cta = miembros[1].cuenta
                for m in miembros[2:]:
                    self.cuenta_uso[m.cuenta] -= 1
                    m.cuenta = cta
                    self.cuenta_uso[cta] += 1
            for m in miembros:
                m.socios_grupo = [x.id for x in miembros if x.id != m.id and x.rol in ("proveedor", "hibrida")]

    def _tasa_altas(self) -> float:
        n = len(self.empresas)
        return n * self.cfg.mundo.churn_mensual / (30 * DIA)

    def _tasa_cambios_banco(self) -> float:
        n_prov = sum(len(v) for v in self.prov_por_cat.values()) / 1.7
        return n_prov * 0.004 / (30 * DIA)

    def _tasa_revisiones_limite(self) -> float:
        dias = self.cfg.mundo.revision_limite_dias_media
        if dias <= 0:
            return 0.0
        return len(self._comp_ids) / (dias * DIA)

    def _programar_proximo(self, t: float, tipo: str, tasa: float):
        if tasa > 0:
            self.programar(t + self.rng.expovariate(tasa), {"tipo": tipo})

    def _siguiente_llegada(self, t: float) -> float:
        lam_max = self.lambda_compras * INTENSIDAD_MAX
        while True:
            t += self.rng.expovariate(lam_max)
            h, d, dm = self.reloj.hora_dia_semana(t)
            if self.rng.random() < intensidad_relativa(h, d, dm) / INTENSIDAD_MAX:
                return t

    # ------------------------------------------------------------------
    # seleccion organica
    # ------------------------------------------------------------------
    def elegir_comprador(self, peso_fn=None, solo_organicos: bool = True) -> Empresa:
        if peso_fn is None:
            if self._comp_dirty:
                acc, cum = 0.0, []
                for i in self._comp_ids:
                    acc += self.empresas[i].actividad
                    cum.append(acc)
                self._comp_cum = cum
                self._comp_dirty = False
            return self.empresas[self.rng.choices(self._comp_ids, cum_weights=self._comp_cum)[0]]
        pesos = [peso_fn(self.empresas[i]) for i in self._comp_ids]
        return self.empresas[self.rng.choices(self._comp_ids, pesos)[0]]

    def _p_nueva_relacion(self, b: Empresa) -> float:
        n = len(b.favoritos)
        return 0.5 if n < 2 else (0.2 if n < 5 else 0.1)

    def elegir_proveedor_nuevo(self, b: Empresa, categoria: Optional[str] = None,
                               solo_hibridas: bool = False) -> Tuple[str, str]:
        rng = self.rng
        cat = categoria or rng.choice(b.cat_pref or CATEGORIAS)
        pool = self.prov_por_cat[cat]
        if solo_hibridas:
            pool = [p for p in pool if self.empresas[p].rol == "hibrida"] or pool
        alpha = self.cfg.mundo.alpha_pa
        pesos = []
        for pid in pool:
            e = self.empresas[pid]
            w = (self.grafo.grado_proveedor(pid) + 1.0) ** alpha
            if e.es_intermediario:
                w *= 2.5
            if e.pais == b.pais:
                w *= 3.0
            if self.t - e.alta < 90 * DIA:
                w *= 4.0       # proveedores legitimos nuevos (negativo duro de T2)
            if pid == b.id:
                w = 0.0
            pesos.append(w)
        if not pool or sum(pesos) <= 0:
            pool = [p for lst in self.prov_por_cat.values() for p in lst if p != b.id]
            pesos = [1.0] * len(pool)
        return rng.choices(pool, pesos)[0], cat

    def elegir_proveedor_organico(self, b: Empresa) -> Tuple[str, str]:
        rng = self.rng
        # afinidad de grupo
        if b.socios_grupo and rng.random() < 0.25:
            pid = rng.choice(b.socios_grupo)
            return pid, self._categoria_para(b, self.empresas[pid])
        if b.favoritos and rng.random() > self._p_nueva_relacion(b):
            pid = rng.choice(b.favoritos)
            return pid, self._categoria_para(b, self.empresas[pid])
        solo_h = b.rol == "hibrida" and rng.random() < self.cfg.mundo.p_hibrida_a_hibrida
        pid, cat = self.elegir_proveedor_nuevo(b, solo_hibridas=solo_h)
        if rng.random() < 0.6:
            b.favoritos.append(pid)
        return pid, cat

    def _categoria_para(self, b: Empresa, s: Empresa) -> str:
        comunes = [c for c in s.categorias if c in b.cat_pref]
        return self.rng.choice(comunes or s.categorias or CATEGORIAS)

    # ------------------------------------------------------------------
    # calculo de features
    # ------------------------------------------------------------------
    def _monto_organico(self, b: Empresa, cat: str) -> float:
        return self._lognorm(self.cfg.mundo.monto_mediana_base * FACTOR_CATEGORIA[cat] * b.escala,
                             self.cfg.mundo.monto_sigma_log)

    def _z_categoria(self, monto: float, cat: str, rel: Optional[Relacion] = None) -> float:
        # Fase 1 (docs/roadmap_correccion.md, B1): antes esta funcion solo dependia de
        # (monto, categoria) -- era invertible exactamente a partir de esos dos campos
        # (max|z_emitido - z(monto,categoria)| = 7e-5 medido), es decir "desviacion_monto_
        # categoria" no usaba ningun historial pese a su nombre. Ahora, si la relacion tiene
        # al menos 2 transacciones previas, el z-score se calcula contra la media y varianza
        # REALES de esa relacion (rel llega con el estado ANTES de este evento: registrar()
        # se llama despues, en _armar_evento). Sin historial suficiente, se usa la misma
        # referencia de categoria que antes -- igual que el resto del esquema, que deja en
        # null los campos de relacion_historica cuando la relacion es nueva.
        if rel is not None and rel.n >= 2:
            media = rel.suma / rel.n
            varianza = max(rel.suma_cuadrados / rel.n - media * media, 1e-9)
            sd = math.sqrt(varianza)
            sd = max(sd, 0.05 * max(media, 1.0))  # evita z explosivo cuando el historial es casi constante
            return (monto - media) / sd
        mu = math.log(self.cfg.mundo.monto_mediana_base * FACTOR_CATEGORIA[cat])
        sd = math.sqrt(self.cfg.mundo.monto_sigma_log ** 2 + SIGMA_COMPRADOR_ESCALA ** 2)
        return (math.log(max(monto, 1e-6)) - mu) / sd

    def _es_horario_habitual(self, b: Empresa, t: float) -> bool:
        h, d, _ = self.reloj.hora_dia_semana(t)
        return (b.hab_inicio <= h <= b.hab_fin) and (d < 5 or b.habil_finde)

    def _sesion(self, b: Empresa, t: float, ov: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        rng, ov = self.rng, (ov or {})
        canal = ov.get("canal") or rng.choices(CANALES, b.canal_w)[0]
        if canal in ("api", "edi"):
            s = {"tipo_dispositivo": "integracion", "dispositivo_reconocido": True,
                 "red_conexion": "corporativa", "autenticacion_reforzada": True,
                 "cambio_contacto_reciente": False}
        else:
            reciente = b.t_cambio_contacto is not None and (t - b.t_cambio_contacto) < 7 * DIA
            s = {
                "tipo_dispositivo": "mobile" if canal == "app_movil" else rng.choices(["desktop", "tablet"], [0.9, 0.1])[0],
                "dispositivo_reconocido": rng.random() > b.p_dispositivo_nuevo,
                "red_conexion": rng.choices(REDES, b.red_w)[0],
                "autenticacion_reforzada": b.mfa and rng.random() < 0.95,
                "cambio_contacto_reciente": bool(reciente or rng.random() < 0.02),
            }
        for k in ("tipo_dispositivo", "dispositivo_reconocido", "red_conexion",
                  "autenticacion_reforzada", "cambio_contacto_reciente"):
            if k in ov:
                s[k] = ov[k]
        s["_canal"] = canal
        return s

    @staticmethod
    def _dias_cambio(s: Empresa, t: float) -> Optional[float]:
        """Dias desde el ultimo cambio legitimo de cuenta del proveedor; null si fue hace > 90 dias."""
        if s.t_cambio_cuenta is None:
            return None
        d = (t - s.t_cambio_cuenta) / DIA
        return d if 0 <= d <= 90 else None

    def _atributos_compartidos(self, u: Empresa, v: Empresa) -> int:
        return int(u.domicilio == v.domicilio) + int(u.representante == v.representante) + int(u.cuenta == v.cuenta)

    def _similitud_factura(self, s: Empresa, t: float, numero: str, comprador: str, monto: float) -> float:
        while s.facturas and t - s.facturas[0][0] > 30 * DIA:
            s.facturas.popleft()
        mejor = 0.0
        for (_, num2, comp2, m2) in s.facturas:
            s_monto = max(0.0, 1.0 - 10.0 * abs(monto - m2) / max(monto, m2, 1e-6))
            if num2 == numero:
                s_num = 1.0
            elif len(num2) == len(numero) and sum(a != b for a, b in zip(num2, numero)) <= 1:
                s_num = 0.8
            else:
                s_num = 0.0
            s_comp = 1.0 if comp2 == comprador else 0.3
            mejor = max(mejor, 0.5 * s_monto + 0.3 * s_num + 0.2 * s_comp)
        return round(mejor, 4)

    def _podar(self, dq: Deque, t: float, ventana: float, idx: int = 0):
        while dq and t - (dq[0][idx] if isinstance(dq[0], tuple) else dq[0]) > ventana:
            dq.popleft()

    def _comportamiento(self, b: Empresa, s_id: str, rel, t: float) -> Dict[str, Any]:
        ventana_vel = self.cfg.mundo.velocity_window_minutes * 60.0
        self._podar(b.eventos_recientes, t, ventana_vel)
        self._podar(b.provs_30d, t, 30 * DIA)
        provs = len({p for (_, p) in b.provs_30d})
        if s_id not in {p for (_, p) in b.provs_30d}:
            provs += 1
        n72, suma72 = 0, 0.0
        if rel is not None:
            self._podar(rel.recientes, t, VENTANA_RECIENTES_72H)
            n72 = len(rel.recientes)
            suma72 = sum(m for (_, m) in rel.recientes)
        return {"transacciones_recientes_comprador": len(b.eventos_recientes),
                "proveedores_distintos_30d": provs,
                "ordenes_mismo_proveedor_72h": n72,
                "_suma72": suma72}

    def _flujo_cuenta(self, c: Cuenta, t: float, salida_actual: float = 0.0):
        while c.flujo and t - c.flujo[0][0] > DIA:
            c.flujo.popleft()
        entradas = sum(x for (_, x) in c.flujo if x > 0)
        salidas = -sum(x for (_, x) in c.flujo if x < 0) + salida_actual
        ultima = max((tt for (tt, x) in c.flujo if x > 0), default=None)
        razon = round(salidas / entradas, 4) if entradas > 0 else None
        horas = round((t - ultima) / HORA, 3) if ultima is not None else None
        return razon, horas

    def _mover(self, origen: Cuenta, destino: Cuenta, monto: float, t: float) -> Dict[str, Any]:
        origen.devengar(t)
        destino.devengar(t)
        if origen is destino:      # tesoreria compartida: movimiento interno, saldo neto sin cambios
            return {"saldo_origen_antes": round(origen.saldo, 2), "saldo_origen_despues": round(origen.saldo, 2),
                    "saldo_destino_antes": round(origen.saldo, 2), "saldo_destino_despues": round(origen.saldo, 2),
                    "_razon": None, "_horas": None}
        if origen.saldo < monto:
            # Fase 1 (docs/roadmap_correccion.md, B5): antes el refuerzo era monto*U(1.0,1.6),
            # es decir una funcion casi determinista DEL MONTO de esta transaccion puntual.
            # Medido en la corrida de referencia: solo 7 de 21.737 eventos caian en esa razon
            # saldo_antes/monto y los 7 eran fraude (precision 1.0) -- una huella de
            # construccion, no una senal de fraude. Ahora el refuerzo se dimensiona contra la
            # ESCALA PROPIA de la cuenta (misma lognormal que ya se usa para fondear cuentas
            # nuevas en crear_empresa), independiente del monto de esta transaccion particular.
            # Esto no resuelve que los saldos nunca bajen fuera de un evento (eso exige
            # modelar el refuerzo como un evento observable -- ver Fase 3, hallazgo B5), solo
            # rompe la correlacion casi perfecta con el monto.
            refuerzo = self._lognorm(self.cfg.mundo.monto_mediana_base * 25.0, 0.8)
            origen.saldo = max(monto * self.rng.uniform(1.01, 1.05), refuerzo)
        razon, horas = self._flujo_cuenta(origen, t, salida_actual=monto)
        s_o_antes, s_d_antes = origen.saldo, destino.saldo
        origen.saldo -= monto
        destino.saldo += monto
        origen.flujo.append((t, -monto))
        destino.flujo.append((t, monto))
        return {"saldo_origen_antes": round(s_o_antes, 2), "saldo_origen_despues": round(origen.saldo, 2),
                "saldo_destino_antes": round(s_d_antes, 2), "saldo_destino_despues": round(destino.saldo, 2),
                "_razon": razon, "_horas": horas}

    # ------------------------------------------------------------------
    # constructor de evento
    # ------------------------------------------------------------------
    def _armar_evento(self, *, tipo: str, t: float, b: Empresa, s: Empresa, monto: float, cat: str,
                      metodo: Optional[str], sesion: Dict[str, Any], cuenta_dest: Cuenta,
                      doc: Optional[Dict[str, Any]], saldo: Optional[Dict[str, Any]],
                      dias_cambio_cuenta: Optional[float], registrar: bool = True) -> Dict[str, Any]:
        rel = self.grafo.rel.get((b.id, s.id))
        ciclo = self.grafo.ciclo_que_cierra(b.id, s.id)
        comp = self._comportamiento(b, s.id, rel, t)
        n_prev = rel.n if rel else 0
        h, d, _ = self.reloj.hora_dia_semana(t)
        utilizacion = round(b.credito_usado / b.limite_credito, 4) if b.limite_credito > 0 else None
        razon_lim = round(monto / b.limite_aprob, 4) if (b.limite_aprob > 0 and tipo == "compra") else None
        suma_vs_lim = round((comp["_suma72"] + monto) / b.limite_aprob, 4) if (b.limite_aprob > 0 and tipo == "compra") else None
        ev = {
            "event_id": self.nuevo_event_id(),
            "schema_version": SCHEMA_VERSION,
            "event_type": tipo,
            "timestamp": self.reloj.iso(t),
            "comprador_id": b.id,
            "proveedor_id": s.id,
            "cuenta_origen_id": b.cuenta,
            "cuenta_destino_id": cuenta_dest.id,
            "monto": round(monto, 2),
            "moneda": MONEDA_POR_PAIS[b.pais],
            "categoria": cat,
            "canal": sesion["_canal"],
            "metodo_pago": metodo,
            "relacion_historica": {
                "fecha_primera_transaccion": self.reloj.iso(rel.primera) if rel else None,
                "num_transacciones_previas": n_prev,
                "dias_desde_ultima_transaccion": round((t - rel.ultima) / DIA, 4) if rel else None,
                "monto_promedio_historico": round(rel.suma / rel.n, 2) if rel and rel.n else None,
                "desviacion_monto_categoria": round(self._z_categoria(monto, cat, rel), 4),
                "limite_credito": round(b.limite_credito, 2),
                "utilizacion_credito": utilizacion,
                "dias_atraso_acumulado": round(b.atraso_acum, 2),
            },
            "documento": {
                "numero_orden_compra": doc.get("oc_num") if doc else None,
                "numero_factura": doc.get("factura") if doc else None,
                "tiene_oc_asociada": bool(doc.get("oc_num")) if doc else False,
                "monto_oc": doc.get("monto_oc") if doc else None,
                "diferencia_pct_factura_oc": doc.get("diff_pct") if doc else None,
                "similitud_factura_max_30d": doc.get("sim") if doc else None,
                "razon_monto_limite_aprobacion": razon_lim,
            },
            "grafo": {
                "grado_conectividad_comprador": self.grafo.grado_comprador(b.id),
                "grado_conectividad_proveedor": self.grafo.grado_proveedor(s.id),
                "forma_parte_de_ciclo": ciclo is not None,
                "longitud_ciclo_detectado": ciclo,
                "reciprocidad": round(self.grafo.reciprocidad(b.id, s.id), 4),
                "atributos_compartidos_n": self._atributos_compartidos(b, s),
            },
            "dispositivo": {k: sesion[k] for k in ("tipo_dispositivo", "dispositivo_reconocido", "red_conexion",
                                                    "autenticacion_reforzada", "cambio_contacto_reciente")},
            "comportamiento": {
                "transacciones_recientes_comprador": comp["transacciones_recientes_comprador"],
                "proveedores_distintos_30d": comp["proveedores_distintos_30d"],
                "ordenes_mismo_proveedor_72h": comp["ordenes_mismo_proveedor_72h"] if tipo == "compra" else None,
                "suma_vs_limite_72h": suma_vs_lim,
                "razon_salida_entrada_24h": saldo["_razon"] if saldo else None,
                "horas_desde_ultima_entrada": saldo["_horas"] if saldo else None,
            },
            "proveedor": {
                "antiguedad_dias": round(max(0.0, (t - s.alta) / DIA), 2),
                "compradores_distintos": self.grafo.grado_proveedor(s.id) + (0 if rel else 1),
                "cuenta_compartida_n": max(0, self.cuenta_uso.get(cuenta_dest.id, 1) - 1),
                "dias_desde_cambio_cuenta_destino": None if dias_cambio_cuenta is None else round(dias_cambio_cuenta, 3),
            },
            "contexto_temporal": {
                "hora_del_dia": h,
                "es_fin_de_semana": d >= 5,
                "es_horario_habitual": self._es_horario_habitual(b, t),
            },
            "geo": {
                "pais_comprador": b.pais,
                "pais_proveedor": s.pais,
                "pais_cuenta_destino": cuenta_dest.pais,
            },
            "saldo": ({k: v for k, v in saldo.items() if not k.startswith("_")} if saldo else
                      {"saldo_origen_antes": None, "saldo_origen_despues": None,
                       "saldo_destino_antes": None, "saldo_destino_despues": None}),
        }
        if registrar:
            r = self.grafo.registrar(b.id, s.id, t, monto)
            r.recientes.append((t, monto))
            b.eventos_recientes.append(t)
            b.provs_30d.append((t, s.id))
        return ev

    # ------------------------------------------------------------------
    # etiquetas (ground truth aparte, con retraso y ruido)
    # ------------------------------------------------------------------
    def _retraso_etiqueta(self, meta: Optional[Dict[str, Any]]) -> float:
        rng = self.rng
        if meta is None or not meta.get("fraude"):
            return max(3 * DIA, self._lognorm(14 * DIA, 0.4))
        tipologia = meta["tipologia"]
        tabla = {
            "toma_de_cuenta": (1 * DIA, 0.8), "proveedor_fantasma": (21 * DIA, 0.5),
            "anillo_colusion": (30 * DIA, 0.5), "fraccionamiento_ordenes": (10 * DIA, 0.6),
            "factura_inflada_duplicada": (21 * DIA, 0.5), "bec_datos_bancarios": (7 * DIA, 0.6),
            "cadena_fondos_paso": (14 * DIA, 0.6),
        }
        if tipologia == "bust_out":
            return rng.uniform(30 * DIA, 90 * DIA)
        med, sig = tabla[tipologia]
        return max(HORA, self._lognorm(med, sig))

    def _registrar_gt(self, ev: Dict[str, Any], t: float, meta: Optional[Dict[str, Any]]):
        if not self.emitiendo:
            return
        rng = self.rng
        real = bool(meta and meta.get("fraude"))
        etiquetado = real
        if real:
            # Fase 1 (docs/roadmap_correccion.md, A5): por defecto (ruido_sistematico=False)
            # esto es exactamente ruido_fn parejo, igual que siempre (MCAR). Si se activa, se
            # modula por tipologia via DIFICULTAD_FN -- un ruido MCAR asume que el analista
            # falla igual en todas las tipologias, lo que no es realista.
            p_fn = self.cfg.fraude.ruido_fn
            if self.cfg.fraude.ruido_sistematico and meta.get("tipologia"):
                p_fn = min(0.95, p_fn * tip.DIFICULTAD_FN.get(meta["tipologia"], 1.0))
            if rng.random() < p_fn:
                etiquetado = False
        elif ev["event_type"] in ("compra", "pago") and rng.random() < self.cfg.fraude.ruido_fp:
            etiquetado = True
        t_et = t + self._retraso_etiqueta(meta)
        gt = {
            "event_id": ev["event_id"],
            "schema_version": SCHEMA_VERSION,
            "timestamp_evento": ev["timestamp"],
            "timestamp_etiqueta": self.reloj.iso(t_et),
            "es_fraude_real": real,
            "es_fraude_etiquetado": etiquetado,
            "tipologia": meta["tipologia"] if meta else None,
            "variante": meta.get("variante") if meta else None,
            "campana_id": meta.get("campana") if meta else None,
            "fase_campana": meta.get("fase") if meta else None,
        }
        self._seq += 1
        heapq.heappush(self.cola_gt, (t_et, self._seq, gt))
        if real:
            self.fraude_emitido[meta["tipologia"]] += 1

    def _salida(self, ev: Dict[str, Any], t: float, meta: Optional[Dict[str, Any]]):
        if self.emitiendo:
            self._buf.append(ev)
            self.total_emitidos += 1
            self._registrar_gt(ev, t, meta)

    # ------------------------------------------------------------------
    # emisores por tipo de intencion
    # ------------------------------------------------------------------
    def _documento_organico(self, b: Empresa, s: Empresa, t: float, monto: float, intent: Dict[str, Any]):
        """Devuelve (monto_factura, doc)."""
        rng = self.rng
        tiene_oc = intent.get("oc")
        if tiene_oc is None:
            tiene_oc = rng.random() > 0.04            # compras urgentes sin OC (negativo duro)
        delta = intent.get("delta")
        if delta is None:
            # tolerancia legitima (impuestos, fletes): 85% exacto, 15% con diferencia
            if rng.random() < 0.85:
                delta = rng.uniform(-0.004, 0.004)
            else:
                delta = rng.uniform(0.005, 0.08) * rng.choice([1, 1, -1])
                if rng.random() < 0.03:
                    delta = rng.uniform(0.08, 0.10)     # solape marginal con fraude
        monto_oc = monto if intent.get("monto_oc") is None else intent["monto_oc"]
        monto_fact = monto_oc * (1.0 + delta) if intent.get("monto_oc") is None else monto
        # numero de factura
        numero = intent.get("numero_factura")
        if numero is None:
            s.contador_factura += 1
            numero = "FAC-%s-%06d" % (s.id[-6:], s.contador_factura)
        # reenvio legitimo tras rechazo (negativo duro): misma factura, mismo comprador
        if intent.get("numero_factura") is None and intent.get("meta") is None and rng.random() < 0.005:
            previas = [x for x in s.facturas if x[2] == b.id and t - x[0] <= 30 * DIA]
            if previas:
                _, numero, _, m_prev = previas[-1]
                monto_fact = m_prev * rng.uniform(0.995, 1.005)
                monto_oc = monto_fact
                delta = 0.0
        oc_num = ("OC-%08d" % rng.randrange(10 ** 8)) if tiene_oc else None
        sim = self._similitud_factura(s, t, numero, b.id, monto_fact)
        diff_pct = round(100.0 * (monto_fact - monto_oc) / monto_oc, 3) if tiene_oc and monto_oc else None
        doc = {"oc_num": oc_num, "factura": numero, "monto_oc": round(monto_oc, 2) if tiene_oc else None,
               "diff_pct": diff_pct, "sim": sim}
        return monto_fact, doc

    def _emitir_compra(self, intent: Dict[str, Any], t: float):
        rng = self.rng
        b = self.empresas[intent["comprador"]] if intent.get("comprador") else None
        if b is None:
            b = self.elegir_comprador()
        if intent.get("proveedor"):
            s = self.empresas[intent["proveedor"]]
            cat = intent.get("categoria") or self._categoria_para(b, s)
        else:
            sid, cat = self.elegir_proveedor_organico(b)
            s = self.empresas[sid]
        if intent.get("categoria"):
            cat = intent["categoria"]
        monto_base = intent.get("monto") or self._monto_organico(b, cat)
        dup = intent.get("dup_de")
        if dup is not None and "numero" in dup:
            # segunda presentacion de una factura ya emitida (T6 duplicada)
            numero = dup["numero"]
            if rng.random() < 0.4:   # variante con un caracter alterado
                d = numero[-1]
                numero = numero[:-1] + str((int(d) + rng.randint(1, 9)) % 10)
            intent = dict(intent, numero_factura=numero)
            monto_base = dup["monto"] * rng.uniform(0.99, 1.01)
            if intent.get("oc"):
                intent["monto_oc"] = monto_base
        metodo = intent.get("metodo") or rng.choices(METODOS, b.metodo_w)[0]
        monto, doc = self._documento_organico(b, s, t, monto_base, intent)
        sesion = self._sesion(b, t, intent.get("sesion"))
        cuenta_dest = self.cuentas[intent.get("cuenta_destino") or s.cuenta]
        dias_cambio = self._dias_cambio(s, t) if cuenta_dest.id == s.cuenta else None
        ev = self._armar_evento(tipo="compra", t=t, b=b, s=s, monto=monto, cat=cat, metodo=metodo,
                                sesion=sesion, cuenta_dest=cuenta_dest, doc=doc, saldo=None,
                                dias_cambio_cuenta=dias_cambio)
        # estado posterior
        if intent.get("ref") is not None:
            intent["ref"].update(numero=doc["factura"], monto=monto)
        s.facturas.append((t, doc["factura"], b.id, monto))
        if metodo in A_CREDITO:
            b.credito_usado += monto
        meta = intent.get("meta")
        self._salida(ev, t, meta)
        # pago programado
        if intent.get("programar_pago", True):
            plazo = PLAZO_DIAS[metodo] * DIA
            atraso = 0.0
            if rng.random() < b.p_atraso:
                atraso = rng.expovariate(1.0 / (9 * DIA))
            t_pago = t + plazo * rng.uniform(0.9, 1.05) + atraso
            pago = {"tipo": "pago", "comprador": b.id, "proveedor": s.id, "categoria": cat, "monto": monto,
                    "metodo": metodo, "doc": doc, "due": t + plazo, "meta": intent.get("pago_meta", meta) if meta else None,
                    "sesion": intent.get("sesion_pago"), "cuenta_destino": intent.get("cuenta_destino_pago")}
            self._agendar_pago(t_pago, pago)
        return ev

    def _agendar_pago(self, t_pago: float, pago: Dict[str, Any]):
        self._seq += 1
        pago["_id"] = self._seq
        pago["_t"] = t_pago
        if not (pago.get("meta") and pago["meta"].get("fraude")):
            self.pagos_pendientes[self._seq] = pago
            self._ids_pagos.append(self._seq)
        self.programar(t_pago, pago)

    def _emitir_pago(self, intent: Dict[str, Any], t: float):
        rng = self.rng
        self.pagos_pendientes.pop(intent.get("_id", -1), None)
        b, s = self.empresas[intent["comprador"]], self.empresas[intent["proveedor"]]
        monto = intent["monto"]
        cuenta_dest = self.cuentas[intent.get("cuenta_destino") or s.cuenta]
        origen = self.cuentas[b.cuenta]
        # cuenta de destino vigente al momento del pago (puede haber cambiado legitimamente)
        dias_cambio = intent.get("dias_cambio")
        if dias_cambio is None and intent.get("cuenta_destino") is None:
            cuenta_dest = self.cuentas[s.cuenta]
            dias_cambio = self._dias_cambio(s, t)
        elif intent.get("cambio_t") is not None:
            dias_cambio = (t - intent["cambio_t"]) / DIA
        sesion = self._sesion(b, t, intent.get("sesion"))
        saldo = self._mover(origen, cuenta_dest, monto, t)
        ev = self._armar_evento(tipo="pago", t=t, b=b, s=s, monto=monto, cat=intent["categoria"],
                                metodo=intent.get("metodo"), sesion=sesion, cuenta_dest=cuenta_dest,
                                doc=intent.get("doc"), saldo=saldo, dias_cambio_cuenta=dias_cambio)
        if intent.get("metodo") in A_CREDITO:
            b.credito_usado = max(0.0, b.credito_usado - monto)
            if intent.get("due") is not None:
                b.atraso_acum += max(0.0, (t - intent["due"]) / DIA)
        self._salida(ev, t, intent.get("meta"))
        # intermediarios legitimos reenvian parte del dinero recibido (negativo duro de T8)
        if s.es_intermediario and rng.random() < 0.8 and intent.get("meta") is None:
            destino = self.elegir_proveedor_nuevo(s)[0]
            self.programar(t + rng.uniform(6, 96) * HORA, {
                "tipo": "transferencia", "origen": s.id, "destino": destino,
                "monto": monto * rng.uniform(0.75, 0.93), "categoria": intent["categoria"], "meta": None})
        return ev

    def _emitir_transferencia(self, intent: Dict[str, Any], t: float):
        """Transferencia entre cuentas de empresas (origen actua como 'comprador')."""
        b, s = self.empresas[intent["origen"]], self.empresas[intent["destino"]]
        monto = intent["monto"]
        origen, dest = self.cuentas[b.cuenta], self.cuentas[s.cuenta]
        sesion = self._sesion(b, t, intent.get("sesion"))
        saldo = self._mover(origen, dest, monto, t)
        ev = self._armar_evento(tipo="transferencia", t=t, b=b, s=s, monto=monto, cat=intent["categoria"],
                                metodo="transferencia_bancaria", sesion=sesion, cuenta_dest=dest, doc=None,
                                saldo=saldo, dias_cambio_cuenta=None)
        self._salida(ev, t, intent.get("meta"))
        return ev

    def _emitir_pago_directo(self, intent: Dict[str, Any], t: float):
        """Pago sin compra asociada (anticipo). Mismo evento 'pago' sin documento."""
        b, s = self.empresas[intent["comprador"]], self.empresas[intent["proveedor"]]
        cuenta_dest = self.cuentas[intent.get("cuenta_destino") or s.cuenta]
        saldo = self._mover(self.cuentas[b.cuenta], cuenta_dest, intent["monto"], t)
        sesion = self._sesion(b, t, intent.get("sesion"))
        dias_cambio = self._dias_cambio(s, t) if cuenta_dest.id == s.cuenta else None
        ev = self._armar_evento(tipo="pago", t=t, b=b, s=s, monto=intent["monto"], cat=intent["categoria"],
                                metodo=intent.get("metodo", "transferencia_bancaria"), sesion=sesion,
                                cuenta_dest=cuenta_dest, doc=None, saldo=saldo, dias_cambio_cuenta=dias_cambio)
        self._salida(ev, t, intent.get("meta"))
        return ev

    def _emitir_cambio_banco(self, intent: Dict[str, Any], t: float):
        """Solicitud de cambio de datos bancarios de un proveedor notificada a un comprador."""
        b, s = self.empresas[intent["comprador"]], self.empresas[intent["proveedor"]]
        nueva = self.cuentas[intent["cuenta_nueva"]]
        sesion = self._sesion(b, t, intent.get("sesion"))
        ev = self._armar_evento(tipo="cambio_datos_bancarios", t=t, b=b, s=s, monto=0.0,
                                cat=intent.get("categoria") or self._categoria_para(b, s), metodo=None,
                                sesion=sesion, cuenta_dest=nueva, doc=None, saldo=None,
                                dias_cambio_cuenta=0.0, registrar=False)
        self._salida(ev, t, intent.get("meta"))
        return ev

    # ------------------------------------------------------------------
    # procesos organicos recurrentes
    # ------------------------------------------------------------------
    def _llegada_organica(self, t: float):
        rng = self.rng
        self.programar(self._siguiente_llegada(t), {"tipo": "llegada_organica"})
        b = self.elegir_comprador()
        intent: Dict[str, Any] = {"tipo": "compra", "comprador": b.id}
        nd = self.cfg.generador.negativos_duros
        # Fase 2 (B2): presupuesto propio, independiente de nd (antes compartia la mitad de nd
        # con la rafaga, abajo). Rango ampliado a 0.55-0.99 para cubrir tanto la banda de T2
        # (tipologias.py: 0.55-0.97) como la de T4 (0.80-0.99) -- antes solo cubria 0.80-0.99 y
        # T2 quedaba sin ningun contrapeso legitimo en la mitad baja de su propio rango.
        if rng.random() < self.cfg.generador.negativos_duros_limite:
            # precio de lista que casualmente cae cerca del limite de aprobacion
            sid, cat = self.elegir_proveedor_organico(b)
            intent.update(proveedor=sid, categoria=cat, monto=b.limite_aprob * rng.uniform(0.55, 0.99))
        if rng.random() < nd:
            # rafaga de reposicion de inventario: varias ordenes en pocas horas
            for k in range(rng.randint(2, 5)):
                sid, cat = self.elegir_proveedor_organico(b)
                self.programar(t + rng.uniform(10, 180) * 60 * (k + 1) * 0.5,
                               {"tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat})
        self._emitir_compra(intent, t)
        # anticipos legitimos (pago sin compra asociada)
        if rng.random() < 0.015:
            sid, cat = self.elegir_proveedor_organico(b)
            self.programar(t + rng.uniform(1, 48) * HORA, {
                "tipo": "pago_directo", "comprador": b.id, "proveedor": sid, "categoria": cat,
                "monto": self._monto_organico(b, cat) * rng.uniform(0.3, 0.6)})
        if self.fraude_activo:
            self._controlador_fraude(t)

    def _alta_entidad(self, t: float):
        rng = self.rng
        self._programar_proximo(t, "alta_entidad", self._tasa_altas())
        u = rng.random()
        rol = "hibrida" if u < 0.03 else ("comprador" if u < 0.55 else "proveedor")
        e = self.crear_empresa(rol, alta=t)
        if e.rol == "hibrida" and rng.random() < self.cfg.mundo.pct_intermediarios_de_hibridas:
            e.es_intermediario = True
            self.intermediarios.append(e.id)

    def _revision_limite_legitima(self, t: float):
        """Revision periodica del limite de credito de un comprador organico (negativo duro de
        T5). Sin evento propio: el cambio queda observable en relacion_historica.limite_credito
        de las compras siguientes, igual que el de T5 (CLAUDE.md #4, principio 4)."""
        rng = self.rng
        self._programar_proximo(t, "revision_limite_legitima", self._tasa_revisiones_limite())
        if not self._comp_ids:
            return
        b = self.empresas[rng.choice(self._comp_ids)]
        if b.limite_credito <= 0:
            return
        if b.atraso_acum >= REVISION_ATRASO_MIN_DIAS:
            if rng.random() < REVISION_P_RECORTE:
                b.limite_credito *= rng.uniform(*REVISION_FACTOR_RECORTE)
            return
        utilizacion = b.credito_usado / b.limite_credito
        p = REVISION_P_AUMENTO_BASE + REVISION_P_AUMENTO_POR_UTILIZACION * min(1.0, utilizacion / 0.6)
        if rng.random() < p:
            b.limite_credito *= rng.uniform(*REVISION_FACTOR_AUMENTO)

    def _cambio_banco_legitimo(self, t: float):
        rng = self.rng
        self._programar_proximo(t, "cambio_banco_legitimo", self._tasa_cambios_banco())
        candidatos = [pid for pid in rng.sample(self._todos_proveedores(), min(30, len(self._todos_proveedores())))
                      if self.grafo.ent.get(pid)]
        if not candidatos:
            return
        s = self.empresas[candidatos[0]]
        pais = s.pais if rng.random() > 0.06 else self.elegir_pais()
        nueva = self.crear_cuenta(pais, saldo=self._lognorm(self.cfg.mundo.monto_mediana_base * 20, 0.8),
                                  ingreso_dia=self.cfg.mundo.monto_mediana_base * 0.3, t=t)
        self.cuenta_uso[s.cuenta] = max(0, self.cuenta_uso.get(s.cuenta, 1) - 1)
        s.cuenta = nueva.id
        s.t_cambio_cuenta = t
        compradores = list(self.grafo.ent[s.id].keys())
        for bid in rng.sample(compradores, min(3, len(compradores))):
            canal = rng.choices(["portal_proveedor", "llamada", "email"], [55, 20, 25])[0]
            verificado = canal != "email" or rng.random() < 0.6
            self.programar(t + rng.uniform(0, 3600), {
                "tipo": "cambio_banco", "comprador": bid, "proveedor": s.id, "cuenta_nueva": nueva.id,
                "meta": None,
                "sesion": {"canal": canal, "autenticacion_reforzada": rng.random() < (0.75 if verificado else 0.3),
                           "dispositivo_reconocido": rng.random() > 0.15}})

    def _todos_proveedores(self) -> List[str]:
        vistos, out = {}, []
        for c in CATEGORIAS:
            for pid in self.prov_por_cat[c]:
                if pid not in vistos:
                    vistos[pid] = 1
                    out.append(pid)
        return out

    # ------------------------------------------------------------------
    # control de prevalencia
    # ------------------------------------------------------------------
    def _controlador_fraude(self, t: float):
        """Lanza campanas cuando el fraude planificado de una tipologia queda por debajo
        de prevalencia * mezcla * (eventos emitidos). Mantiene la prevalencia objetivo
        sin depender de estimaciones del tamano de cada campana."""
        fc = self.cfg.fraude
        base = self.total_emitidos + 60
        for nombre in TIPOLOGIAS:
            peso = fc.mezcla.get(nombre, 0.0)
            if peso <= 0:
                continue
            if nombre == "bust_out" and self.horizonte is not None:
                continue      # preplanificado en calentar(): su construccion puede preceder al horizonte
            objetivo = fc.prevalencia * peso * base
            lanzar = self.fraude_planeado[nombre] < objetivo
            if fc.min_campanas_tipologia > 0 and self.horizonte:
                util = max(1.0, self.horizonte - tip.DURACION_MAX[nombre])
                if nombre == "bust_out":
                    continue
                esperadas = fc.min_campanas_tipologia * min(1.0, max(0.0, t / util))
                if self.campanas_lanzadas[nombre] < esperadas:
                    lanzar = True
            if lanzar and self.rng.random() < 0.3:
                if self.horizonte is not None and t + tip.DURACION_MAX[nombre] > self.horizonte:
                    continue
                n = tip.LANZADORES[nombre](self, t)
                if n:
                    self.fraude_planeado[nombre] += n
                    self.campanas_lanzadas[nombre] += 1

    def deriva(self, t: float) -> float:
        """Factor en [0,1] por el que se atenuan las senales de las campanas con el tiempo."""
        di = self.cfg.fraude.deriva_intensidad
        if di <= 0 or self.horizonte is None:
            return 1.0
        frac = min(1.0, max(0.0, t / self.horizonte))
        return 1.0 - di * frac

    # ------------------------------------------------------------------
    # bucle principal
    # ------------------------------------------------------------------
    def _procesar(self, intent: Dict[str, Any], t: float):
        tipo = intent["tipo"]
        if tipo == "llegada_organica":
            self._llegada_organica(t)
        elif tipo == "compra":
            self._emitir_compra(intent, t)
        elif tipo == "pago":
            self._emitir_pago(intent, t)
        elif tipo == "pago_directo":
            self._emitir_pago_directo(intent, t)
        elif tipo == "transferencia":
            self._emitir_transferencia(intent, t)
        elif tipo == "cambio_banco":
            self._emitir_cambio_banco(intent, t)
        elif tipo == "alta_entidad":
            self._alta_entidad(t)
        elif tipo == "cambio_banco_legitimo":
            self._cambio_banco_legitimo(t)
        elif tipo == "revision_limite_legitima":
            self._revision_limite_legitima(t)
        elif tipo == "accion":
            intent["fn"](self, t)
        else:
            raise ValueError("intencion desconocida: %s" % tipo)

    def avanzar_hasta(self, t_fin: float):
        """Procesa todas las intenciones con t <= t_fin (en orden). Devuelve
        (eventos_de_features, eventos_de_ground_truth_vencidos)."""
        self._buf = []
        limite = self.cfg.generador.max_eventos
        tope = False
        while self.cola and self.cola[0][0] <= t_fin:
            if limite and self.total_emitidos >= limite:
                tope = True
                break
            t, _, intent = heapq.heappop(self.cola)
            self.t = t
            self._procesar(intent, t)
        if not tope:
            self.t = max(self.t, t_fin)
        eventos = self._buf
        self._buf = []
        gts = []
        while self.cola_gt and self.cola_gt[0][0] <= self.t:
            gts.append(heapq.heappop(self.cola_gt)[2])
        return eventos, gts

    def proximo_fuera_horario(self, b: Empresa, t: float, max_horas: int = 11) -> float:
        """Primer instante >= t (dentro de max_horas) fuera del horario habitual del comprador."""
        for k in range(max_horas + 1):
            tt = t + k * HORA + self.rng.uniform(0, 3000)
            if not self._es_horario_habitual(b, tt):
                return tt
        return t

    def _preplanificar_bust_out(self):
        """T5 (bust-out) dura semanas o meses: para que haya explotaciones a lo largo de TODO el
        horizonte (y no solo al inicio) se planifican al arrancar, con la construccion retro-
        fechada (puede caer en el calentamiento, donde no se emite)."""
        fc = self.cfg.fraude
        peso = fc.mezcla.get("bust_out", 0.0)
        if peso <= 0 or self.horizonte is None:
            return
        horas = self.horizonte / HORA
        total_esperado = self.cfg.generador.eventos_por_hora_sim * horas
        n = max(fc.min_campanas_bust_out, int(round(fc.prevalencia * peso * total_esperado / 11.0)))
        t_ini = self.t
        for k in range(n):
            # explotaciones estratificadas en el horizonte (dejando 25 dias para completarse)
            lo = (self.horizonte - 25 * DIA) * k / n
            hi = (self.horizonte - 25 * DIA) * (k + 1) / n
            inicio = self.rng.uniform(max(lo, 14 * DIA), max(hi, 14 * DIA + 1))
            n_f = tip.lanzar_t5(self, t_ini, exploit_start=inicio)
            self.fraude_planeado["bust_out"] += n_f
            self.campanas_lanzadas["bust_out"] += 1

    def calentar(self):
        """Simula el periodo previo (sin emitir eventos ni etiquetas) para que el grafo,
        las relaciones y las cuentas tengan historia al comenzar la emision."""
        self._preplanificar_bust_out()
        self.emitiendo = False
        self.fraude_activo = False
        self.avanzar_hasta(0.0)
        self.t = 0.0
        self.emitiendo = True
        self.fraude_activo = True

    def drenar_etiquetas(self):
        """Devuelve TODAS las etiquetas pendientes (modo offline, al terminar)."""
        out = []
        while self.cola_gt:
            out.append(heapq.heappop(self.cola_gt)[2])
        return out

    def estadisticas(self) -> Dict[str, Any]:
        return {
            "eventos_emitidos": self.total_emitidos,
            "fraude_real_por_tipologia": dict(self.fraude_emitido),
            "fraude_real_total": sum(self.fraude_emitido.values()),
            "campanas_lanzadas": dict(self.campanas_lanzadas),
            "empresas": len(self.empresas),
            "cuentas": len(self.cuentas),
            "aristas": len(self.grafo.rel),
        }
