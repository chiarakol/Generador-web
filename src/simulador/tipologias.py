"""Campanas de fraude (T1..T8). Cada lanzador programa en el motor una secuencia de
intenciones con estado (una campana tiene identificador, fases y duracion) y devuelve
cuantos eventos fraudulentos planifico. Ningun lanzador escribe etiquetas: la etiqueta
nace del campo 'meta' de la intencion y se publica aparte (ground truth).

Los parametros son hipotesis de diseno (documento de especificacion, seccion 7);
ningun valor esta calibrado con datos reales."""
import math
from typing import Any, Dict, List

from src.simulador.entidades import CATEGORIAS, CATEGORIAS_DIFICILES, FACTOR_CATEGORIA, PAISES, REDES
from src.simulador.reloj import DIA, HORA

# Dias minimos que deben quedar antes del horizonte para lanzar una campana (con menos, la
# campana quedaria casi toda fuera de la ventana observada). Las campanas pueden terminar
# despues del horizonte: sus eventos posteriores simplemente no se observan (censura).
DURACION_MAX = {
    "toma_de_cuenta": 1 * DIA,
    "proveedor_fantasma": 10 * DIA,
    "anillo_colusion": 10 * DIA,
    "fraccionamiento_ordenes": 6 * DIA,
    "bust_out": 25 * DIA,
    "factura_inflada_duplicada": 3 * DIA,
    "bec_datos_bancarios": 15 * DIA,
    "cadena_fondos_paso": 3 * DIA,
}

# Fase 4 (docs/roadmap_correccion.md, 3.2(a), B4-T5): factor del aumento de linea que pide el
# bust-out a mitad de la construccion. Antes era x2,0 fijo -- un factor constante que ningun
# comprador legitimo producia (ni siquiera cambiaban de limite). Ahora es un rango contenido en
# el de las revisiones organicas (motor.REVISION_FACTOR_AUMENTO). HIPOTESIS DE DISENO.
FACTOR_SUBIDA_LIMITE_T5 = (1.5, 2.5)

# Fase 1 (docs/roadmap_correccion.md, A5): multiplicador de RUIDO_ETIQUETA_FN cuando
# RUIDO_ETIQUETA_SISTEMATICO=true (config.py). >1 = un analista real la pasa por alto mas
# seguido que el promedio (tipologia sutil, dificil de verificar); <1 = la detecta mas facil
# (tipologia obvia). HIPOTESIS DE DISENO, no calibrado con datos reales -- igual que
# DURACION_MAX arriba, documentar como tal si se usa en resultados.
DIFICULTAD_FN = {
    "toma_de_cuenta": 0.6,            # dispositivo/red anomalos: relativamente visible
    "proveedor_fantasma": 1.8,        # requiere auditar al proveedor, no solo la transaccion
    "anillo_colusion": 1.4,           # exige ver el grafo, no una transaccion aislada
    "fraccionamiento_ordenes": 0.7,   # suma simple contra un limite conocido
    "bust_out": 1.0,                  # la fase de explotacion es visible; la de construccion no aplica (no es fraude)
    "factura_inflada_duplicada": 1.6, # exige comparar contra la OC/factura anterior
    "bec_datos_bancarios": 1.3,       # exige verificar el canal de la solicitud
    "cadena_fondos_paso": 1.1,        # exige correlacionar varias cuentas de paso
}


def _meta(sim, tipologia: str, campana: str, fase: str, variante=None, fraude: bool = True) -> Dict[str, Any]:
    return {"tipologia": tipologia, "variante": variante, "campana": campana, "fase": fase, "fraude": fraude}


def _tiempos(rng, t0: float, t1: float, n: int) -> List[float]:
    return sorted([t0] + [rng.uniform(t0, t1) for _ in range(n - 1)])


# ----------------------------------------------------------------------
# T1 Toma de cuenta (Account Takeover)
# ----------------------------------------------------------------------
def lanzar_t1(sim, t: float) -> int:
    rng, fc = sim.rng, sim.cfg.fraude
    d = sim.deriva(t)
    camp = sim.nueva_campana("T1")
    # las cuentas con autenticacion reforzada son victimas menos probables
    b = sim.elegir_comprador(peso_fn=lambda e: 0.4 if e.mfa else 1.0)
    variante = "credencial_robada" if rng.random() < fc.t1_p_dispositivo_nuevo else "sesion_secuestrada"
    if rng.random() < fc.t1_p_fuera_horario * d:
        t = sim.proximo_fuera_horario(b, t)
    cambio_contacto = rng.random() < (fc.t1_p_cambio_contacto if variante == "credencial_robada" else 0.15) * d
    if cambio_contacto:
        b.t_cambio_contacto = t - rng.uniform(0.05, 3.0) * DIA
    n = rng.randint(2, 6)
    meta = _meta(sim, "toma_de_cuenta", camp, "ataque", variante)
    tc = t
    for k in range(n):
        if variante == "credencial_robada":
            sesion = {
                "dispositivo_reconocido": False,
                "red_conexion": rng.choices(REDES, [5, 25, 35, 10, 25])[0],
                "autenticacion_reforzada": rng.random() < 0.10,
                "cambio_contacto_reciente": cambio_contacto,
            }
        else:
            sesion = {
                "dispositivo_reconocido": True,
                "autenticacion_reforzada": b.mfa,
                "cambio_contacto_reciente": cambio_contacto,
            }
        sesion["canal"] = rng.choices(["web", "app_movil"], [0.7, 0.3])[0]
        # proveedores y categorias poco habituales para el comprador
        otras = [c for c in CATEGORIAS if c not in b.cat_pref]
        cat = rng.choice(otras) if (otras and rng.random() < 0.7) else rng.choice(b.cat_pref)
        sid, cat = sim.elegir_proveedor_nuevo(b, categoria=cat)
        monto = sim._monto_organico(b, cat) * rng.uniform(0.9, 2.2)
        sim.programar(tc, {"tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat,
                           "monto": monto, "metodo": rng.choice(["contado", "transferencia_bancaria"]),
                           "programar_pago": False, "sesion": sesion, "meta": meta})
        tc += rng.expovariate(1.0 / (40 * 60.0))
    return n


# ----------------------------------------------------------------------
# T2 Proveedor fantasma (empresa pantalla)
# ----------------------------------------------------------------------
def lanzar_t2(sim, t: float) -> int:
    rng, fc = sim.rng, sim.cfg.fraude
    d = sim.deriva(t)
    camp = sim.nueva_campana("T2")
    complicidad = rng.random() < fc.t2_p_complicidad
    variante = "con_complicidad_interna" if complicidad else "sin_complicidad"
    ghost = sim.crear_empresa("proveedor", alta=t - rng.uniform(5, 80) * DIA, organica=False)
    ghost.categorias = [rng.choice(CATEGORIAS_DIFICILES)]
    ghost.es_fantasma = True
    if rng.random() < fc.t2_p_pais_cuenta_distinto * d:
        # cuenta de cobro en otro pais (senal de la especificacion). La cuenta se acaba de crear y
        # todavia no tiene movimientos, asi que basta con fijar su pais. Va ANTES de las hermanas
        # para que hereden esta misma cuenta.
        sim.cuentas[ghost.cuenta].pais = rng.choice([x for x in PAISES if x != ghost.pais])
    if rng.random() < fc.t2_p_cuenta_compartida * d:
        # empresas pantalla hermanas: comparten cuenta, domicilio y representante
        # Fase 2 (B8): con 2 hermanas, proveedor.cuenta_compartida_n llega a 2 -- un nivel que
        # los grupos empresariales legitimos (_crear_grupos_legitimos) nunca alcanzan con su
        # tamano maximo actual (su techo real es 1). Eso volvia cuenta_compartida_n==2 una
        # marca pura de T2 (medido: 36/36). Tope bajado a 1 hermana, mismo techo que lo
        # legitimo puede producir.
        for _ in range(1):
            hermana = sim.crear_empresa("proveedor", alta=t - rng.uniform(5, 200) * DIA, organica=False)
            sim.cuenta_uso[hermana.cuenta] = max(0, sim.cuenta_uso.get(hermana.cuenta, 1) - 1)
            hermana.cuenta = ghost.cuenta
            sim.cuenta_uso[ghost.cuenta] += 1
            hermana.domicilio, hermana.representante = ghost.domicilio, ghost.representante
    compradores = []
    for _ in range(rng.choice([1, 1, 2])):
        compradores.append(sim.elegir_comprador())
    n_fact = rng.randint(3, 12)
    tiempos = _tiempos(rng, t, t + rng.uniform(7, 60) * DIA, n_fact)
    p_oc = 0.15 if complicidad else 0.85
    meta_f = _meta(sim, "proveedor_fantasma", camp, "facturacion", variante)
    meta_p = _meta(sim, "proveedor_fantasma", camp, "cobro", variante)
    for tk in tiempos:
        b = rng.choice(compradores)
        sim.programar(tk, {
            "tipo": "compra", "comprador": b.id, "proveedor": ghost.id, "categoria": ghost.categorias[0],
            "monto": b.limite_aprob * rng.uniform(0.55, 0.97),      # bajo el umbral de aprobacion
            "metodo": rng.choice(["credito_30", "credito_60"]),
            "oc": rng.random() < p_oc, "meta": meta_f, "pago_meta": meta_p})
    return 2 * n_fact


# ----------------------------------------------------------------------
# T3 Anillo de colusion y facturacion circular
# ----------------------------------------------------------------------
def lanzar_t3(sim, t: float) -> int:
    rng, fc = sim.rng, sim.cfg.fraude
    d = sim.deriva(t)
    camp = sim.nueva_campana("T3")
    # Fase 2 (docs/roadmap_correccion.md, B8 -- hallazgo surgido al subir MIN_CAMPANAS_TIPOLOGIA
    # en 2.3): con k=6 (el maximo que CYCLE_DETECTION_MAX_HOPS=6 puede detectar), TODA actividad
    # organica del dataset deja longitud_ciclo_detectado=6 reservada en exclusiva para anillo_
    # colusion -- la activad legitima nunca cierra un ciclo detectado de esa longitud (medido:
    # 31/31 eventos con longitud=6 eran fraude). En longitud 5 la actividad legitima SI aparece
    # con frecuencia (25 de 42 eventos), asi que deja de ser una marca pura. Tope bajado a 5.
    k = 2 if rng.random() < 0.20 else rng.randint(3, 5)
    variante = "colusion_directa" if k == 2 else "facturacion_circular_%d" % k
    miembros = []
    reclutados = set()  # ids de miembros reales ya existentes (complices) -- ver nota abajo
    for _ in range(k):
        # ~30 % de los miembros son empresas reales ya existentes (complices), el resto empresas pantalla
        candidatas = [h for h in sim.hibridas if not sim.empresas[h].es_intermediario
                      and sim.empresas[h] not in miembros]
        if candidatas and rng.random() < 0.30:
            nuevo = sim.empresas[rng.choice(candidatas)]
            reclutados.add(nuevo.id)
        else:
            nuevo = sim.crear_empresa("hibrida", alta=t - rng.uniform(20, 300) * DIA, organica=False)
        miembros.append(nuevo)
    # Fase 3 (docs/roadmap_correccion.md, B4, opcion B): domicilio/representante solo se
    # comparten entre miembros CREADOS por la campana (empresas pantalla). Un complice real
    # reclutado de sim.hibridas mantiene su propia identidad -- mutar su domicilio/representante
    # registrado sin ningun evento observable seria una fuga de estado (y tambien menos
    # realista: un complice real no cambia su domicilio legal para unirse a un anillo; la
    # colusion se ve en el comportamiento transaccional, no en un atributo fabricado).
    pantalla = [m for m in miembros if m.id not in reclutados]
    if len(pantalla) >= 2 and rng.random() < fc.t3_p_atributo_compartido * d:
        dom = pantalla[0].domicilio
        for m in pantalla:
            m.domicilio = dom
    for i in range(k):
        origen, destino = miembros[i], miembros[(i + 1) % k]
        if destino.id not in reclutados and rng.random() < 0.25 * d:
            destino.representante = origen.representante
    cat = rng.choice(CATEGORIAS_DIFICILES)
    loops = rng.randint(2, 4)
    m0 = sim._lognorm(sim.cfg.mundo.monto_mediana_base * 3.0 * FACTOR_CATEGORIA[cat], 0.5)
    meta = _meta(sim, "anillo_colusion", camp, "circulacion", variante)
    tc = t
    for _ in range(loops):
        for i in range(k):
            a, b = miembros[i], miembros[(i + 1) % k]
            sim.programar(tc, {
                "tipo": "compra", "comprador": a.id, "proveedor": b.id, "categoria": cat,
                "monto": m0 * rng.uniform(0.96, 1.04), "metodo": "transferencia_bancaria",
                "oc": rng.random() < 0.5, "programar_pago": False, "meta": meta})
            tc += rng.expovariate(1.0 / (rng.uniform(1.0, 3.0) * DIA))
    return k * loops


# ----------------------------------------------------------------------
# T4 Fraccionamiento de ordenes
# ----------------------------------------------------------------------
def lanzar_t4(sim, t: float) -> int:
    rng = sim.rng
    camp = sim.nueva_campana("T4")
    b = sim.elegir_comprador()
    if b.favoritos and rng.random() < 0.6:
        sid = rng.choice(b.favoritos)
        cat = sim._categoria_para(b, sim.empresas[sid])
    else:
        sid, cat = sim.elegir_proveedor_nuevo(b)
    n = rng.randint(2, 6)
    tiempos = _tiempos(rng, t, t + rng.uniform(1, 5) * DIA, n)
    meta = _meta(sim, "fraccionamiento_ordenes", camp, "fraccionamiento")
    for tk in tiempos:
        sim.programar(tk, {
            "tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat,
            "monto": b.limite_aprob * rng.uniform(0.80, 0.99), "oc": True,
            "meta": meta, "pago_meta": None})
    return n


# ----------------------------------------------------------------------
# T5 Bust-out (agotamiento de credito)
# ----------------------------------------------------------------------
def lanzar_t5(sim, t: float, exploit_start=None) -> int:
    """Con exploit_start=None (modo continuo) la construccion empieza en t y la explotacion
    ~4-12 semanas despues. Con exploit_start (campanas preplanificadas en horizonte finito) la
    construccion se ajusta para terminar en ese instante (puede empezar en el calentamiento)."""
    rng = sim.rng
    camp = sim.nueva_campana("T5")
    base = sim.cfg.mundo.monto_mediana_base
    b = sim.crear_empresa("comprador", alta=t - rng.uniform(10, 90) * DIA, organica=False,
                          tamano=sim._lognorm(1.0, 0.5))
    b.limite_credito = base * b.escala * rng.uniform(8.0, 14.0)
    b.p_atraso = 0.04
    semanas = rng.randint(4, 12)
    if exploit_start is not None:
        semanas = max(2, min(semanas, int((exploit_start - t) / (7 * DIA))))
        t = exploit_start - semanas * 7 * DIA
    t_fin_build = t + semanas * 7 * DIA
    proveedores = [sim.elegir_proveedor_nuevo(b)[0] for _ in range(rng.randint(2, 4))]
    meta_b = _meta(sim, "bust_out", camp, "construccion", "bust_out", fraude=False)
    # fase de construccion: pedidos pequenos pagados a tiempo, con limite creciente
    tc = t
    while tc < t_fin_build:
        sid = rng.choice(proveedores)
        cat = sim._categoria_para(b, sim.empresas[sid])
        sim.programar(tc, {"tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat,
                           "monto": sim._monto_organico(b, cat) * 0.35,
                           "metodo": rng.choice(["credito_30", "credito_60"]), "meta": meta_b})
        tc += rng.expovariate(1.0 / (3 * DIA))

    factor_subida = rng.uniform(*FACTOR_SUBIDA_LIMITE_T5)

    def _subir_limite(s, tt):
        b.limite_credito *= factor_subida

    def _empezar_atrasos(s, tt):
        b.p_atraso = 0.6

    sim.programar(t + 0.5 * semanas * 7 * DIA, {"tipo": "accion", "fn": _subir_limite})
    sim.programar(t + 0.75 * semanas * 7 * DIA, {"tipo": "accion", "fn": _empezar_atrasos})
    # fase de explotacion: se agota la linea con proveedores nuevos y establecidos
    n = rng.randint(6, 16)
    dur = rng.uniform(7, 21) * DIA
    limite_final = b.limite_credito * factor_subida
    meta_e = _meta(sim, "bust_out", camp, "explotacion", "bust_out")
    tiempos = _tiempos(rng, t_fin_build, t_fin_build + dur, n)
    for i, tk in enumerate(tiempos):
        if rng.random() < 0.5:
            sid, cat = sim.elegir_proveedor_nuevo(b)
        else:
            sid = rng.choice(proveedores)
            cat = sim._categoria_para(b, sim.empresas[sid])
        sim.programar(tk, {"tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat,
                           "monto": limite_final * rng.uniform(0.8, 1.3) / n, "metodo": "credito_90",
                           "programar_pago": False, "meta": meta_e})
    return n


# ----------------------------------------------------------------------
# T6 Factura inflada o duplicada
# ----------------------------------------------------------------------
def lanzar_t6(sim, t: float) -> int:
    rng = sim.rng
    camp = sim.nueva_campana("T6")
    b = sim.elegir_comprador()
    sid, cat = sim.elegir_proveedor_organico(b)
    monto_oc = sim._monto_organico(b, cat)
    if rng.random() < 0.55:
        variante = "inflada"
        delta = rng.uniform(0.08, 0.40)
        sim.programar(t, {"tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat,
                          "monto": monto_oc * (1 + delta), "monto_oc": monto_oc, "oc": True,
                          "meta": _meta(sim, "factura_inflada_duplicada", camp, "presentacion", variante)})
        return 2
    variante = "duplicada"
    ref: Dict[str, Any] = {}
    meta_o = _meta(sim, "factura_inflada_duplicada", camp, "original", variante, fraude=False)
    sim.programar(t, {"tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat,
                      "monto": monto_oc, "oc": True, "ref": ref, "meta": meta_o})
    sim.programar(t + rng.uniform(1, 30) * DIA, {
        "tipo": "compra", "comprador": b.id, "proveedor": sid, "categoria": cat, "monto": monto_oc,
        "oc": rng.random() < 0.7, "dup_de": ref,
        "meta": _meta(sim, "factura_inflada_duplicada", camp, "duplicado", variante)})
    return 2


# ----------------------------------------------------------------------
# T7 Suplantacion de datos bancarios del proveedor (BEC)
# ----------------------------------------------------------------------
def lanzar_t7(sim, t: float) -> int:
    rng, fc = sim.rng, sim.cfg.fraude
    d = sim.deriva(t)
    pend = sim.pagos_pendientes
    if len(sim._ids_pagos) > 2 * len(pend) + 100:
        sim._ids_pagos = list(pend.keys())
    objetivo = None
    for _ in range(60):
        if not sim._ids_pagos:
            return 0
        p = pend.get(rng.choice(sim._ids_pagos))
        if p is None or p.get("meta") is not None or p.get("cuenta_destino") is not None:
            continue
        if t + 0.5 * DIA <= p["_t"] <= t + 14 * DIA:
            objetivo = p
            break
    if objetivo is None:
        return 0
    camp = sim.nueva_campana("T7")
    b, s = sim.empresas[objetivo["comprador"]], sim.empresas[objetivo["proveedor"]]
    # otros pagos pendientes del mismo par dentro de la ventana
    hijack = [objetivo]
    for p in list(pend.values()):
        if len(hijack) >= 3:
            break
        if (p is not objetivo and p["comprador"] == b.id and p["proveedor"] == s.id and p.get("meta") is None
                and p.get("cuenta_destino") is None and t + 0.5 * DIA <= p["_t"] <= t + 14 * DIA):
            hijack.append(p)
    otros = [x for x in PAISES if x != s.pais]
    pais = s.pais if rng.random() > fc.t7_p_pais_distinto * d else rng.choice(otros)
    atk = sim.crear_cuenta(pais, saldo=rng.uniform(0, 500), ingreso_dia=0.0, t=t)
    t_cambio = t
    if rng.random() < fc.t7_p_fuera_horario * d:
        t_cambio = min(sim.proximo_fuera_horario(b, t), min(p["_t"] for p in hijack) - 0.2 * DIA)
        t_cambio = max(t, t_cambio)
    meta_c = _meta(sim, "bec_datos_bancarios", camp, "solicitud", "cambio_cuenta")
    meta_p = _meta(sim, "bec_datos_bancarios", camp, "desvio", "cambio_cuenta")
    sim.programar(t_cambio, {
        "tipo": "cambio_banco", "comprador": b.id, "proveedor": s.id, "cuenta_nueva": atk.id, "meta": meta_c,
        "sesion": {"canal": rng.choices(["email", "portal_proveedor", "llamada"], [70, 15, 15])[0],
                   "autenticacion_reforzada": rng.random() < fc.t7_p_verificacion_alterna + (1.0 - d) * 0.4,
                   "dispositivo_reconocido": rng.random() > 0.8,
                   "red_conexion": rng.choices(REDES, [10, 30, 20, 5, 35])[0]}})
    for p in hijack:
        pend.pop(p["_id"], None)
        p["cuenta_destino"] = atk.id
        p["cambio_t"] = t_cambio
        p["meta"] = meta_p
    return 1 + len(hijack)


# ----------------------------------------------------------------------
# T8 Cadena de fondos de paso (pass-through)
# ----------------------------------------------------------------------
def lanzar_t8(sim, t: float) -> int:
    rng = sim.rng
    camp = sim.nueva_campana("T8")
    saltos = rng.randint(2, 4)
    fan_in = rng.randint(3, 8)
    niveles: List[List[Any]] = []
    tamanos = [1] + [rng.choice([1, 2]) for _ in range(saltos - 1)] + [1]
    for tam in tamanos:
        nivel = []
        for _ in range(tam):
            usados = [x.id for lv in niveles for x in lv] + [x.id for x in nivel]
            candidatas = [h for h in sim.hibridas if not sim.empresas[h].es_intermediario and h not in usados]
            if candidatas and rng.random() < 0.30:
                e = sim.empresas[rng.choice(candidatas)]          # empresa real usada como cuenta de paso
            else:
                e = sim.crear_empresa("hibrida", alta=t - rng.uniform(5, 120) * DIA, organica=False,
                                      tamano=sim._lognorm(0.5, 0.4))
                c = sim.cuentas[e.cuenta]
                c.saldo, c.ingreso_dia = rng.uniform(0, 200), 0.0   # cuentas de paso: sin ingresos propios
            nivel.append(e)
        niveles.append(nivel)
    cat = rng.choice(CATEGORIAS)
    meta = _meta(sim, "cadena_fondos_paso", camp, "fan_in", "cadena_%d_saltos" % saltos)
    meta_t = _meta(sim, "cadena_fondos_paso", camp, "traspaso", "cadena_%d_saltos" % saltos)
    # fan-in: varios pagadores (empresas controladas) depositan en el colector
    recibido = {niveles[0][0].id: 0.0}
    t_in = _tiempos(rng, t, t + rng.uniform(1, 12) * HORA, fan_in)
    for tk in t_in:
        if rng.random() < 0.5:
            pagador = sim.elegir_comprador()                      # comprador real usado como mula
        else:
            pagador = sim.crear_empresa("comprador", alta=t - rng.uniform(5, 150) * DIA, organica=False,
                                        tamano=sim._lognorm(0.6, 0.4))
        monto = sim._lognorm(sim.cfg.mundo.monto_mediana_base * 0.8, 0.5)
        recibido[niveles[0][0].id] += monto
        sim.programar(tk, {"tipo": "pago_directo", "comprador": pagador.id, "proveedor": niveles[0][0].id,
                           "categoria": cat, "monto": monto, "meta": meta})
    # traspasos entre niveles
    n_eventos = fan_in
    t_base = t_in[-1]
    for l in range(len(niveles) - 1):
        siguientes = niveles[l + 1]
        for e in niveles[l]:
            ret = rng.uniform(0.02, 0.15)
            salida_total = recibido.get(e.id, 0.0) * (1.0 - ret)
            for dest in siguientes:
                monto = salida_total / len(siguientes)
                recibido[dest.id] = recibido.get(dest.id, 0.0) + monto
                t_base_e = t_base + rng.uniform(0.5, 10.0) * HORA
                sim.programar(t_base_e, {"tipo": "transferencia", "origen": e.id, "destino": dest.id,
                                         "monto": monto, "categoria": cat, "meta": meta_t})
                n_eventos += 1
        t_base += 10.0 * HORA + 1.0
    return n_eventos


LANZADORES = {
    "toma_de_cuenta": lanzar_t1,
    "proveedor_fantasma": lanzar_t2,
    "anillo_colusion": lanzar_t3,
    "fraccionamiento_ordenes": lanzar_t4,
    "bust_out": lanzar_t5,
    "factura_inflada_duplicada": lanzar_t6,
    "bec_datos_bancarios": lanzar_t7,
    "cadena_fondos_paso": lanzar_t8,
}
