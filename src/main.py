"""Generador de eventos sinteticos B2B v2 (por tipologias).

Modos:
  - EN VIVO (por defecto): avanza el reloj simulado a RELOJ_ESCALA segundos simulados por
    segundo real y publica a Event Hubs (DRY_RUN=false) o a JSONL local (DRY_RUN=true).
  - OFFLINE (--offline): genera el horizonte completo sin esperar y deja el dataset ordenado
    por tiempo en --salida. Es el modo para validar el generador y para experimentos
    reproducibles (misma --seed => mismo dataset).

Ejemplos:
    python -m src.main --offline --dias 182 --seed 42 --salida output_s42
    python -m src.main                    # en vivo, segun .env
"""
import argparse
import asyncio
import copy
import hashlib
import json
import os
import signal
import time

from src.config import SCHEMA_VERSION, config, TIPOLOGIAS
from src.producer.eventhub_producer import ConsolePublisher, create_publisher
from src.schema.event_split import hay_fuga_de_etiqueta
from src.schema.validator import validar_evento_features, validar_evento_ground_truth
from src.simulador.motor import Simulador
from src.simulador.reloj import DIA


def huella_config(cfg) -> str:
    def _d(o):
        return o.__dict__ if hasattr(o, "__dict__") else str(o)
    texto = json.dumps(cfg, default=lambda o: {k: _d(v) if hasattr(v, "__dict__") else v for k, v in o.__dict__.items()},
                       sort_keys=True)
    return hashlib.sha256(texto.encode()).hexdigest()[:16]


def _validar_lote(features, gts, strict: bool):
    ok_f, ok_g = [], []
    for ev in features:
        if hay_fuga_de_etiqueta(ev):
            raise RuntimeError(f"Fuga de etiqueta en features: {ev['event_id']}")
        r = validar_evento_features(ev)
        if r.valid:
            ok_f.append(ev)
        elif strict:
            raise RuntimeError(f"Evento {ev['event_id']} invalido: {r.errors}")
        else:
            print(f"Evento {ev['event_id']} no paso el schema, descartado: {r.errors}")
    for g in gts:
        r = validar_evento_ground_truth(g)
        if r.valid:
            ok_g.append(g)
        elif strict:
            raise RuntimeError(f"Ground truth {g['event_id']} invalido: {r.errors}")
        else:
            print(f"Ground truth {g['event_id']} no paso el schema, descartado: {r.errors}")
    return ok_f, ok_g


def escribir_manifiesto(sim: Simulador, directorio: str, seed: int, extra=None):
    cfg = sim.cfg
    m = {
        "schema_version": SCHEMA_VERSION,
        "seed": seed,
        "huella_config": huella_config(cfg),
        "config": json.loads(json.dumps(cfg, default=lambda o: o.__dict__)),
        "estadisticas": sim.estadisticas(),
    }
    if extra:
        m.update(extra)
    os.makedirs(directorio, exist_ok=True)
    with open(os.path.join(directorio, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False, indent=2)


async def correr_offline(dias: float, seed: int, salida: str, validar: bool = True) -> Simulador:
    cfg = copy.deepcopy(config)
    cfg.reloj.horizonte_dias = dias
    cfg.generador.seed = seed
    sim = Simulador(cfg, seed=seed)
    pub = ConsolePublisher(output_dir=salida, verbose=False)
    t0 = time.time()
    sim.calentar()
    horizonte = dias * DIA
    paso = DIA
    t = 0.0
    while t < horizonte:
        t = min(horizonte, t + paso)
        features, gts = sim.avanzar_hasta(t)
        if validar:
            features, gts = _validar_lote(features, gts, strict=True)
        await pub.publish_batch(features, "features")
        await pub.publish_batch(gts, "ground_truth")
        if cfg.generador.max_eventos and sim.total_emitidos >= cfg.generador.max_eventos:
            break
    resto = sim.drenar_etiquetas()
    if validar:
        _, resto = _validar_lote([], resto, strict=True)
    await pub.publish_batch(resto, "ground_truth")
    await pub.close()
    escribir_manifiesto(sim, salida, seed, {"segundos_reales": round(time.time() - t0, 2),
                                            "modo": "offline", "dias_simulados": dias})
    return sim


async def _publicar(publisher, eventos, destino, conteo):
    """Fase 5 (5.5): antes un error de publish_batch se imprimia y el lote se perdia sin que
    el resumen final lo restara. Ahora los reintentos y el registro de no enviados viven en
    el publisher; aqui se acumula el conteo y, si aun asi salta un error no previsto, el lote
    entero va al registro (puede haber salido en parte: se anota como tal)."""
    if not eventos:
        return
    try:
        res = await publisher.publish_batch(eventos, destino)
        conteo[destino]["publicados"] += res.enviados
        conteo[destino]["no_enviados"] += res.no_enviados
    except Exception as err:
        print(f"Error no previsto publicando {len(eventos)} eventos a {destino} "
              f"(van al registro de no enviados; pueden haber salido en parte): {err}")
        publisher.registro.registrar(eventos, destino, err, intentos=0)
        conteo[destino]["no_enviados"] += len(eventos)


async def correr_en_vivo():
    cfg = config
    print("== Generador de eventos sinteticos B2B v2 (por tipologias) ==")
    print(f"Modo: {'DRY_RUN (local, sin Azure)' if cfg.dry_run else 'PRODUCCION (Event Hub real)'}")
    print(f"Seed={cfg.generador.seed} | prevalencia={cfg.fraude.prevalencia} | "
          f"reloj x{cfg.reloj.escala:g} | horizonte={cfg.reloj.horizonte_dias} dias")
    sim = Simulador(cfg)
    print(f"Mundo: {len(sim.empresas)} empresas | {len(sim.hibridas)} hibridas | "
          f"{len(sim.intermediarios)} intermediarios | calentando {cfg.reloj.warmup_dias} dias simulados...")
    sim.calentar()
    publisher = create_publisher()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    paso_sim = cfg.reloj.intervalo_seg * cfg.reloj.escala
    horizonte = cfg.reloj.horizonte_dias * DIA if cfg.reloj.horizonte_dias > 0 else None
    t = 0.0
    conteo = {d: {"publicados": 0, "no_enviados": 0} for d in ("features", "ground_truth")}
    try:
        while not stop.is_set():
            t += paso_sim
            if horizonte is not None and t > horizonte:
                print("Horizonte simulado alcanzado.")
                break
            features, gts = sim.avanzar_hasta(t)
            features, gts = _validar_lote(features, gts, strict=False)
            await _publicar(publisher, features, "features", conteo)
            await _publicar(publisher, gts, "ground_truth", conteo)
            if cfg.generador.max_eventos and sim.total_emitidos >= cfg.generador.max_eventos:
                print("MAX_EVENTOS alcanzado.")
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg.reloj.intervalo_seg)
            except asyncio.TimeoutError:
                pass
    finally:
        # Fase 1 (docs/roadmap_correccion.md, D4): el modo offline ya vaciaba cola_gt al
        # terminar (drenar_etiquetas); el modo en vivo no lo hacia, asi que toda etiqueta
        # de ground truth que todavia estuviera esperando su timestamp_etiqueta (por
        # ejemplo, las de bust_out, que tardan 30-90 dias simulados) se perdia en silencio
        # al detener el generador. Ahora se vacia y publica igual que el modo offline.
        resto = sim.drenar_etiquetas()
        if resto:
            _, resto = _validar_lote([], resto, strict=False)
            await _publicar(publisher, resto, "ground_truth", conteo)
        print(f"\nCerrando conexion... ({len(resto)} etiquetas pendientes drenadas)")
        print(json.dumps(sim.estadisticas(), ensure_ascii=False))
        # Linea aparte (no dentro de estadisticas()): el resumen del simulador debe seguir
        # siendo identico al de la replica local en DRY_RUN (CLAUDE.md 10.4).
        print("Publicacion: " + json.dumps({
            **conteo, "archivo_no_enviados": publisher.ruta_no_enviados}, ensure_ascii=False))
        if any(c["no_enviados"] for c in conteo.values()):
            print("ATENCION: hubo eventos NO enviados; ver el archivo de arriba antes de verificar la corrida.")
        await publisher.close()


def _args():
    p = argparse.ArgumentParser()
    p.add_argument("--offline", action="store_true", help="genera el horizonte completo sin esperar")
    p.add_argument("--dias", type=float, default=None, help="dias simulados (offline)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--salida", default=None, help="carpeta de salida (offline)")
    p.add_argument("--sin-validar", action="store_true", help="omite la validacion JSON Schema (offline)")
    return p.parse_args()


if __name__ == "__main__":
    a = _args()
    if a.offline:
        dias = a.dias if a.dias is not None else (config.reloj.horizonte_dias or 182.0)
        seed = a.seed if a.seed is not None else config.generador.seed
        salida = a.salida or os.path.join(config.output_dir, f"offline_s{seed}")
        sim = asyncio.run(correr_offline(dias, seed, salida, validar=not a.sin_validar))
        print(json.dumps(sim.estadisticas(), ensure_ascii=False, indent=2))
    else:
        # En Windows, add_signal_handler no existe (NotImplementedError, ver correr_en_vivo):
        # Ctrl+C llega como KeyboardInterrupt comun, no via el asyncio.Event `stop`. El
        # try/finally de correr_en_vivo ya corre igual (incluye el drenaje de etiquetas), pero
        # sin esto el proceso termina con un traceback en vez de un cierre explicado.
        try:
            asyncio.run(correr_en_vivo())
        except KeyboardInterrupt:
            print("\nInterrumpido por el usuario (Ctrl+C).")
