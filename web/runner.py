"""Control del generador en vivo desde la web: iniciar / detener a voluntad.

Reutiliza sin cambios el simulador, la validacion y los publishers de `src/`. El bucle replica
`src.main.correr_en_vivo`, pero la parada llega por un asyncio.Event propio (boton) en vez de
una senal del sistema, y el progreso se expone en `resumen()`.
Una sola corrida a la vez: el simulador vive en memoria de este proceso."""
import asyncio
import copy
import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from collections import deque
from typing import Any, Deque, Dict, List, Optional

from src.config import config
from src.main import _publicar, _validar_lote
from src.producer.eventhub_producer import EventHubPublisher, Publisher, ResultadoPublicacion
from src.simulador.motor import Simulador
from src.simulador.reloj import DIA

# Holgura entre el fin de una corrida y el inicio simulado de la siguiente.
MARGEN_ENTRE_CORRIDAS = timedelta(days=1)
RUTA_ESTADO = os.path.join(config.output_dir, "estado_reloj.json")


def _parse(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)


def _iso(d: datetime) -> str:
    return d.isoformat(timespec="seconds").replace("+00:00", "Z")


def _leer_fin_anterior() -> Optional[datetime]:
    try:
        with open(RUTA_ESTADO, encoding="utf-8") as f:
            return _parse(json.load(f)["fin_simulado"])
    except (OSError, ValueError, KeyError):
        return None


def _guardar_fin(fin: datetime) -> None:
    try:
        os.makedirs(os.path.dirname(RUTA_ESTADO) or ".", exist_ok=True)
        with open(RUTA_ESTADO, "w", encoding="utf-8") as f:
            json.dump({"fin_simulado": _iso(fin)}, f)
    except OSError:
        pass  # disco efimero o de solo lectura: queda solo el valor en memoria


class PublisherMemoria(Publisher):
    """Modo prueba (sin Azure, sin disco): cuenta los eventos y guarda los ultimos para la vista previa."""

    def __init__(self, recientes: int = 25):
        self.recientes: Dict[str, Deque[Dict[str, Any]]] = {
            "features": deque(maxlen=recientes), "ground_truth": deque(maxlen=recientes)}

    async def publish_batch(self, events: List[Dict[str, Any]], destino) -> ResultadoPublicacion:
        self.recientes[destino].extend(events)
        return ResultadoPublicacion(enviados=len(events))

    async def close(self) -> None:
        pass


class Corrida:
    def __init__(self):
        self._reiniciar()
        self._tarea: Optional[asyncio.Task] = None
        self._fin_simulado: Optional[datetime] = _leer_fin_anterior()

    def _reiniciar(self) -> None:
        self.estado = "inactivo"  # inactivo|calentando|corriendo|deteniendo|finalizado|error
        self.mensaje = ""
        self.modo: Optional[str] = None
        self.params: Dict[str, Any] = {}
        self.inicio_real: Optional[float] = None
        self.fin_real: Optional[float] = None
        self.t_sim = 0.0
        self.estadisticas: Dict[str, Any] = {}
        self.conteo = {d: {"publicados": 0, "no_enviados": 0} for d in ("features", "ground_truth")}
        self.archivo_no_enviados: Optional[str] = None
        self.publisher: Optional[Publisher] = None
        self._stop = asyncio.Event()

    @property
    def activa(self) -> bool:
        return self._tarea is not None and not self._tarea.done()

    def iniciar(self, *, dry_run: bool, seed: Optional[int], horizonte_dias: Optional[float],
                reloj_escala: Optional[float], sim_start: Optional[str] = None) -> None:
        if self.activa:
            raise RuntimeError("Ya hay una corrida en curso; detenerla antes de iniciar otra.")
        cfg = copy.deepcopy(config)
        cfg.dry_run = dry_run
        # Sin semilla explicita cada inicio es un mundo nuevo (event_id distintos); con semilla
        # explicita el dataset se repite exacto. La semilla usada queda en `parametros`.
        cfg.generador.seed = seed if seed is not None else secrets.randbelow(2**31)
        if horizonte_dias is not None:
            cfg.reloj.horizonte_dias = horizonte_dias
        if reloj_escala is not None:
            cfg.reloj.escala = reloj_escala
        # Cada inicio arranca el tiempo simulado DESPUES del fin de la corrida anterior: el reloj
        # nunca retrocede (la ventana/watermark de CAL y la guardia as-of de CADS trabajan sobre
        # el timestamp simulado). Primera corrida: SIM_START del entorno, o `sim_start` explicito.
        if sim_start:
            inicio = _parse(sim_start)
        elif self._fin_simulado is not None:
            inicio = self._fin_simulado + MARGEN_ENTRE_CORRIDAS
        else:
            inicio = _parse(cfg.reloj.inicio_iso)
        cfg.reloj.inicio_iso = _iso(inicio)
        self._inicio_sim = inicio
        self._reiniciar()
        self.modo = "prueba (sin Azure)" if dry_run else "Event Hub"
        self.params = {"seed": cfg.generador.seed, "horizonte_dias": cfg.reloj.horizonte_dias,
                       "reloj_escala": cfg.reloj.escala, "intervalo_seg": cfg.reloj.intervalo_seg,
                       "inicio_simulado": cfg.reloj.inicio_iso, "prevalencia": cfg.fraude.prevalencia, "warmup_dias": cfg.reloj.warmup_dias}
        self.inicio_real = time.time()
        self.estado = "calentando"
        self._tarea = asyncio.create_task(self._correr(cfg, dry_run))

    def detener(self) -> None:
        if self.activa:
            self.estado = "deteniendo"
            self._stop.set()

    async def esperar_fin(self) -> None:
        if self._tarea is not None:
            await asyncio.gather(self._tarea, return_exceptions=True)

    async def _correr(self, cfg, dry_run: bool) -> None:
        publisher: Optional[Publisher] = None
        sim: Optional[Simulador] = None
        try:
            publisher = PublisherMemoria() if dry_run else EventHubPublisher()
            self.publisher = publisher
            # Construir el mundo y calentar es CPU puro (segundos): fuera del bucle de eventos
            # para que la API siga respondiendo y /detener funcione incluso durante el calentamiento.
            sim = await asyncio.to_thread(Simulador, cfg)
            await asyncio.to_thread(sim.calentar)
            self.estadisticas = sim.estadisticas()
            if not self._stop.is_set():
                self.estado = "corriendo"
            paso_sim = cfg.reloj.intervalo_seg * cfg.reloj.escala
            horizonte = cfg.reloj.horizonte_dias * DIA if cfg.reloj.horizonte_dias > 0 else None
            t = 0.0
            while not self._stop.is_set():
                t += paso_sim
                if horizonte is not None and t > horizonte:
                    self.mensaje = "Horizonte simulado alcanzado."
                    break
                features, gts = sim.avanzar_hasta(t)
                features, gts = _validar_lote(features, gts, strict=False)
                await _publicar(publisher, features, "features", self.conteo)
                await _publicar(publisher, gts, "ground_truth", self.conteo)
                self.t_sim = t
                self.estadisticas = sim.estadisticas()
                if cfg.generador.max_eventos and sim.total_emitidos >= cfg.generador.max_eventos:
                    self.mensaje = "MAX_EVENTOS alcanzado."
                    break
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=cfg.reloj.intervalo_seg)
                except asyncio.TimeoutError:
                    pass
            if self._stop.is_set():
                self.mensaje = "Detenida por el usuario."
        except Exception as err:  # noqa: BLE001 -- se informa en la API, no se oculta
            self.estado = "error"
            self.mensaje = f"{type(err).__name__}: {err}"
        finally:
            # Igual que el modo en vivo original: las etiquetas aun pendientes (p. ej. bust_out) se
            # publican al terminar para no perderlas.
            try:
                if sim is not None and publisher is not None:
                    resto = sim.drenar_etiquetas()
                    if resto:
                        _, resto = _validar_lote([], resto, strict=False)
                        await _publicar(publisher, resto, "ground_truth", self.conteo)
                    self.estadisticas = sim.estadisticas()
                if publisher is not None:
                    self.archivo_no_enviados = publisher.ruta_no_enviados
                    await publisher.close()
            except Exception as err:  # noqa: BLE001
                self.mensaje = (self.mensaje + " | " if self.mensaje else "") + f"Error al cerrar: {err}"
            if sim is not None:
                self._fin_simulado = self._inicio_sim + timedelta(seconds=self.t_sim)
                _guardar_fin(self._fin_simulado)
            self.fin_real = time.time()
            if self.estado != "error":
                self.estado = "finalizado"

    def resumen(self) -> Dict[str, Any]:
        fin = self.fin_real or time.time()
        return {
            "estado": self.estado, "mensaje": self.mensaje, "modo": self.modo, "parametros": self.params,
            "segundos_reales": round(fin - self.inicio_real, 1) if self.inicio_real else 0,
            "dias_simulados": round(self.t_sim / DIA, 2),
            "estadisticas": self.estadisticas, "publicacion": self.conteo,
            "archivo_no_enviados": self.archivo_no_enviados,
        }

    def recientes(self) -> Dict[str, list]:
        if isinstance(self.publisher, PublisherMemoria):
            return {k: list(v)[-10:] for k, v in self.publisher.recientes.items()}
        return {}
