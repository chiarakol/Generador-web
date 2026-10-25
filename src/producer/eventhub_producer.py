import asyncio
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Tuple

from src.config import SCHEMA_VERSION, config, productor

Destino = Literal["features", "ground_truth"]


@dataclass
class ResultadoPublicacion:
    """Cuantos eventos de un publish_batch llegaron al destino y cuantos quedaron en el
    registro de no enviados (5.5: antes un error descartaba el lote y nadie lo contaba)."""
    enviados: int = 0
    no_enviados: int = 0


class RegistroNoEnviados:
    """Fase 5 (docs/roadmap_correccion.md, 5.5): todo evento que no se pudo publicar tras los
    reintentos se escribe aqui (JSON Lines, una linea por evento, con destino, error e
    intentos) en vez de perderse en silencio. El archivo se crea solo si hace falta."""

    def __init__(self, directorio: str):
        self.directorio = directorio
        self.ruta: Optional[str] = None
        self.total = {"features": 0, "ground_truth": 0}
        self._f = None

    def registrar(self, eventos: List[Dict[str, Any]], destino: Destino, error: BaseException,
                  intentos: int) -> None:
        if not eventos:
            return
        if self._f is None:
            os.makedirs(self.directorio, exist_ok=True)
            sello = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self.ruta = os.path.join(self.directorio, f"no_enviados_{sello}.jsonl")
            self._f = open(self.ruta, "a", encoding="utf-8")
        ahora = datetime.now(timezone.utc).isoformat()
        for evt in eventos:
            self._f.write(json.dumps({
                "destino": destino, "error": f"{type(error).__name__}: {error}",
                "intentos": intentos, "registrado_en": ahora, "evento": evt,
            }, ensure_ascii=False) + "\n")
        self._f.flush()
        self.total[destino] += len(eventos)

    def close(self) -> None:
        if self._f is not None:
            self._f.close()
            self._f = None


def es_reintentable(err: BaseException) -> bool:
    """Errores que no se arreglan reintentando: credenciales (AuthenticationError), evento
    malformado o demasiado grande (EventDataError, ValueError). El resto (red, timeout,
    servicio ocupado, conexion perdida) se reintenta."""
    if isinstance(err, (ValueError, TypeError)):
        return False
    try:
        from azure.eventhub.exceptions import AuthenticationError, EventDataError
    except ImportError:
        return True
    return not isinstance(err, (AuthenticationError, EventDataError))


async def con_reintentos(intento: Callable[[], Awaitable[Any]], intentos: int,
                         espera_inicial: float, espera_max: float, descripcion: str,
                         dormir: Callable[[float], Awaitable[None]] = asyncio.sleep
                         ) -> Tuple[Any, Optional[BaseException], int]:
    """Ejecuta intento() hasta `intentos` veces, con espera exponencial (espera_inicial,
    x2, ... con tope espera_max). Devuelve (resultado, None, n) si sale bien, o
    (None, ultimo_error, n) si se agotan los intentos o el error no es reintentable."""
    espera = espera_inicial
    for n in range(1, intentos + 1):
        try:
            return await intento(), None, n
        except Exception as err:  # noqa: BLE001 -- se clasifica abajo y nunca se oculta
            if n == intentos or not es_reintentable(err):
                print(f"[productor] {descripcion}: fallo definitivo en el intento {n}/{intentos}: "
                      f"{type(err).__name__}: {err}")
                return None, err, n
            print(f"[productor] {descripcion}: intento {n}/{intentos} fallo "
                  f"({type(err).__name__}: {err}); reintento en {espera:g} s")
            await dormir(espera)
            espera = min(espera * 2, espera_max)
    raise AssertionError("inalcanzable")


class Publisher(ABC):
    _registro: Optional[RegistroNoEnviados] = None

    @abstractmethod
    async def publish_batch(self, events: List[Dict[str, Any]], destino: Destino) -> ResultadoPublicacion: ...

    @abstractmethod
    async def close(self) -> None: ...

    @property
    def registro(self) -> RegistroNoEnviados:
        if self._registro is None:
            self._registro = RegistroNoEnviados(os.path.join(os.getcwd(), config.output_dir))
        return self._registro

    @property
    def ruta_no_enviados(self) -> Optional[str]:
        """Archivo de no enviados, o None si no hubo ninguno (no crea el registro)."""
        return self._registro.ruta if self._registro is not None else None


class ConsolePublisher(Publisher):
    """Modo local (DRY_RUN=true): sin red. Escribe el dataset en JSON Lines dentro de
    config.output_dir (un archivo por hub) e imprime UNA linea de resumen por lote."""

    def __init__(self, output_dir: str = None, verbose: bool = True, anexar: bool = False):
        output_dir = output_dir or os.path.join(os.getcwd(), config.output_dir)
        os.makedirs(output_dir, exist_ok=True)
        self.verbose = verbose
        modo = "a" if anexar else "w"
        self.features_path = os.path.join(output_dir, f"{config.event_hub.features_hub_name}.jsonl")
        self.ground_truth_path = os.path.join(output_dir, f"{config.event_hub.ground_truth_hub_name}.jsonl")
        self._f = open(self.features_path, modo, encoding="utf-8")
        self._g = open(self.ground_truth_path, modo, encoding="utf-8")
        if verbose:
            print(f"[DRY_RUN] Dataset local: {self.features_path} | {self.ground_truth_path}")

    async def publish_batch(self, events: List[Dict[str, Any]], destino: Destino) -> ResultadoPublicacion:
        f = self._f if destino == "features" else self._g
        for evt in events:
            f.write(json.dumps(evt, ensure_ascii=False) + "\n")
        f.flush()
        if self.verbose and events:
            etiqueta = "eventos-features" if destino == "features" else "eventos-ground-truth"
            ultimo = events[-1].get("timestamp") or events[-1].get("timestamp_etiqueta")
            print(f"[DRY_RUN -> {etiqueta}] {len(events)} eventos (hasta {ultimo})")
        return ResultadoPublicacion(enviados=len(events))

    async def close(self) -> None:
        self._f.close()
        self._g.close()
        if self._registro is not None:
            self._registro.close()


class EventHubPublisher(Publisher):
    """Dos EventHubProducerClient (features, ground_truth) sobre el mismo namespace Basic.
    Sin cambios de infraestructura respecto a la version anterior: mismos hubs, mismas
    particiones. El sistema de deteccion nunca debe tener permiso de lectura sobre el
    hub de ground truth.

    Fase 5 (5.5): cada lote se parte en sub-lotes del tamano maximo del hub y cada sub-lote
    se reintenta POR SEPARADO. Reintentar el lote entero reenviaria los sub-lotes que ya
    habian salido y duplicaria eventos en el hub (Delta los absorbe por event_id desde 5.4,
    pero las ventanas de CAL/2a/2b los contarian dos veces). Lo que falla tras todos los
    intentos va al RegistroNoEnviados. Residual declarado: si un envio falla por timeout
    DESPUES de que el servicio lo acepto, el reintento duplica ese sub-lote (Event Hubs es
    "al menos una vez"; el SDK no lo puede distinguir)."""

    def __init__(self, clientes: Optional[Dict[str, Any]] = None,
                 dormir: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self._dormir = dormir
        if clientes is not None:  # pruebas sin Azure (src/producer/prueba_productor.py)
            self._clients = clientes
            return
        from azure.eventhub.aio import EventHubProducerClient

        hub_names = {
            "features": config.event_hub.features_hub_name,
            "ground_truth": config.event_hub.ground_truth_hub_name,
        }
        if config.event_hub.use_managed_identity:
            from azure.identity.aio import DefaultAzureCredential

            if not config.event_hub.fully_qualified_namespace:
                raise ValueError("Falta EVENT_HUB_NAMESPACE cuando USE_MANAGED_IDENTITY=true.")
            credential = DefaultAzureCredential()
            self._clients = {
                k: EventHubProducerClient(
                    fully_qualified_namespace=config.event_hub.fully_qualified_namespace,
                    eventhub_name=n, credential=credential)
                for k, n in hub_names.items()
            }
        else:
            if not config.event_hub.connection_string:
                raise ValueError("Falta EVENT_HUB_CONNECTION_STRING (nivel NAMESPACE, sin EntityPath) "
                                 "o activa USE_MANAGED_IDENTITY=true con EVENT_HUB_NAMESPACE.")
            self._clients = {
                k: EventHubProducerClient.from_connection_string(
                    conn_str=config.event_hub.connection_string, eventhub_name=n)
                for k, n in hub_names.items()
            }

    @staticmethod
    def _event_data(evt: Dict[str, Any]):
        from azure.eventhub import EventData

        data = EventData(json.dumps(evt))
        data.properties = {"schema_version": SCHEMA_VERSION}
        return data

    async def _reintentar(self, intento, descripcion):
        return await con_reintentos(intento, productor.intentos, productor.espera_inicial_seg,
                                    productor.espera_max_seg, descripcion, self._dormir)

    async def _particionar(self, client, events):
        """Parte `events` en sub-lotes que caben en un batch del hub. No envia ni registra
        nada (se puede reintentar sin efectos). Devuelve tambien los eventos que no caben ni
        solos en un batch vacio (no reintentables)."""
        sublotes, actual, grandes = [], [], []
        batch = await client.create_batch()
        for evt in events:
            data = self._event_data(evt)
            try:
                batch.add(data)
                actual.append(evt)
                continue
            except ValueError:
                pass
            if actual:
                sublotes.append(actual)
                actual = []
                batch = await client.create_batch()
            try:
                batch.add(data)
                actual.append(evt)
            except ValueError as err:
                grandes.append((evt, err))
        if actual:
            sublotes.append(actual)
        return sublotes, grandes

    async def _send_to(self, key: Destino, events: List[Dict[str, Any]]) -> ResultadoPublicacion:
        client = self._clients[key]
        res = ResultadoPublicacion()
        partes, err, n = await self._reintentar(
            lambda: self._particionar(client, events), f"{key}: preparar lote de {len(events)}")
        if err is not None:
            self.registro.registrar(events, key, err, n)
            res.no_enviados = len(events)
            return res
        sublotes, grandes = partes
        for evt, err_grande in grandes:
            self.registro.registrar([evt], key, err_grande, intentos=0)
        res.no_enviados = len(grandes)
        for i, sub in enumerate(sublotes, 1):
            async def intento(sub=sub):
                batch = await client.create_batch()
                for evt in sub:
                    batch.add(self._event_data(evt))
                await client.send_batch(batch)

            _, err, n = await self._reintentar(
                intento, f"{key}: sub-lote {i}/{len(sublotes)} ({len(sub)} eventos)")
            if err is None:
                res.enviados += len(sub)
            else:
                self.registro.registrar(sub, key, err, n)
                res.no_enviados += len(sub)
        return res

    async def publish_batch(self, events: List[Dict[str, Any]], destino: Destino) -> ResultadoPublicacion:
        if not events:
            return ResultadoPublicacion()
        return await self._send_to(destino, events)

    async def close(self) -> None:
        for client in self._clients.values():
            await client.close()
        if self._registro is not None:
            self._registro.close()


def create_publisher() -> Publisher:
    return ConsolePublisher() if config.dry_run else EventHubPublisher()
