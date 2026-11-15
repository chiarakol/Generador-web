"""Lectores reales del pipeline consumidor: tablas Delta (en Azure Storage) y Redis.

Solo lectura y sin Spark. Codigo propio de este repo (no se importa nada del proyecto de referencia).

Variables de entorno (todas en Render o `.env` local; nunca en el repo):
  PIPELINE_ENTORNO     prefijo de tablas, claves Redis y blobs ('test', 'tesis', ...)
  STORAGE_CONN_STRING  connection string de la cuenta de Storage (SECRETO; basta permiso de lectura)
  REDIS_HOST           host de Azure Cache for Redis (sin el puerto; se usa 6380 con TLS)
  REDIS_KEY            clave de acceso de Redis (SECRETO). Si falta REDIS_HOST/REDIS_KEY, las
                       mediciones de Redis se omiten y el resultado queda 'parcial'."""
import asyncio
import json
import os
import re
from typing import Any, Dict, List, Optional

from web.drenado import LectorPipeline

CONTENEDOR_DELTA = "delta"
PUERTO_REDIS = 6380
_ENTORNO_VALIDO = re.compile(r"^[A-Za-z0-9_-]+$")  # va dentro de rutas y de un patron SCAN


class LectorDelta:
    """Cuenta filas de una tabla Delta sumando `stats.numRecords` de los archivos vivos
    (add - remove) del `_delta_log`. Sin Spark.

    Es incremental: los commits JSON son inmutables, asi que cada lectura solo descarga los
    commits nuevos y conserva el estado. Si la tabla tiene checkpoint pero el commit 0 ya no
    esta (logs viejos limpiados), el conteo seria un subconteo silencioso: se falla fuerte."""

    def __init__(self, contenedor, tabla: str):
        self._cont = contenedor
        self._tabla = tabla
        self._vivos: Dict[str, Optional[int]] = {}
        self._ultima: int = -1  # ultima version de commit aplicada
        self.sin_stats = 0

    def _versiones_disponibles(self) -> Dict[int, str]:
        prefijo = f"{self._tabla}/_delta_log/"
        out: Dict[int, str] = {}
        for b in self._cont.list_blobs(name_starts_with=prefijo):
            resto = b.name[len(prefijo):]
            if re.fullmatch(r"\d{20}\.json", resto):
                out[int(resto[:20])] = b.name
        return out

    def filas(self) -> int:
        """Filas vivas hoy (0 si la tabla aun no existe)."""
        disponibles = self._versiones_disponibles()
        if not disponibles:
            return 0
        if self._ultima < 0 and 0 not in disponibles:
            raise RuntimeError(
                f"{self._tabla}: el _delta_log no tiene el commit 0 (hay checkpoint y se limpiaron "
                "los JSON viejos); sumar solo los commits restantes daria un subconteo.")
        siguiente = self._ultima + 1
        while siguiente in disponibles:  # solo versiones contiguas
            texto = self._cont.download_blob(disponibles[siguiente]).readall().decode("utf-8")
            for linea in texto.splitlines():
                if not linea.strip():
                    continue
                accion = json.loads(linea)
                if "add" in accion:
                    stats = json.loads(accion["add"].get("stats") or "{}")
                    self._vivos[accion["add"]["path"]] = stats.get("numRecords")
                elif "remove" in accion:
                    self._vivos.pop(accion["remove"]["path"], None)
            self._ultima = siguiente
            siguiente += 1
        self.sin_stats = sum(1 for n in self._vivos.values() if n is None)
        return sum(n or 0 for n in self._vivos.values())


class LectorRedis:
    """Cuenta las claves `{entorno}:pendiente:cads:*` con SCAN (nunca KEYS)."""

    def __init__(self, cliente, entorno: str):
        self._r = cliente
        self._patron = f"{entorno}:pendiente:cads:*"

    def pendientes(self) -> int:
        return sum(1 for _ in self._r.scan_iter(match=self._patron, count=1000))


class LectorReal(LectorPipeline):
    def __init__(self, entorno: str, delta_cads: LectorDelta, delta_eval: LectorDelta,
                 redis_lector: Optional[LectorRedis]):
        self._entorno = entorno
        self._cads, self._eval, self._redis = delta_cads, delta_eval, redis_lector

    def _leer_sync(self) -> Dict[str, Any]:
        lectura: Dict[str, Any] = {"cads": self._cads.filas(), "evaluadas": self._eval.filas(),
                                   "pendientes": None}
        avisos: List[str] = []
        if self._cads.sin_stats or self._eval.sin_stats:
            avisos.append("hay archivos Delta sin stats.numRecords: el conteo es un minimo")
        if self._redis is None:
            avisos.append("Redis no configurado: pendientes no medido")
        else:
            try:
                lectura["pendientes"] = self._redis.pendientes()
            except Exception as err:  # noqa: BLE001 -- Redis puede no existir (se borra entre sesiones)
                avisos.append(f"Redis no disponible ({type(err).__name__}): pendientes no medido")
        if avisos:
            lectura["avisos"] = avisos
        return lectura

    async def leer(self) -> Dict[str, Any]:
        # Las librerias de Azure y Redis son sincronas: fuera del event loop.
        return await asyncio.to_thread(self._leer_sync)


def _requerida(nombre: str) -> str:
    valor = os.getenv(nombre, "").strip()
    if not valor:
        raise ValueError(f"Falta la variable de entorno {nombre}.")
    return valor


def entorno_desde_env() -> str:
    entorno = _requerida("PIPELINE_ENTORNO")
    if not _ENTORNO_VALIDO.match(entorno):
        raise ValueError("PIPELINE_ENTORNO solo admite letras, digitos, '_' y '-'.")
    return entorno


def crear_lector_real() -> LectorReal:
    """Construye el lector con las variables de entorno; falla fuerte si faltan o son invalidas."""
    entorno = entorno_desde_env()
    conn = _requerida("STORAGE_CONN_STRING")
    try:
        from azure.storage.blob import ContainerClient
        contenedor = ContainerClient.from_connection_string(conn, CONTENEDOR_DELTA)
    except ImportError:
        raise
    except Exception:  # noqa: BLE001 -- no repetir el valor del secreto en el mensaje
        raise ValueError("STORAGE_CONN_STRING no es un connection string valido de Azure Storage.")

    redis_lector: Optional[LectorRedis] = None
    host, clave = os.getenv("REDIS_HOST", "").strip(), os.getenv("REDIS_KEY", "").strip()
    if bool(host) != bool(clave):
        raise ValueError("REDIS_HOST y REDIS_KEY deben definirse juntas (o ninguna).")
    if host:
        import redis
        cliente = redis.Redis(host=host, port=PUERTO_REDIS, password=clave, ssl=True,
                              socket_timeout=10, socket_connect_timeout=10)
        redis_lector = LectorRedis(cliente, entorno)

    return LectorReal(entorno,
                      LectorDelta(contenedor, f"{entorno}/decisiones_cads"),
                      LectorDelta(contenedor, f"{entorno}/decisiones_evaluadas"),
                      redis_lector)
