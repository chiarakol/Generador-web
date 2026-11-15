"""Verificacion previa a iniciar una corrida a Event Hub: solo LEE, no cambia nada.

Existe porque dos veces seguidas se publico con el pipeline sin listo (CADS sin modelo; streams que aun
no leian del Event Hub) y los eventos se perdieron: el job lee desde el FINAL del stream, asi que lo
publicado antes de que arranque nunca se consume. Comprueba, para PIPELINE_ENTORNO:

  1. modelo_bandit   snapshots/{entorno}/ tiene el modelo de riesgo y el bandit inicial que CADS exige
  2. streams         delta/{entorno}/_checkpoints/ tiene el checkpoint de CADA stream (cal, 2a, 2b,
                     cads, 3b): un checkpoint existe solo desde que el stream arranco
  3. job_activo      el job streaming tiene un run activo en Databricks

Cada una devuelve ok | falla | no_verificado (si falta la credencial para comprobarla). La web SOLO
ADVIERTE (confirmacion del navegador); no bloquea. Usa STORAGE_CONN_STRING (la de lectura).
Un checkpoint de una prueba anterior daria un falso 'ok': por eso el flujo exige limpiar antes."""
import asyncio
import os
from typing import Any, Callable, Dict, List, Optional

from web.databricks import MENSAJE_SIN_AZURE, ClienteDatabricks, azure_habilitado, crear_cliente_desde_entorno
from web.lectores import entorno_desde_env

# Prefijos (sin la version) de la carpeta de checkpoint de cada stream: delta/{entorno}/_checkpoints/{job}
STREAMS = {"cal": "CAL", "capa2a": "2a", "capa2b": "2b", "capa3-cads": "CADS", "capa3b": "3b"}
TOPE_BLOBS = 20000  # no recorrer sin fin un listado enorme


class Prechequeo:
    def __init__(self):
        # Sustituibles en pruebas.
        self.fabrica_contenedor: Callable[[str], Any] = self._contenedor_real
        self.fabrica_cliente_db: Callable[[], Optional[ClienteDatabricks]] = self._cliente_real

    @staticmethod
    def _contenedor_real(nombre: str):
        conn = os.getenv("STORAGE_CONN_STRING", "").strip()
        if not conn:
            return None
        from azure.storage.blob import ContainerClient
        try:
            return ContainerClient.from_connection_string(conn, nombre)
        except Exception:  # noqa: BLE001 -- no repetir el secreto
            raise ValueError("STORAGE_CONN_STRING no es un connection string valido.")

    @staticmethod
    def _cliente_real() -> Optional[ClienteDatabricks]:
        if not (os.getenv("DATABRICKS_HOST", "").strip() and os.getenv("DATABRICKS_TOKEN", "").strip()
                and os.getenv("NOMBRE_JOB_STREAMING", "").strip()):
            return None
        return crear_cliente_desde_entorno()

    # -- comprobaciones ---------------------------------------------------------------------------
    def _modelo_bandit(self, entorno: str) -> Dict[str, Any]:
        from web.limpieza import rutas_conservar
        cont = self.fabrica_contenedor("snapshots")
        if cont is None:
            return {"nombre": "modelo_bandit", "estado": "no_verificado", "detalle": "falta STORAGE_CONN_STRING"}
        existentes = {b.name for b in cont.list_blobs(name_starts_with=f"{entorno}/")}
        faltan = sorted(r.split("/", 1)[1] for r in rutas_conservar(entorno) if r not in existentes)
        if faltan:
            return {"nombre": "modelo_bandit", "estado": "falla",
                    "detalle": f"faltan en snapshots/{entorno}/: {', '.join(faltan)} (CADS no arranca sin ellos)"}
        return {"nombre": "modelo_bandit", "estado": "ok", "detalle": f"modelo y bandit inicial presentes en snapshots/{entorno}/"}

    def _streams(self, entorno: str) -> Dict[str, Any]:
        cont = self.fabrica_contenedor("delta")
        if cont is None:
            return {"nombre": "streams", "estado": "no_verificado", "detalle": "falta STORAGE_CONN_STRING"}
        prefijo = f"{entorno}/_checkpoints/"
        vistos = set()
        for i, b in enumerate(cont.list_blobs(name_starts_with=prefijo)):
            carpeta = b.name[len(prefijo):].split("/", 1)[0]
            for clave in STREAMS:
                if carpeta.startswith(clave):
                    vistos.add(clave)
            if len(vistos) == len(STREAMS) or i >= TOPE_BLOBS:
                break
        faltan = [STREAMS[k] for k in STREAMS if k not in vistos]
        if faltan:
            return {"nombre": "streams", "estado": "falla",
                    "detalle": f"sin checkpoint de: {', '.join(faltan)}. Esos streams aun no arrancaron: "
                               "lo que se publique ahora no lo leeran (esperar y reintentar)"}
        return {"nombre": "streams", "estado": "ok", "detalle": f"checkpoints de los {len(STREAMS)} streams presentes (arrancaron)"}

    def _job_activo(self) -> Dict[str, Any]:
        cliente = self.fabrica_cliente_db()
        if cliente is None:
            return {"nombre": "job_activo", "estado": "no_verificado",
                    "detalle": "faltan DATABRICKS_HOST/DATABRICKS_TOKEN/NOMBRE_JOB_STREAMING"}
        jobs = cliente.buscar_jobs()
        if not jobs:
            return {"nombre": "job_activo", "estado": "falla", "detalle": f"no existe el job '{cliente.nombre_job}'"}
        activos = sum(len(cliente.runs_activos(j["job_id"])) for j in jobs)
        if not activos:
            return {"nombre": "job_activo", "estado": "falla", "detalle": f"'{cliente.nombre_job}' no tiene ningun run activo: levantarlo"}
        return {"nombre": "job_activo", "estado": "ok", "detalle": f"'{cliente.nombre_job}': {activos} run(s) activo(s)"}

    def ejecutar_sync(self) -> Dict[str, Any]:
        entorno = entorno_desde_env()
        checks: List[Dict[str, Any]] = []
        for nombre, f in (("modelo_bandit", lambda: self._modelo_bandit(entorno)), ("streams", lambda: self._streams(entorno)),
                          ("job_activo", self._job_activo)):
            try:
                checks.append(f())
            except Exception as err:  # noqa: BLE001 -- un fallo al comprobar no es un 'ok': se informa
                checks.append({"nombre": nombre, "estado": "no_verificado", "detalle": f"no se pudo comprobar ({type(err).__name__}: {err})"})
        return {"entorno": entorno, "checks": checks,
                "listo": all(c["estado"] == "ok" for c in checks),
                "hay_fallas": any(c["estado"] == "falla" for c in checks)}

    async def ejecutar(self) -> Dict[str, Any]:
        if not azure_habilitado():
            raise RuntimeError(MENSAJE_SIN_AZURE)
        return await asyncio.to_thread(self.ejecutar_sync)  # Azure y Databricks son sincronos
