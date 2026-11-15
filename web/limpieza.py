"""Limpieza del entorno del consumidor desde la web (equivale al `limpiar_entorno.sh` del original,
que se usa solo como guia: no se importa ni se ejecuta).

IRREVERSIBLE. Borra, para el entorno PIPELINE_ENTORNO y solo ese:
  1. pausa los jobs del entorno y cancela sus runs (un stream vivo reescribiria checkpoints)
  2. Storage, contenedor 'delta':      {entorno}/*   (tablas Delta + _checkpoints)
  3. Storage, contenedor 'snapshots':  {entorno}/*   (bandit, grafo, baseline DTRO)
  4. Redis: claves {entorno}:*         (se omite, avisando, si no hay Redis o no se alcanza)
  5. verifica que no quede nada con el prefijo.
No toca los Event Hubs (no se pueden vaciar: retencion de 1 dia) ni las definiciones de los jobs.

Variables (en Render; nunca en el repo):
  LIMPIEZA_STORAGE_CONN_STRING  SECRETO. Credencial de Storage CON permiso de borrado. Sin ella la
                                limpieza queda no disponible (la credencial de lectura del drenado
                                es otra: STORAGE_CONN_STRING).
  LIMPIEZA_PERMITE_TESIS        true para permitir limpiar PIPELINE_ENTORNO=tesis (defecto false)
  NOMBRES_JOBS_PAUSA            opcional, separados por coma; por defecto
                                NOMBRE_JOB_STREAMING, capa4-dtro-{entorno}, capa5-shap-lite-{entorno}
Ademas usa PIPELINE_ENTORNO, DATABRICKS_HOST/TOKEN y REDIS_HOST/REDIS_KEY (opcional)."""
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from web.databricks import ClienteDatabricks, _bool_env, crear_cliente_desde_entorno
from web.lectores import crear_cliente_redis, entorno_desde_env

CONTENEDORES = ("delta", "snapshots")
PASOS = ("pausar_jobs", "borrar_delta", "borrar_snapshots", "borrar_redis", "verificar")
FRASE_TESIS = "BORRAR TESIS"


def _ahora() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class ErrorLimpieza(RuntimeError):
    """Paso fallido: la limpieza se detiene y se informa; es idempotente, se puede repetir."""


class Limpieza:
    def __init__(self):
        self.estado: Optional[Dict[str, Any]] = None
        self.ultimo_lanzamiento: Optional[Dict[str, Any]] = None
        self._tarea: Optional[asyncio.Task] = None
        # Fabricas sustituibles en pruebas (por defecto, las reales segun el entorno).
        self.fabrica_contenedor: Callable[[str], Any] = self._contenedor_real
        self.fabrica_redis: Callable[[], Any] = crear_cliente_redis
        self.fabrica_clientes_db: Callable[[str], List[ClienteDatabricks]] = self._clientes_reales

    # -- configuracion ----------------------------------------------------------------------------
    @property
    def configurada(self) -> bool:
        return bool(os.getenv("LIMPIEZA_STORAGE_CONN_STRING", "").strip())

    @property
    def activa(self) -> bool:
        return self._tarea is not None and not self._tarea.done()

    def entorno(self) -> Optional[str]:
        try:
            return entorno_desde_env()
        except ValueError:
            return None

    def frase_confirmacion(self, entorno: str) -> str:
        return FRASE_TESIS if entorno == "tesis" else entorno

    def validar_arranque(self) -> None:
        """Falla fuerte al arrancar si la configuracion de limpieza es invalida."""
        _bool_env("LIMPIEZA_PERMITE_TESIS", False)

    def _contenedor_real(self, nombre: str):
        from azure.storage.blob import ContainerClient
        conn = os.getenv("LIMPIEZA_STORAGE_CONN_STRING", "").strip()
        if not conn:
            raise ErrorLimpieza("Falta LIMPIEZA_STORAGE_CONN_STRING.")
        try:
            return ContainerClient.from_connection_string(conn, nombre)
        except Exception:  # noqa: BLE001 -- no repetir el secreto en el mensaje
            raise ErrorLimpieza("LIMPIEZA_STORAGE_CONN_STRING no es un connection string valido.")

    def _clientes_reales(self, entorno: str) -> List[ClienteDatabricks]:
        crudo = os.getenv("NOMBRES_JOBS_PAUSA", "").strip()
        if crudo:
            nombres = [n.strip() for n in crudo.split(",") if n.strip()]
        else:
            stream = os.getenv("NOMBRE_JOB_STREAMING", "").strip()
            if not stream:
                raise ErrorLimpieza("Falta NOMBRE_JOB_STREAMING (o NOMBRES_JOBS_PAUSA).")
            nombres = [stream, f"capa4-dtro-{entorno}", f"capa5-shap-lite-{entorno}"]
        return [crear_cliente_desde_entorno(n) for n in nombres]

    # -- API --------------------------------------------------------------------------------------
    def resumen(self) -> Dict[str, Any]:
        return {"configurada": self.configurada, "entorno": self.entorno(),
                "permite_tesis": _bool_env("LIMPIEZA_PERMITE_TESIS", False),
                "estado": self.estado, "ultimo_lanzamiento": self.ultimo_lanzamiento}

    def iniciar(self, confirmar: str, bloqueo: Optional[str]) -> None:
        """Valida TODO antes de borrar nada. Lanza ErrorLimpieza con el motivo si no procede.
        `bloqueo`: motivo ya calculado por quien conoce la corrida (None si no hay)."""
        if not self.configurada:
            raise ErrorLimpieza("La limpieza no esta configurada (falta LIMPIEZA_STORAGE_CONN_STRING).")
        try:
            entorno = entorno_desde_env()
        except ValueError as err:
            raise ErrorLimpieza(str(err))
        if entorno == "tesis" and not _bool_env("LIMPIEZA_PERMITE_TESIS", False):
            raise ErrorLimpieza("Limpiar 'tesis' borra las metricas de la tesis: exige LIMPIEZA_PERMITE_TESIS=true.")
        if confirmar != self.frase_confirmacion(entorno):
            raise ErrorLimpieza(f"Confirmacion incorrecta: escribir exactamente '{self.frase_confirmacion(entorno)}'.")
        if self.activa:
            raise ErrorLimpieza("Ya hay una limpieza en curso.")
        if bloqueo:
            raise ErrorLimpieza(bloqueo)
        self.estado = {"fase": "limpiando", "entorno": entorno, "inicio": _ahora(), "fin": None,
                       "pasos": [{"nombre": n, "estado": "pendiente", "detalle": ""} for n in PASOS]}
        self._tarea = asyncio.create_task(asyncio.to_thread(self._ejecutar, entorno))

    async def esperar(self) -> None:
        if self._tarea is not None:
            await asyncio.gather(self._tarea, return_exceptions=True)

    # -- ejecucion (hilo aparte: Azure y Databricks son sincronos) --------------------------------
    def _paso(self, nombre: str) -> Dict[str, Any]:
        return next(p for p in self.estado["pasos"] if p["nombre"] == nombre)

    def _ejecutar(self, entorno: str) -> None:
        omitido_redis = False
        actual = None
        try:
            for nombre in PASOS:
                actual = self._paso(nombre)
                actual["estado"] = "en_curso"
                if nombre == "pausar_jobs":
                    detalle, estado = self._pausar_jobs(entorno), "ok"
                elif nombre in ("borrar_delta", "borrar_snapshots"):
                    detalle, estado = self._borrar_contenedor(nombre.split("_")[1], entorno), "ok"
                elif nombre == "borrar_redis":
                    detalle, estado = self._borrar_redis(entorno)
                    omitido_redis = estado == "omitido"
                else:
                    detalle, estado = self._verificar(entorno, omitido_redis), "ok"
                actual["detalle"], actual["estado"] = detalle, estado
            self.estado["fase"] = "limpio_parcial" if omitido_redis else "limpio"
        except Exception as err:  # noqa: BLE001 -- se informa, no se oculta; no se sigue borrando
            if actual is not None:
                actual["estado"], actual["detalle"] = "error", f"{type(err).__name__}: {err}"
            self.estado["fase"] = "error"
        finally:
            self.estado["fin"] = _ahora()

    def _pausar_jobs(self, entorno: str) -> str:
        partes = []
        for cliente in self.fabrica_clientes_db(entorno):
            r = cliente.pausar(omitir_inexistente=True)
            if r.get("inexistente"):
                partes.append(f"{cliente.nombre_job}: no existe (nada que pausar)")
                continue
            if not r["completa"]:
                raise ErrorLimpieza(f"{cliente.nombre_job}: aun hay runs activos tras cancelar; no se borra nada.")
            acciones = "; ".join(a for j in r["jobs"] for a in j["acciones"])
            partes.append(f"{cliente.nombre_job}: {acciones}")
        return " | ".join(partes)

    def _borrar_contenedor(self, nombre: str, entorno: str) -> str:
        prefijo = f"{entorno}/"
        cont = self.fabrica_contenedor(nombre)
        # Hijos antes que padres (cuentas con namespace jerarquico listan tambien carpetas).
        blobs = sorted((b.name for b in cont.list_blobs(name_starts_with=prefijo)), reverse=True)
        if any(not n.startswith(prefijo) for n in blobs):
            raise ErrorLimpieza("El listado devolvio blobs fuera del prefijo; se aborta por seguridad.")

        def borrar(n: str) -> None:
            try:
                cont.delete_blob(n)
            except Exception as err:  # noqa: BLE001
                if type(err).__name__ != "ResourceNotFoundError":  # ya borrado: idempotente
                    raise
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(borrar, blobs))
        return f"{nombre}/{prefijo}*: {len(blobs)} blob(s) borrados"

    def _borrar_redis(self, entorno: str):
        patron = f"{entorno}:*"
        try:
            r = self.fabrica_redis()
            if r is None:
                return "Redis no configurado (REDIS_HOST/REDIS_KEY): se omite", "omitido"
            borradas = 0
            for clave in r.scan_iter(match=patron, count=500):
                borradas += r.delete(clave)
        except ValueError as err:  # REDIS_HOST sin REDIS_KEY o viceversa: configuracion invalida
            raise ErrorLimpieza(str(err))
        except Exception as err:  # noqa: BLE001 -- Redis puede no existir (se borra entre sesiones)
            return f"Redis no alcanzable ({type(err).__name__}); se omite. Si existe, limpiarlo a mano", "omitido"
        return f"{borradas} clave(s) {patron} borradas", "ok"

    def _verificar(self, entorno: str, omitido_redis: bool) -> str:
        prefijo = f"{entorno}/"
        quedan = []
        for nombre in CONTENEDORES:
            cont = self.fabrica_contenedor(nombre)
            n = sum(1 for _ in cont.list_blobs(name_starts_with=prefijo))
            if n:
                quedan.append(f"{nombre}/{prefijo}: {n} blob(s)")
        if not omitido_redis:
            r = self.fabrica_redis()
            n = sum(1 for _ in r.scan_iter(match=f"{entorno}:*", count=500))
            if n:
                quedan.append(f"Redis {entorno}:*: {n} clave(s)")
        if quedan:
            raise ErrorLimpieza("Quedan restos, no dar el entorno por limpio: " + "; ".join(quedan))
        return f"nada con el prefijo '{prefijo}' en delta/snapshots" + ("" if omitido_redis else f" ni claves '{entorno}:*'")

    # -- levantar el job (accion separada, nunca automatica) --------------------------------------
    async def lanzar_job(self, confirmar: str, bloqueo: Optional[str]) -> Dict[str, Any]:
        entorno = entorno_desde_env()
        if confirmar != self.frase_confirmacion(entorno):
            raise ErrorLimpieza(f"Confirmacion incorrecta: escribir exactamente '{self.frase_confirmacion(entorno)}'.")
        if self.activa:
            raise ErrorLimpieza("Hay una limpieza en curso.")
        if bloqueo:
            raise ErrorLimpieza(bloqueo)
        cliente = crear_cliente_desde_entorno()  # NOMBRE_JOB_STREAMING
        try:
            r = await cliente.lanzar_async()
        except RuntimeError as err:
            raise ErrorLimpieza(str(err))
        self.ultimo_lanzamiento = {"job": cliente.nombre_job, **r, "en": _ahora()}
        return self.ultimo_lanzamiento
