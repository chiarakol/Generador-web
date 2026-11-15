"""Persistencia del registro de drenado (F, G y lecturas) para sobrevivir a un reinicio.

DRENADO_PERSISTENCIA:
  archivo (defecto)  OUTPUT_DIR/estado_drenado.json. En Render el disco es EFIMERO: un reinicio o
                     redeploy lo borra (salvo que se monte un disco persistente en OUTPUT_DIR).
                     Por eso el registro lleva un aviso que la UI muestra mientras se vigila.
  blob               PREPARADO, NO ACTIVADO POR DEFECTO ni probado contra Azure real. Escribe un
                     blob JSON en la cuenta de Storage; exige una credencial con permiso de
                     ESCRITURA (STORAGE_CONN_STRING es solo lectura por decision: se usa DRENADO_BLOB_CONN_STRING) y el
                     contenedor DRENADO_BLOB_CONTENEDOR ya creado. Activarlo es decision del usuario."""
import json
import os
from typing import Any, Dict, Optional

from src.config import config

AVISO_ARCHIVO = ("F y G se guardan en disco local; en Render un reinicio o redeploy durante la "
                 "vigilancia los pierde (sin disco persistente) y no se podra reanudar.")


class Almacen:
    descripcion = ""
    aviso: Optional[str] = None

    def guardar(self, registro: Dict[str, Any]) -> None:  # pragma: no cover - interfaz
        raise NotImplementedError

    def cargar(self) -> Optional[Dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    def borrar(self) -> None:  # pragma: no cover
        raise NotImplementedError


class AlmacenArchivo(Almacen):
    descripcion = "archivo local"
    aviso = AVISO_ARCHIVO

    def __init__(self, ruta: str):
        self._ruta = ruta

    def guardar(self, registro: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(self._ruta) or ".", exist_ok=True)
        tmp = self._ruta + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(registro, f)
        os.replace(tmp, self._ruta)  # atomico: nunca queda un JSON a medias

    def cargar(self) -> Optional[Dict[str, Any]]:
        try:
            with open(self._ruta, encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as err:
            print(f"Registro de drenado ilegible en {self._ruta}, se ignora: {err}")
            return None

    def borrar(self) -> None:
        try:
            os.remove(self._ruta)
        except FileNotFoundError:
            pass
        except OSError as err:
            print(f"No se pudo borrar {self._ruta}: {err}")


class AlmacenBlob(Almacen):
    """Preparado, no activado por defecto. `contenedor` es un ContainerClient (inyectable en pruebas)."""
    descripcion = "blob en Azure Storage"
    aviso = None  # sobrevive a reinicios de Render

    def __init__(self, contenedor, nombre: str):
        self._cont = contenedor
        self._nombre = nombre

    def guardar(self, registro: Dict[str, Any]) -> None:
        self._cont.upload_blob(self._nombre, json.dumps(registro).encode("utf-8"), overwrite=True)

    def cargar(self) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(self._cont.download_blob(self._nombre).readall().decode("utf-8"))
        except Exception as err:  # noqa: BLE001 -- el blob puede no existir aun
            if type(err).__name__ == "ResourceNotFoundError":
                return None
            raise

    def borrar(self) -> None:
        try:
            self._cont.delete_blob(self._nombre)
        except Exception as err:  # noqa: BLE001
            if type(err).__name__ != "ResourceNotFoundError":
                raise


def crear_almacen() -> Almacen:
    modo = os.getenv("DRENADO_PERSISTENCIA", "archivo").strip().lower()
    if modo == "archivo":
        return AlmacenArchivo(os.path.join(config.output_dir, "estado_drenado.json"))
    if modo == "blob":
        from web.lectores import entorno_desde_env, _requerida
        from azure.storage.blob import ContainerClient
        cont = ContainerClient.from_connection_string(
            _requerida("DRENADO_BLOB_CONN_STRING"), _requerida("DRENADO_BLOB_CONTENEDOR"))
        return AlmacenBlob(cont, f"{entorno_desde_env()}/estado_drenado.json")
    raise ValueError(f"DRENADO_PERSISTENCIA invalida: {modo!r} (use 'archivo' o 'blob')")
