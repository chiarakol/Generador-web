"""Cliente minimo de la Jobs API 2.1 de Databricks para PAUSAR el job streaming (solo libreria estandar).

Codigo propio de este repo. Variables (en Render; nunca en el repo):
  DATABRICKS_HOST       https://adb-XXXX.azuredatabricks.net  (con https://, sin '/' final; no es secreto)
  DATABRICKS_TOKEN      PAT (SECRETO); idealmente solo con permiso sobre ese job
  NOMBRE_JOB_STREAMING  nombre EXACTO del job (p. ej. pipeline-streaming-test)

Pausar = lo que hace el original: para cada job con ese nombre, pausar lo que lo relanzaria solo
(`schedule`, y ademas `continuous` y `trigger`: un job continuo se relanzaria tras cancelar su run)
y cancelar los runs activos. Es idempotente: repetirlo no daña. Despausar no se hace desde aqui."""
import asyncio
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional

API = "/api/2.1"
DISPARADORES = ("schedule", "continuous", "trigger")  # ajustes con pause_status que relanzan el job


class ClienteDatabricks:
    def __init__(self, host: str, token: str, nombre_job: str, *, espera_cancelacion_s: float = 60.0,
                 dormir: Callable[[float], None] = time.sleep):
        self._host, self._token, self.nombre_job = host, token, nombre_job
        self._espera, self._dormir = espera_cancelacion_s, dormir

    # -- HTTP -------------------------------------------------------------------------------------
    def _api(self, metodo: str, ruta: str, cuerpo: Optional[dict] = None, params: Optional[dict] = None):
        url = self._host + API + ruta + ("?" + urllib.parse.urlencode(params) if params else "")
        req = urllib.request.Request(
            url, method=metodo, data=None if cuerpo is None else json.dumps(cuerpo).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                texto = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:  # el token viaja en un header: nunca se incluye en el mensaje
            detalle = e.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"{metodo} {API}{ruta} -> HTTP {e.code}: {detalle}") from None
        except urllib.error.URLError as e:
            raise RuntimeError(f"{metodo} {API}{ruta} -> sin conexion: {e.reason}") from None
        return json.loads(texto) if texto.strip() else {}

    # -- Consultas --------------------------------------------------------------------------------
    def buscar_jobs(self) -> List[dict]:
        """Todos los jobs cuyo nombre es exactamente `nombre_job` (puede haber duplicados)."""
        encontrados: List[dict] = []
        page_token = None
        while True:
            params = {"name": self.nombre_job, "limit": 100}
            if page_token:
                params["page_token"] = page_token
            r = self._api("GET", "/jobs/list", params=params)
            encontrados += [j for j in r.get("jobs", []) if j.get("settings", {}).get("name") == self.nombre_job]
            page_token = r.get("next_page_token")
            if not page_token:
                return encontrados

    def runs_activos(self, job_id: int) -> List[dict]:
        runs: List[dict] = []
        page_token = None
        while True:
            params = {"job_id": job_id, "active_only": "true", "limit": 25}
            if page_token:
                params["page_token"] = page_token
            r = self._api("GET", "/jobs/runs/list", params=params)
            runs += r.get("runs", [])
            page_token = r.get("next_page_token")
            if not page_token:
                return runs

    # -- Pausa ------------------------------------------------------------------------------------
    def pausar(self) -> Dict[str, Any]:
        """Pausa y cancela. Devuelve {"jobs": [{job_id, acciones}], "completa": bool}.
        Lanza RuntimeError si el job no existe o la API falla (no se oculta)."""
        jobs = self.buscar_jobs()
        if not jobs:
            raise RuntimeError(f"No existe ningun job llamado '{self.nombre_job}'.")
        resultado: List[dict] = []
        completa = True
        for job in jobs:
            jid = job["job_id"]
            acciones: List[str] = []
            settings = self._api("GET", "/jobs/get", params={"job_id": jid}).get("settings", {})
            for clave in DISPARADORES:
                cfg = settings.get(clave)
                if cfg and cfg.get("pause_status") != "PAUSED":
                    self._api("POST", "/jobs/update",
                              {"job_id": jid, "new_settings": {clave: dict(cfg, pause_status="PAUSED")}})
                    acciones.append(f"{clave} pausado")
                elif cfg:
                    acciones.append(f"{clave} ya estaba pausado")
            for run in self.runs_activos(jid):
                self._api("POST", "/jobs/runs/cancel", {"run_id": run["run_id"]})
                acciones.append(f"run {run['run_id']} cancelado")
            # La cancelacion es asincrona: se confirma que ya no queda ningun run activo.
            limite = time.monotonic() + self._espera
            while self.runs_activos(jid):
                if time.monotonic() >= limite:
                    completa = False
                    acciones.append("AVISO: aun hay runs activos tras la espera")
                    break
                self._dormir(3)
            resultado.append({"job_id": jid, "acciones": acciones or ["sin disparadores ni runs activos"]})
        return {"jobs": resultado, "completa": completa}

    async def pausar_async(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self.pausar)  # urllib es sincrono: fuera del event loop


def _bool_env(nombre: str, defecto: bool) -> bool:
    crudo = os.getenv(nombre, "").strip().lower()
    if crudo == "":
        return defecto
    if crudo in ("true", "1", "si"):
        return True
    if crudo in ("false", "0", "no"):
        return False
    raise ValueError(f"{nombre} debe ser true o false, recibido {crudo!r}")


def pausa_activada() -> bool:
    return _bool_env("DRENADO_PAUSAR", False)


def crear_cliente_desde_entorno() -> ClienteDatabricks:
    """Falla fuerte si falta algo o es invalido (convencion del proyecto)."""
    def req(n: str) -> str:
        v = os.getenv(n, "").strip()
        if not v:
            raise ValueError(f"Falta la variable de entorno {n} (necesaria con DRENADO_PAUSAR=true).")
        return v
    host = req("DATABRICKS_HOST").rstrip("/")
    if not host.startswith("https://"):
        raise ValueError("DATABRICKS_HOST debe empezar con https:// (el token no viaja por http).")
    return ClienteDatabricks(host, req("DATABRICKS_TOKEN"), req("NOMBRE_JOB_STREAMING"))
