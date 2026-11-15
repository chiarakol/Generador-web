"""Vigilancia del drenado del pipeline tras `finalizado` (etapa 1: SOLO mide y muestra, no pausa).

Cada ~DRENADO_INTERVALO_MIN minutos se lee el estado del consumidor con un `LectorPipeline` y se
evalua, sobre DOS LECTURAS CONSECUTIVAS, que:
  a. decisiones_cads == F                      (el job proceso todos los features)
  b. decisiones_evaluadas no cambio            (dejo de crecer)
  c. pendientes de Redis ~ 0                   (None = no medible; se declara resultado parcial)
  d. huerfanos = G - evaluadas                 (solo se registra, no es condicion de exito)

El lector es una interfaz: aqui van el simulado y el "ninguno". Los lectores reales (Delta en
Storage y Redis) se agregan despues como otra implementacion, sin tocar la logica de abajo."""
import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence


def _entero_env(nombre: str, defecto: int, minimo: int = 0) -> int:
    crudo = os.getenv(nombre)
    if crudo is None or crudo.strip() == "":
        return defecto
    try:
        valor = int(crudo)
    except ValueError:
        raise ValueError(f"{nombre} debe ser un entero, recibido {crudo!r}")
    if valor < minimo:
        raise ValueError(f"{nombre} debe ser >= {minimo}, recibido {valor}")
    return valor


class LectorPipeline:
    """Una lectura del consumidor. Devuelve {"cads": int, "evaluadas": int, "pendientes": int|None}."""

    async def leer(self) -> Dict[str, Optional[int]]:  # pragma: no cover - interfaz
        raise NotImplementedError


class LectorSimulado(LectorPipeline):
    """Reproduce una secuencia fija de lecturas; la ultima se repite (el pipeline 'se estabiliza')."""

    def __init__(self, secuencia: Sequence[Dict[str, Optional[int]]]):
        if not secuencia:
            raise ValueError("LectorSimulado necesita al menos una lectura.")
        self._secuencia = list(secuencia)
        self._i = 0

    async def leer(self) -> Dict[str, Optional[int]]:
        lectura = self._secuencia[min(self._i, len(self._secuencia) - 1)]
        self._i += 1
        return dict(lectura)

    @classmethod
    def desde_F_G(cls, F: int, G: int) -> "LectorSimulado":
        """Pipeline de juguete que drena en 3 lecturas: sube, casi termina y se estabiliza."""
        return cls([
            {"cads": F // 2, "evaluadas": G // 4, "pendientes": G // 2},
            {"cads": F, "evaluadas": int(G * 0.9), "pendientes": 3},
            {"cads": F, "evaluadas": int(G * 0.97), "pendientes": 0},
            {"cads": F, "evaluadas": int(G * 0.97), "pendientes": 0},
        ])


def crear_lector(F: int, G: int) -> Optional[LectorPipeline]:
    """Elige el lector segun DRENADO_LECTOR: 'auto' (defecto), 'real', 'simulado' o 'ninguno'.

    'auto' usa el real si hay PIPELINE_ENTORNO o STORAGE_CONN_STRING (y entonces exige ambas);
    si no hay ninguna, no vigila."""
    modo = os.getenv("DRENADO_LECTOR", "auto").strip().lower()
    if modo == "auto":
        hay = bool(os.getenv("PIPELINE_ENTORNO", "").strip() or os.getenv("STORAGE_CONN_STRING", "").strip())
        modo = "real" if hay else "ninguno"
    if modo == "ninguno":
        return None
    if modo == "simulado":
        return LectorSimulado.desde_F_G(F, G)
    if modo == "real":
        from web.lectores import crear_lector_real  # import tardio: evita ciclo y dependencias opcionales
        return crear_lector_real()
    raise ValueError(f"DRENADO_LECTOR invalido: {modo!r} (use 'auto', 'real', 'simulado' o 'ninguno')")


def evaluar(F: int, G: int, previa: Dict[str, Any], actual: Dict[str, Any],
            tolerancia_pendientes: int = 0) -> Dict[str, Any]:
    """Condiciones sobre el par (lectura previa, lectura actual). Cada una: True/False/None(n/d)."""
    a = previa["cads"] == F and actual["cads"] == F
    b = previa["evaluadas"] == actual["evaluadas"]
    pend = [previa.get("pendientes"), actual.get("pendientes")]
    c = None if any(p is None for p in pend) else all(p <= tolerancia_pendientes for p in pend)
    medibles = [x for x in (a, b, c) if x is not None]
    return {
        "a_cads_igual_F": a,
        "b_evaluadas_estable": b,
        "c_pendientes_cero": c,
        "huerfanos": G - actual["evaluadas"],  # d: solo se registra
        "cumplidas": all(medibles),
        "parcial": c is None,  # sin Redis: el resultado no cubre la condicion c
    }


async def _pausar(drenado: Dict[str, Any], pausar: Callable[[], Awaitable[Dict[str, Any]]],
                  guardar: Optional[Callable[[], Awaitable[Any]]], fase_ok: str) -> None:
    """Pausa el job. La fase pasa por 'pausando' (bloquea iniciar) y termina en `fase_ok` o 'error_pausa'.
    Un fallo NO se oculta ni se reintenta a ciegas: el job puede seguir corriendo y gastando credito."""
    drenado["fase"] = "pausando"
    drenado["pausa"] = {"estado": "en_curso", "inicio": _ahora()}
    if guardar:
        await guardar()
    try:
        r = await pausar()
    except Exception as err:  # noqa: BLE001
        drenado["pausa"] = {"estado": "error", "error": f"{type(err).__name__}: {err}", "fin": _ahora()}
        drenado["fase"] = "error_pausa"
    else:
        drenado["pausa"] = {"estado": "pausado", "completa": r["completa"], "jobs": r["jobs"], "fin": _ahora()}
        drenado["fase"] = fase_ok
    if guardar:
        await guardar()


def _no_pausa(drenado: Dict[str, Any], motivo: str) -> None:
    drenado["pausa"] = {"estado": "no_aplicada", "motivo": motivo}


def _ahora() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


async def vigilar(drenado: Dict[str, Any], lector: LectorPipeline, *, intervalo_s: float,
                  tope_s: float, tolerancia_pendientes: int = 0, transcurrido_s: float = 0.0,
                  guardar: Optional[Callable[[], Awaitable[Any]]] = None,
                  pausar: Optional[Callable[[], Awaitable[Dict[str, Any]]]] = None,
                  pausar_parcial: bool = False, tope_accion: str = "avisar",
                  dormir: Callable[[float], Awaitable[Any]] = asyncio.sleep) -> None:
    """Actualiza `drenado` en sitio (lo lee la API).

    Fases finales: 'cumplido' (condiciones ok, sin pausar), 'pausado' (ok y job pausado),
    'tope_agotado' (sin pausar), 'pausado_incompleto' (tope agotado y tope_accion='pausar'),
    'error_pausa'. La pausa solo ocurre si se pasa `pausar` (DRENADO_PAUSAR=true).
    Con resultado parcial (sin Redis) NO se pausa salvo `pausar_parcial`.

    `transcurrido_s`: tiempo ya vigilado antes de un reinicio (el tope no se reinicia).
    `guardar`: se invoca tras cada cambio para persistir el registro."""
    F, G = drenado["F"], drenado["G"]
    inicio = time.monotonic() - transcurrido_s
    lecturas: List[Dict[str, Any]] = drenado["lecturas"]
    while True:
        try:
            crudo = await lector.leer()
        except Exception as err:  # noqa: BLE001 -- se informa, no se oculta; se reintenta
            drenado["ultimo_error"] = f"{type(err).__name__}: {err}"
        else:
            drenado.pop("ultimo_error", None)
            lectura = {**crudo, "huerfanos": G - crudo["evaluadas"], "en": _ahora()}
            if lecturas:
                lectura["condiciones"] = evaluar(F, G, lecturas[-1], lectura, tolerancia_pendientes)
            lecturas.append(lectura)
            if lecturas[-1].get("condiciones", {}).get("cumplidas"):
                drenado["fase"] = "cumplido"
                drenado["parcial"] = lecturas[-1]["condiciones"]["parcial"]
                drenado["huerfanos_final"] = lecturas[-1]["huerfanos"]
                if pausar is None:
                    _no_pausa(drenado, "pausa automatica desactivada (DRENADO_PAUSAR)")
                elif drenado["parcial"] and not pausar_parcial:
                    _no_pausa(drenado, "resultado parcial (sin Redis) y DRENADO_PAUSAR_PARCIAL=false")
                else:
                    await _pausar(drenado, pausar, guardar, "pausado")
                    return
                if guardar:
                    await guardar()
                return
        if time.monotonic() - inicio >= tope_s:
            drenado["fase"] = "tope_agotado"
            if pausar is not None and tope_accion == "pausar":
                await _pausar(drenado, pausar, guardar, "pausado_incompleto")
                return
            _no_pausa(drenado, "tope agotado: el job SIGUE corriendo (DRENADO_TOPE_ACCION=avisar)"
                      if pausar is not None else "pausa automatica desactivada (DRENADO_PAUSAR)")
            if guardar:
                await guardar()
            return
        if guardar:
            await guardar()
        await dormir(intervalo_s)


def parametros_desde_entorno() -> Dict[str, Any]:
    """Falla fuerte ante valores invalidos (convencion del proyecto)."""
    from web.databricks import _bool_env
    accion = os.getenv("DRENADO_TOPE_ACCION", "avisar").strip().lower()
    if accion not in ("avisar", "pausar"):
        raise ValueError(f"DRENADO_TOPE_ACCION invalida: {accion!r} (use 'avisar' o 'pausar')")
    return {
        "intervalo_s": _entero_env("DRENADO_INTERVALO_MIN", 2, minimo=0) * 60,
        "tope_s": _entero_env("DRENADO_TOPE_MIN", 60, minimo=1) * 60,
        "tolerancia_pendientes": _entero_env("DRENADO_PENDIENTES_TOL", 0, minimo=0),
        "pausar_parcial": _bool_env("DRENADO_PAUSAR_PARCIAL", False),
        "tope_accion": accion,
    }
