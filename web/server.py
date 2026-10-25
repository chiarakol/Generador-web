"""API + interfaz del generador. Un solo proceso, un solo worker (el simulador vive en memoria).

Variable de entorno propia de la web:
  ADMIN_TOKEN   token que exige POST /api/iniciar y /api/detener. Sin token solo se permite el
                modo prueba (sin Azure): publicar a Event Hub exige estar autenticado."""
import hmac
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from web.runner import Corrida

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
ESTATICO = Path(__file__).parent / "static"
corrida: Optional[Corrida] = None


@asynccontextmanager
async def ciclo_vida(_app: FastAPI):
    global corrida
    corrida = Corrida()
    yield
    # Render envia SIGTERM al redesplegar o reiniciar: se detiene ordenadamente para drenar etiquetas.
    corrida.detener()
    await corrida.esperar_fin()


app = FastAPI(title="Generador B2B de eventos de fraude", lifespan=ciclo_vida)


def _autenticado(authorization: Optional[str]) -> bool:
    if not ADMIN_TOKEN:
        return False
    esperado = f"Bearer {ADMIN_TOKEN}"
    return hmac.compare_digest((authorization or "").encode(), esperado.encode())


class ParamsInicio(BaseModel):
    dry_run: bool = True
    seed: Optional[int] = None
    horizonte_dias: Optional[float] = Field(default=None, ge=0)
    reloj_escala: Optional[float] = Field(default=None, gt=0)
    sim_start: Optional[str] = None  # ISO 8601; por defecto continua tras la corrida anterior


@app.get("/salud")
def salud():
    return {"ok": True}


@app.get("/api/estado")
def estado():
    return {**corrida.resumen(), "token_configurado": bool(ADMIN_TOKEN)}


@app.get("/api/recientes")
def recientes():
    return corrida.recientes()


@app.post("/api/iniciar")
async def iniciar(p: ParamsInicio, authorization: Optional[str] = Header(default=None)):
    autenticado = _autenticado(authorization)
    if ADMIN_TOKEN and not autenticado:
        raise HTTPException(401, "Token invalido o ausente.")
    if not p.dry_run and not autenticado:
        raise HTTPException(401, "Publicar a Event Hub exige configurar ADMIN_TOKEN y enviarlo.")
    try:
        corrida.iniciar(dry_run=p.dry_run, seed=p.seed, horizonte_dias=p.horizonte_dias,
                        reloj_escala=p.reloj_escala, sim_start=p.sim_start)
    except ValueError:
        raise HTTPException(400, "sim_start no es una fecha ISO 8601 valida.")
    except RuntimeError as err:
        raise HTTPException(409, str(err))
    return corrida.resumen()


@app.post("/api/detener")
async def detener(authorization: Optional[str] = Header(default=None)):
    if ADMIN_TOKEN and not _autenticado(authorization):
        raise HTTPException(401, "Token invalido o ausente.")
    if not corrida.activa:
        raise HTTPException(409, "No hay una corrida en curso.")
    corrida.detener()
    return corrida.resumen()


@app.get("/")
def indice():
    return FileResponse(ESTATICO / "index.html")
