# Generador B2B web

Interfaz web (FastAPI) para iniciar y detener el generador de eventos de fraude B2B.
`src/` es una copia sin cambios del generador original; `web/` es lo nuevo.

## Local
```
pip install -r requirements.txt
uvicorn web.server:app --port 8000        # un solo worker
```
Abrir http://localhost:8000. Sin `ADMIN_TOKEN` solo funciona el modo prueba (sin Azure).

## Render
1. Subir esta carpeta a un repositorio y crear un Blueprint con `render.yaml` (o un Web Service Docker).
2. Cargar `EVENT_HUB_CONNECTION_STRING` (namespace, sin `EntityPath`) en el panel; `ADMIN_TOKEN` se genera solo.
3. Plan **Starter** o superior y **1 instancia**: el plan free se duerme y el estado vive en memoria.
4. En la pagina, escribir el token. Sin marcar "Modo prueba" publica a Event Hub.

## API
- `GET /api/estado`, `GET /api/recientes`
- `POST /api/iniciar` `{dry_run, seed, horizonte_dias, reloj_escala}` (Bearer `ADMIN_TOKEN`)
- `POST /api/detener` (Bearer `ADMIN_TOKEN`) — vacia las etiquetas pendientes de ground truth antes de cerrar.
- Un reinicio o redespliegue de Render detiene la corrida (SIGTERM → parada ordenada).
