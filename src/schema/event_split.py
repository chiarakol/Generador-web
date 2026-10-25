"""Separacion features / ground truth.

En v2 el simulador ya emite los dos mensajes por separado (la etiqueta nunca
vive dentro del evento de features). Este modulo solo conserva la lista de
campos prohibidos en features y una verificacion defensiva usada por main.py y
por las pruebas."""
from typing import Any, Dict

CAMPOS_PROHIBIDOS_EN_FEATURES = {
    "es_fraude", "es_fraude_real", "es_fraude_etiquetado", "tipo_fraude_simulado", "tipologia",
    "variante", "campana_id", "fase_campana", "timestamp_etiqueta", "event_source", "raw_source_fields",
}


def hay_fuga_de_etiqueta(features_event: Dict[str, Any]) -> bool:
    return any(k in features_event for k in CAMPOS_PROHIBIDOS_EN_FEATURES)
