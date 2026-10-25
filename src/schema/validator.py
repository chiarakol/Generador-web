import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from jsonschema import Draft7Validator

_SCHEMA_DIR = os.path.dirname(__file__)


def _load(filename: str) -> Draft7Validator:
    with open(os.path.join(_SCHEMA_DIR, filename), "r", encoding="utf-8") as f:
        return Draft7Validator(json.load(f))


_features_validator = _load("features_event_schema.json")
_ground_truth_validator = _load("ground_truth_schema.json")


@dataclass
class ValidationResult:
    valid: bool
    errors: Optional[List[str]]


def _run(validator: Draft7Validator, event: Dict[str, Any]) -> ValidationResult:
    errores = sorted(validator.iter_errors(event), key=lambda e: list(map(str, e.path)))
    if not errores:
        return ValidationResult(True, None)
    return ValidationResult(False, [f"{'/'.join(str(p) for p in e.path)}: {e.message}" for e in errores])


def validar_evento_features(event: Dict[str, Any]) -> ValidationResult:
    """additionalProperties=false: si una etiqueta se colara en features, se rechaza."""
    return _run(_features_validator, event)


def validar_evento_ground_truth(event: Dict[str, Any]) -> ValidationResult:
    return _run(_ground_truth_validator, event)
