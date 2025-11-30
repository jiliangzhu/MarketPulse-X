from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status
import yaml

ALLOWED_RULE_KEYS = {"type", "name", "enabled", "params", "outputs", "tags", "description"}
REQUIRED_RULE_KEYS = {"type", "name", "outputs"}


def validate_rule_payload(rule_yaml: str, max_bytes: int) -> dict[str, Any]:
    if len(rule_yaml.encode("utf-8")) > max_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="rule payload too large",
        )
    try:
        rule_dict = yaml.safe_load(rule_yaml) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - safety net
        raise HTTPException(status_code=400, detail="invalid yaml payload") from exc
    if not isinstance(rule_dict, dict):
        raise HTTPException(status_code=400, detail="rule payload must be a mapping")
    missing = REQUIRED_RULE_KEYS - rule_dict.keys()
    if missing:
        raise HTTPException(status_code=400, detail=f"missing required fields: {', '.join(sorted(missing))}")
    extra_keys = set(rule_dict.keys()) - ALLOWED_RULE_KEYS
    if extra_keys:
        raise HTTPException(status_code=400, detail=f"unsupported fields: {', '.join(sorted(extra_keys))}")
    outputs = rule_dict.get("outputs")
    if not isinstance(outputs, dict):
        raise HTTPException(status_code=400, detail="outputs must be an object")
    if "level" not in outputs:
        raise HTTPException(status_code=400, detail="outputs.level is required")
    params = rule_dict.get("params")
    if params is not None and not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="params must be an object if provided")
    return rule_dict
