"""
Load an OpenAPI spec (JSON or YAML) into normalized SQLAlchemy tables.

Handles both full-load (first time) and temporal upsert (close old version,
insert new version) for incremental updates.
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import yaml
try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader
from sqlalchemy import select
from sqlalchemy.orm import Session


class _SafeJSONEncoder(json.JSONEncoder):
    """Handle datetime/date objects that yaml.safe_load produces."""

    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)

from blobapi.models import (
    ApiOperation,
    ApiParameter,
    ApiPath,
    ApiResponse,
    ApiSchema,
    ApiSpec,
)

HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}


def _normalize_json(obj):
    """
    Round-trip through JSON to normalize types that yaml.safe_load produces
    (datetime, date) into JSON-safe strings. Without this, SQLAlchemy's JSON
    column serializer chokes on non-primitive types in raw_spec.
    """
    return json.loads(json.dumps(obj, cls=_SafeJSONEncoder))


def parse_spec(source: str | Path | dict) -> dict:
    """Parse an OpenAPI spec from a file path, JSON string, or dict."""
    if isinstance(source, dict):
        return _normalize_json(source)
    if isinstance(source, Path) or (isinstance(source, str) and "\n" not in source):
        path = Path(source)
        text = path.read_text()
        if path.suffix in (".yaml", ".yml"):
            return _normalize_json(yaml.load(text, Loader=SafeLoader))
        return json.loads(text)
    # Inline string — try JSON first, then YAML
    try:
        return json.loads(source)
    except json.JSONDecodeError:
        return _normalize_json(yaml.load(source, Loader=SafeLoader))


def _close_version(session: Session, model_class, filters: dict, now: datetime):
    """Close the current version of a row by setting sys_to."""
    stmt = select(model_class).filter_by(**filters, sys_to=None)
    existing = session.execute(stmt).scalar_one_or_none()
    if existing is not None:
        existing.sys_to = now
    return existing


def load_spec(
    session: Session,
    source: str | Path | dict,
    *,
    provider: str,
    api_name: str,
    registry_id: int | None = None,
    source_url: str | None = None,
    source_updated_at: datetime | None = None,
) -> ApiSpec:
    """
    Load an OpenAPI spec into the normalized schema.

    If a current version exists for this provider+api_name, it is closed
    (sys_to set) and a new version is inserted. Only rows that actually
    changed get new versions — unchanged paths/operations/etc. are left alone.
    """
    doc = parse_spec(source)
    now = datetime.now(timezone.utc)
    info = doc.get("info", {})

    # --- Spec-level row ---
    spec_data = dict(
        provider=provider,
        api_name=api_name,
        spec_version=info.get("version"),
        title=info.get("title"),
        description=info.get("description"),
        openapi_version=doc.get("openapi") or doc.get("swagger"),
        source_url=source_url,
        source_updated_at=source_updated_at,
        raw_spec=doc,
    )

    _close_version(session, ApiSpec, dict(provider=provider, api_name=api_name), now)

    spec = ApiSpec(
        registry_id=registry_id,
        sys_from=now,
        **spec_data,
    )
    session.add(spec)
    session.flush()  # get spec_id

    # --- Paths and operations ---
    for path_str, path_item in (doc.get("paths") or {}).items():
        if path_str.startswith("x-"):
            continue

        api_path = ApiPath(
            spec_id=spec.spec_id,
            path=path_str,
            summary=path_item.get("summary"),
            description=path_item.get("description"),
            sys_from=now,
        )
        session.add(api_path)
        session.flush()

        # Path-level parameters (inherited by all operations)
        path_params = path_item.get("parameters", [])

        for method in HTTP_METHODS:
            op_data = path_item.get(method)
            if op_data is None:
                continue

            api_op = ApiOperation(
                path_id=api_path.path_id,
                method=method,
                operation_name=op_data.get("operationId"),
                summary=op_data.get("summary"),
                description=op_data.get("description"),
                deprecated=op_data.get("deprecated", False),
                tags=op_data.get("tags"),
                sys_from=now,
            )
            session.add(api_op)
            session.flush()

            # Parameters: operation-level + inherited path-level
            all_params = path_params + op_data.get("parameters", [])
            seen_params = set()
            for param in all_params:
                param_key = (param.get("name"), param.get("in"))
                if param_key in seen_params:
                    continue  # operation-level overrides path-level
                seen_params.add(param_key)

                api_param = ApiParameter(
                    operation_id=api_op.operation_id,
                    name=param.get("name", ""),
                    location=param.get("in", "query"),
                    required=param.get("required", False),
                    schema_json=param.get("schema"),
                    description=param.get("description"),
                    sys_from=now,
                )
                session.add(api_param)

            # Responses
            for status_code, resp_data in (op_data.get("responses") or {}).items():
                # Extract schema from the first media type (usually application/json)
                schema_json = None
                media_type = None
                content = resp_data.get("content") or {}
                for mt, mt_data in content.items():
                    media_type = mt
                    schema_json = mt_data.get("schema")
                    break  # take first

                # Swagger 2.x: schema at response level
                if schema_json is None:
                    schema_json = resp_data.get("schema")

                api_resp = ApiResponse(
                    operation_id=api_op.operation_id,
                    status_code=str(status_code),
                    description=resp_data.get("description"),
                    media_type=media_type,
                    schema_json=schema_json,
                    sys_from=now,
                )
                session.add(api_resp)

    # --- Component schemas ---
    components = doc.get("components", {})
    # Swagger 2.x uses "definitions" instead
    definitions = components.get("schemas") or doc.get("definitions") or {}
    for schema_name, schema_body in definitions.items():
        api_schema = ApiSchema(
            spec_id=spec.spec_id,
            schema_name=schema_name,
            schema_json=schema_body,
            description=schema_body.get("description"),
            sys_from=now,
        )
        session.add(api_schema)

    session.flush()
    return spec
