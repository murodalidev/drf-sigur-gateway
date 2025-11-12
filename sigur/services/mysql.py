from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import pymysql
from django.core.serializers.json import DjangoJSONEncoder
from pymysql import MySQLError


class MySQLServiceError(Exception):
    """Base class for MySQL service errors."""


class MySQLConfigurationError(MySQLServiceError):
    """Raised when required configuration is missing."""


class MySQLConnectionError(MySQLServiceError):
    """Raised when the connection cannot be established."""


class MySQLExecutionError(MySQLServiceError):
    """Raised when the SQL execution fails."""


class MySQLParameterError(MySQLServiceError):
    """Raised when provided query parameters do not satisfy the SQL placeholders."""

    def __init__(self, message: str, *, missing_params: Iterable[str] | None = None) -> None:
        super().__init__(message)
        self.missing_params: List[str] = list(missing_params or [])


class MySQLDatabase(str, Enum):
    MAIN = 'main'
    LOG = 'log'

    @classmethod
    def from_value(cls, value: str) -> 'MySQLDatabase':
        try:
            return cls(value)
        except ValueError as exc:
            raise MySQLConfigurationError(f"Noma'lum ma'lumotlar bazasi: {value}") from exc


@dataclass
class MySQLConfig:
    host: str
    user: str
    password: str
    database: str
    port: int
    charset: str


def _to_json_safe(data: Any) -> Any:
    """Convert database result values to JSON serializable objects."""
    return json.loads(json.dumps(data, cls=DjangoJSONEncoder))


NAMED_PARAM_PATTERN = re.compile(r"%\((?P<name>[A-Za-z_][A-Za-z0-9_]*)\)s")


def extract_named_params(raw_sql: str) -> set[str]:
    """Return the set of named placeholders present in the raw SQL."""
    return {match.group('name') for match in NAMED_PARAM_PATTERN.finditer(raw_sql)}


def _validate_params(raw_sql: str, params: Mapping[str, Any] | Sequence[Any] | None) -> Mapping[str, Any] | Sequence[Any] | None:
    """Ensure that provided params satisfy the placeholders in the SQL string."""
    required_named_params = extract_named_params(raw_sql)

    if not required_named_params:
        return params

    if params is None:
        missing_sorted = sorted(required_named_params)
        raise MySQLParameterError(
            "SQL nomlangan parametrlar talab qiladi. Quyidagilar yetishmaydi: "
            + ', '.join(missing_sorted),
            missing_params=missing_sorted,
        )

    if not isinstance(params, Mapping):
        raise MySQLParameterError("Nomlangan parametrlar uchun dict yoki mapping ko'rinishidagi `params` kutilgan.")

    missing = [name for name in required_named_params if name not in params]
    if missing:
        missing_sorted = sorted(missing)
        raise MySQLParameterError(
            "SQL bajarish uchun quyidagi parametrlar yetishmaydi: " + ', '.join(missing_sorted),
            missing_params=missing_sorted,
        )

    return params


def _collect_config(target: MySQLDatabase) -> MySQLConfig:
    missing: list[str] = []

    def _get_env(key: str, *, default: str | None = None, required: bool = False) -> str | None:
        specific_key = f"MYSQL_{target.name.upper()}_{key}"
        generic_key = f"MYSQL_{key}"

        value = os.getenv(specific_key)
        if value is not None and value != '':
            return value

        fallback = os.getenv(generic_key, default)
        if fallback is not None and fallback != '':
            return fallback

        if required:
            missing.extend([specific_key, generic_key])
        return fallback

    host = _get_env('HOST', required=True)
    user = _get_env('USER', required=True)
    database = _get_env('DATABASE', required=True)

    if missing:
        raise MySQLConfigurationError(
            "MySQL sozlamalari to'liq emas. Quyidagi muhit o'zgaruvchilaridan kamida bittasi kerakli qiymatga ega bo'lishi zarur: "
            + ', '.join(missing)
        )

    password = _get_env('PASSWORD', default='')
    port_raw = _get_env('PORT', default='3306') or '3306'
    charset = _get_env('CHARSET', default='utf8mb4') or 'utf8mb4'

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise MySQLConfigurationError(f"MYSQL_PORT butun son bo'lishi kerak. Olingan qiymat: {port_raw}") from exc

    return MySQLConfig(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        charset=charset,
    )


def execute_raw_sql(
    raw_sql: str,
    *,
    params: Mapping[str, Any] | Sequence[Any] | None = None,
    target: MySQLDatabase = MySQLDatabase.MAIN,
) -> Dict[str, Any]:
    """
    Execute a raw SQL query against the configured MySQL database.

    Returns a JSON-serialisable dictionary with either the fetched rows
    or metadata about the executed statement. Supports both positional
    (`%s`) and named (`%(name)s`) query parameters via the `params` argument.
    """
    if isinstance(target, str):
        target = MySQLDatabase.from_value(target)

    config = _collect_config(target)
    params = _validate_params(raw_sql, params)

    try:
        connection = pymysql.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            database=config.database,
            port=config.port,
            charset=config.charset,
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )
    except MySQLError as exc:
        raise MySQLConnectionError(
            f"MySQL bilan ulanishda xatolik: {exc.args[1] if len(exc.args) > 1 else exc}"
        ) from exc

    try:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(raw_sql, params)
            else:
                cursor.execute(raw_sql)

            if cursor.description:
                rows: Iterable[Dict[str, Any]] = cursor.fetchall()
                data: List[Dict[str, Any]] = [_to_json_safe(row) for row in rows]
                connection.commit()
                return {'type': 'result_set', 'rows': data, 'rowcount': len(data)}

            affected = cursor.rowcount
            last_row_id = cursor.lastrowid
            connection.commit()
            return {'type': 'ack', 'rowcount': affected, 'lastrowid': last_row_id}

    except MySQLError as exc:
        connection.rollback()
        raise MySQLExecutionError(
            f"SQL bajarishda xatolik: {exc.args[1] if len(exc.args) > 1 else exc}"
        ) from exc
    finally:
        connection.close()

