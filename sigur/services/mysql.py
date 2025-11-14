from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

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
    """Convert database result values to JSON serializable objects.
    
    Bytes objects are automatically converted to base64-encoded strings
    to make them JSON serializable (useful for binary data like photos).
    """
    if isinstance(data, bytes):
        # Convert bytes to base64 string for JSON serialization
        return base64.b64encode(data).decode('utf-8')
    elif isinstance(data, dict):
        return {key: _to_json_safe(value) for key, value in data.items()}
    elif isinstance(data, (list, tuple)):
        return [_to_json_safe(item) for item in data]
    else:
        # For other types, use Django's JSON encoder which handles dates, decimals, etc.
        try:
            return json.loads(json.dumps(data, cls=DjangoJSONEncoder))
        except (TypeError, ValueError):
            # Fallback: convert to string if still not serializable
            return str(data)


NAMED_PARAM_PATTERN = re.compile(r"%\((?P<name>[A-Za-z_][A-Za-z0-9_]*)\)s")
COLON_PARAM_PATTERN = re.compile(r"(?<!:):(?P<name>[A-Za-z_][A-Za-z0-9_]*)")
POSITIONAL_PARAM_PATTERN = re.compile(r"(?<!%)%(?!\()s")


def extract_named_params(raw_sql: str) -> set[str]:
    """Return the set of named placeholders present in the raw SQL."""
    return {match.group('name') for match in NAMED_PARAM_PATTERN.finditer(raw_sql)}


def analyse_placeholders(raw_sql: str) -> Tuple[str, set[str], int]:
    """
    Convert alternative placeholder styles to PyMySQL format and detect named/positional usage.

    Returns a tuple of (normalised_sql, named_params, positional_count).
    """
    normalised_sql = COLON_PARAM_PATTERN.sub(lambda m: f"%({m.group('name')})s", raw_sql)
    named_params = extract_named_params(normalised_sql)
    positional_count = len(POSITIONAL_PARAM_PATTERN.findall(normalised_sql))

    if named_params and positional_count:
        raise MySQLParameterError(
            "API  so'rovda nomlangan (`%(name)s`) va pozitsion (`%s`) parametrlar aralashtirilgan. "
            "Iltimos faqat nomlangan parametr uslubidan foydalaning."
        )

    return normalised_sql, named_params, positional_count


def get_required_named_params(raw_sql: str) -> List[str]:
    """Return sorted list of required named parameters for the given SQL."""
    _, params, _ = analyse_placeholders(raw_sql)
    return sorted(params)


def _validate_params(
    normalised_sql: str,
    required_named_params: set[str],
    positional_count: int,
    params: Mapping[str, Any] | Sequence[Any] | None,
) -> Mapping[str, Any] | Sequence[Any] | None:
    """Ensure that provided params satisfy the placeholders in the SQL string."""
    if positional_count:
        if params is None:
            raise MySQLParameterError(
                f"API bajarish uchun {positional_count} ta pozitsion parametr talab etiladi, "
                "ammo hech qanday parametr yuborilmadi."
            )

        if isinstance(params, Mapping):
            ordered_values: List[Any] = list(params.values())
        elif isinstance(params, Sequence) and not isinstance(params, (str, bytes)):
            ordered_values = list(params)
        else:
            raise MySQLParameterError(
                "Pozitsion parametrlar uchun ro'yxat yoki tuple ko'rinishidagi `params` kutilgan."
            )

        if len(ordered_values) != positional_count:
            raise MySQLParameterError(
                f"API bajarish uchun {positional_count} ta pozitsion parametr talab etiladi, "
                f"ammo {len(ordered_values)} ta qiymat yuborildi."
            )

        return tuple(ordered_values)

    if not required_named_params:
        if params:
            extras: str
            if isinstance(params, Mapping):
                extras = ', '.join(sorted(str(key) for key in params.keys()))
            else:
                extras = f"{len(params)} ta pozitsion qiymat"
            raise MySQLParameterError(
                "Ushbu API so'rov parametrlarni qabul qilmaydi, ammo quyidagilar yuborildi: " + extras
            )
        return None

    if params is None:
        missing_sorted = sorted(required_named_params)
        raise MySQLParameterError(
            "API nomlangan parametrlar talab qiladi. Quyidagilar yetishmaydi: "
            + ', '.join(missing_sorted),
            missing_params=missing_sorted,
        )

    if not isinstance(params, Mapping):
        raise MySQLParameterError("Nomlangan parametrlar uchun dict yoki mapping ko'rinishidagi `params` kutilgan.")

    missing = [name for name in required_named_params if name not in params]
    if missing:
        missing_sorted = sorted(missing)
        raise MySQLParameterError(
            "API bajarish uchun quyidagi parametrlar yetishmaydi: " + ', '.join(missing_sorted),
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

    normalised_sql, required_named_params, positional_count = analyse_placeholders(raw_sql)
    params = _validate_params(normalised_sql, required_named_params, positional_count, params)

    config = _collect_config(target)

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
                cursor.execute(normalised_sql, params)
            else:
                cursor.execute(normalised_sql)

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
            f"API bajarishda xatolik: {exc.args[1] if len(exc.args) > 1 else exc}"
        ) from exc
    finally:
        connection.close()

