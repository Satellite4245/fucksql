from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Iterable
from typing import Any, Literal, Sequence, cast
from dataclasses import dataclass
from contextlib import asynccontextmanager
import aiosqlite

ALLOWED_TYPES = frozenset({"INTEGER", "TEXT", "REAL", "BLOB", "NULL"})
SQL_DEFAULT_KEYWORDS = frozenset({
    "CURRENT_TIME",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "NULL",
    "TRUE",
    "FALSE",
})

ALLOWED_OR_ACTIONS = frozenset({"IGNORE", "REPLACE", "ABORT", "FAIL", "ROLLBACK"})
ALLOWED_ON_ACTIONS = frozenset({"CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"})
ON_ACTION = Literal["CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"]
ORDER_DIR = Literal["ASC", "DESC"]
OR_ACTION = Literal["IGNORE", "REPLACE", "ABORT", "FAIL", "ROLLBACK"]


class _AsyncReentrantLock:
    """동일한 asyncio.Task 내에서의 재진입(Reentrancy)을 허용하는 비동기 락.
    서로 다른 코루틴 간의 동시 접근은 줄을 세우며, 동일 코루틴 내의 중첩 트랜잭션(SAVEPOINT) 및
    트랜잭션 내 개별 쿼리는 데드락 없이 즉시 허용합니다.
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task[Any] | None = None
        self._count = 0

    async def acquire(self) -> None:
        me = asyncio.current_task()
        if me is not None and self._owner is me:
            self._count += 1
            return
        await self._lock.acquire()
        self._owner = me
        self._count = 1

    def release(self) -> None:
        me = asyncio.current_task()
        if self._owner is None or (me is not None and self._owner is not me):
            raise RuntimeError("Cannot release unacquired lock")
        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    async def __aenter__(self) -> _AsyncReentrantLock:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


def _escape_identifier(name: str) -> str:
    return name.replace('"', '""')


def _parse_order_by(
        order_by: str | tuple[str, ORDER_DIR] | list[tuple[str, ORDER_DIR]] | list[str] | Sequence[Any] | None,
) -> str:
    """order_by 인자를 안전하게 파싱하여 SQL ORDER BY 절 문자열을 반환합니다.
    임의의 문자열 주입(SQL Injection)을 방지하기 위해 각 컬럼명의 식별자 유효성을 검증하고 이스케이프합니다.
    단일 튜플, 튜플의 튜플, 리스트, 문자열 등 다양한 형식을 지원합니다.
    """
    if not order_by:
        return ""

    clauses: list[str] = []

    def _parse_single(column: str, direction: str | None = None) -> str:
        col = column.strip().strip('"')
        if not col.isidentifier():
            raise ValueError(f"Invalid column name in ORDER BY: '{column}'")
        esc_col = _escape_identifier(col)
        if direction is not None:
            dir_upper = direction.strip().upper()
            if dir_upper not in ("ASC", "DESC"):
                raise ValueError(f"Direction must be 'ASC' or 'DESC', got '{direction}'")
            return f'"{esc_col}" {dir_upper}'
        return f'"{esc_col}" ASC'

    if isinstance(order_by, tuple) and len(order_by) == 2 and isinstance(order_by[0], str) and isinstance(order_by[1], str) and order_by[1].strip().upper() in ("ASC", "DESC"):
        clauses.append(_parse_single(order_by[0], order_by[1]))
    elif isinstance(order_by, (list, tuple)):
        if not order_by:
            return ""
        for item in order_by:
            if isinstance(item, (tuple, list)) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], str):
                clauses.append(_parse_single(item[0], item[1]))
            elif isinstance(item, str):
                parts = item.split()
                if len(parts) == 1:
                    clauses.append(_parse_single(parts[0]))
                elif len(parts) == 2:
                    clauses.append(_parse_single(parts[0], parts[1]))
                else:
                    raise ValueError(f"Invalid ORDER BY item: '{item}'")
            else:
                raise TypeError("Items in ORDER BY must be (col, 'ASC'|'DESC') or string.")
    elif isinstance(order_by, str):
        items = [s.strip() for s in order_by.split(",") if s.strip()]
        if not items:
            raise ValueError("ORDER BY string cannot be empty.")
        for item in items:
            parts = item.split()
            if len(parts) == 1:
                clauses.append(_parse_single(parts[0]))
            elif len(parts) == 2:
                clauses.append(_parse_single(parts[0], parts[1]))
            else:
                raise ValueError(f"Invalid ORDER BY clause segment: '{item}'")
    else:
        raise TypeError("Order_by must be a str, tuple[str, str], list, or sequence of order clauses.")

    return f"ORDER BY {', '.join(clauses)}"


@dataclass(frozen=True)
class ForeignKey:
    target_table: str
    target_column: str
    on_delete: ON_ACTION | None = None
    on_update: ON_ACTION | None = None

    def __post_init__(self):
        if not isinstance(self.target_table, str) or not self.target_table.isidentifier():
            raise ValueError(f"Invalid target table name: '{self.target_table}'")
        if not isinstance(self.target_column, str) or not self.target_column.isidentifier():
            raise ValueError(f"Invalid target column name: '{self.target_column}'")

        if self.on_delete is not None:
            del_upper = self.on_delete.upper()
            if del_upper not in ALLOWED_ON_ACTIONS:
                raise ValueError(f"Invalid on_delete action '{self.on_delete}'. Allowed: {', '.join(sorted(ALLOWED_ON_ACTIONS))}")
            object.__setattr__(self, "on_delete", del_upper)

        if self.on_update is not None:
            upd_upper = self.on_update.upper()
            if upd_upper not in ALLOWED_ON_ACTIONS:
                raise ValueError(f"Invalid on_update action '{self.on_update}'. Allowed: {', '.join(sorted(ALLOWED_ON_ACTIONS))}")
            object.__setattr__(self, "on_update", upd_upper)

    def to_sql(self) -> str:
        esc_tbl = _escape_identifier(self.target_table)
        esc_col = _escape_identifier(self.target_column)
        sql_cmd = f'REFERENCES "{esc_tbl}"("{esc_col}")'
        if self.on_delete:
            sql_cmd += f" ON DELETE {self.on_delete}"
        if self.on_update:
            sql_cmd += f" ON UPDATE {self.on_update}"
        return sql_cmd


def _format_default_value(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        if not math.isfinite(val):
            raise ValueError(f"Float default value must be finite (not inf, -inf, or nan), got {val}")
        return str(val)
    if isinstance(val, str):
        if val.upper() in SQL_DEFAULT_KEYWORDS:
            return val.upper()
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(val, bytes):
        return f"X'{val.hex().upper()}'"
    raise TypeError(
        f"Unsupported default value type: {type(val).__name__}. "
        f"Allowed: None, bool, int, float, str, bytes."
    )


@dataclass(frozen=True)
class Column:
    name: str
    data_type: Literal["INTEGER", "TEXT", "REAL", "BLOB", "NULL"]
    primary_key: bool = False
    autoincrement: bool = False
    not_null: bool = False
    unique: bool = False
    default: Any = None
    check: str | None = None
    foreign_key: ForeignKey | None = None

    def __post_init__(self):
        if not isinstance(self.name, str):
            raise TypeError(f"Column name must be a string, got {type(self.name).__name__}.")
        if not self.name.isidentifier():
            raise ValueError(f"Invalid column name: '{self.name}'")
        if not isinstance(self.data_type, str):
            raise TypeError(f"Data type must be a string, got {type(self.data_type).__name__}.")

        upper_type = self.data_type.upper()
        if upper_type not in ALLOWED_TYPES:
            raise ValueError(
                f"Invalid data type '{self.data_type}'. Allowed types: {', '.join(sorted(ALLOWED_TYPES))}."
            )
        object.__setattr__(self, "data_type", upper_type)

        if self.autoincrement and (not self.primary_key or self.data_type != "INTEGER"):
            raise ValueError("AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY.")

    def to_sql(self) -> str:
        esc_name = _escape_identifier(self.name)
        sql_cmd = [f'"{esc_name}"', self.data_type]
        if self.primary_key:
            sql_cmd.append("PRIMARY KEY")
            if self.autoincrement:
                sql_cmd.append("AUTOINCREMENT")
        if self.not_null:
            sql_cmd.append("NOT NULL")
        if self.unique:
            sql_cmd.append("UNIQUE")

        if self.default is not None:
            sql_cmd.append(f"DEFAULT {_format_default_value(self.default)}")

        if self.check:
            sql_cmd.append(f"CHECK ({self.check})")
        if self.foreign_key:
            sql_cmd.append(self.foreign_key.to_sql())
        return " ".join(sql_cmd)


class FUCKsqlite:
    def __init__(
            self,
            db_name: str,
            use_foreign_key: bool = False,
            busy_timeout_ms: int = 5000,
            autocommit: bool = True,
    ):
        if isinstance(busy_timeout_ms, bool) or not isinstance(busy_timeout_ms, int) or busy_timeout_ms < 0:
            raise TypeError(f"busy_timeout_ms must be a non-negative integer, got {busy_timeout_ms!r}")

        self.db_name = db_name
        self.use_foreign_key = use_foreign_key
        self.busy_timeout_ms = busy_timeout_ms
        self.autocommit = autocommit
        self.conn: aiosqlite.Connection | None = None
        self._in_transaction = False
        self._savepoint_count = 0
        self._lock = _AsyncReentrantLock()

    def _get_connection(self) -> aiosqlite.Connection:
        """연결 상태를 검증하고 aiosqlite.Connection 인스턴스를 반환합니다."""
        if self.conn is None:
            raise RuntimeError(f"Database is not connected. Database Name: {self.db_name}")
        return self.conn

    async def connect(self) -> None:
        if self.conn is not None:
            return
        self.conn = await aiosqlite.connect(self.db_name)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        if self.use_foreign_key:
            await self.conn.execute("PRAGMA foreign_keys=ON")
        await self.conn.commit()

    async def commit(self) -> None:
        async with self._lock:
            conn = self._get_connection()
            await conn.commit()

    async def rollback(self) -> None:
        async with self._lock:
            conn = self._get_connection()
            await conn.rollback()

    async def close(self) -> None:
        async with self._lock:
            if self.conn is not None:
                if not self._in_transaction:
                    try:
                        await self.conn.commit()
                    except Exception:
                        pass
                await self.conn.close()
                self.conn = None
                self._in_transaction = False
                self._savepoint_count = 0

    async def __aenter__(self) -> FUCKsqlite:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @asynccontextmanager
    async def transaction(self):
        """에러 발생 또는 비동기 태스크 취소 시 ROLLBACK, 정상 완료 시 COMMIT을 보장하는 트랜잭션 컨텍스트 매니저.
        비동기 재진입 락(_AsyncReentrantLock)을 통해 동시성 꼬임과 중첩 트랜잭션 데드락을 방지하며,
        중첩 호출 시 SAVEPOINT를 활용하여 중첩 트랜잭션을 안전하게 지원합니다.
        """
        async with self._lock:
            conn = self._get_connection()
            if self._in_transaction:
                # 중첩 트랜잭션: SAVEPOINT 활용
                self._savepoint_count += 1
                sp_name = f"sp_{self._savepoint_count}"
                await conn.execute(f'SAVEPOINT "{sp_name}"')
                try:
                    yield self
                    await conn.execute(f'RELEASE SAVEPOINT "{sp_name}"')
                except BaseException:
                    await conn.execute(f'ROLLBACK TO SAVEPOINT "{sp_name}"')
                    raise
            else:
                # 최상위 트랜잭션
                self._in_transaction = True
                try:
                    yield self
                    await conn.commit()
                except BaseException:
                    await conn.rollback()
                    raise
                finally:
                    self._in_transaction = False

    async def _auto_commit_if_needed(self) -> None:
        if self.autocommit and not self._in_transaction:
            conn = self._get_connection()
            await conn.commit()

    def _normalize_params(
            self,
            params: Sequence[Any] | dict[str, Any] | Any | None,
    ) -> Sequence[Any] | dict[str, Any]:
        """params가 None, dict, 단일 원소(int, str 등), 또는 Sequence/Iterable일 때 바인딩 가능한 형식으로 정규화합니다."""
        if params is None:
            return ()
        if isinstance(params, (list, tuple, dict)):
            return params
        if isinstance(params, (str, bytes)):
            return (params,)
        if isinstance(params, Iterable):
            return tuple(params)
        return (params,)

    def _build_where_clause(
            self,
            where: str | None,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
    ) -> tuple[str, Sequence[Any] | dict[str, Any]]:
        """where Raw SQL 문자열과 파라미터를 파싱하여 WHERE SQL 절과 바인딩할 파라미터 시퀀스/딕셔너리를 반환합니다."""
        if not where:
            return "", ()

        if not isinstance(where, str):
            raise TypeError(f"where must be a raw SQL string, got {type(where).__name__}")

        where_str = where.strip()
        if not where_str:
            return "", ()

        clause = where_str if re.match(r"^where\s+", where_str, re.IGNORECASE) else f"WHERE {where_str}"
        val_list = self._normalize_params(params)
        return clause, val_list

    async def create_table(
            self,
            table_name: str,
            columns: list[Column],
            if_not_exists: bool = True,
    ) -> None:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not columns:
            raise TypeError("At least one column must be provided.")
        if not all(isinstance(column, Column) for column in columns):
            raise TypeError("All items in columns must be of type Column.")

        async with self._lock:
            conn = self._get_connection()
            if_not_exists_cmd = "IF NOT EXISTS " if if_not_exists else ""
            cols_def = ", ".join(col.to_sql() for col in columns)

            esc_tbl = _escape_identifier(table_name)
            sql_cmd = f'CREATE TABLE {if_not_exists_cmd}"{esc_tbl}" ({cols_def})'
            await conn.execute(sql_cmd)
            await self._auto_commit_if_needed()

    async def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        async with self._lock:
            conn = self._get_connection()
            if_exists_cmd = "IF EXISTS " if if_exists else ""
            esc_tbl = _escape_identifier(table_name)
            sql_cmd = f'DROP TABLE {if_exists_cmd}"{esc_tbl}"'

            await conn.execute(sql_cmd)
            await self._auto_commit_if_needed()

    async def insert(
            self,
            table_name: str,
            data: dict[str, Any],
            or_action: OR_ACTION | None = None,
    ) -> int | None:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not data:
            raise ValueError("Data dictionary cannot be empty.")

        for col in data.keys():
            if not isinstance(col, str) or not col.isidentifier():
                raise ValueError(f"Invalid column name: '{col}'")

        if or_action is not None:
            if not isinstance(or_action, str) or or_action.upper() not in ALLOWED_OR_ACTIONS:
                raise ValueError(f"Invalid or_action '{or_action}'. Allowed: {', '.join(sorted(ALLOWED_OR_ACTIONS))}")

        async with self._lock:
            conn = self._get_connection()
            columns = list(data.keys())
            values = list(data.values())

            cols_str = ", ".join(f'"{_escape_identifier(col)}"' for col in columns)
            placeholders = ", ".join("?" for _ in columns)

            or_cmd = f"OR {or_action.upper()} " if or_action else ""
            esc_tbl = _escape_identifier(table_name)
            sql_cmd = f'INSERT {or_cmd}INTO "{esc_tbl}" ({cols_str}) VALUES ({placeholders})'

            cur = await conn.execute(sql_cmd, values)
            await self._auto_commit_if_needed()
            return cur.lastrowid

    async def inserts(self, table_name: str, data_list: list[dict[str, Any]]) -> int:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not isinstance(data_list, list):
            raise TypeError(f"Data list must be a list, got {type(data_list).__name__}")
        if not data_list:
            return 0

        for row in data_list:
            if not isinstance(row, dict):
                raise TypeError(f"All items in data_list must be dicts, got {type(row).__name__}")
            if not row:
                raise ValueError("Row dictionary in data_list cannot be empty.")
            for col in row.keys():
                if not isinstance(col, str) or not col.isidentifier():
                    raise ValueError(f"Invalid column name: '{col}'")

        async with self._lock:
            conn = self._get_connection()

            # O(N) 순서 보존 컬럼 추출
            columns = list(dict.fromkeys(k for row in data_list for k in row))
            if not columns:
                raise ValueError("No valid columns found in data_list.")

            cols_str = ", ".join(f'"{_escape_identifier(col)}"' for col in columns)
            placeholders = ", ".join("?" for _ in columns)

            values = [tuple(row.get(col) for col in columns) for row in data_list]

            esc_tbl = _escape_identifier(table_name)
            sql_cmd = f'INSERT INTO "{esc_tbl}" ({cols_str}) VALUES ({placeholders})'
            cur = await conn.executemany(sql_cmd, values)
            await self._auto_commit_if_needed()
            return cur.rowcount

    async def select(
            self,
            table_name: str,
            columns: list[str] | tuple[str, ...] | None = None,
            where: str | None = None,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
            order_by: str | tuple[str, ORDER_DIR] | list[tuple[str, ORDER_DIR]] | list[str] | Sequence[Any] | None = None,
            limit: int | None = None,
            offset: int | None = None,
    ) -> list[dict[str, Any]]:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        if columns is not None:
            if not isinstance(columns, (list, tuple)):
                raise TypeError(f"columns must be a list or tuple of strings, got {type(columns).__name__}")
            for col in columns:
                if not isinstance(col, str) or not col.isidentifier():
                    raise ValueError(f"Invalid column name: '{col}'")

        esc_tbl = _escape_identifier(table_name)
        cols = ", ".join(f'"{_escape_identifier(col)}"' for col in columns) if columns else "*"
        sql_cmd = [f'SELECT {cols} FROM "{esc_tbl}"']

        where_clause, values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)

        order_clause = _parse_order_by(order_by)
        if order_clause:
            sql_cmd.append(order_clause)

        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError("Limit must be a non-negative integer.")
            sql_cmd.append(f"LIMIT {limit}")

            if offset is not None:
                if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                    raise ValueError("Offset must be a non-negative integer.")
                sql_cmd.append(f"OFFSET {offset}")
        elif offset is not None:
            if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                raise ValueError("Offset must be a non-negative integer.")
            sql_cmd.append(f"LIMIT -1 OFFSET {offset}")

        full_sql = " ".join(sql_cmd)

        async with self._lock:
            conn = self._get_connection()
            async with conn.execute(full_sql, values) as cur:
                rows = await cur.fetchall()
                return cast(list[dict[str, Any]], [dict(row) for row in rows])

    async def select_one(
            self,
            table_name: str,
            columns: list[str] | tuple[str, ...] | None = None,
            where: str | None = None,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
            order_by: str | tuple[str, ORDER_DIR] | list[tuple[str, ORDER_DIR]] | list[str] | Sequence[Any] | None = None,
    ) -> dict[str, Any] | None:
        results = await self.select(
            table_name=table_name,
            columns=columns,
            where=where,
            params=params,
            order_by=order_by,
            limit=1,
        )
        return results[0] if results else None

    async def update(
            self,
            table_name: str,
            data: dict[str, Any],
            where: str | None = None,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
            or_action: OR_ACTION | None = None,
            allow_all: bool = False,
    ) -> int:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not data:
            raise ValueError("Data dictionary cannot be empty.")

        for col in data.keys():
            if not isinstance(col, str) or not col.isidentifier():
                raise ValueError(f"Invalid column name: '{col}'")

        if not (where and where.strip()) and not allow_all:
            raise ValueError("Where is required to update the table. Or enable allow_all=True.")

        if or_action is not None:
            if not isinstance(or_action, str) or or_action.upper() not in ALLOWED_OR_ACTIONS:
                raise ValueError(f"Invalid or_action '{or_action}'. Allowed: {', '.join(sorted(ALLOWED_OR_ACTIONS))}")

        cols = []
        values = []
        for col, val in data.items():
            esc_col = _escape_identifier(col)
            cols.append(f'"{esc_col}" = ?')
            values.append(val)

        or_cmd = f"OR {or_action.upper()} " if or_action else ""
        esc_tbl = _escape_identifier(table_name)
        sql_cmd = [f'UPDATE {or_cmd}"{esc_tbl}" SET {", ".join(cols)}']

        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)
            if isinstance(where_values, dict):
                raise TypeError("Named dict parameters are not supported together with qmark positional update values.")
            values.extend(where_values)

        full_sql = " ".join(sql_cmd)

        async with self._lock:
            conn = self._get_connection()
            cur = await conn.execute(full_sql, values)
            await self._auto_commit_if_needed()
            return cur.rowcount

    async def delete(
            self,
            table_name: str,
            where: str | None = None,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
            allow_all: bool = False,
    ) -> int:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not (where and where.strip()) and not allow_all:
            raise ValueError("Where is required to delete the table. Or enable allow_all=True.")

        sql_cmd = [f'DELETE FROM "{_escape_identifier(table_name)}"']

        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)

        full_sql = " ".join(sql_cmd)

        async with self._lock:
            conn = self._get_connection()
            cur = await conn.execute(full_sql, where_values)
            await self._auto_commit_if_needed()
            return cur.rowcount

    async def execute(
            self,
            sql_cmd: str,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
    ) -> aiosqlite.Cursor:
        if not isinstance(sql_cmd, str):
            raise TypeError(f"sql_cmd must be a string, got {type(sql_cmd).__name__}")

        val_list = self._normalize_params(params)
        async with self._lock:
            conn = self._get_connection()
            cur = await conn.execute(sql_cmd, val_list)
            await self._auto_commit_if_needed()
            return cur

    async def fetch(
            self,
            sql_cmd: str,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
    ) -> list[dict[str, Any]]:
        if not isinstance(sql_cmd, str):
            raise TypeError(f"sql_cmd must be a string, got {type(sql_cmd).__name__}")

        val_list = self._normalize_params(params)
        async with self._lock:
            conn = self._get_connection()
            async with conn.execute(sql_cmd, val_list) as cur:
                rows = await cur.fetchall()
                return cast(list[dict[str, Any]], [dict(row) for row in rows])

    async def fetch_one(
            self,
            sql_cmd: str,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(sql_cmd, str):
            raise TypeError(f"sql_cmd must be a string, got {type(sql_cmd).__name__}")

        val_list = self._normalize_params(params)
        async with self._lock:
            conn = self._get_connection()
            async with conn.execute(sql_cmd, val_list) as cur:
                row = await cur.fetchone()
                return dict(row) if row is not None else None

    async def count(
            self,
            table_name: str,
            columns: str | None = None,
            distinct: bool = False,
            where: str | None = None,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
    ) -> int:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not columns and distinct:
            raise ValueError("DISTINCT aggregate must have exactly one argument")
        if columns is not None and not isinstance(columns, str):
            raise TypeError(f"columns must be a string or None, got {type(columns).__name__}")

        if columns:
            if not columns.isidentifier():
                raise ValueError(f"Invalid column name: {columns}")
            col_target = f'COUNT({"DISTINCT " if distinct else ""}"{_escape_identifier(columns)}")'
        else:
            col_target = "COUNT(*)"

        sql_cmd = [f'SELECT {col_target} AS cnt FROM "{_escape_identifier(table_name)}"']

        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)

        full_sql = " ".join(sql_cmd)

        async with self._lock:
            conn = self._get_connection()
            async with conn.execute(full_sql, where_values) as cur:
                row = await cur.fetchone()
                return row["cnt"] if row else 0

    async def exists(
            self,
            table_name: str,
            where: str | None = None,
            params: Sequence[Any] | dict[str, Any] | Any | None = None,
    ) -> bool:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        sql_cmd = [f'SELECT 1 FROM "{_escape_identifier(table_name)}"']
        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)
        sql_cmd.append("LIMIT 1")

        full_sql = " ".join(sql_cmd)

        async with self._lock:
            conn = self._get_connection()
            async with conn.execute(full_sql, where_values) as cur:
                row = await cur.fetchone()
                return row is not None

    async def create_index(
            self,
            index_name: str,
            table_name: str,
            columns: str | list[str] | tuple[str, ...],
            unique: bool = False,
            if_not_exists: bool = True,
    ) -> None:
        if not isinstance(index_name, str) or not index_name.isidentifier():
            raise ValueError(f"Invalid index name: {index_name}")
        if not isinstance(table_name, str) or not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        col_list = [columns] if isinstance(columns, str) else list(columns)
        if not col_list:
            raise ValueError("At least one column must be provided for index")
        for col in col_list:
            if not isinstance(col, str) or not col.isidentifier():
                raise ValueError(f"Invalid column name for index: '{col}'")

        unique_cmd = "UNIQUE " if unique else ""
        if_not_exists_cmd = "IF NOT EXISTS " if if_not_exists else ""

        esc_idx = _escape_identifier(index_name)
        esc_tbl = _escape_identifier(table_name)
        cols_str = ", ".join(f'"{_escape_identifier(c)}"' for c in col_list)

        sql_cmd = f'CREATE {unique_cmd}INDEX {if_not_exists_cmd}"{esc_idx}" ON "{esc_tbl}" ({cols_str})'

        async with self._lock:
            conn = self._get_connection()
            await conn.execute(sql_cmd)
            await self._auto_commit_if_needed()

    async def drop_index(self, index_name: str, if_exists: bool = True) -> None:
        if not isinstance(index_name, str) or not index_name.isidentifier():
            raise ValueError(f"Invalid index name: {index_name}")

        if_exists_cmd = "IF EXISTS " if if_exists else ""
        esc_idx = _escape_identifier(index_name)

        sql_cmd = f'DROP INDEX {if_exists_cmd}"{esc_idx}"'

        async with self._lock:
            conn = self._get_connection()
            await conn.execute(sql_cmd)
            await self._auto_commit_if_needed()

    async def table_exists(self, table_name: str) -> bool:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        return await self.exists(
            table_name="sqlite_master",
            where="type='table' AND name = ?",
            params=table_name,
        )

    async def list_tables(self) -> list[str]:
        rows = await self.fetch(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row["name"] for row in rows]
