from __future__ import annotations

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

ON_ACTION = Literal["CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"]
ORDER_DIR = Literal["ASC", "DESC"]
OR_ACTION = Literal["IGNORE", "REPLACE", "ABORT", "FAIL", "ROLLBACK"]


def _escape_identifier(name: str) -> str:
    return name.replace('"', '""')


@dataclass(frozen=True)
class ForeignKey:
    target_table: str
    target_column: str
    on_delete: ON_ACTION | None = None
    on_update: ON_ACTION | None = None

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
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, str):
        if val.upper() in SQL_DEFAULT_KEYWORDS:
            return val.upper()
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(val, bytes):
        return f"X'{val.hex().upper()}'"
    return f"({str(val)})"


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
        self.db_name = db_name
        self.use_foreign_key = use_foreign_key
        self.busy_timeout_ms = busy_timeout_ms
        self.autocommit = autocommit
        self.conn: aiosqlite.Connection | None = None
        self._in_transaction = False

    def _get_connection(self) -> aiosqlite.Connection:
        """연결 상태를 검증하고 aiosqlite.Connection 인스턴스를 반환합니다."""
        if self.conn is None:
            raise RuntimeError(f"Database is not connected. Database Name: {self.db_name}")
        return self.conn

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.db_name)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        if self.use_foreign_key:
            await self.conn.execute("PRAGMA foreign_keys=ON")
        await self.conn.commit()

    async def commit(self) -> None:
        conn = self._get_connection()
        await conn.commit()

    async def rollback(self) -> None:
        conn = self._get_connection()
        await conn.rollback()

    async def close(self) -> None:
        if self.conn is not None:
            if not self._in_transaction:
                try:
                    await self.conn.commit()
                except Exception:
                    pass
            await self.conn.close()
            self.conn = None
            self._in_transaction = False

    async def __aenter__(self) -> FUCKsqlite:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @asynccontextmanager
    async def transaction(self):
        """에러 발생 시 ROLLBACK, 정상 완료 시 COMMIT을 보장하는 트랜잭션 컨텍스트 매니저."""
        conn = self._get_connection()
        prev_in_tx = self._in_transaction
        self._in_transaction = True
        try:
            yield self
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            self._in_transaction = prev_in_tx

    async def _auto_commit_if_needed(self) -> None:
        if self.autocommit and not self._in_transaction:
            conn = self._get_connection()
            await conn.commit()

    def _normalize_params(self, params: Sequence[Any] | Any | None) -> Sequence[Any]:
        """params가 None, 단일 원소(int, str 등), 또는 Sequence(list, tuple)일 때 바인딩 가능한 Sequence로 정규화합니다."""
        if params is None:
            return ()
        if isinstance(params, (list, tuple)):
            return params
        if isinstance(params, set):
            return list(params)
        return (params,)

    def _build_where_clause(
            self,
            where: str | None,
            params: Sequence[Any] | Any | None = None,
    ) -> tuple[str, Sequence[Any]]:
        """where Raw SQL 문자열과 파라미터를 파싱하여 WHERE SQL 절과 바인딩할 파라미터 시퀀스를 반환합니다."""
        if not where:
            return "", ()

        if not isinstance(where, str):
            raise TypeError(f"where must be a raw SQL string, got {type(where).__name__}")

        where_str = where.strip()
        if not where_str:
            return "", ()

        clause = where_str if where_str.upper().startswith("WHERE ") else f"WHERE {where_str}"
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

        conn = self._get_connection()
        columns = list(data.keys())
        values = list(data.values())

        cols_str = ", ".join(f'"{_escape_identifier(col)}"' for col in columns)
        placeholders = ", ".join("?" for _ in columns)

        or_cmd = f"OR {or_action} " if or_action else ""
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

        conn = self._get_connection()

        # O(N) 순서 보존 컬럼 추출
        columns = list(dict.fromkeys(k for row in data_list for k in row))
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
            columns: list[str] | None = None,
            where: str | None = None,
            params: Sequence[Any] | Any | None = None,
            order_by: str | tuple[str, ORDER_DIR] | list[tuple[str, ORDER_DIR]] | None = None,
            limit: int | None = None,
            offset: int | None = None,
    ) -> list[dict[str, Any]]:
        conn = self._get_connection()

        esc_tbl = _escape_identifier(table_name)
        cols = ", ".join(f'"{_escape_identifier(col)}"' for col in columns) if columns else "*"
        sql_cmd = [f'SELECT {cols} FROM "{esc_tbl}"']

        where_clause, values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)

        if order_by:
            if isinstance(order_by, tuple) and len(order_by) == 2 and isinstance(order_by[0], str):
                order_list = [order_by]
            elif isinstance(order_by, list):
                order_list = order_by
            elif isinstance(order_by, str):
                sql_cmd.append(f"ORDER BY {order_by}")
                order_list = None
            else:
                raise TypeError("Order_by must be a tuple[str, str], list[tuple[str, str]], or str.")

            if order_list is not None:
                order_clauses = []
                for i in order_list:
                    if not (isinstance(i, tuple) and len(i) == 2):
                        raise ValueError("Each order item must be a tuple of (column_name, 'ASC'|'DESC').")
                    column, direction = i
                    direction_upper = direction.upper()
                    if direction_upper not in ("ASC", "DESC"):
                        raise ValueError(f"Direction must be 'ASC' or 'DESC', got '{direction}'")
                    esc_col = _escape_identifier(column)
                    order_clauses.append(f'"{esc_col}" {direction_upper}')
                sql_cmd.append(f"ORDER BY {', '.join(order_clauses)}")

        if limit is not None:
            if not isinstance(limit, int) or limit < 0:
                raise ValueError("Limit must be a non-negative integer.")
            sql_cmd.append(f"LIMIT {limit}")

            if offset is not None:
                if not isinstance(offset, int) or offset < 0:
                    raise ValueError("Offset must be a non-negative integer.")
                sql_cmd.append(f"OFFSET {offset}")

        full_sql = " ".join(sql_cmd)

        async with conn.execute(full_sql, values) as cur:
            rows = await cur.fetchall()
            return cast(list[dict[str, Any]], [dict(row) for row in rows])

    async def select_one(
            self,
            table_name: str,
            columns: list[str] | None = None,
            where: str | None = None,
            params: Sequence[Any] | Any | None = None,
            order_by: str | tuple[str, ORDER_DIR] | list[tuple[str, ORDER_DIR]] | None = None,
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
            params: Sequence[Any] | Any | None = None,
            or_action: OR_ACTION | None = None,
            allow_all: bool = False,
    ) -> int:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not data:
            raise ValueError("Data dictionary cannot be empty.")
        if not where and not allow_all:
            raise ValueError("Where is required to update the table. Or enable allow_all=True.")

        conn = self._get_connection()

        cols = []
        values = []
        for col, val in data.items():
            esc_col = _escape_identifier(col)
            cols.append(f'"{esc_col}" = ?')
            values.append(val)

        or_cmd = f"OR {or_action} " if or_action else ""
        esc_tbl = _escape_identifier(table_name)
        sql_cmd = [f'UPDATE {or_cmd}"{esc_tbl}" SET {", ".join(cols)}']

        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)
            values.extend(where_values)

        full_sql = " ".join(sql_cmd)
        cur = await conn.execute(full_sql, values)
        await self._auto_commit_if_needed()
        return cur.rowcount

    async def delete(
            self,
            table_name: str,
            where: str | None = None,
            params: Sequence[Any] | Any | None = None,
            allow_all: bool = False,
    ) -> int:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not where and not allow_all:
            raise ValueError("Where is required to delete the table. Or enable allow_all=True.")

        conn = self._get_connection()

        sql_cmd = [f'DELETE FROM "{_escape_identifier(table_name)}"']

        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)

        full_sql = " ".join(sql_cmd)

        cur = await conn.execute(full_sql, where_values)
        await self._auto_commit_if_needed()
        return cur.rowcount

    async def execute(
            self,
            sql_cmd: str,
            params: Sequence[Any] | Any | None = None,
    ) -> aiosqlite.Cursor:
        if not isinstance(sql_cmd, str):
            raise TypeError(f"sql_cmd must be a string, got {type(sql_cmd).__name__}")

        conn = self._get_connection()
        val_list = self._normalize_params(params)
        cur = await conn.execute(sql_cmd, val_list)
        await self._auto_commit_if_needed()
        return cur

    async def fetch(
            self,
            sql_cmd: str,
            params: Sequence[Any] | Any | None = None,
    ) -> list[dict[str, Any]]:
        if not isinstance(sql_cmd, str):
            raise TypeError(f"sql_cmd must be a string, got {type(sql_cmd).__name__}")

        conn = self._get_connection()
        val_list = self._normalize_params(params)
        async with conn.execute(sql_cmd, val_list) as cur:
            rows = await cur.fetchall()
            return cast(list[dict[str, Any]], [dict(row) for row in rows])

    async def fetch_one(
            self,
            sql_cmd: str,
            params: Sequence[Any] | Any | None = None,
    ) -> dict[str, Any] | None:
        result = await self.fetch(sql_cmd, params)
        return result[0] if result else None

    async def count(
            self,
            table_name: str,
            columns: str | None = None,
            distinct: bool = False,
            where: str | None = None,
            params: Sequence[Any] | Any | None = None,
    ) -> int:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not columns and distinct:
            raise ValueError("DISTINCT aggregate must have exactly one argument")
        if columns is not None and not isinstance(columns, str):
            raise TypeError(f"columns must be a string or None, got {type(columns).__name__}")

        conn = self._get_connection()

        if columns:
            col_target = f'COUNT({"DISTINCT " if distinct else ""}"{_escape_identifier(columns)}")'
        else:
            col_target = "COUNT(*)"

        sql_cmd = [f'SELECT {col_target} AS cnt FROM "{_escape_identifier(table_name)}"']

        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)

        full_sql = " ".join(sql_cmd)

        async with conn.execute(full_sql, where_values) as cur:
            row = await cur.fetchone()
            return row["cnt"] if row else 0

    async def exists(
            self,
            table_name: str,
            where: str | None = None,
            params: Sequence[Any] | Any | None = None,
    ) -> bool:
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name).__name__}")
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        conn = self._get_connection()

        sql_cmd = [f'SELECT 1 FROM "{_escape_identifier(table_name)}"']
        where_clause, where_values = self._build_where_clause(where, params)
        if where_clause:
            sql_cmd.append(where_clause)
        sql_cmd.append("LIMIT 1")

        full_sql = " ".join(sql_cmd)
        async with conn.execute(full_sql, where_values) as cur:
            row = await cur.fetchone()
            return row is not None

    async def create_index(
            self,
            index_name: str,
            table_name: str,
            columns: str | list[str],
            unique: bool = False,
            if_not_exists: bool = True,
    ) -> None:
        if not isinstance(index_name, str) or not index_name.isidentifier():
            raise ValueError(f"Invalid index name: {index_name}")
        if not isinstance(table_name, str) or not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        conn = self._get_connection()

        col_list = [columns] if isinstance(columns, str) else list(columns)
        if not col_list:
            raise ValueError("At least one column must be provided for index")

        unique_cmd = "UNIQUE " if unique else ""
        if_not_exists_cmd = "IF NOT EXISTS " if if_not_exists else ""

        esc_idx = _escape_identifier(index_name)
        esc_tbl = _escape_identifier(table_name)
        cols_str = ", ".join(f'"{_escape_identifier(c)}"' for c in col_list)

        sql_cmd = f'CREATE {unique_cmd}INDEX {if_not_exists_cmd}"{esc_idx}" ON "{esc_tbl}" ({cols_str})'
        await conn.execute(sql_cmd)
        await self._auto_commit_if_needed()

    async def drop_index(self, index_name: str, if_exists: bool = True) -> None:
        if not isinstance(index_name, str) or not index_name.isidentifier():
            raise ValueError(f"Invalid index name: {index_name}")

        conn = self._get_connection()

        if_exists_cmd = "IF EXISTS " if if_exists else ""
        esc_idx = _escape_identifier(index_name)

        sql_cmd = f'DROP INDEX {if_exists_cmd}"{esc_idx}"'
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
