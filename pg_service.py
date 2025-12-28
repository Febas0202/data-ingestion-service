import json
import re
from decimal import Decimal
from typing import Any, Dict, List

import psycopg
from psycopg import sql


def pg_conninfo_from_env() -> str:
    import os
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    pwd = os.getenv("PG_PASSWORD")
    if not all([db, user, pwd]):
        raise ValueError("Variáveis PG_DB, PG_USER, PG_PASSWORD são obrigatórias no .env")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def table_name_from_endpoint(endpoint: str) -> str:
    out = []
    for i, ch in enumerate(endpoint):
        if ch.isupper() and i > 0 and (endpoint[i - 1].islower() or (i + 1 < len(endpoint) and endpoint[i + 1].islower())):
            out.append("_")
        out.append(ch.lower())
    name = "".join(out).replace("-", "_").strip("_")
    return f"api_{name}"


def sanitize_col(name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", str(name).strip())
    name = re.sub(r"_+", "_", name).strip("_").lower()
    if not name:
        name = "col"
    if name[0].isdigit():
        name = f"c_{name}"
    return name[:63]


def flatten_json(obj: dict, parent_key: str = "", sep: str = "_") -> dict:
    items = {}
    for k, v in (obj or {}).items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
        new_key = sanitize_col(new_key)

        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        elif isinstance(v, list):
            items[new_key] = v  # fica jsonb
        else:
            items[new_key] = v
    return items


def try_parse_timestamp(s: str) -> bool:
    if not isinstance(s, str):
        return False
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z)?)?$", s))


def infer_pg_type(value: Any) -> str:
    if value is None:
        return "text"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "bigint"
    if isinstance(value, float) or isinstance(value, Decimal):
        return "numeric"
    if isinstance(value, str):
        if try_parse_timestamp(value):
            return "timestamptz"
        return "varchar(255)" if len(value) <= 255 else "text"
    if isinstance(value, (dict, list)):
        return "jsonb"
    return "text"


def unify_types(t1: str, t2: str) -> str:
    if t1 == t2:
        return t1
    if "jsonb" in (t1, t2):
        return "jsonb"
    if (t1 == "numeric" and t2 == "bigint") or (t2 == "numeric" and t1 == "bigint"):
        return "numeric"
    if "timestamptz" in (t1, t2):
        return "text"
    if "text" in (t1, t2):
        return "text"
    if ("varchar" in t1 or "varchar" in t2) and (t1 in ("bigint", "numeric") or t2 in ("bigint", "numeric")):
        return "text"
    return "text"


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    conn.commit()


def drop_table_if_exists(conn: psycopg.Connection, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
        )
    conn.commit()


def drop_all_tables_in_schema(conn: psycopg.Connection, schema: str) -> int:
    """
    🔥 Derruba TODAS as tabelas existentes no schema informado.
    Isso garante que tabelas antigas (ex: clientes_mt) também sejam removidas.
    Retorna a quantidade de tabelas removidas.
    """
    # lista tabelas do schema
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = %s
            """,
            (schema,)
        )
        tables = [r[0] for r in cur.fetchall()]

    if not tables:
        return 0

    dropped = 0
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(schema),
                    sql.Identifier(t)
                )
            )
            dropped += 1

    conn.commit()
    return dropped


def ensure_table_base(conn: psycopg.Connection, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                _id BIGSERIAL PRIMARY KEY,
                _fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                _endpoint TEXT NOT NULL
            )
        """).format(sql.Identifier(schema), sql.Identifier(table)))
    conn.commit()


def get_existing_columns(conn: psycopg.Connection, schema: str, table: str) -> Dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        rows = cur.fetchall()
    return {r[0]: r[1] for r in rows}


def add_column(conn: psycopg.Connection, schema: str, table: str, col: str, pg_type: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(col),
                sql.SQL(pg_type)
            )
        )
    conn.commit()


def extract_rows_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict) and "dados" in payload and isinstance(payload["dados"], list):
        data = payload["dados"]
    elif isinstance(payload, list):
        data = payload
    elif isinstance(payload, dict):
        data = [payload]
    else:
        data = [{"valor": payload}]

    rows: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            rows.append(flatten_json(item))
        else:
            rows.append({"valor": item})
    return rows


def insert_rows_batch(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    endpoint: str,
    rows: List[Dict[str, Any]],
    batch_size: int = 500,
    recreate_table_each_run: bool = True
) -> int:
    if not rows:
        return 0

    all_cols = set()
    for r in rows:
        all_cols.update(r.keys())
    all_cols = [c for c in sorted(all_cols) if c not in ("_id", "_fetched_at", "_endpoint")]

    col_types: Dict[str, str] = {}
    for c in all_cols:
        current = None
        for r in rows:
            if c in r and r[c] is not None:
                t = infer_pg_type(r[c])
                current = t if current is None else unify_types(current, t)
        col_types[c] = current or "text"

    ensure_schema(conn, schema)

    if recreate_table_each_run:
        drop_table_if_exists(conn, schema, table)

    ensure_table_base(conn, schema, table)

    existing = get_existing_columns(conn, schema, table)
    for c in all_cols:
        if c not in existing:
            add_column(conn, schema, table, c, col_types[c])

    base_cols = ["_endpoint"] + all_cols

    insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, base_cols)),
        sql.SQL(", ").join([sql.Placeholder()] * len(base_cols))
    )

    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            values = []

            for r in chunk:
                row_vals = [endpoint]
                for c in all_cols:
                    v = r.get(c)
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    row_vals.append(v)
                values.append(tuple(row_vals))

            cur.executemany(insert_sql, values)
            total += len(chunk)

    conn.commit()
    return total


# nesse arquivo pg_service.py você tem funções auxiliares para interagir com o PostgreSQL,
# como criar schemas, criar tabelas, adicionar colunas, inserir dados em lotes e inferir tipos de dados.
# essas funções são usadas no main.py para gerenciar o banco de dados onde os dados da API serão armazenados.        