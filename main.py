import os
import time
import logging
from datetime import datetime
from typing import Any, Dict, List

import yaml
import psycopg
from psycopg import sql  # ✅ NOVO (para DROP SCHEMA seguro)
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

from api_service import api_login, api_fetch_endpoint
from pg_service import (
    pg_conninfo_from_env,
    table_name_from_endpoint,
    extract_rows_from_payload,
    insert_rows_batch,
    drop_all_tables_in_schema,
)


# ----------------------------
# Logging (TXT + rotação 10KB)
# ----------------------------
def setup_logger() -> logging.Logger:
    os.makedirs("log", exist_ok=True)

    fname = datetime.now().strftime("log%d%m%Y.txt")
    path = os.path.join("log", fname)

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = RotatingFileHandler(
        path,
        maxBytes=10_000,
        backupCount=50,
        encoding="utf-8"
    )

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    return logger


# ----------------------------
# Config
# ----------------------------
def load_clients(path: str = "clients.yml") -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    clients = data.get("clients", [])
    if not isinstance(clients, list):
        raise ValueError("clients.yml inválido: 'clients' precisa ser uma lista.")
    return clients


def env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


# ----------------------------
# 🔥 NOVO: Remove schemas órfãos
# (schemas existentes no banco, mas que não estão mais no clients.yml)
# ----------------------------
def drop_orphan_schemas(conn: psycopg.Connection, keep_schemas: List[str], logger: logging.Logger) -> int:
    keep = {s.strip() for s in keep_schemas if s and str(s).strip()}

    # schemas que NUNCA deve apagar
    protected = {"public", "information_schema", "pg_catalog", "pg_toast"}
    keep |= protected

    # pega schemas existentes (exceto os internos pg_*)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
              AND nspname <> 'information_schema'
        """)
        existing = [r[0] for r in cur.fetchall()]

    removed = 0
    for s in existing:
        if s in keep:
            continue

        # segurança extra: não remover schemas protegidos
        if s in protected:
            continue

        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(s)))
            conn.commit()
            logger.info(f"Schema órfão removido: {s}")
            removed += 1
        except Exception as e:
            logger.exception(f"Falha ao remover schema órfão '{s}': {e}")

    return removed


# ----------------------------
# Execução por ciclo
# ----------------------------
def run_cycle(logger: logging.Logger) -> None:
    load_dotenv()

    base_url = os.getenv("API_BASE_URL", "").rstrip("/")
    if not base_url:
        raise ValueError("API_BASE_URL é obrigatório no .env")

    verify_ssl = env_bool("API_VERIFY_SSL", default=False)
    if not verify_ssl:
        urllib3.disable_warnings(InsecureRequestWarning)

    clients = load_clients("clients.yml")
    if not clients:
        logger.warning("Nenhum cliente encontrado em clients.yml")
        return

    conninfo = pg_conninfo_from_env()

    with psycopg.connect(conninfo) as conn:
        # 🔥 NOVO: sempre remove schemas que não estão mais no clients.yml
        keep = [str(c.get("schema", "")).strip() for c in clients]
        removed = drop_orphan_schemas(conn, keep, logger)
        if removed:
            logger.info(f"Schemas órfãos removidos: {removed}")
        else:
            logger.info("Nenhum schema órfão para remover.")

        for c in clients:
            name = c.get("name", "SemNome")
            schema = c.get("schema")
            usuario = c.get("usuario")
            senha = c.get("senha")
            identificador = c.get("identificador")
            endpoints = c.get("endpoints", [])

            if not all([schema, usuario, senha, identificador]):
                logger.error(f"Cliente '{name}' inválido (faltando schema/usuario/senha/identificador). Pulando.")
                continue
            if not endpoints:
                logger.warning(f"Cliente '{name}' sem endpoints. Pulando.")
                continue

            logger.info(f"=== Cliente: {name} | schema={schema} ===")

            try:
                auth = api_login(
                    base_url=base_url,
                    usuario=str(usuario),
                    senha=str(senha),
                    identificador=str(identificador),
                    verify_ssl=verify_ssl,
                    logger=logger
                )
            except Exception as e:
                logger.exception(f"Falha no login do cliente '{name}': {e}")
                continue

            sessao = auth["sessao"]
            idUsuario = auth["idUsuario"]
            ident = auth["identificador"]

            # 🔥 mantém seu comportamento: derruba todas as tabelas do schema antes de recriar
            try:
                dropped = drop_all_tables_in_schema(conn, str(schema))
                logger.info(f"Schema {schema}: tabelas removidas={dropped}")
            except Exception as e:
                logger.exception(f"Falha ao dropar tabelas do schema '{schema}': {e}")
                continue

            # endpoints pode ser:
            # - "ObterClientes"
            # - { endpoint: "ObterClientes", table: "clientes" }
            for ep in endpoints:
                try:
                    if isinstance(ep, str):
                        endpoint_name = ep
                        table = table_name_from_endpoint(endpoint_name)
                    elif isinstance(ep, dict):
                        endpoint_name = str(ep.get("endpoint", "")).strip()
                        custom_table = str(ep.get("table", "")).strip()

                        if not endpoint_name:
                            logger.error(f"Endpoint inválido no yml (faltando 'endpoint') no cliente '{name}'. Pulando.")
                            continue

                        table = custom_table if custom_table else table_name_from_endpoint(endpoint_name)
                    else:
                        logger.error(f"Formato inválido em endpoints no cliente '{name}': {ep}. Pulando.")
                        continue

                    payload = api_fetch_endpoint(
                        base_url=base_url,
                        endpoint=str(endpoint_name),
                        sessao=sessao,
                        idUsuario=idUsuario,
                        identificador=ident,
                        verify_ssl=verify_ssl,
                        logger=logger
                    )

                    rows = extract_rows_from_payload(payload)

                    inserted = insert_rows_batch(
                        conn=conn,
                        schema=str(schema),
                        table=str(table),
                        endpoint=str(endpoint_name),
                        rows=rows,
                        batch_size=500,
                        recreate_table_each_run=False  # já limpamos o schema no começo
                    )

                    logger.info(f"Gravação COLUNAR OK | {schema}.{table} | linhas={inserted}")

                except Exception as e:
                    logger.exception(f"Erro endpoint '{ep}' cliente '{name}': {e}")


def main():
    logger = setup_logger()
    load_dotenv()

    minutes = int(os.getenv("RUN_EVERY_MINUTES", "20"))
    seconds = max(60, minutes * 60)

    logger.info("Iniciando ETL (multi-client / schemas).")
    logger.info(f"Agendamento: a cada {minutes} minuto(s).")

    while True:
        start = time.time()
        try:
            run_cycle(logger)
        except Exception as e:
            logger.exception(f"Erro geral do ciclo: {e}")

        elapsed = time.time() - start
        sleep_for = max(1, seconds - elapsed)
        logger.info(f"Ciclo finalizado em {elapsed:.1f}s. Próxima execução em {sleep_for:.0f}s.")
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()

# Nesse arquivo você tem a lógica principal do ETL, que lê o arquivo clients.yml,
# faz o login na API para cada cliente, busca os dados de cada endpoint e grava no PostgreSQL.
# também há uma nova funcionalidade que remove schemas órfãos do banco de dados, ou seja, schemas que não estão mais listados no clients.yml.
# para cada cliente, o script cria ou limpa o schema correspondente antes de inserir os dados.      