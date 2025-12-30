This project ingests data from a REST API and loads it into PostgreSQL.

## Getting Started

First, create and activate a virtual environment, then install dependencies:

```
python -m venv .venv
# Windows (PowerShell)
.venv\Scripts\Activate.ps1
# Linux/macOS
source .venv/bin/activate

pip install requests psycopg[binary] python-dotenv pyyaml urllib3

```


Create a .env file in the project root:

```
API_BASE_URL=https://seu-dominio-ou-ip:9910
API_VERIFY_SSL=false
RUN_EVERY_MINUTES=20

PG_HOST=localhost
PG_PORT=5432
PG_DB=seu_banco
PG_USER=seu_usuario
PG_PASSWORD=sua_senha
```

Configure the clients and endpoints in clients.yml:

```

clients:
  - name: Cliente A
    schema: cliente_a
    usuario: seu_usuario_api
    senha: sua_senha_api
    identificador: seu_identificador
    endpoints:
      - endpoint: EndpointUm
        table: tabela_customizada_opcional
      - endpoint: OutroEndpoint

```

The script runs in a continuous loop and executes a full cycle every RUN_EVERY_MINUTES.

How it works

For each configured client:

Authenticates in the API (POST /Login).

Ensures the client schema exists in PostgreSQL.

Removes all tables inside the client schema.

Calls each endpoint (POST) and writes the returned JSON into a relational table.

Table naming:

If table is provided in clients.yml, it is used.

Otherwise, the table name is generated automatically from the endpoint as api_<snake_case_endpoint>.

JSON handling:

If the API returns a dictionary with dados as a list, the script uses dados.

If it returns a list, it uses the list.

If it returns a dictionary, it wraps it as a single row.

Nested objects are flattened into column names using _ as separator.

Base columns created in every table:

_id (BIGSERIAL, primary key)

_fetched_at (TIMESTAMPTZ, default NOW())

_endpoint (TEXT)

Project structure

main.py: orchestration, scheduling, YAML loading, per-client/per-endpoint loop

api_service.py: API login and endpoint calls (HTTP)

pg_service.py: PostgreSQL schema/table utilities, flattening, type inference, batch insert

clients.yml: client and endpoint configuration

log/: log files (created automatically)

Operational notes

This project performs a full refresh by design:

Schemas in the database that are not present in clients.yml may be removed as orphan schemas.

All tables inside each client schema are dropped at the start of every cycle.

Review this behavior before using in production environments.

Learn More

Requests (HTTP): https://requests.readthedocs.io/

Psycopg 3 (PostgreSQL): https://www.psycopg.org/psycopg3/docs/

PostgreSQL: https://www.postgresql.org/docs/

YAML: https://yaml.org/

Deploy

You can run this as a long-running process on a server. Common options:

Linux: systemd service

Windows Server: Task Scheduler (run at startup) or NSSM (as a service)

Container: Docker (recommended when you want isolated runtime)

For production, keep .env out of version control and use secure secret management where possible.
