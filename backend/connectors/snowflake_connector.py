"""
Snowflake connector — queries a designated KPI or financial data table
using the Snowflake SQL API (REST — no snowflake-connector-python needed).

Credentials required (Render env vars):
  SNOWFLAKE_ACCOUNT    — e.g. xy12345.us-east-1
  SNOWFLAKE_USER       — service account username
  SNOWFLAKE_PASSWORD   — service account password
  SNOWFLAKE_WAREHOUSE  — e.g. COMPUTE_WH
  SNOWFLAKE_DATABASE   — e.g. ANALYTICS
  SNOWFLAKE_SCHEMA     — e.g. PUBLIC (default)
  SNOWFLAKE_TABLE      — the table/view with monthly KPI data (e.g. MONTHLY_KPI)

Expected table schema (Snowflake):
  PERIOD    VARCHAR   — YYYY-MM format
  + one column per KPI key (e.g. REVENUE_GROWTH, GROSS_MARGIN, ...)

The connector reads the table and maps each row to a revenue record.
"""
from __future__ import annotations

import base64
import logging
import os
import time
import httpx

from .base import BaseConnector, ConnectorError

log = logging.getLogger("connectors.snowflake")

_MAX_ROWS = 10000

_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT", "")      # e.g. xy12345.us-east-1
_USER      = os.environ.get("SNOWFLAKE_USER", "")
_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD", "")
_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "")
_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
_TABLE     = os.environ.get("SNOWFLAKE_TABLE", "MONTHLY_KPI")


def _snowflake_base(account: str) -> str:
    return f"https://{account}.snowflakecomputing.com"


def _get_token(account: str, user: str, password: str,
               warehouse: str = "", database: str = "",
               schema: str = "") -> str:
    """Obtain a Snowflake OAuth-style session token via REST login."""
    url = f"{_snowflake_base(account)}/session/v1/login-request"
    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    wh = warehouse or _WAREHOUSE
    db = database or _DATABASE
    sch = schema or _SCHEMA
    log.info("[Snowflake] Authenticating as %s on account %s", user, account)
    with httpx.Client(timeout=20) as client:
        r = client.post(
            url,
            headers={"Authorization": f"Basic {creds}", "Content-Type": "application/json"},
            json={"data": {"ACCOUNT_NAME": account}},
            params={"warehouse": wh, "databaseName": db, "schemaName": sch},
        )
    if r.status_code != 200:
        raise ConnectorError(f"Snowflake login failed: {r.status_code} {r.text[:200]}")
    data = r.json().get("data", {})
    token = data.get("token")
    if not token:
        raise ConnectorError("Snowflake login succeeded but no token returned")
    log.info("[Snowflake] Authentication successful")
    return token


def _run_query(account: str, token: str, sql: str,
               database: str = "", schema: str = "",
               warehouse: str = "",
               max_retries: int = 3) -> list[dict]:
    """Execute a SQL query and return list of row dicts, with retry."""
    url = f"{_snowflake_base(account)}/api/v2/statements"
    db = database or _DATABASE
    sch = schema or _SCHEMA
    wh = warehouse or _WAREHOUSE

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            with httpx.Client(timeout=60) as client:
                r = client.post(
                    url,
                    headers={
                        "Authorization":  f"Snowflake Token=\"{token}\"",
                        "Content-Type":   "application/json",
                        "X-Snowflake-Authorization-Token-Type": "SESSION",
                    },
                    json={
                        "statement": sql,
                        "timeout":   50,
                        "database":  db,
                        "schema":    sch,
                        "warehouse": wh,
                    },
                )
            if r.status_code in (200, 202):
                break
            if r.status_code >= 500 and attempt < max_retries:
                wait = 2 ** attempt
                log.warning("[Snowflake] Server error %d, retry in %ds "
                            "(attempt %d/%d)",
                            r.status_code, wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            raise ConnectorError(
                f"Snowflake query error {r.status_code}: {r.text[:300]}")
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                httpx.PoolTimeout, httpx.ConnectTimeout) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = 2 ** attempt
                log.warning("[Snowflake] Network error %s, retry in %ds "
                            "(attempt %d/%d)",
                            type(exc).__name__, wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            raise ConnectorError(
                f"Snowflake network error after {max_retries} retries: {exc}"
            ) from exc
    else:
        raise ConnectorError(
            f"Snowflake query failed after {max_retries} retries: {last_exc}")

    data     = r.json()
    col_defs = data.get("resultSetMetaData", {}).get("rowType", [])
    col_names = [c["name"].lower() for c in col_defs]
    rows     = data.get("data", [])
    log.info("[Snowflake] Query returned %d rows", len(rows))
    return [dict(zip(col_names, row)) for row in rows]


class SnowflakeConnector(BaseConnector):
    SOURCE_NAME = "snowflake"
    AUTH_TYPE   = "api_key"   # credentials set in env vars / connector config

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            acct = credentials.get("account") or _ACCOUNT
            user = credentials.get("user") or _USER
            pwd  = credentials.get("password") or _PASSWORD
            if not all([acct, user, pwd]):
                return False
            _get_token(acct, user, pwd)
            return True
        except Exception:
            return False

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        acct  = credentials.get("account")   or _ACCOUNT
        user  = credentials.get("user")      or _USER
        pwd   = credentials.get("password")  or _PASSWORD
        wh    = credentials.get("warehouse") or _WAREHOUSE
        db    = credentials.get("database")  or _DATABASE
        sch   = credentials.get("schema")    or _SCHEMA
        table = credentials.get("table")     or _TABLE

        if not all([acct, user, pwd, db, table]):
            raise ConnectorError(
                "Snowflake: set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, "
                "SNOWFLAKE_DATABASE, and SNOWFLAKE_TABLE in Render."
            )

        log.info("[Snowflake] Extracting from %s.%s.%s for workspace %s",
                 db, sch, table, workspace_id)

        token = _get_token(acct, user, pwd,
                           warehouse=wh, database=db, schema=sch)
        sql = (f'SELECT * FROM "{db}"."{sch}"."{table}" '
               f'ORDER BY PERIOD DESC LIMIT {_MAX_ROWS}')
        records = _run_query(acct, token, sql,
                             database=db, schema=sch, warehouse=wh)

        log.info("[Snowflake] Extracted %d records from %s", len(records), table)

        # Return as generic revenue entity — transformer will normalise column names
        return [
            {"entity_type": "revenue", "records": records},
        ]
