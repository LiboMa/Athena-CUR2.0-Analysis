#!/usr/bin/env python3
"""
Athena MCP Server — A Model Context Protocol server for AWS Athena.

Provides tools to query Athena, list databases/tables, and inspect schemas.
Communicates via stdio (stdin/stdout) using the MCP protocol.

Usage:
    python athena_mcp_server.py

Environment Variables:
    AWS_REGION           — AWS region (default: us-east-1)
    ATHENA_WORKGROUP     — Athena workgroup (default: primary)
    ATHENA_OUTPUT_BUCKET — S3 output location for query results
                           (default: s3://aws-athena-query-results-{account_id}-{region}/)
    ATHENA_DATABASE      — Default database (optional)

Requires:
    pip install mcp boto3
"""

import asyncio
import json
import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT_BUCKET = os.environ.get("ATHENA_OUTPUT_BUCKET", "")
ATHENA_DEFAULT_DATABASE = os.environ.get("ATHENA_DATABASE", "default")
QUERY_TIMEOUT_SECONDS = int(os.environ.get("ATHENA_QUERY_TIMEOUT", "120"))
MAX_RESULTS = int(os.environ.get("ATHENA_MAX_RESULTS", "1000"))

# ---------------------------------------------------------------------------
# Athena Client
# ---------------------------------------------------------------------------

athena_client = boto3.client("athena", region_name=AWS_REGION)


def _get_output_location() -> str:
    """Resolve the S3 output location for Athena query results."""
    if ATHENA_OUTPUT_BUCKET:
        return ATHENA_OUTPUT_BUCKET
    # Attempt to get from workgroup config
    try:
        wg = athena_client.get_work_group(WorkGroup=ATHENA_WORKGROUP)
        location = (
            wg.get("WorkGroup", {})
            .get("Configuration", {})
            .get("ResultConfiguration", {})
            .get("OutputLocation", "")
        )
        if location:
            return location
    except ClientError:
        pass
    raise ValueError(
        "No output location configured. Set ATHENA_OUTPUT_BUCKET env var "
        "or configure the workgroup's output location."
    )


def _wait_for_query(execution_id: str) -> dict:
    """Poll until the query finishes or times out."""
    start = time.time()
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)
        state = response["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return response["QueryExecution"]
        if time.time() - start > QUERY_TIMEOUT_SECONDS:
            athena_client.stop_query_execution(QueryExecutionId=execution_id)
            raise TimeoutError(
                f"Query {execution_id} timed out after {QUERY_TIMEOUT_SECONDS}s"
            )
        time.sleep(1)


def _fetch_results(execution_id: str, statement_type: str = "DML") -> dict:
    """Fetch query results and return as structured data.

    Athena returns a header row as the first row ONLY for DML (SELECT)
    statements. For UTILITY/DDL statements (SHOW, DESCRIBE) the first row is
    already data, so we must NOT skip it — otherwise the first record is lost.
    """
    paginator = athena_client.get_paginator("get_query_results")
    pages = paginator.paginate(
        QueryExecutionId=execution_id,
        PaginationConfig={"MaxItems": MAX_RESULTS},
    )

    columns = []
    rows = []
    first_page = True
    skip_header = statement_type == "DML"

    for page in pages:
        result_set = page["ResultSet"]
        if first_page:
            columns = [
                col["Label"] or col["Name"]
                for col in result_set["ResultSetMetadata"]["ColumnInfo"]
            ]
            # Only DML (SELECT) results carry a header row in Rows[0].
            data_rows = result_set["Rows"][1:] if skip_header else result_set["Rows"]
            first_page = False
        else:
            data_rows = result_set["Rows"]

        for row in data_rows:
            rows.append([datum.get("VarCharValue", "") for datum in row["Data"]])

    return {"columns": columns, "rows": rows, "row_count": len(rows)}


def run_query(sql: str, database: str | None = None) -> dict:
    """Execute an Athena SQL query and return results."""
    db = database or ATHENA_DEFAULT_DATABASE
    params: dict[str, Any] = {
        "QueryString": sql,
        "WorkGroup": ATHENA_WORKGROUP,
    }
    if db:
        params["QueryExecutionContext"] = {"Database": db}

    output_location = _get_output_location()
    params["ResultConfiguration"] = {"OutputLocation": output_location}

    response = athena_client.start_query_execution(**params)
    execution_id = response["QueryExecutionId"]

    execution = _wait_for_query(execution_id)
    status = execution["Status"]

    if status["State"] == "FAILED":
        reason = status.get("StateChangeReason", "Unknown error")
        return {"error": reason, "execution_id": execution_id, "state": "FAILED"}

    if status["State"] == "CANCELLED":
        return {"error": "Query was cancelled", "execution_id": execution_id, "state": "CANCELLED"}

    # Fetch results
    statement_type = execution.get("StatementType", "DML")
    results = _fetch_results(execution_id, statement_type)
    stats = execution.get("Statistics", {})
    results["execution_id"] = execution_id
    results["statement_type"] = statement_type
    results["execution_time_ms"] = stats.get("EngineExecutionTimeInMillis", 0)
    results["data_scanned_bytes"] = stats.get("DataScannedInBytes", 0)
    return results


# ---------------------------------------------------------------------------
# Helpers for identifier quoting and schema parsing
# ---------------------------------------------------------------------------

def _bq(identifier: str) -> str:
    """Backtick-quote an identifier for Hive-style statements (SHOW/DESCRIBE).

    Athena SHOW/DESCRIBE (UTILITY) statements use Hive backtick quoting;
    double quotes are NOT accepted there. Embedded backticks are escaped.
    """
    return "`" + identifier.replace("`", "``") + "`"


def _dq(identifier: str) -> str:
    """Double-quote an identifier for DML (SELECT / Trino) statements."""
    return '"' + identifier.replace('"', '""') + '"'


def _parse_describe(rows: list) -> list:
    """Parse Athena DESCRIBE output rows into structured column definitions.

    Each DESCRIBE row looks like:  'col_name<tab>data_type<tab>comment'
    The output also contains a '# Partition Information' section and blank
    lines which are skipped. Partition columns are flagged.
    """
    columns = []
    in_partition_section = False
    for row in rows:
        cell = (row[0] if row else "").strip()
        if not cell:
            continue
        if cell.startswith("#"):
            if "partition" in cell.lower():
                in_partition_section = True
            continue
        parts = [p.strip() for p in cell.split("\t")]
        # collapse any empty fragments produced by padding
        parts = [p for p in parts if p != ""] or [cell]
        name = parts[0]
        dtype = parts[1] if len(parts) > 1 else ""
        comment = parts[2] if len(parts) > 2 else ""
        columns.append({
            "column": name,
            "type": dtype,
            "comment": comment,
            "partition_key": in_partition_section,
        })
    return columns


def _list_partitions(db: str, table: str, sample_limit: int = 200) -> dict:
    """List partition columns and sample partition values.

    `SHOW PARTITIONS` is not supported by the Athena engine in use and does
    not work for partition-projection tables anyway. Instead we read the
    partition keys from information_schema and sample distinct values.
    """
    col_sql = (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{db}' AND table_name = '{table}' "
        "AND extra_info = 'partition key' ORDER BY ordinal_position"
    )
    cols_res = run_query(col_sql, db)
    if "error" in cols_res:
        return cols_res
    part_cols = [r[0] for r in cols_res.get("rows", []) if r]

    if not part_cols:
        return {
            "partition_columns": [],
            "partition_values": [],
            "note": "Table has no partition keys, or partitions are not "
                    "enumerable (e.g. partition projection).",
        }

    select_cols = ", ".join(_dq(c) for c in part_cols)
    sample_sql = (
        f"SELECT DISTINCT {select_cols} FROM {_dq(db)}.{_dq(table)} "
        f"ORDER BY {select_cols} LIMIT {int(sample_limit)}"
    )
    sample_res = run_query(sample_sql, db)
    return {
        "partition_columns": part_cols,
        "partition_values": sample_res.get("rows", []),
        "value_count": sample_res.get("row_count", 0),
        "note": sample_res.get("error"),
    }


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

server = Server("athena-mcp-server")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """Register available tools."""
    return [
        Tool(
            name="query",
            description=(
                "Execute a SQL query on AWS Athena and return results. "
                "Supports standard SQL (Trino/Presto syntax). "
                "Results are returned as columns + rows (max 1000 rows by default)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to execute",
                    },
                    "database": {
                        "type": "string",
                        "description": f"Target database (default: {ATHENA_DEFAULT_DATABASE})",
                    },
                },
                "required": ["sql"],
            },
        ),
        Tool(
            name="list_databases",
            description="List all available databases (catalogs) in Athena.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="list_tables",
            description="List all tables in a specific database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": f"Database name (default: {ATHENA_DEFAULT_DATABASE})",
                    },
                },
            },
        ),
        Tool(
            name="get_table_schema",
            description=(
                "Get the schema (columns, types, comments) of a specific table. "
                "Returns column names, data types, and any comments."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Table name",
                    },
                    "database": {
                        "type": "string",
                        "description": f"Database name (default: {ATHENA_DEFAULT_DATABASE})",
                    },
                },
                "required": ["table"],
            },
        ),
        Tool(
            name="get_table_preview",
            description="Preview the first N rows of a table (default 10).",
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Table name",
                    },
                    "database": {
                        "type": "string",
                        "description": f"Database name (default: {ATHENA_DEFAULT_DATABASE})",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of rows to preview (default: 10)",
                        "default": 10,
                    },
                },
                "required": ["table"],
            },
        ),
        Tool(
            name="list_partitions",
            description="Show partition columns and sample partition values for a table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Table name",
                    },
                    "database": {
                        "type": "string",
                        "description": f"Database name (default: {ATHENA_DEFAULT_DATABASE})",
                    },
                },
                "required": ["table"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool invocations.

    Athena dialect notes:
      * SELECT (DML) uses double-quoted identifiers (Trino).
      * SHOW / DESCRIBE (UTILITY) use backtick-quoted identifiers (Hive);
        double quotes are rejected by the parser.
    Blocking boto3 calls run in a worker thread so the asyncio event loop
    (and MCP protocol handling) stays responsive.
    """
    try:
        if name == "query":
            result = await asyncio.to_thread(
                run_query, arguments["sql"], arguments.get("database")
            )

        elif name == "list_databases":
            result = await asyncio.to_thread(run_query, "SHOW DATABASES")

        elif name == "list_tables":
            db = arguments.get("database", ATHENA_DEFAULT_DATABASE)
            result = await asyncio.to_thread(
                run_query, f"SHOW TABLES IN {_bq(db)}", db
            )

        elif name == "get_table_schema":
            db = arguments.get("database", ATHENA_DEFAULT_DATABASE)
            table = arguments["table"]
            raw = await asyncio.to_thread(
                run_query, f"DESCRIBE {_bq(db)}.{_bq(table)}", db
            )
            if "error" in raw:
                result = raw
            else:
                result = {
                    "database": db,
                    "table": table,
                    "columns": _parse_describe(raw.get("rows", [])),
                    "execution_id": raw.get("execution_id"),
                }

        elif name == "get_table_preview":
            db = arguments.get("database", ATHENA_DEFAULT_DATABASE)
            table = arguments["table"]
            limit = int(arguments.get("limit", 10))
            result = await asyncio.to_thread(
                run_query,
                f"SELECT * FROM {_dq(db)}.{_dq(table)} LIMIT {limit}",
                db,
            )

        elif name == "list_partitions":
            db = arguments.get("database", ATHENA_DEFAULT_DATABASE)
            table = arguments["table"]
            result = await asyncio.to_thread(_list_partitions, db, table)

        else:
            result = {"error": f"Unknown tool: {name}"}

    except Exception as e:
        result = {"error": f"{type(e).__name__}: {e}"}

    return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
