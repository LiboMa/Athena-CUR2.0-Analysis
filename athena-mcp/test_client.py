#!/usr/bin/env python3
"""
End-to-end test client for athena_mcp_server.py.

Spawns the server over stdio (exactly like Quick / Quick Desktop would),
initializes the MCP session, lists tools, and invokes every tool.
Also cross-checks list_databases against a direct boto3 call to detect
the suspected "skip first row" off-by-one bug.
"""

import asyncio
import json
import os
import sys

import boto3
from mcp import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

SERVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "athena_mcp_server.py")
REGION = "us-east-1"
OUTPUT = "s3://cost-explorer-billing-bedrock/athena-results/"
DB = "cur_db"
TABLE = "cur_iam_bedrock"


def parse(result):
    """Extract the JSON payload from a tool call result."""
    txt = result.content[0].text
    try:
        return json.loads(txt)
    except Exception:
        return {"_raw": txt}


async def main():
    env = dict(os.environ)
    env.update({
        "AWS_REGION": REGION,
        "ATHENA_WORKGROUP": "primary",
        "ATHENA_OUTPUT_BUCKET": OUTPUT,
        "ATHENA_DATABASE": DB,
    })

    params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)

    results_summary = {}

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # 1) list_tools
            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]
            print("== list_tools ==")
            print("tools:", tool_names)
            results_summary["list_tools"] = tool_names

            # 2) list_databases
            r = parse(await session.call_tool("list_databases", {}))
            print("\n== list_databases ==")
            print(json.dumps(r, ensure_ascii=False)[:800])
            results_summary["list_databases"] = r

            # 3) list_tables
            r = parse(await session.call_tool("list_tables", {"database": DB}))
            print("\n== list_tables (cur_db) ==")
            print(json.dumps(r, ensure_ascii=False)[:800])
            results_summary["list_tables"] = r

            # 4) get_table_schema
            r = parse(await session.call_tool("get_table_schema", {"database": DB, "table": TABLE}))
            print("\n== get_table_schema ==")
            print(json.dumps(r, ensure_ascii=False)[:800])
            results_summary["get_table_schema"] = r

            # 5) get_table_preview
            r = parse(await session.call_tool("get_table_preview", {"database": DB, "table": TABLE, "limit": 3}))
            print("\n== get_table_preview (limit 3) ==")
            print(json.dumps(r, ensure_ascii=False)[:800])
            results_summary["get_table_preview"] = r

            # 6) list_partitions
            r = parse(await session.call_tool("list_partitions", {"database": DB, "table": TABLE}))
            print("\n== list_partitions ==")
            print(json.dumps(r, ensure_ascii=False)[:800])
            results_summary["list_partitions"] = r

            # 7) query (aggregate by resource id)
            sql = (
                "SELECT line_item_product_code AS service, "
                "SUM(line_item_unblended_cost) AS cost FROM cur_iam_bedrock "
                "WHERE billing_period = '2026-06' GROUP BY 1 ORDER BY cost DESC LIMIT 5"
            )
            r = parse(await session.call_tool("query", {"sql": sql, "database": DB}))
            print("\n== query (top services) ==")
            print(json.dumps(r, ensure_ascii=False)[:800])
            results_summary["query"] = r

    # ---- Cross-check list_databases vs direct boto3 (detect off-by-one) ----
    print("\n== CROSS-CHECK: direct boto3 SHOW DATABASES ==")
    ath = boto3.client("athena", region_name=REGION)
    qid = ath.start_query_execution(
        QueryString="SHOW DATABASES",
        WorkGroup="primary",
        ResultConfiguration={"OutputLocation": OUTPUT},
    )["QueryExecutionId"]
    import time
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    raw = ath.get_query_results(QueryExecutionId=qid)
    direct_dbs = [row["Data"][0].get("VarCharValue", "") for row in raw["ResultSet"]["Rows"]]
    print("direct SHOW DATABASES rows:", direct_dbs)

    mcp_dbs = [r[0] for r in results_summary["list_databases"].get("rows", [])]
    print("MCP list_databases rows  :", mcp_dbs)
    missing = [d for d in direct_dbs if d not in mcp_dbs]
    print("MISSING in MCP output (lost rows):", missing)

    # ---- Verdict ----
    print("\n================ VERDICT ================")
    ok = True
    checks = []

    def chk(name, cond, detail=""):
        nonlocal ok
        ok = ok and cond
        checks.append((name, cond, detail))

    chk("list_tools has 6 tools", len(results_summary["list_tools"]) == 6, str(results_summary["list_tools"]))
    chk("list_databases no error", "error" not in results_summary["list_databases"])
    chk("list_databases not missing rows", not missing, f"missing={missing}")
    chk("list_tables finds cur_iam_bedrock",
        any(TABLE in (r[0] if r else "") for r in results_summary["list_tables"].get("rows", [])),
        str(results_summary["list_tables"].get("rows")))
    sch = results_summary["get_table_schema"]
    chk("get_table_schema no error", "error" not in sch, sch.get("error", ""))
    chk("get_table_schema has columns", len(sch.get("columns", [])) > 0, f"n={len(sch.get('columns', []))}")
    chk("get_table_schema includes first column bill_bill_type",
        any(c.get("column") == "bill_bill_type" for c in sch.get("columns", [])),
        "first schema col present?")
    chk("get_table_schema flags billing_period as partition key",
        any(c.get("column") == "billing_period" and c.get("partition_key") for c in sch.get("columns", [])),
        "partition flag")
    part = results_summary["list_partitions"]
    chk("list_partitions no error", "error" not in part, part.get("error", ""))
    chk("list_partitions finds billing_period",
        "billing_period" in part.get("partition_columns", []),
        str(part.get("partition_columns")))
    prev = results_summary["get_table_preview"]
    chk("get_table_preview no error", "error" not in prev, prev.get("error", ""))
    chk("get_table_preview returns 3 rows", prev.get("row_count", 0) == 3, f"row_count={prev.get('row_count')}")
    q = results_summary["query"]
    chk("query no error", "error" not in q, q.get("error", ""))
    chk("query returns rows", q.get("row_count", 0) > 0, f"row_count={q.get('row_count')}")

    for name, cond, detail in checks:
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {detail}" if not cond else ""))

    print("\nRESULT:", "ALL PASS ✅" if ok else "FAILURES ❌")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
