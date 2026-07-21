# Athena MCP Server

一个本地的 [Model Context Protocol](https://modelcontextprotocol.io) (MCP) 服务器，
通过 stdio 把 AWS Athena 暴露成一组工具，供 **Quick / Quick Desktop**（或任何 MCP 客户端）调用。

## 提供的工具

| 工具 | 说明 |
|------|------|
| `query` | 执行任意 SQL（Trino/Presto 语法），返回 columns + rows（默认上限 1000 行） |
| `list_databases` | 列出所有数据库 |
| `list_tables` | 列出某个数据库下的所有表 |
| `get_table_schema` | 返回表结构（列名、类型、是否分区键），已解析为结构化 JSON |
| `get_table_preview` | 预览表前 N 行（默认 10） |
| `list_partitions` | 返回分区列及采样分区值（兼容分区投影表） |

## 环境要求

- Python **3.10+**（本机用 `loadenv` 加载的虚拟环境为 3.12.7）
- 依赖：`mcp`、`boto3`（已装在 `/Users/malibo/MyDev/venv`）
- 已配置可用的 AWS 凭证（本机 `aws sts get-caller-identity` 正常，账号 `533267047935`）

> 本机运行解释器固定用：`/Users/malibo/MyDev/venv/bin/python`
> （等价于先 `loadenv` 再 `python`）

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AWS_REGION` | `us-east-1` | AWS 区域 |
| `ATHENA_WORKGROUP` | `primary` | Athena 工作组 |
| `ATHENA_OUTPUT_BUCKET` | 空 | **必填**：查询结果输出 S3 位置。`primary` 工作组未配置默认输出位置，必须显式设置，否则 `query` 会报错 |
| `ATHENA_DATABASE` | `default` | 默认数据库；本项目设为 `cur_db` |
| `ATHENA_QUERY_TIMEOUT` | `120` | 单条查询超时（秒） |
| `ATHENA_MAX_RESULTS` | `1000` | 单次返回最大行数 |

## 在 Quick / Quick Desktop 中接入

Quick / Quick Desktop 使用标准的 MCP `mcpServers` 配置。把下面这段加入客户端的 MCP 配置
（可参考同目录 `mcp_config.example.json`）：

```json
{
  "mcpServers": {
    "athena": {
      "command": "/Users/malibo/MyDev/venv/bin/python",
      "args": ["/Users/malibo/Desktop/athena-mcp/athena_mcp_server.py"],
      "env": {
        "AWS_REGION": "us-east-1",
        "AWS_PROFILE": "default",
        "ATHENA_WORKGROUP": "primary",
        "ATHENA_OUTPUT_BUCKET": "s3://cost-explorer-billing-bedrock/athena-results/",
        "ATHENA_DATABASE": "cur_db"
      }
    }
  }
}
```

要点：
- `command` 必须用虚拟环境里的 Python 绝对路径（MCP 客户端不会执行 shell alias，所以 `loadenv` 在这里不可用，要直接指向 `venv/bin/python`）。
- `args` 用服务器脚本的绝对路径。
- 凭证：如果用命名 profile，把 `AWS_PROFILE` 改成对应名字；如果用环境变量/SSO，确保 Quick 启动时能读到。
- 改完配置后重启 Quick / Quick Desktop，即可在工具列表里看到 `athena` 的 6 个工具。

## 本地自测

不依赖任何 MCP 客户端，直接用自带的端到端测试脚本（会通过 stdio 拉起服务器并逐个调用工具）：

```bash
loadenv                                   # 或直接用下面的绝对路径
/Users/malibo/MyDev/venv/bin/python /Users/malibo/Desktop/athena-mcp/test_client.py
```

期望输出末尾为：`RESULT: ALL PASS ✅`（共 14 项检查）。

## 已知行为 / 注意事项

- **成本**：每次工具调用都会真实执行 Athena 查询并扫描 S3 数据，按扫描量计费。`query` 时尽量带分区过滤（如 `WHERE billing_period = '2026-06'`）。
- **分区投影表**：`list_partitions` 通过 `information_schema` 读取分区列，再用 `SELECT DISTINCT` 采样分区值；这对分区投影表也有效（该引擎不支持 `SHOW PARTITIONS`）。
- **权限**：执行账号需要 Athena 查询、读取数据所在 S3 桶、以及写入结果桶（`ATHENA_OUTPUT_BUCKET`）的权限。
- **安全**：该服务器可执行任意 SQL，仅限本地受信环境使用；不要将其暴露到网络。

## 修复记录（相对最初版本）

测试发现并修复了 4 个会导致工具不可用的问题：

1. **丢首行数据**：原 `_fetch_results` 无条件把结果第一行当表头丢弃。但只有 `SELECT`(DML) 才有表头行，`SHOW`/`DESCRIBE`(UTILITY) 第一行就是数据 → 导致 `list_databases` 丢掉 `cur_db`、`DESCRIBE` 丢掉首列。改为按 `StatementType` 判断。
2. **`list_tables` 语法错误**：`SHOW TABLES IN "db"` 双引号非法 → 改用反引号 `SHOW TABLES IN \`db\``。
3. **`get_table_schema` 语法错误**：`DESCRIBE "db"."t"` 双引号非法 → 改用反引号，并把输出解析成结构化的 `columns`（含 `partition_key` 标记）。
4. **`list_partitions` 不可用**：该引擎不支持 `SHOW PARTITIONS`，且分区投影表无法枚举 → 改用 `information_schema` + `SELECT DISTINCT`。

附带改进：阻塞型 boto3 调用改为 `asyncio.to_thread` 执行，避免阻塞事件循环；异常信息附带异常类型名。

## 文件清单

| 文件 | 说明 |
|------|------|
| `athena_mcp_server.py` | MCP 服务器（已修复、可用） |
| `test_client.py` | 端到端自测脚本（stdio 拉起服务器 + 14 项断言） |
| `mcp_config.example.json` | Quick / Quick Desktop 的 MCP 配置示例 |
| `README.md` | 本文档 |
