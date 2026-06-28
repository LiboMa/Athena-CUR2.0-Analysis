# AWS CUR 按资源 ID 出账单 — 操作文档

> 目标：使用已开启的 CUR 2.0（Data Exports），通过 Amazon Athena 按**资源 ID（Resource ID）**查询成本。
> 本文档记录了完整的环境信息、操作流程，以及实际执行验证的结果。
> 生成时间：2026-06-26

---

## 1. 环境信息

| 项目 | 值 |
|------|-----|
| AWS 账号 | `123456789012` |
| 区域 | `us-east-1`（CUR / Data Exports 为全局服务，统一在 us-east-1） |
| 使用的导出 | **my-cur-export**（Data Export / CUR 2.0） |
| 数据 S3 桶 | `my-cur-bucket` |
| 数据路径 | `s3://my-cur-bucket/cur-export/my-cur-export/data/` |
| 数据格式 | Parquet (snappy)，按 `BILLING_PERIOD` 分区 |
| 资源 ID | 已开启（`INCLUDE_RESOURCES = TRUE`，含 `line_item_resource_id` 字段） |
| 时间粒度 | HOURLY（小时级） |
| 当前数据 | 2026-05、2026-06 两个账单月 |

> 备注：账号下还有另一个导出 `my-bedrock`（路径 `s3://my-cur-bucket/other-export/`），它**未开启资源 ID**、按月粒度、CSV 格式，**不能用于按资源出账**，本文档不使用它。

---

## 2. Athena 环境

| 项目 | 值 |
|------|-----|
| Workgroup | `primary`（Athena engine v3） |
| 查询结果输出位置 | `s3://my-cur-bucket/athena-results/` |
| 数据库 | `cur_db` |
| 表 | `cur_db.cur_iam_bedrock` |

> `primary` workgroup 原本没有配置默认结果输出位置，本流程在每次查询时通过 `--result-configuration OutputLocation=...` 显式指定。也可以在控制台为 workgroup 配置一次默认输出位置，之后查询无需再指定。

---

## 3. 操作流程

### 步骤 1：定位 CUR（Data Export）位置
```bash
# 旧版 CUR（本账号为空）
aws cur describe-report-definitions --region us-east-1

# 新版 Data Exports（CUR 2.0）
aws bcm-data-exports list-exports --region us-east-1
aws bcm-data-exports get-export --region us-east-1 \
  --export-arn <ExportArn>     # 从中读取 S3Bucket / S3Prefix
```

### 步骤 2：读取 Manifest 获取精确字段类型
```bash
aws s3 cp \
  "s3://my-cur-bucket/cur-export/my-cur-export/metadata/BILLING_PERIOD=2026-06/my-cur-export-Manifest.json" - \
  --region us-east-1
```
Manifest 的 `columns` 字段给出每列的名称与类型（其中 `cost_category`、`discount`、`product`、`resource_tags`、`tags` 为 `map`，在 Athena 中映射为 `map<string,string>`）。

### 步骤 3：创建数据库
```sql
CREATE DATABASE IF NOT EXISTS cur_db;
```

### 步骤 4：创建外部表（DDL）
完整 DDL 见同目录 `cur_athena.sql`。关键点：
- `STORED AS PARQUET`，`LOCATION` 指向 `.../data/`
- 使用**分区投影**（partition projection），分区列 `billing_period` 形如 `2026-06`，范围 `2026-05,NOW`，按月递增
- 好处：新账单月数据**自动可查**，无需 `MSCK REPAIR TABLE` 或手工添加分区

### 步骤 5：执行查询（按资源 ID 出账）
见下方"查询参考"与 `cur_athena.sql`。

### 通过 AWS CLI 执行 Athena 查询的通用方式
```bash
# 提交查询
QID=$(aws athena start-query-execution --region us-east-1 \
  --work-group primary \
  --result-configuration OutputLocation=s3://my-cur-bucket/athena-results/ \
  --query-string "SELECT ...;" \
  --query QueryExecutionId --output text)

# 查状态
aws athena get-query-execution --region us-east-1 --query-execution-id "$QID"

# 取结果
aws athena get-query-results --region us-east-1 --query-execution-id "$QID"
```

---

## 4. 验证结果（实际执行）

执行 Q1（2026-06，按资源 ID 汇总未混合成本，Top 20），查询成功，扫描数据约 5.4 MB，耗时约 1.2 秒。Top 结果（USD）：

| 排名 | 资源 ID | 服务 | 区域 | 成本(USD) |
|----|---------|------|------|----------|
| 1 | inference-profile/us.anthropic.claude-opus-4-8 | AmazonBedrockService | us-east-1 | 1354.44 |
| 2 | fsx/fs-07d1649df5a697e89 | AmazonFSx | ap-southeast-1 | 1157.01 |
| 3 | inference-profile/global.anthropic.claude-opus-4-6-v1 | AmazonBedrockService | us-east-1 | 884.67 |
| 4 | es/domain/os2 | AmazonES | ap-southeast-1 | 712.69 |
| 5 | inference-profile/us.anthropic.claude-fable-5 | AmazonBedrockService | us-east-1 | 699.55 |
| 6 | i-0659ecc10fe07a4e4 | AmazonEC2 | ap-northeast-1 | 685.72 |
| 7 | efs/fs-0f795a9ddaee0453e | AmazonEFS | us-east-2 | 485.93 |
| 8 | i-04b6db800036f0c04 | AmazonEC2 | us-west-2 | 472.36 |
| 9 | acm-pca/.../36517b5e... | AWSCertificateManager | us-west-2 | 400.00 |
| 10 | rds/mcs-graph-dbinstance1 | AmazonNeptune | us-east-1 | 381.73 |

（完整结果含更多资源，CSV 在 `s3://my-cur-bucket/athena-results/` 下对应 QueryExecutionId。）

> 资源 ID 较长的已做缩写显示，实际为完整 ARN。

---

## 5. 查询参考

> 成本字段统一用 `line_item_unblended_cost`（未混合成本，最贴近账单）。
> 每条查询都建议带 `billing_period = 'YYYY-MM'` 以减少扫描量、降低 Athena 费用。

完整 SQL 见 `cur_athena.sql`，包含：
- **Q1** 某月每个资源 ID 的总成本（核心需求）
- **Q2** 单个指定资源 ID 的成本明细
- **Q3** 资源按天的成本趋势
- **Q4** 按服务汇总
- **Q5** Bedrock 成本按资源/模型下钻
- **Q6** 排除税/抵扣/退款，只看实际用量成本
- **Q7** 按 tag（项目/团队）分摊
- **Q8** 按 资源 ID + IAM Principal 出账（谁用哪个资源花了多少钱；仅含有 principal 的行）
- **Q9** 所有资源 + IAM Principal（无 principal 显示为 `(no principal)`，不丢数据）
- **Q10** 按 IAM Principal 汇总（先看谁花钱最多，再下钻）

> **关于 `line_item_iam_principal`**：该列仅在导出开启 `INCLUDE_IAM_PRINCIPAL_DATA=TRUE` 时存在（本导出已开启），记录产生费用的 IAM 用户/角色 ARN。它主要由 Bedrock 等服务填充；多数基础设施资源（EC2/FSx/RDS 等）此列为空。同一资源可能被不同 principal 使用而拆成多行，这正是按 principal 的归因。它是 CUR 的独立列，**不是资源标签（tag）**。

### 资源 ID + IAM Principal + Tag 三维联合（Q11–Q13）

Tag 存放在 `resource_tags`（及 `tags`）这两个 `map<string,string>` 字段里，用 `resource_tags['key']` 取值。用户自定义 tag 在 CUR 2.0 中 key 会带 **`user_` 前缀**。

- **辅助查询**：先用 `UNNEST(map_keys(resource_tags))` 发现数据里实际有哪些 tag key。本账号 2026-06 实际存在：`user_name`、`user_project`、`user_owner`、`user_parallelcluster_cluster_name`。
- **Q11** 资源 ID + IAM Principal + 多个 Tag 列的全景报表（每个 tag 取一列）。
- **Q12** 按 单个 Tag + IAM Principal 汇总（如按 owner 看每人/每角色花费）。
- **Q13** 用 `UNNEST(resource_tags)` 把所有 tag 动态平铺成 key=value 多行（tag key 不固定时适用）。

> **重要：三个维度通常是互补的，很少同时出现在同一行。** 实测显示：Bedrock 资源有 `iam_principal`（谁调用）但无 tag（推理配置打不了 tag）；基础设施资源（EC2/EFS/FSx）有 tag（project/owner/name）但无 `iam_principal`。放在一张表里能得到全景视图，但每行未必三者俱全，空值用 `-` / `(no principal)` / `(untagged)` 占位。

---

## 6. 注意事项

- **成本口径**：`line_item_unblended_cost` 为未混合成本。如需摊销视角（含 RI/SP 摊销），可改用 `reservation_effective_cost` / `savings_plan_savings_plan_effective_cost` 等字段组合。
- **排除非用量行**：账单含 `Tax`、`Credit`、`Refund` 等类型，用 `line_item_line_item_type` 过滤（见 Q6）。
- **数据延迟**：CUR 数据通常有约 24 小时延迟，当月数据每天多次刷新（本导出 RefreshCadence 为 SYNCHRONOUS）。
- **新月份**：得益于分区投影，下个账单月数据落到 S3 后无需改表即可查询；只需保证查询里 `billing_period` 用对值。
- **价格预估**：本流程用于分析**历史/当前**成本。未来价格预测/账单预估请使用 AWS 官方 Pricing Calculator（https://calculator.aws）。

---

## 7. 文件清单（本目录）

| 文件 | 说明 |
|------|------|
| `README.md` | 本操作文档 |
| `cur_athena.sql` | 建库 + 建表 DDL + 查询 SQL（Q1–Q16） |
| `panorama_2026-06.csv` | **全景数据导出**（2026-06，3742 个分组，不丢数据，合计 = 账单总额） |

---

## 8. 全景数据（不丢数据）— Q14/Q15/Q16

为得到"不丢任何数据"的全景表，关键是**不加任何行过滤**。之前的查询都带 `line_item_resource_id <> ''`，会丢掉没有资源 ID 的费用。

- **Q14** 全景表：`resource_id + iam_principal + 所有 tag + charge_type + region`，所有维度空值用占位符（`(no resource id)`、`(no principal)`、`-`），既不丢行也保留归因。
- **Q15** 对账校验：当月账单总额。
- **Q16** 单独查看"无资源 ID"那部分究竟是什么。

### 实测对账结果（2026-06）

| 指标 | 值 |
|------|-----|
| 当月账单总额 | **$29,827.01** |
| 原始明细行数 | 1,142,147 |
| 全景表分组行数 | 3,742 |
| 全景表 cost 合计 | **$29,827.01** |
| 与总额差异 | **0.000000** ✅ 一行不丢 |

### 重要发现：近一半费用没有资源 ID

| charge_type | 成本 (USD) |
|-------------|-----------|
| SavingsPlanRecurringFee | 14,475.58 |
| Usage（主要是 AWSSupportBusiness） | 2,661.78 |
| BundledDiscount | -35.77 |
| SavingsPlanNegation | -2,568.41 |
| **无资源 ID 合计** | **14,533.18（占总额 48.7%）** |

> 这就是为什么"按资源 ID 出账"必须搭配全景表对账：本账号约 **48.7%** 的费用（Savings Plan 月费、Support 费用、整笔折扣等）天生没有资源 ID。只看资源级查询会漏掉将近一半账单。
>
> 注：Savings Plan 的机制是 `SavingsPlanRecurringFee`（月承诺费，无资源 ID）+ `SavingsPlanNegation`（抵消按需价，负数），而被覆盖的用量以 `SavingsPlanCoveredUsage` 记在具体资源上。三者合计才是净成本。
