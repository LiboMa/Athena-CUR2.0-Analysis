-- =============================================================
-- AWS CUR 2.0 (Data Exports) Athena 建表 + 查询
-- 导出名: my-cur-export
-- 数据位置: s3://my-cur-bucket/cur-export/my-cur-export/data/
-- 格式: Parquet (snappy)，按 BILLING_PERIOD 分区
-- =============================================================

-- ① 建数据库（一次即可）
CREATE DATABASE IF NOT EXISTS cur_db;


-- ② 建外部表（DDL）
--    使用分区投影 partition projection，新月份数据自动可查，无需 MSCK REPAIR
CREATE EXTERNAL TABLE IF NOT EXISTS cur_db.cur_iam_bedrock (
  bill_bill_type string,
  bill_billing_entity string,
  bill_billing_period_end_date timestamp,
  bill_billing_period_start_date timestamp,
  bill_invoice_id string,
  bill_invoicing_entity string,
  bill_payer_account_id string,
  bill_payer_account_name string,
  capacity_reservation_capacity_reservation_arn string,
  capacity_reservation_capacity_reservation_status string,
  capacity_reservation_capacity_reservation_type string,
  cost_category map<string,string>,
  discount map<string,string>,
  discount_bundled_discount double,
  discount_total_discount double,
  identity_line_item_id string,
  identity_time_interval string,
  line_item_availability_zone string,
  line_item_blended_cost double,
  line_item_blended_rate string,
  line_item_currency_code string,
  line_item_iam_principal string,
  line_item_legal_entity string,
  line_item_line_item_description string,
  line_item_line_item_type string,
  line_item_net_unblended_cost double,
  line_item_net_unblended_rate string,
  line_item_normalization_factor double,
  line_item_normalized_usage_amount double,
  line_item_operation string,
  line_item_product_code string,
  line_item_resource_id string,
  line_item_tax_type string,
  line_item_unblended_cost double,
  line_item_unblended_rate string,
  line_item_usage_account_id string,
  line_item_usage_account_name string,
  line_item_usage_amount double,
  line_item_usage_end_date timestamp,
  line_item_usage_start_date timestamp,
  line_item_usage_type string,
  line_item_user_identifier string,
  pricing_currency string,
  pricing_lease_contract_length string,
  pricing_offering_class string,
  pricing_public_on_demand_cost double,
  pricing_public_on_demand_rate string,
  pricing_purchase_option string,
  pricing_rate_code string,
  pricing_rate_id string,
  pricing_term string,
  pricing_unit string,
  product map<string,string>,
  product_comment string,
  product_fee_code string,
  product_fee_description string,
  product_from_location string,
  product_from_location_type string,
  product_from_region_code string,
  product_instance_family string,
  product_instance_type string,
  product_instancesku string,
  product_location string,
  product_location_type string,
  product_operation string,
  product_pricing_unit string,
  product_product_family string,
  product_region_code string,
  product_servicecode string,
  product_sku string,
  product_to_location string,
  product_to_location_type string,
  product_to_region_code string,
  product_usagetype string,
  reservation_amortized_upfront_cost_for_usage double,
  reservation_amortized_upfront_fee_for_billing_period double,
  reservation_availability_zone string,
  reservation_effective_cost double,
  reservation_end_time string,
  reservation_modification_status string,
  reservation_net_amortized_upfront_cost_for_usage double,
  reservation_net_amortized_upfront_fee_for_billing_period double,
  reservation_net_effective_cost double,
  reservation_net_recurring_fee_for_usage double,
  reservation_net_unused_amortized_upfront_fee_for_billing_period double,
  reservation_net_unused_recurring_fee double,
  reservation_net_upfront_value double,
  reservation_normalized_units_per_reservation string,
  reservation_number_of_reservations string,
  reservation_recurring_fee_for_usage double,
  reservation_reservation_a_r_n string,
  reservation_start_time string,
  reservation_subscription_id string,
  reservation_total_reserved_normalized_units string,
  reservation_total_reserved_units string,
  reservation_units_per_reservation string,
  reservation_unused_amortized_upfront_fee_for_billing_period double,
  reservation_unused_normalized_unit_quantity double,
  reservation_unused_quantity double,
  reservation_unused_recurring_fee double,
  reservation_upfront_value double,
  resource_tags map<string,string>,
  savings_plan_amortized_upfront_commitment_for_billing_period double,
  savings_plan_end_time string,
  savings_plan_instance_type_family string,
  savings_plan_net_amortized_upfront_commitment_for_billing_period double,
  savings_plan_net_recurring_commitment_for_billing_period double,
  savings_plan_net_savings_plan_effective_cost double,
  savings_plan_offering_type string,
  savings_plan_payment_option string,
  savings_plan_purchase_term string,
  savings_plan_recurring_commitment_for_billing_period double,
  savings_plan_region string,
  savings_plan_savings_plan_a_r_n string,
  savings_plan_savings_plan_effective_cost double,
  savings_plan_savings_plan_rate double,
  savings_plan_start_time string,
  savings_plan_total_commitment_to_date double,
  savings_plan_used_commitment double,
  split_line_item_actual_usage double,
  split_line_item_net_split_cost double,
  split_line_item_net_unused_cost double,
  split_line_item_parent_resource_id string,
  split_line_item_public_on_demand_split_cost double,
  split_line_item_public_on_demand_unused_cost double,
  split_line_item_reserved_usage double,
  split_line_item_split_cost double,
  split_line_item_split_usage double,
  split_line_item_split_usage_ratio double,
  split_line_item_unused_cost double,
  tags map<string,string>
)
PARTITIONED BY (billing_period string)
STORED AS PARQUET
LOCATION 's3://my-cur-bucket/cur-export/my-cur-export/data/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.billing_period.type' = 'date',
  'projection.billing_period.format' = 'yyyy-MM',
  'projection.billing_period.range' = '2026-05,NOW',
  'projection.billing_period.interval' = '1',
  'projection.billing_period.interval.unit' = 'MONTHS',
  'storage.location.template' = 's3://my-cur-bucket/cur-export/my-cur-export/data/BILLING_PERIOD=${billing_period}'
);


-- =============================================================
-- 查询 SQL
-- 说明:
--  * 成本字段一般用 line_item_unblended_cost（未混合成本，最接近账单）
--  * 过滤 line_item_line_item_type 排除 Tax/Credit/Refund 可只看用量成本
--  * billing_period 形如 '2026-06'，强烈建议每个查询都带上以减少扫描量
-- =============================================================

-- 【Q1】按资源 ID 出账单：某月每个资源的总成本（核心需求）
SELECT
    line_item_resource_id                  AS resource_id,
    line_item_product_code                 AS service,
    product_region_code                    AS region,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_resource_id <> ''
GROUP BY line_item_resource_id, line_item_product_code, product_region_code
ORDER BY cost_usd DESC;


-- 【Q2】查单个指定资源 ID 的成本明细（替换成你的资源 ID）
SELECT
    line_item_resource_id                  AS resource_id,
    line_item_product_code                 AS service,
    line_item_usage_type                   AS usage_type,
    line_item_operation                    AS operation,
    line_item_line_item_type               AS charge_type,
    SUM(line_item_usage_amount)            AS usage_amount,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_resource_id = 'PUT-YOUR-RESOURCE-ID-HERE'
GROUP BY line_item_resource_id, line_item_product_code,
         line_item_usage_type, line_item_operation, line_item_line_item_type
ORDER BY cost_usd DESC;


-- 【Q3】按资源 ID + 按天 的成本趋势（小时粒度数据可下钻到天）
SELECT
    date(line_item_usage_start_date)       AS usage_date,
    line_item_resource_id                  AS resource_id,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_resource_id <> ''
GROUP BY date(line_item_usage_start_date), line_item_resource_id
ORDER BY usage_date, cost_usd DESC;


-- 【Q4】按服务汇总（对账用：先看服务总额再下钻到资源）
SELECT
    line_item_product_code                 AS service,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
GROUP BY line_item_product_code
ORDER BY cost_usd DESC;


-- 【Q5】Bedrock 成本按资源/模型下钻（你的导出名暗示主要关注 Bedrock）
SELECT
    line_item_resource_id                  AS resource_id,
    line_item_usage_type                   AS usage_type,
    line_item_operation                    AS operation,
    SUM(line_item_usage_amount)            AS usage_amount,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_product_code = 'AmazonBedrock'
GROUP BY line_item_resource_id, line_item_usage_type, line_item_operation
ORDER BY cost_usd DESC;


-- 【Q6】只看实际用量成本（排除税/抵扣/退款），并按资源出账
SELECT
    line_item_resource_id                  AS resource_id,
    line_item_product_code                 AS service,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_resource_id <> ''
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage', 'SavingsPlanCoveredUsage')
GROUP BY line_item_resource_id, line_item_product_code
ORDER BY cost_usd DESC;


-- 【Q7】按 tag 出账（按项目/团队分摊）。把 'Project' 换成你的标签 key
SELECT
    tags['Project']                        AS project_tag,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
GROUP BY tags['Project']
ORDER BY cost_usd DESC;


-- 【Q8】按 资源 ID + IAM Principal 出账（谁在用哪个资源花了多少钱）
--   line_item_iam_principal 是导出中 INCLUDE_IAM_PRINCIPAL_DATA=TRUE 才有的列，
--   记录产生该费用的 IAM 用户/角色 ARN。注意：主要由 Bedrock 等服务填充，
--   多数基础设施资源（EC2/FSx/RDS 等）该字段为空，因此下面过滤了空值。
SELECT
    line_item_resource_id                  AS resource_id,
    line_item_iam_principal                AS iam_principal,
    line_item_product_code                 AS service,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_resource_id <> ''
  AND line_item_iam_principal <> ''
GROUP BY line_item_resource_id, line_item_iam_principal, line_item_product_code
ORDER BY cost_usd DESC;


-- 【Q9】所有资源 + IAM Principal（无 principal 的显示为 '(no principal)'，不丢数据）
SELECT
    line_item_resource_id                                       AS resource_id,
    COALESCE(NULLIF(line_item_iam_principal, ''), '(no principal)') AS iam_principal,
    line_item_product_code                                      AS service,
    SUM(line_item_unblended_cost)                               AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_resource_id <> ''
GROUP BY line_item_resource_id,
         COALESCE(NULLIF(line_item_iam_principal, ''), '(no principal)'),
         line_item_product_code
ORDER BY cost_usd DESC;


-- 【Q10】按 IAM Principal 汇总（先看谁花钱最多，再下钻到资源）
SELECT
    line_item_iam_principal                AS iam_principal,
    SUM(line_item_unblended_cost)          AS cost_usd,
    COUNT(DISTINCT line_item_resource_id)  AS distinct_resources
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_iam_principal <> ''
GROUP BY line_item_iam_principal
ORDER BY cost_usd DESC;


-- =============================================================
-- 资源 ID + IAM Principal + Tag 三维联合统计
-- =============================================================

-- 【辅助】先发现数据里实际有哪些 tag key（用户自定义 tag 在 CUR 2.0 中带 user_ 前缀）
--   本账号 2026-06 实际存在: user_name, user_project, user_owner, user_parallelcluster_cluster_name
SELECT tk AS tag_key,
       COUNT(*) AS rows_cnt,
       SUM(line_item_unblended_cost) AS cost_usd
FROM cur_db.cur_iam_bedrock
CROSS JOIN UNNEST(map_keys(resource_tags)) AS t(tk)
WHERE billing_period = '2026-06'
GROUP BY tk
ORDER BY cost_usd DESC;


-- 【Q11】资源 ID + IAM Principal + 多个 Tag 列（全景报表，每个 tag 取成一列）
--   注意: 三个维度通常互补——Bedrock 资源有 principal 无 tag；
--         基础设施资源（EC2/EFS/FSx）有 tag 无 principal。空值已用占位符显示。
SELECT
    line_item_resource_id                                          AS resource_id,
    COALESCE(NULLIF(line_item_iam_principal, ''), '(no principal)') AS iam_principal,
    line_item_product_code                                         AS service,
    COALESCE(NULLIF(resource_tags['user_project'], ''), '-')       AS tag_project,
    COALESCE(NULLIF(resource_tags['user_owner'], ''), '-')         AS tag_owner,
    COALESCE(NULLIF(resource_tags['user_name'], ''), '-')          AS tag_name,
    SUM(line_item_unblended_cost)                                  AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND line_item_resource_id <> ''
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY cost_usd DESC;


-- 【Q12】按 单个 Tag + IAM Principal 汇总（如：按 owner 看每人/每角色花费）
--   把 'user_owner' 换成你关心的 tag key
SELECT
    COALESCE(NULLIF(resource_tags['user_owner'], ''), '(untagged)') AS tag_owner,
    COALESCE(NULLIF(line_item_iam_principal, ''), '(no principal)') AS iam_principal,
    SUM(line_item_unblended_cost)                                  AS cost_usd,
    COUNT(DISTINCT line_item_resource_id)                          AS distinct_resources
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
GROUP BY 1, 2
ORDER BY cost_usd DESC;


-- 【Q13】把所有 tag 以 key=value 形式平铺成多行（无需预先知道 tag key，动态展开）
--   适合 tag key 不固定、想一次性看全部 tag 维度的场景
SELECT
    line_item_resource_id                  AS resource_id,
    COALESCE(NULLIF(line_item_iam_principal, ''), '(no principal)') AS iam_principal,
    tag_key,
    tag_value,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
CROSS JOIN UNNEST(resource_tags) AS t(tag_key, tag_value)
WHERE billing_period = '2026-06'
  AND line_item_resource_id <> ''
GROUP BY line_item_resource_id,
         COALESCE(NULLIF(line_item_iam_principal, ''), '(no principal)'),
         tag_key, tag_value
ORDER BY cost_usd DESC;


-- =============================================================
-- 全景报表（不丢数据）：覆盖全部账单行，合计 = 当月账单总额
-- 关键：不加 line_item_resource_id<>'' 等任何行过滤，否则会丢掉
--       无资源 ID 的费用（Savings Plan 月费、Support 费、整笔折扣等）
--       —— 本账号这部分高达约 48.7%！
-- 所有维度空值用占位符显示，保证既不丢行也能看清归因。
-- =============================================================

-- 【Q14】全景表：资源ID + IAM Principal + 所有 Tag + 计费类型 + 区域
SELECT
    COALESCE(NULLIF(line_item_resource_id, ''), '(no resource id)')                  AS resource_id,
    COALESCE(NULLIF(line_item_iam_principal, ''), '(no principal)')                  AS iam_principal,
    line_item_product_code                                                           AS service,
    line_item_line_item_type                                                         AS charge_type,
    COALESCE(NULLIF(product_region_code, ''), '-')                                   AS region,
    COALESCE(NULLIF(resource_tags['user_project'], ''), '-')                         AS tag_project,
    COALESCE(NULLIF(resource_tags['user_owner'], ''), '-')                           AS tag_owner,
    COALESCE(NULLIF(resource_tags['user_name'], ''), '-')                            AS tag_name,
    COALESCE(NULLIF(resource_tags['user_parallelcluster_cluster_name'], ''), '-')    AS tag_cluster,
    SUM(line_item_unblended_cost)                                                    AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY cost_usd DESC;


-- 【Q15】对账校验：证明全景表没丢数据（grand_total 应等于 Q14 各行 cost_usd 之和）
SELECT
    SUM(line_item_unblended_cost)          AS grand_total_usd,
    COUNT(*)                               AS raw_line_items
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06';


-- 【Q16】看清"无资源 ID"那部分到底是什么（之前被过滤掉的近一半费用）
SELECT
    line_item_line_item_type               AS charge_type,
    line_item_product_code                 AS service,
    SUM(line_item_unblended_cost)          AS cost_usd
FROM cur_db.cur_iam_bedrock
WHERE billing_period = '2026-06'
  AND (line_item_resource_id IS NULL OR line_item_resource_id = '')
GROUP BY line_item_line_item_type, line_item_product_code
ORDER BY cost_usd DESC;
