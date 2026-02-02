--Inserir snapshot D-1 (append-only)

--Rodar esse INSERT para cada as_of_date (run).
--Ele não atualiza nada: só insere uma versão nova.


WITH
params AS (
  SELECT
    date '2023-01-07'                 AS as_of_date,
    timestamp '2023-01-08 00:00:00'   AS cutoff_ts,
    '2023-01-07_d-1'::text            AS run_id
),
purchase_latest AS (
  SELECT *
  FROM (
    SELECT
      p.*,
      ROW_NUMBER() OVER (
        PARTITION BY p.purchase_id, p.purchase_partition
        ORDER BY p.event_ts DESC, p.ingestion_ts DESC
      ) AS rn
    FROM raw.purchase_cdc p
    CROSS JOIN params
    WHERE p.ingestion_ts < params.cutoff_ts
  ) x
  WHERE rn = 1
),
extra_latest AS (
  SELECT *
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY e.purchase_id, e.purchase_partition
        ORDER BY e.event_ts DESC, e.ingestion_ts DESC
      ) AS rn
    FROM raw.purchase_extra_info_cdc e
    CROSS JOIN params
    WHERE e.ingestion_ts < params.cutoff_ts
  ) x
  WHERE rn = 1
),
items_latest AS (
  SELECT *
  FROM (
    SELECT
      i.*,
      ROW_NUMBER() OVER (
        PARTITION BY i.prod_item_id, i.prod_item_partition
        ORDER BY i.event_ts DESC, i.ingestion_ts DESC
      ) AS rn
    FROM raw.product_item_cdc i
    CROSS JOIN params
    WHERE i.ingestion_ts < params.cutoff_ts
  ) x
  WHERE rn = 1
),
    MAX(ingestion_ts) AS max_item_ingestion
  FROM items_latest
  WHERE op <> 'd'
  GROUP BY 1,2
),
eligible_purchases AS (
  SELECT
    p.purchase_id,
    p.purchase_partition,
    p.order_date::date AS transaction_date,
    e.subsidiary,
    i.amount_total AS gmv_amount,
    GREATEST(p.ingestion_ts, e.ingestion_ts, i.max_item_ingestion) AS max_ingestion_ts
  FROM purchase_latest p
  JOIN extra_latest e
    ON e.purchase_id = p.purchase_id
   AND e.purchase_partition = p.purchase_partition
  JOIN items_by_purchase i
    ON i.purchase_id = p.purchase_id
   AND i.purchase_partition = p.purchase_partition
  WHERE p.release_date IS NOT NULL
    AND (p.purchase_status IS NULL OR p.purchase_status NOT IN ('CANCELADA','REEMBOLSADA'))
    AND p.op <> 'd'
)
INSERT INTO mart.gmv_daily_subsidiary_hist (
  transaction_date,
  subsidiary,
  as_of_date,
  gmv_amount,
  purchases_count,
  source_max_ingestion_ts,
  run_id
)
SELECT
  ep.transaction_date,
  ep.subsidiary,
  params.as_of_date,
  SUM(ep.gmv_amount) AS gmv_amount,
  COUNT(*) AS purchases_count,
  MAX(ep.max_ingestion_ts) AS source_max_ingestion_ts,
  params.run_id
FROM eligible_purchases ep
CROSS JOIN params
GROUP BY 1,2,3,7;
items_by_purchase AS (
  SELECT
    purchase_id,
    purchase_partition,
    SUM(COALESCE(purchase_value,0) * COALESCE(item_quantity,1)) AS amount_total,




-----------========================================================================
-- Run 2: as_of 2023-01-10 (cutoff 2023-01-11 00:00)
WITH
params AS (
  SELECT
    date '2023-01-10'                 AS as_of_date,
    timestamp '2023-01-11 00:00:00'   AS cutoff_ts,
    '2023-01-10_d-1'::text            AS run_id
),
purchase_latest AS (
  SELECT *
  FROM (
    SELECT
      p.*,
      ROW_NUMBER() OVER (
        PARTITION BY p.purchase_id, p.purchase_partition
        ORDER BY p.event_ts DESC, p.ingestion_ts DESC
      ) AS rn
    FROM raw.purchase_cdc p
    CROSS JOIN params
    WHERE p.ingestion_ts < params.cutoff_ts
  ) x
  WHERE rn = 1
),
extra_latest AS (
  SELECT *
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY e.purchase_id, e.purchase_partition
        ORDER BY e.event_ts DESC, e.ingestion_ts DESC
      ) AS rn
    FROM raw.purchase_extra_info_cdc e
    CROSS JOIN params
    WHERE e.ingestion_ts < params.cutoff_ts
  ) x
  WHERE rn = 1
),
items_latest AS (
  SELECT *
  FROM (
    SELECT
      i.*,
      ROW_NUMBER() OVER (
        PARTITION BY i.prod_item_id, i.prod_item_partition
        ORDER BY i.event_ts DESC, i.ingestion_ts DESC
      ) AS rn
    FROM raw.product_item_cdc i
    CROSS JOIN params
    WHERE i.ingestion_ts < params.cutoff_ts
  ) x
  WHERE rn = 1
),
items_by_purchase AS (
  SELECT
    purchase_id,
    purchase_partition,
    SUM(COALESCE(purchase_value,0) * COALESCE(item_quantity,1)) AS amount_total,
    MAX(ingestion_ts) AS max_item_ingestion
  FROM items_latest
  WHERE op <> 'd'
  GROUP BY 1,2
),
eligible_purchases AS (
  SELECT
    p.purchase_id,
    p.purchase_partition,
    p.order_date::date AS transaction_date,
    e.subsidiary,
    i.amount_total AS gmv_amount,
    GREATEST(p.ingestion_ts, e.ingestion_ts, i.max_item_ingestion) AS max_ingestion_ts
  FROM purchase_latest p
  JOIN extra_latest e
    ON e.purchase_id = p.purchase_id
   AND e.purchase_partition = p.purchase_partition
  JOIN items_by_purchase i
    ON i.purchase_id = p.purchase_id
   AND i.purchase_partition = p.purchase_partition
  WHERE p.release_date IS NOT NULL
    AND (p.purchase_status IS NULL OR p.purchase_status NOT IN ('CANCELADA','REEMBOLSADA'))
    AND p.op <> 'd'
)
INSERT INTO mart.gmv_daily_subsidiary_hist (
  transaction_date,
  subsidiary,
  as_of_date,
  gmv_amount,
  purchases_count,
  source_max_ingestion_ts,
  run_id
)
SELECT
  ep.transaction_date,
  ep.subsidiary,
  params.as_of_date,
  SUM(ep.gmv_amount) AS gmv_amount,
  COUNT(*) AS purchases_count,
  MAX(ep.max_ingestion_ts) AS source_max_ingestion_ts,
  params.run_id
FROM eligible_purchases ep
CROSS JOIN params
GROUP BY 1,2,3,7;
