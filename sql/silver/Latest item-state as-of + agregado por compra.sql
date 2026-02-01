WITH items_latest AS (
  SELECT *
  FROM (
    SELECT
      i.*,
      ROW_NUMBER() OVER (
        PARTITION BY i.prod_item_id, i.prod_item_partition
        ORDER BY i.event_ts DESC, i.ingestion_ts DESC
      ) AS rn
    FROM raw.product_item_cdc i
    WHERE i.ingestion_ts < :cutoff_ts
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
)
SELECT * FROM items_by_purchase;
