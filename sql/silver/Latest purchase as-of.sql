-- :cutoff_ts (ex.: '2023-01-08 00:00:00')
WITH purchase_latest AS (
  SELECT *
  FROM (
    SELECT
      p.*,
      ROW_NUMBER() OVER (
        PARTITION BY p.purchase_id, p.purchase_partition
        ORDER BY p.event_ts DESC, p.ingestion_ts DESC
      ) AS rn
    FROM raw.purchase_cdc p
    WHERE p.ingestion_ts < :cutoff_ts
  ) x
  WHERE rn = 1
)
SELECT * FROM purchase_latest;
