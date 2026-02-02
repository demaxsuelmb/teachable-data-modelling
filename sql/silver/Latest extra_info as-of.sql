WITH extra_latest AS (
  SELECT *
  FROM (
    SELECT
      e.*,
      ROW_NUMBER() OVER (
        PARTITION BY e.purchase_id, e.purchase_partition
        ORDER BY e.event_ts DESC, e.ingestion_ts DESC
      ) AS rn
    FROM raw.purchase_extra_info_cdc e
    WHERE e.ingestion_ts < :cutoff_ts
  ) x
  WHERE rn = 1
)
SELECT * FROM extra_latest;
