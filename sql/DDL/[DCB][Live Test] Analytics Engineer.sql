CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;



-- purchase_cdc
-- Grão: 1 evento de alteração de compra (CDC).

CREATE TABLE IF NOT EXISTS raw.purchase_cdc (
  purchase_id           BIGINT NOT NULL,
  purchase_partition    BIGINT NOT NULL,
  buyer_id              BIGINT NULL,
  prod_item_id          BIGINT NULL,
  prod_item_partition   BIGINT NULL,
  producer_id           BIGINT NULL,
  order_date            DATE NULL,
  release_date          DATE NULL,
  purchase_total_value  NUMERIC(18,2) NULL,
  purchase_status       TEXT NULL,
  -- CDC
  op                    CHAR(1) NOT NULL CHECK (op IN ('c','u','d')),
  event_ts              TIMESTAMP NOT NULL,   -- quando o evento ocorreu na origem
  ingestion_ts          TIMESTAMP NOT NULL,   -- quando chegou no lake/warehouse
  -- colunas auxiliares (deriváveis)
  transaction_date      DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (purchase_id, purchase_partition, event_ts, ingestion_ts)
);

CREATE INDEX IF NOT EXISTS ix_purchase_cdc_ingestion
  ON raw.purchase_cdc (ingestion_ts);

CREATE INDEX IF NOT EXISTS ix_purchase_cdc_event
  ON raw.purchase_cdc (purchase_id, purchase_partition, event_ts DESC, ingestion_ts DESC);




-- product_item_cdc
--Grão: 1 evento CDC por item (pode haver múltiplos itens por compra).

CREATE TABLE IF NOT EXISTS raw.product_item_cdc (
  prod_item_id         BIGINT NOT NULL,
  prod_item_partition  BIGINT NOT NULL,
  purchase_id          BIGINT NULL,  -- ajuda no join (se existir no source)
  purchase_partition   BIGINT NULL,
  product_id           BIGINT NULL,
  item_quantity        INT NULL,
  purchase_value       NUMERIC(18,2) NULL,
  -- CDC
  op                   CHAR(1) NOT NULL CHECK (op IN ('c','u','d')),
  event_ts             TIMESTAMP NOT NULL,
  ingestion_ts         TIMESTAMP NOT NULL,
  transaction_date     DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (prod_item_id, prod_item_partition, event_ts, ingestion_ts)
);


CREATE INDEX IF NOT EXISTS ix_product_item_cdc_ingestion
  ON raw.product_item_cdc (ingestion_ts);

CREATE INDEX IF NOT EXISTS ix_product_item_cdc_event
  ON raw.product_item_cdc (prod_item_id, prod_item_partition, event_ts DESC, ingestion_ts DESC);

CREATE INDEX IF NOT EXISTS ix_product_item_cdc_purchase
  ON raw.product_item_cdc (purchase_id, purchase_partition);


-- purchase_extra_info_cdc
-- Grão: 1 evento CDC por compra para atributos extras (subsidiary).
CREATE TABLE IF NOT EXISTS raw.purchase_extra_info_cdc (
  purchase_id          BIGINT NOT NULL,
  purchase_partition   BIGINT NOT NULL,
  subsidiary           TEXT NULL,
  -- CDC
  op                   CHAR(1) NOT NULL CHECK (op IN ('c','u','d')),
  event_ts             TIMESTAMP NOT NULL,
  ingestion_ts         TIMESTAMP NOT NULL,
  transaction_date     DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (purchase_id, purchase_partition, event_ts, ingestion_ts)
);

CREATE INDEX IF NOT EXISTS ix_extra_info_cdc_ingestion
  ON raw.purchase_extra_info_cdc (ingestion_ts);

CREATE INDEX IF NOT EXISTS ix_extra_info_cdc_event
  ON raw.purchase_extra_info_cdc (purchase_id, purchase_partition, event_ts DESC, ingestion_ts DESC);



-- order_transaction_cost_hist_cdc (bônus)
-- Grão: 1 evento por componente de custo (ex.: VAT, installment) por compra.
CREATE TABLE IF NOT EXISTS raw.order_transaction_cost_hist_cdc (
  purchase_id            BIGINT NOT NULL,
  purchase_partition     BIGINT NOT NULL,
  order_transaction_cost_vat_value          NUMERIC(18,2) NULL,
  order_transaction_cost_installment_value  NUMERIC(18,2) NULL,
  order_transaction_cost_date               DATE NULL,
  -- CDC
  op                     CHAR(1) NOT NULL CHECK (op IN ('c','u','d')),
  event_ts               TIMESTAMP NOT NULL,
  ingestion_ts           TIMESTAMP NOT NULL,
  transaction_date       DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (purchase_id, purchase_partition, event_ts, ingestion_ts)
);

CREATE INDEX IF NOT EXISTS ix_cost_cdc_ingestion
  ON raw.order_transaction_cost_hist_cdc (ingestion_ts);

CREATE INDEX IF NOT EXISTS ix_cost_cdc_event
  ON raw.order_transaction_cost_hist_cdc (purchase_id, purchase_partition, event_ts DESC, ingestion_ts DESC);
