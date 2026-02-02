-- DDL (particionada por transaction_date)
-- Em Postgres, particionamento é real. Vamos criar a tabela “pai” e depois partições por mês (suficiente pro case). 

CREATE TABLE IF NOT EXISTS mart.gmv_daily_subsidiary_hist (
  transaction_date        DATE NOT NULL,
  subsidiary              TEXT NOT NULL,
  as_of_date              DATE NOT NULL,     -- snapshot/run date
  gmv_amount              NUMERIC(18,2) NOT NULL,
  purchases_count         INTEGER NOT NULL,
  source_max_ingestion_ts TIMESTAMP NOT NULL,
  run_id                  TEXT NOT NULL,
  PRIMARY KEY (transaction_date, subsidiary, as_of_date)
) PARTITION BY RANGE (transaction_date);


-- Partições exemplo (crie conforme o intervalo do seu teste)
CREATE TABLE IF NOT EXISTS mart.gmv_daily_subsidiary_hist_2023_01
  PARTITION OF mart.gmv_daily_subsidiary_hist
  FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');


CREATE TABLE IF NOT EXISTS mart.gmv_daily_subsidiary_hist_2023_02
  PARTITION OF mart.gmv_daily_subsidiary_hist
  FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');


-- View “current” (100% sem UPDATE)
CREATE OR REPLACE VIEW mart.gmv_daily_subsidiary_current AS
SELECT h.*
FROM mart.gmv_daily_subsidiary_hist h
WHERE h.as_of_date = (SELECT MAX(as_of_date) FROM mart.gmv_daily_subsidiary_hist);

