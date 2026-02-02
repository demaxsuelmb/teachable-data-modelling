/* ============================================================
   RUN-ALL (RESET TOTAL) — Case Teachable (Data Engineer)
   Banco: teachable_case
   Schemas: raw, mart

   Objetivo:
   - Criar tabelas RAW CDC-friendly (append-only) com:
       event_ts (quando ocorreu na origem)
       ingestion_ts (quando chegou no lake/warehouse)
       op (c/u/d)
   - Criar tabela final GOLD (append-only por snapshot / as_of_date):
       GMV diário por subsidiária
       Particionada por transaction_date
       Suporta "as-of queries" SEM reescrever o passado
   - Inserir dados mockados para demonstrar:
       eventos fora de ordem
       evento tardio (late arriving)
       correção tardia (reprocessamento)
   - Executar 2 snapshots (2 runs) para evidenciar o comportamento temporal
   - Bônus: reconciliar GMV com custos (contribution margin)

   COMO LER:
   1) RAW: não corrigimos nada; só armazenamos eventos (append-only).
   2) SILVER: para cada snapshot, pegamos o "último estado conhecido" até um cutoff.
   3) GOLD: agregamos e INSERT (append-only) por as_of_date.

   ============================================================ */

-- ============================================================
-- 0) Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

-- ============================================================
-- 1) RESET: apagamos apenas objetos do case
--    (isso garante reprodutibilidade do run-all)
-- ============================================================

-- View depende da tabela, então removemos primeiro
DROP VIEW IF EXISTS mart.gmv_daily_subsidiary_current;

-- Partições primeiro, depois a tabela "pai"
DROP TABLE IF EXISTS mart.gmv_daily_subsidiary_hist_2023_02;
DROP TABLE IF EXISTS mart.gmv_daily_subsidiary_hist_2023_01;
DROP TABLE IF EXISTS mart.gmv_daily_subsidiary_hist;

-- RAW tables
DROP TABLE IF EXISTS raw.order_transaction_cost_hist_cdc;
DROP TABLE IF EXISTS raw.purchase_extra_info_cdc;
DROP TABLE IF EXISTS raw.product_item_cdc;
DROP TABLE IF EXISTS raw.purchase_cdc;

-- ============================================================
-- 2) RAW (BRONZE) — Tabelas CDC-friendly (append-only)
--    IMPORTANTE: no RAW, evitamos FKs rígidas.
--    Motivo: CDC pode chegar fora de ordem e uma FK quebraria ingestão.
-- ============================================================

/* ------------------------------------------------------------
   2.1 purchase_cdc
   Grão: 1 evento CDC por (purchase_id, purchase_partition)
   Uma purchase pode ter múltiplos eventos (create/update/reprocess).
   PK composta inclui event_ts + ingestion_ts para permitir múltiplas versões.
------------------------------------------------------------ */
CREATE TABLE raw.purchase_cdc (
  purchase_id           BIGINT NOT NULL,
  purchase_partition    BIGINT NOT NULL,
  buyer_id              BIGINT NULL,
  prod_item_id          BIGINT NULL,
  prod_item_partition   BIGINT NULL,
  producer_id           BIGINT NULL,
  order_date            DATE NULL,
  release_date          DATE NULL,        -- pagamento confirmado quando NOT NULL
  purchase_total_value  NUMERIC(18,2) NULL,
  purchase_status       TEXT NULL,        -- ex.: INICIADA, APROVADA, CANCELADA, REEMBOLSADA
  op                    CHAR(1) NOT NULL CHECK (op IN ('c','u','d')), -- create/update/delete
  event_ts              TIMESTAMP NOT NULL, -- quando ocorreu na origem
  ingestion_ts          TIMESTAMP NOT NULL, -- quando entrou no lake/warehouse
  -- derivado para facilitar debug; não é obrigatório no enunciado
  transaction_date      DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (purchase_id, purchase_partition, event_ts, ingestion_ts)
);

CREATE INDEX ix_purchase_cdc_ingestion
  ON raw.purchase_cdc (ingestion_ts);

CREATE INDEX ix_purchase_cdc_latest
  ON raw.purchase_cdc (purchase_id, purchase_partition, event_ts DESC, ingestion_ts DESC);


/* ------------------------------------------------------------
   2.2 product_item_cdc
   Grão: 1 evento CDC por item (prod_item_id, prod_item_partition)
   Itens podem ser vários por purchase (dependendo do domínio).
------------------------------------------------------------ */
CREATE TABLE raw.product_item_cdc (
  prod_item_id         BIGINT NOT NULL,
  prod_item_partition  BIGINT NOT NULL,
  -- referência lógica (pode vir do source; ajuda no join)
  purchase_id          BIGINT NULL,
  purchase_partition   BIGINT NULL,
  product_id           BIGINT NULL,
  item_quantity        INT NULL,
  purchase_value       NUMERIC(18,2) NULL, -- valor unitário do item
  op                   CHAR(1) NOT NULL CHECK (op IN ('c','u','d')),
  event_ts             TIMESTAMP NOT NULL,
  ingestion_ts         TIMESTAMP NOT NULL,
  transaction_date     DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (prod_item_id, prod_item_partition, event_ts, ingestion_ts)
);

CREATE INDEX ix_product_item_cdc_ingestion
  ON raw.product_item_cdc (ingestion_ts);

CREATE INDEX ix_product_item_cdc_latest
  ON raw.product_item_cdc (prod_item_id, prod_item_partition, event_ts DESC, ingestion_ts DESC);

CREATE INDEX ix_product_item_cdc_purchase
  ON raw.product_item_cdc (purchase_id, purchase_partition);


/* ------------------------------------------------------------
   2.3 purchase_extra_info_cdc
   Grão: 1 evento CDC por (purchase_id, purchase_partition)
   Contém atributo dimensional: subsidiária
------------------------------------------------------------ */
CREATE TABLE raw.purchase_extra_info_cdc (
  purchase_id          BIGINT NOT NULL,
  purchase_partition   BIGINT NOT NULL,
  subsidiary           TEXT NULL,
  op                   CHAR(1) NOT NULL CHECK (op IN ('c','u','d')),
  event_ts             TIMESTAMP NOT NULL,
  ingestion_ts         TIMESTAMP NOT NULL,
  transaction_date     DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (purchase_id, purchase_partition, event_ts, ingestion_ts)
);

CREATE INDEX ix_extra_info_cdc_ingestion
  ON raw.purchase_extra_info_cdc (ingestion_ts);

CREATE INDEX ix_extra_info_cdc_latest
  ON raw.purchase_extra_info_cdc (purchase_id, purchase_partition, event_ts DESC, ingestion_ts DESC);


/* ------------------------------------------------------------
   2.4 order_transaction_cost_hist_cdc (BÔNUS)
   Grão: 1 evento CDC de custos por (purchase_id, purchase_partition)
   Serve para reconciliar GMV com custos (margem/contribuição)
------------------------------------------------------------ */
CREATE TABLE raw.order_transaction_cost_hist_cdc (
  purchase_id            BIGINT NOT NULL,
  purchase_partition     BIGINT NOT NULL,
  order_transaction_cost_vat_value          NUMERIC(18,2) NULL,
  order_transaction_cost_installment_value  NUMERIC(18,2) NULL,
  order_transaction_cost_date               DATE NULL,
  op                     CHAR(1) NOT NULL CHECK (op IN ('c','u','d')),
  event_ts               TIMESTAMP NOT NULL,
  ingestion_ts           TIMESTAMP NOT NULL,
  transaction_date       DATE GENERATED ALWAYS AS (ingestion_ts::date) STORED,
  PRIMARY KEY (purchase_id, purchase_partition, event_ts, ingestion_ts)
);

CREATE INDEX ix_cost_cdc_ingestion
  ON raw.order_transaction_cost_hist_cdc (ingestion_ts);

CREATE INDEX ix_cost_cdc_latest
  ON raw.order_transaction_cost_hist_cdc (purchase_id, purchase_partition, event_ts DESC, ingestion_ts DESC);

-- ============================================================
-- 3) GOLD — Tabela final analítica (append-only por snapshot)
--    Grão: 1 linha por (transaction_date, subsidiary, as_of_date)
--    Partição: transaction_date (requisito)
-- ============================================================

CREATE TABLE mart.gmv_daily_subsidiary_hist (
  transaction_date        DATE NOT NULL,  -- partição e dimensão temporal do GMV
  subsidiary              TEXT NOT NULL,  -- dimensão de negócio
  as_of_date              DATE NOT NULL,  -- data do snapshot (data do "run")
  gmv_amount              NUMERIC(18,2) NOT NULL,
  purchases_count         INTEGER NOT NULL,
  -- Auditoria/linhagem: até qual ingestion_ts este snapshot foi construído
  source_max_ingestion_ts TIMESTAMP NOT NULL,
  run_id                  TEXT NOT NULL,
  PRIMARY KEY (transaction_date, subsidiary, as_of_date)
) PARTITION BY RANGE (transaction_date);

-- Partições exemplo (crie conforme range do seu teste real)
CREATE TABLE mart.gmv_daily_subsidiary_hist_2023_01
  PARTITION OF mart.gmv_daily_subsidiary_hist
  FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE mart.gmv_daily_subsidiary_hist_2023_02
  PARTITION OF mart.gmv_daily_subsidiary_hist
  FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');

-- View "current": evita UPDATE; sempre aponta para o snapshot mais recente
CREATE OR REPLACE VIEW mart.gmv_daily_subsidiary_current AS
SELECT h.*
FROM mart.gmv_daily_subsidiary_hist h
WHERE h.as_of_date = (SELECT MAX(as_of_date) FROM mart.gmv_daily_subsidiary_hist);

-- ============================================================
-- 4) MOCK DATA — Inserimos CDC para provar late arriving e correções
-- ============================================================

/*
Cenário:
- 3 purchases (1,2,3) no partition 202301
- Subsidiaries S1 e S2
- Dois snapshots:
  - as_of 2023-01-07 (cutoff 2023-01-08 00:00) => ainda não conhece correções tardias
  - as_of 2023-01-10 (cutoff 2023-01-11 00:00) => já conhece correções tardias

Comportamentos:
- Purchase 2: inicialmente APROVADA e depois vira CANCELADA (correção tardia)
- Purchase 3: inicialmente INICIADA sem release_date e depois recebe release_date (evento tardio)
- Extra_info de purchase 2: subsidiária corrigida (late), mas purchase 2 é cancelada (não entra no GMV)
- Custos: purchase 3 tem correção tardia de VAT (bônus)
*/

-- 4.1 purchase CDC
INSERT INTO raw.purchase_cdc
(purchase_id, purchase_partition, buyer_id, prod_item_id, prod_item_partition, producer_id,
 order_date, release_date, purchase_total_value, purchase_status, op, event_ts, ingestion_ts)
VALUES
-- Purchase 1: já pago e aprovado (entra no GMV desde o primeiro snapshot)
(1, 202301, 10, 1001, 202301, 500,
 '2023-01-06', '2023-01-06', 100.00, 'APROVADA',
 'c', '2023-01-06 10:00:00', '2023-01-07 02:00:00'),

-- Purchase 2: aparece como aprovada no snapshot 2023-01-07,
-- mas chega correção tardia cancelando no snapshot 2023-01-10
(2, 202301, 11, 1002, 202301, 501,
 '2023-01-05', '2023-01-05', 200.00, 'APROVADA',
 'c', '2023-01-05 09:00:00', '2023-01-07 02:10:00'),
(2, 202301, 11, 1002, 202301, 501,
 '2023-01-05', '2023-01-05', 200.00, 'CANCELADA',
 'u', '2023-01-05 12:00:00', '2023-01-10 01:00:00'),

-- Purchase 3: inicialmente sem release_date (não entra no GMV no snapshot 2023-01-07)
-- depois recebe release_date (entra no GMV no snapshot 2023-01-10)
(3, 202301, 12, 1003, 202301, 502,
 '2023-01-06', NULL, 150.00, 'INICIADA',
 'c', '2023-01-06 08:00:00', '2023-01-07 02:20:00'),
(3, 202301, 12, 1003, 202301, 502,
 '2023-01-06', '2023-01-06', 150.00, 'APROVADA',
 'u', '2023-01-06 08:05:00', '2023-01-10 01:10:00');


-- 4.2 extra_info CDC
INSERT INTO raw.purchase_extra_info_cdc
(purchase_id, purchase_partition, subsidiary, op, event_ts, ingestion_ts)
VALUES
(1, 202301, 'S1', 'c', '2023-01-06 10:00:00', '2023-01-07 02:01:00'),
(2, 202301, 'S1', 'c', '2023-01-05 09:00:00', '2023-01-07 02:11:00'),
-- correção tardia de subsidiária para purchase 2
(2, 202301, 'S2', 'u', '2023-01-05 09:30:00', '2023-01-10 01:05:00'),
(3, 202301, 'S2', 'c', '2023-01-06 08:00:00', '2023-01-07 02:21:00');


-- 4.3 product_item CDC
INSERT INTO raw.product_item_cdc
(prod_item_id, prod_item_partition, purchase_id, purchase_partition, product_id, item_quantity, purchase_value,
 op, event_ts, ingestion_ts)
VALUES
(1001, 202301, 1, 202301, 9001, 1, 100.00, 'c', '2023-01-06 10:00:00', '2023-01-07 02:00:30'),
(1002, 202301, 2, 202301, 9002, 1, 200.00, 'c', '2023-01-05 09:00:00', '2023-01-07 02:10:30'),
(1003, 202301, 3, 202301, 9003, 1, 150.00, 'c', '2023-01-06 08:00:00', '2023-01-07 02:20:30');


-- 4.4 costs CDC (bônus)
INSERT INTO raw.order_transaction_cost_hist_cdc
(purchase_id, purchase_partition, order_transaction_cost_vat_value, order_transaction_cost_installment_value,
 order_transaction_cost_date, op, event_ts, ingestion_ts)
VALUES
-- custos iniciais vistos já no snapshot de 2023-01-07
(1, 202301, 2.00, 1.00, '2023-01-06', 'c', '2023-01-06 10:00:00', '2023-01-07 03:00:00'),
(2, 202301, 4.00, 2.00, '2023-01-05', 'c', '2023-01-05 09:00:00', '2023-01-07 03:00:00'),
(3, 202301, 3.00, 1.50, '2023-01-06', 'c', '2023-01-06 08:00:00', '2023-01-07 03:00:00'),
-- correção tardia de VAT para purchase 3 (aparece apenas no snapshot de 2023-01-10)
(3, 202301, 2.50, 1.50, '2023-01-06', 'u', '2023-01-06 08:00:00', '2023-01-10 03:00:00');

-- ============================================================
-- 5) EXECUÇÃO DOS SNAPSHOTS (ETL) — append-only inserts
-- ============================================================

/* ============================================================
   Função lógica (conceito):
   - Definimos um cutoff_ts para determinar "o que era conhecido" naquele dia
   - Pegamos o último evento por chave (latest state as-of cutoff)
   - Aplicamos regra de elegibilidade para GMV
   - Agregamos por transaction_date e subsidiary
   - Inserimos na tabela final com as_of_date (snapshot)
   ============================================================ */

-- ------------------------------------------------------------
-- RUN 1: as_of_date = 2023-01-07
-- cutoff_ts = 2023-01-08 00:00:00
-- ------------------------------------------------------------
WITH
params AS (
  SELECT
    DATE '2023-01-07' AS as_of_date,
    TIMESTAMP '2023-01-08 00:00:00' AS cutoff_ts,
    '2023-01-07_d-1'::text AS run_id
),
/* purchase_latest:
   Para cada compra (purchase_id + purchase_partition),
   escolhemos o último evento conhecido até o cutoff.
   Ordenação: event_ts DESC, depois ingestion_ts DESC.
*/
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
/* extra_latest:
   Mesma lógica do purchase_latest, mas para a tabela de subsidiária.
*/
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
/* items_latest:
   Latest state por item (prod_item_id + prod_item_partition).
   Depois agregaremos para compra.
*/
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
/* items_by_purchase:
   Agrega itens por compra (purchase_id + purchase_partition).
   GMV do purchase = SUM(valor_unitario * quantidade)
   max_item_ingestion ajuda na auditoria do snapshot.
*/
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
/* eligible_purchases:
   Junta purchase + extra_info + itens e aplica regras de GMV:
   - release_date NOT NULL => pagamento confirmado
   - status NOT IN ('CANCELADA','REEMBOLSADA')
   - op <> 'd' (evento de delete não deve contabilizar)
*/
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
/* Inserção na GOLD:
   - Append-only: gera linhas para este snapshot (as_of_date)
   - Agregação final no grão do case (dia + subsidiária)
*/
INSERT INTO mart.gmv_daily_subsidiary_hist
(transaction_date, subsidiary, as_of_date, gmv_amount, purchases_count, source_max_ingestion_ts, run_id)
SELECT
  ep.transaction_date,
  ep.subsidiary,
  (SELECT as_of_date FROM params),
  SUM(ep.gmv_amount) AS gmv_amount,
  COUNT(*) AS purchases_count,
  MAX(ep.max_ingestion_ts) AS source_max_ingestion_ts,
  (SELECT run_id FROM params)
FROM eligible_purchases ep
GROUP BY 1,2;


-- ------------------------------------------------------------
-- RUN 2: as_of_date = 2023-01-10
-- cutoff_ts = 2023-01-11 00:00:00
-- ------------------------------------------------------------
WITH
params AS (
  SELECT
    DATE '2023-01-10' AS as_of_date,
    TIMESTAMP '2023-01-11 00:00:00' AS cutoff_ts,
    '2023-01-10_d-1'::text AS run_id
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
INSERT INTO mart.gmv_daily_subsidiary_hist
(transaction_date, subsidiary, as_of_date, gmv_amount, purchases_count, source_max_ingestion_ts, run_id)
SELECT
  ep.transaction_date,
  ep.subsidiary,
  (SELECT as_of_date FROM params),
  SUM(ep.gmv_amount) AS gmv_amount,
  COUNT(*) AS purchases_count,
  MAX(ep.max_ingestion_ts) AS source_max_ingestion_ts,
  (SELECT run_id FROM params)
FROM eligible_purchases ep
GROUP BY 1,2;

-- ============================================================
-- 6) CONSULTAS DE VALIDAÇÃO (o que muda entre snapshots)
-- ============================================================

-- Snapshot 1: como o GMV era visto em 2023-01-07
SELECT *
FROM mart.gmv_daily_subsidiary_hist
WHERE as_of_date = DATE '2023-01-07'
ORDER BY transaction_date, subsidiary;

-- Snapshot 2: como o GMV era visto em 2023-01-10 (após correções tardias)
SELECT *
FROM mart.gmv_daily_subsidiary_hist
WHERE as_of_date = DATE '2023-01-10'
ORDER BY transaction_date, subsidiary;

-- Visão "atual" (snapshot mais recente)
SELECT *
FROM mart.gmv_daily_subsidiary_current
ORDER BY transaction_date, subsidiary;

-- ============================================================
-- 7) CAMADA ANALÍTICA (REQUIRED) — GMV diário por subsidiária
-- ============================================================

-- "Hoje" (sem join, simples para qualquer analista)
SELECT
  transaction_date,
  subsidiary,
  gmv_amount
FROM mart.gmv_daily_subsidiary_current
ORDER BY 1,2;

-- "As-of" (consulta histórica; ex.: como estava no snapshot do dia 07)
SELECT
  transaction_date,
  subsidiary,
  gmv_amount
FROM mart.gmv_daily_subsidiary_hist
WHERE as_of_date = DATE '2023-01-07'
ORDER BY 1,2;

-- ============================================================
-- 8) BÔNUS — Reconciliação GMV vs custos (margem/contribuição)
-- ============================================================

/*
Nesta query, demonstramos como o modelo permite reconciliar GMV com custos,
utilizando a tabela de custos CDC-friendly.

Regra:
- custo_total = VAT + installment (somatório dos componentes disponíveis)
- contribution_margin = GMV - custo_total

Importante:
- Pegamos o "latest state as-of" também para custos (cost_latest).
- Demonstramos no snapshot final (cutoff do run 2).
*/

WITH
params AS (
  SELECT TIMESTAMP '2023-01-11 00:00:00' AS cutoff_ts
),
purchase_latest AS (
  SELECT *
  FROM (
    SELECT
      p.*,
      ROW_NUMBER() OVER (
        PARTITION BY p.purchase_id, p.purchase_partition
        ORDER BY p.event_ts DESC, p.ingestion_ts DESC
      ) rn
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
      ) rn
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
      ) rn
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
    SUM(COALESCE(purchase_value,0) * COALESCE(item_quantity,1)) AS gmv_amount
  FROM items_latest
  WHERE op <> 'd'
  GROUP BY 1,2
),
cost_latest AS (
  SELECT *
  FROM (
    SELECT
      c.*,
      ROW_NUMBER() OVER (
        PARTITION BY c.purchase_id, c.purchase_partition
        ORDER BY c.event_ts DESC, c.ingestion_ts DESC
      ) rn
    FROM raw.order_transaction_cost_hist_cdc c
    CROSS JOIN params
    WHERE c.ingestion_ts < params.cutoff_ts
  ) x
  WHERE rn = 1
),
eligible_purchases AS (
  SELECT
    p.purchase_id,
    p.purchase_partition,
    p.order_date::date AS transaction_date,
    e.subsidiary,
    i.gmv_amount
  FROM purchase_latest p
  JOIN extra_latest e
    ON e.purchase_id = p.purchase_id AND e.purchase_partition = p.purchase_partition
  JOIN items_by_purchase i
    ON i.purchase_id = p.purchase_id AND i.purchase_partition = p.purchase_partition
  WHERE p.release_date IS NOT NULL
    AND (p.purchase_status IS NULL OR p.purchase_status NOT IN ('CANCELADA','REEMBOLSADA'))
    AND p.op <> 'd'
)
SELECT
  ep.transaction_date,
  ep.subsidiary,
  SUM(ep.gmv_amount) AS gmv,
  SUM(
    COALESCE(cl.order_transaction_cost_vat_value,0) +
    COALESCE(cl.order_transaction_cost_installment_value,0)
  ) AS cost,
  SUM(ep.gmv_amount) -
  SUM(
    COALESCE(cl.order_transaction_cost_vat_value,0) +
    COALESCE(cl.order_transaction_cost_installment_value,0)
  ) AS contribution_margin
FROM eligible_purchases ep
LEFT JOIN cost_latest cl
  ON cl.purchase_id = ep.purchase_id
 AND cl.purchase_partition = ep.purchase_partition
GROUP BY 1,2
ORDER BY 1,2;
