# Teachable — Case Técnico (Data Engineer)
Este repositório contém uma solução **reprodutível** para o case técnico de Engenharia de Dados, com foco em:

- **Modelagem histórica e imutável (append-only)**
- **CDC / eventos fora de ordem / eventos tardios / reprocessamento**
- Construção de uma tabela analítica final de **GMV diário por subsidiária**
- Suporte a consultas **"as-of"** (ex.: “GMV de Jan/2023 como era visto em 31/Mar vs hoje”)
- Camada analítica simples (consumo sem joins)
- Bônus: **reconciliação com custos** (tópico avançado)

---

## 1. Contexto e objetivo
O **GMV (Gross Merchandise Value)** representa o valor monetário total de transações concluídas, considerando apenas compras:
- com pagamento confirmado (**`release_date` preenchida**)
- e **não canceladas/reembolsadas**

Os dados chegam no data lake/warehouse como **eventos CDC** (Change Data Capture), podendo:
- chegar fora de ordem
- chegar com atraso (late arriving)
- ser reprocessados/corrigidos

**Objetivo do case:** desenhar um pipeline e um modelo final **imutável e historicamente consistente**, capaz de gerar **GMV diário por subsidiária** e suportar **as-of queries**.

---

## 2. Estratégia adotada (visão geral)
### 2.1 Camadas
**RAW (Bronze / eventos CDC — append-only)**
- `raw.purchase_cdc`
- `raw.product_item_cdc`
- `raw.purchase_extra_info_cdc`
- `raw.order_transaction_cost_hist_cdc` (bônus)

**SILVER (estado “as-of” / latest-state por cutoff)**
- Em cada execução, calculamos o “estado mais recente conhecido” de cada entidade até um `cutoff_ts`.

**GOLD (tabela final analítica — append-only)**
- `mart.gmv_daily_subsidiary_hist` (histórico por snapshot)
- `mart.gmv_daily_subsidiary_current` (view para a versão mais recente)

### 2.2 Por que snapshots append-only?
A regra central do case é **não reescrever o passado**.
Portanto, a cada execução (batch D-1), geramos um novo snapshot identificado por `as_of_date`:

- o snapshot antigo continua existindo (imutabilidade)
- o snapshot novo incorpora correções e eventos tardios
- consultas “as-of” viram apenas filtros em `as_of_date`

---

## 3. Grão, partição e imutabilidade
### 3.1 Grão (grain) — tabela final
**Grão escolhido:**  
> **1 linha por (`transaction_date`, `subsidiary`, `as_of_date`)**

Ou seja:
- `transaction_date`: dia contábil/operacional do GMV
- `subsidiary`: dimensão de negócio
- `as_of_date`: data do snapshot (data do “run”)

Isso permite:
- consultar o histórico: `WHERE as_of_date = 'YYYY-MM-DD'`
- consultar “hoje”: pegar o maior `as_of_date`

### 3.2 Partição
A tabela final é **particionada por `transaction_date`**.
No Postgres, usamos partitioning por RANGE (ex.: por mês).

### 3.3 Estratégia de imutabilidade (append-only)
- Não usamos `UPDATE` para “corrigir” snapshots antigos
- Correções entram como **novas linhas** com um novo `as_of_date`
- O “estado atual” é uma **VIEW** que seleciona o snapshot mais recente

---

## 4. Entregáveis do case (mapeamento)
### 4.1 DDL da tabela analítica final
- `mart.gmv_daily_subsidiary_hist`
- particionada por `transaction_date`
- chave primária composta por (`transaction_date`, `subsidiary`, `as_of_date`)
- contém campos de auditoria:
  - `source_max_ingestion_ts`
  - `run_id`

### 4.2 Lógica de ETL/ELT
- ETL em SQL com CTEs:
  - `purchase_latest` (latest state as-of)
  - `extra_latest`
  - `items_latest` + agregação por compra
  - filtro de elegibilidade (released e não cancelada)
  - agregação por dia/subsidiary
- A execução é demonstrada com **2 runs** (2 snapshots) para evidenciar:
  - correção tardia
  - evento tardio (late arriving)

### 4.3 Exemplo de saída
- O script injeta dados mockados e executa dois snapshots:
  - `as_of_date = 2023-01-07`
  - `as_of_date = 2023-01-10`
- Assim, é possível comparar como o GMV “mudou” sem reescrever o passado.

### 4.4 Camada analítica (query final)
- Consulta simples (sem joins) via:
  - `mart.gmv_daily_subsidiary_current` (hoje)
  - ou `mart.gmv_daily_subsidiary_hist` (as-of)

### 4.5 Arquitetura
- RAW (CDC) → SILVER (latest as-of) → GOLD (snapshots append-only)

### 4.6 Bônus (custos)
- `raw.order_transaction_cost_hist_cdc`
- Query de reconciliação:
  - `GMV - (VAT + installment)` = `contribution_margin`
- Demonstra evolução para finanças/margem sem ferir imutabilidade.

---

## 5. Como executar
### Pré-requisitos
- PostgreSQL
- Banco: `teachable_case`
- Permissão para criar schemas/tabelas/views

### Execução
1. Abra o arquivo:
   - `sql/run_all.sql`
2. Execute inteiro no Postgres (DBeaver/psql).
3. Valide os resultados com as queries ao final do arquivo.

---

## 6. Como validar rapidamente
Após rodar o script:
- Deve existir histórico em:
  - `mart.gmv_daily_subsidiary_hist`
- Deve existir a view:
  - `mart.gmv_daily_subsidiary_current`

### Checagens:
**Snapshot 2023-01-07**:
- Inclui compras liberadas até o cutoff e ainda não conhecia correções tardias.

**Snapshot 2023-01-10**:
- Já incorpora:
  - cancelamento tardio de uma compra (ela sai do GMV)
  - liberação tardia de outra compra (ela entra no GMV)

Isso prova:
- **as-of** funcionando
- **imutabilidade** preservada
- **late arriving** incorporado sem update do passado

---

## 7. Limitações e decisões (importante)
- Dados mockados são propositalmente simples para demonstrar o comportamento temporal.
- Integridade referencial (FK) não é aplicada no RAW, pois CDC pode chegar fora de ordem.
- Em produção, o pipeline seria incremental:
  - detectando quais `transaction_date` foram impactadas por eventos ingeridos desde o último run
  - recalculando apenas partições afetadas (mas sempre gerando novo snapshot)

---

## 8. Próximos passos (evolução para tempo real)
- Evoluir para streaming (Kappa/Lakehouse):
  - ingestão contínua no bronze
  - materialização “as-of” com watermark
  - snapshots diários mantidos como Gold para auditoria/reconciliação
- Expor métrica via camada semântica:
  - métrica `gmv` e `contribution_margin`
  - governança de definições

---

## 9. Contato
Fico à disposição para explicar decisões de modelagem, trade-offs e como eu evoluiria isso para produção.
