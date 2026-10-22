-- SQL Editor. Cria a tabela de eventos de CDC usada pelo SCD Type 2 (sv_carteiras_scd2).
CREATE TABLE IF NOT EXISTS fininsight_dev.bronze.bz_composicao_carteiras_cdc (
  carteira_id STRING,
  ticker      STRING,
  peso        DOUBLE,
  data_evento TIMESTAMP,
  operacao    STRING      -- UPSERT ou DELETE
);

INSERT INTO fininsight_dev.bronze.bz_composicao_carteiras_cdc VALUES
  ('conservadora', 'PETR4.SA', 0.30, TIMESTAMP'2026-01-02 09:00:00', 'UPSERT'),
  ('conservadora', 'ITUB4.SA', 0.70, TIMESTAMP'2026-01-02 09:00:00', 'UPSERT'),
  ('conservadora', 'PETR4.SA', 0.20, TIMESTAMP'2026-03-01 09:00:00', 'UPSERT'),  -- rebalanceamento
  ('conservadora', 'VALE3.SA', 0.10, TIMESTAMP'2026-03-01 09:00:00', 'UPSERT'),
  ('conservadora', 'ITUB4.SA', 0.70, TIMESTAMP'2026-06-01 09:00:00', 'DELETE'),  -- saiu da carteira
  ('agressiva',    'AAPL',     0.50, TIMESTAMP'2026-01-02 09:00:00', 'UPSERT'),
  ('agressiva',    'MSFT',     0.50, TIMESTAMP'2026-01-02 09:00:00', 'UPSERT');
