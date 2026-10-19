-- Databricks SQL Editor, conectado a um SQL Warehouse (serverless recomendado).
-- Cria a Streaming Table E agenda uma atualizacao a cada hora, tudo em um comando.
CREATE OR REFRESH STREAMING TABLE fininsight_dev.bronze.bz_b3_cotacoes_sql
  SCHEDULE EVERY 1 HOUR
AS
SELECT
  *,
  current_timestamp()   AS _ingested_at,
  _metadata.file_path   AS _source_file
FROM STREAM read_files(
  '/Volumes/fininsight_dev/landing/raw/b3/',
  format => 'csv',
  header => true
);

-- Conferir
SELECT * FROM fininsight_dev.bronze.bz_b3_cotacoes_sql LIMIT 10;
