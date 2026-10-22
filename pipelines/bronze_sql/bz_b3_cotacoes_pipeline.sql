-- Arquivo de pipeline (Declarative Pipeline em SQL): pipelines/bronze_sql/bz_b3_cotacoes_pipeline.sql
-- Sem SCHEDULE: quem agenda e o pipeline/Job que contem este arquivo.
CREATE OR REFRESH STREAMING TABLE bz_b3_cotacoes_pipe
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

-- Materialized View no mesmo arquivo: agregacao recalculada de forma incremental
CREATE OR REFRESH MATERIALIZED VIEW mv_b3_volume_por_dia
AS
SELECT data_pregao, count(*) AS papeis, sum(volume) AS volume_total
FROM bz_b3_cotacoes_pipe
GROUP BY data_pregao;
