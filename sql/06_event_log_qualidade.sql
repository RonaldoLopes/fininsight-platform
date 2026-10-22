-- SQL Editor. Metricas de expectations (qualidade de dados) de uma tabela do pipeline Silver.
-- Troque a tabela pela que quiser inspecionar.
WITH eventos AS (
  SELECT timestamp, details
  FROM event_log(TABLE(fininsight_dev.silver.sv_precos_ativos))
  WHERE event_type = 'flow_progress'
)
SELECT
  timestamp,
  exp.name                AS expectation,
  exp.dataset             AS tabela,
  exp.passed_records      AS aprovados,
  exp.failed_records      AS reprovados
FROM eventos
LATERAL VIEW explode(
  from_json(details:flow_progress.data_quality.expectations,
            'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')
) t AS exp
ORDER BY timestamp DESC;
