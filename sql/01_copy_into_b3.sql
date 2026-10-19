-- Databricks SQL Editor (ou celula %sql de notebook). Cole tudo e execute.
-- 1) A tabela de destino precisa existir; sem colunas, o COPY INTO cria o schema na primeira carga.
CREATE TABLE IF NOT EXISTS fininsight_dev.bronze.bz_b3_cotacoes_copyinto;

-- 2) Carga incremental e idempotente: arquivos ja carregados sao ignorados.
COPY INTO fininsight_dev.bronze.bz_b3_cotacoes_copyinto
FROM '/Volumes/fininsight_dev/landing/raw/b3/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- 3) Conferir o resultado (a tabela de demonstracao nao alimenta a Silver)
SELECT count(*) AS linhas FROM fininsight_dev.bronze.bz_b3_cotacoes_copyinto;
DESCRIBE HISTORY fininsight_dev.bronze.bz_b3_cotacoes_copyinto;

-- 4) Rode o COPY INTO de novo: num_affected_rows = 0, porque nenhum arquivo novo apareceu.
-- Para recarregar tudo de proposito, use COPY_OPTIONS ('force' = 'true').
