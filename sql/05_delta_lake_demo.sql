-- SQL Editor (warehouse) ou celulas %sql.
-- Tabela usada: bronze.bz_b3_cotacoes (criada pelo pipeline Bronze).

-- 1) Historico de operacoes (cada linha = uma versao da tabela)
DESCRIBE HISTORY fininsight_dev.bronze.bz_b3_cotacoes;

-- 2) Time travel por versao ou por data
SELECT count(*) FROM fininsight_dev.bronze.bz_b3_cotacoes VERSION AS OF 0;
SELECT count(*) FROM fininsight_dev.bronze.bz_b3_cotacoes TIMESTAMP AS OF '2026-10-01T00:00:00Z';

-- 3) Detalhes fisicos: formato, arquivos, tamanho, localizacao
DESCRIBE DETAIL fininsight_dev.bronze.bz_b3_cotacoes;

-- 4) Tabelas MANAGED pelo pipeline: a manutencao (OPTIMIZE/VACUUM) e automatica.
--    Em tabelas Delta suas (ex.: copia de demonstracao) voce roda manualmente:
CREATE OR REPLACE TABLE fininsight_dev.bronze.demo_delta AS
SELECT * FROM fininsight_dev.bronze.bz_b3_cotacoes;

OPTIMIZE fininsight_dev.bronze.demo_delta ZORDER BY (codigo, data_pregao);
VACUUM fininsight_dev.bronze.demo_delta RETAIN 168 HOURS;

-- 5) MERGE (upsert): corrige/insere linhas de forma atomica
MERGE INTO fininsight_dev.bronze.demo_delta AS alvo
USING (SELECT 'PETR4' AS codigo, DATE'2026-10-02' AS data_pregao, 99.99 AS preco_fechamento) AS fonte
  ON alvo.codigo = fonte.codigo AND alvo.data_pregao = fonte.data_pregao
WHEN MATCHED THEN UPDATE SET alvo.preco_fechamento = fonte.preco_fechamento
WHEN NOT MATCHED THEN INSERT (codigo, data_pregao, preco_fechamento)
  VALUES (fonte.codigo, fonte.data_pregao, fonte.preco_fechamento);

-- 6) Desfazer: voltar a tabela para a versao anterior ao MERGE
DESCRIBE HISTORY fininsight_dev.bronze.demo_delta;
RESTORE TABLE fininsight_dev.bronze.demo_delta TO VERSION AS OF 1;

-- 7) Limpeza da demonstracao
DROP TABLE IF EXISTS fininsight_dev.bronze.demo_delta;
