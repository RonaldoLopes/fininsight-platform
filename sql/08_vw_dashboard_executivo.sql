-- SQL Editor (warehouse). View materializada consumida pelo dashboard / Power BI.
CREATE OR REPLACE MATERIALIZED VIEW fininsight_dev.gold.vw_dashboard_executivo AS
SELECT
  a.ticker, a.setor, r.data_sk,
  r.sharpe_ratio, r.var_95_1d_pct, r.drawdown_maximo_252d,
  s.score_risco, n.sentiment_medio
FROM fininsight_dev.gold.fato_risco_var_sharpe r
JOIN fininsight_dev.gold.dim_ativo a ON a.ativo_sk = r.ativo_sk
LEFT JOIN fininsight_dev.gold.fato_score_ml s
  ON s.ativo_sk = r.ativo_sk AND s.data_sk = r.data_sk
LEFT JOIN (
  SELECT ativo_sk, data_sk, avg(sentiment_score) AS sentiment_medio
  FROM fininsight_dev.gold.fato_sentiment_noticias
  GROUP BY ativo_sk, data_sk
) n ON n.ativo_sk = r.ativo_sk AND n.data_sk = r.data_sk;
