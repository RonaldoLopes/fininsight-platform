# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Insights diarios em linguagem natural (grava `gold.fato_insights_genai`)

# COMMAND ----------

from pyspark.sql import functions as F

MODELO = "databricks-meta-llama-3-3-70b-instruct"   # endpoint de Foundation Model API

kpis = spark.sql("""
    SELECT a.ticker, round(r.sharpe_ratio, 2) AS sharpe, round(r.var_95_1d_pct * 100, 2) AS var95_pct,
           round(r.drawdown_maximo_252d * 100, 1) AS drawdown_pct
    FROM fininsight_dev.gold.fato_risco_var_sharpe r
    JOIN fininsight_dev.gold.dim_ativo a ON a.ativo_sk = r.ativo_sk
    WHERE r.data_sk = (SELECT max(data_sk) FROM fininsight_dev.gold.fato_risco_var_sharpe)
""")
data_ref = spark.sql(
    "SELECT max(data_sk) AS d FROM fininsight_dev.gold.fato_risco_var_sharpe"
).first()["d"]

texto_kpis = kpis.toPandas().to_json(orient="records")
prompt = (
    "Voce e um analista de mercado. Em ate 5 linhas, em portugues, resuma riscos e oportunidades "
    "do dia com base nestes KPIs (JSON). Cite os tickers com maior drawdown e menor Sharpe. "
    f"KPIs: {texto_kpis}"
)

# COMMAND ----------

# o prompt vai como DADO (coluna), nunca concatenado dentro do texto SQL
(
    spark.createDataFrame([(data_ref, prompt)], "data_sk DATE, prompt STRING")
    .withColumn("insight", F.expr(f"ai_query('{MODELO}', prompt)"))
    .withColumn("modelo", F.lit(MODELO))
    .withColumn("gerado_em", F.current_timestamp())
    .drop("prompt")
    .write.mode("append").option("mergeSchema", "true")
    .saveAsTable("fininsight_dev.gold.fato_insights_genai")
)
ultimos = spark.table("fininsight_dev.gold.fato_insights_genai").orderBy(F.col("gerado_em").desc())
display(ultimos.limit(3))
