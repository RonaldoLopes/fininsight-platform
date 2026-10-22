# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Feature Engineering (Feature Table no Unity Catalog)
# MAGIC As features derivam da Silver (fonte unica de verdade): nenhuma formula e recalculada aqui.
# MAGIC Pre-requisito: pipeline `fininsight_silver` executado com historico (backfill `period="2y"`).

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS fininsight_dev.ml")

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
TABELA = "fininsight_dev.ml.features_ativos"

features = (
    spark.table("fininsight_dev.silver.sv_retornos_volatilidade")
    .select("ticker", "data_pregao", "retorno_log_1d", "sma_21d", "sma_63d",
            "vol_21d_anualizada", "rsi_14d", "zscore_preco_63d")
    .dropna(subset=["ticker", "data_pregao"])      # chaves primarias nao podem ser nulas
)

if not spark.catalog.tableExists(TABELA):
    fe.create_table(
        name=TABELA,
        primary_keys=["ticker", "data_pregao"],
        timestamp_keys=["data_pregao"],
        df=features,
        description="Features financeiras diarias por ativo (retorno, medias, vol, RSI, z-score)",
    )
else:
    fe.write_table(name=TABELA, df=features, mode="merge")

display(spark.table(TABELA).limit(10))
