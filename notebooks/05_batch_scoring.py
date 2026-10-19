# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Batch scoring: grava `gold.fato_score_ml`
# MAGIC Usa o modelo `@champion` (ver ml/promover_modelo.py) sobre o ultimo pregao de cada ativo.

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from pyspark.sql.window import Window

mlflow.set_registry_uri("databricks-uc")
MODELO_URI = "models:/fininsight_dev.ml.classificador_risco@champion"
FEATURES = ["vol_21d_anualizada", "rsi_14d", "zscore_preco_63d", "retorno_log_1d"]

predict = mlflow.pyfunc.spark_udf(spark, MODELO_URI, result_type="int")

# COMMAND ----------

ultimo = (
    spark.table("fininsight_dev.silver.sv_retornos_volatilidade")
    .dropna(subset=FEATURES)
    .withColumn(
        "rn",
        F.row_number().over(Window.partitionBy("ticker").orderBy(F.col("data_pregao").desc())),
    )
    .filter("rn = 1")
)
dim = spark.table("fininsight_dev.gold.dim_ativo").select("ticker", "ativo_sk")

score = (
    ultimo.join(dim, "ticker")
    .withColumn("score_risco", predict(*[F.col(c) for c in FEATURES]))
    .select("ativo_sk", F.col("data_pregao").alias("data_sk"), "score_risco",
            F.current_timestamp().alias("calculado_em"))
)
(score.write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("fininsight_dev.gold.fato_score_ml"))
display(spark.table("fininsight_dev.gold.fato_score_ml"))
