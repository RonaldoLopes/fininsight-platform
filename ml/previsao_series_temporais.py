# Databricks notebook source
# MAGIC %md
# MAGIC # Previsao do dolar (Prophet)
# MAGIC A serie `sv_cambio` vem do BCB com no maximo 20 pontos por coleta; para uma previsao util,
# MAGIC acumule varios dias de coleta ou amplie o historico (veja o capitulo 20).

# COMMAND ----------

# MAGIC %pip install prophet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import mlflow.prophet
import pandas as pd
from prophet import Prophet
from pyspark.sql import functions as F

serie = (
    spark.table("fininsight_dev.silver.sv_cambio")
    .select(F.col("data_cotacao").alias("ds"), F.col("ptax_venda").alias("y"))
    .orderBy("ds")
    .toPandas()
)
serie["ds"] = pd.to_datetime(serie["ds"])

TESTE = 5                                    # ultimos 5 pontos ficam de fora para medir o erro
treino, teste = serie.iloc[:-TESTE], serie.iloc[-TESTE:]

# COMMAND ----------

with mlflow.start_run(run_name="previsao_dolar_prophet"):
    modelo = Prophet(daily_seasonality=False, weekly_seasonality=True)
    modelo.fit(treino)

    previsao = modelo.predict(teste[["ds"]])
    mae = (previsao["yhat"].values - teste["y"].values).__abs__().mean()
    mlflow.log_metric("mae", float(mae))
    mlflow.log_param("pontos_treino", len(treino))
    mlflow.prophet.log_model(modelo, artifact_path="modelo_dolar")

    futuro = modelo.make_future_dataframe(periods=10)
    display(modelo.predict(futuro)[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(10))
