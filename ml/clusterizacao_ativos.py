# Databricks notebook source
# MAGIC %md
# MAGIC # Clusterizacao de ativos por perfil de risco/retorno (KMeans)

# COMMAND ----------

import mlflow
import mlflow.sklearn
from pyspark.sql import functions as F
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

perfil = (
    spark.table("fininsight_dev.silver.sv_retornos_volatilidade")
    .groupBy("ticker")
    .agg(F.avg("retorno_log_1d").alias("retorno_medio"), F.avg("vol_21d_anualizada").alias("vol_media"))
    .dropna()
    .toPandas()
    .set_index("ticker")
)

K = min(3, len(perfil))                       # nao pode haver mais clusters que ativos
X = StandardScaler().fit_transform(perfil[["retorno_medio", "vol_media"]])

with mlflow.start_run(run_name="kmeans_ativos"):
    km = KMeans(n_clusters=K, random_state=42, n_init=10).fit(X)
    mlflow.log_param("k", K)
    mlflow.log_metric("inertia", float(km.inertia_))
    mlflow.sklearn.log_model(km, artifact_path="kmeans")

perfil["cluster_risco"] = km.labels_
resultado = spark.createDataFrame(perfil.reset_index())
resultado.write.mode("overwrite").saveAsTable("fininsight_dev.ml.cluster_ativos")
display(resultado.orderBy("cluster_risco", "vol_media"))
