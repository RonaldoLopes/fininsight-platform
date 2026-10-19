# Databricks notebook source
# MAGIC %md
# MAGIC # Promover modelo: aponta o alias `champion` para a versao mais recente
# MAGIC No Unity Catalog nao existem mais "stages" (Staging/Production): usa-se **alias**.

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
MODELO = "fininsight_dev.ml.classificador_risco"

client = MlflowClient()
versoes = client.search_model_versions(f"name = '{MODELO}'")
ultima = max(int(v.version) for v in versoes)

client.set_registered_model_alias(MODELO, "champion", ultima)
print(f"{MODELO}@champion -> versao {ultima}")
