# Databricks notebook source
# MAGIC %md
# MAGIC # Classificador de risco (Gradient Boosting)
# MAGIC Alvo: o ativo cai mais de 5% nos proximos 21 pregoes (`alto_risco = 1`).
# MAGIC Precisa de historico: rode a coleta com `PERIODO_YF = "2y"` e atualize a Silver antes.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, roc_auc_score

mlflow.set_registry_uri("databricks-uc")           # modelos versionados no Unity Catalog
MODELO = "fininsight_dev.ml.classificador_risco"
HORIZONTE = 21
LIMITE_QUEDA = -0.05
FEATURES = ["vol_21d_anualizada", "rsi_14d", "zscore_preco_63d", "retorno_log_1d"]

# COMMAND ----------

janela = Window.partitionBy("ticker").orderBy("data_pregao")
base = (
    spark.table("fininsight_dev.silver.sv_retornos_volatilidade")
    .withColumn("preco_futuro", F.lead("preco_fechamento", HORIZONTE).over(janela))
    .withColumn("retorno_futuro", F.col("preco_futuro") / F.col("preco_fechamento") - 1)
    .withColumn("alto_risco", (F.col("retorno_futuro") < LIMITE_QUEDA).cast("int"))
    .dropna(subset=FEATURES + ["retorno_futuro"])
)
pdf = (
    base.select("ticker", "data_pregao", *FEATURES, "alto_risco")
    .toPandas()
    .sort_values("data_pregao")
)
print(f"{len(pdf)} linhas | proporcao de alto risco: {pdf['alto_risco'].mean():.2%}")

# separacao TEMPORAL (treina no passado, testa no futuro) -- evita vazamento de informacao
corte = int(len(pdf) * 0.8)
treino, teste = pdf.iloc[:corte], pdf.iloc[corte:]

# COMMAND ----------

with mlflow.start_run(run_name="classificador_risco_gb"):
    clf = GradientBoostingClassifier(n_estimators=200, max_depth=3, learning_rate=0.05, random_state=42)
    clf.fit(treino[FEATURES], treino["alto_risco"])

    pred = clf.predict(teste[FEATURES])
    mlflow.log_params({"n_estimators": 200, "max_depth": 3, "learning_rate": 0.05,
                       "horizonte_pregoes": HORIZONTE, "limite_queda": LIMITE_QUEDA})
    mlflow.log_metric("accuracy", accuracy_score(teste["alto_risco"], pred))
    if teste["alto_risco"].nunique() > 1:                      # AUC exige as duas classes no teste
        proba = clf.predict_proba(teste[FEATURES])[:, 1]
        mlflow.log_metric("auc", roc_auc_score(teste["alto_risco"], proba))

    mlflow.sklearn.log_model(
        clf, artifact_path="modelo",
        signature=infer_signature(teste[FEATURES], pred),
        registered_model_name=MODELO,
    )
