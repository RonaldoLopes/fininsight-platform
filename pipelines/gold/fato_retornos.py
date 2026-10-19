from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="fato_retornos",
    comment="Fato: retorno logaritmico diario e retorno acumulado por ativo/dia",
    table_properties={"quality": "gold"},
)
def fato_retornos():
    dim = spark.read.table("dim_ativo").select("ticker", "ativo_sk")
    acumulada = (
        Window.partitionBy("ticker").orderBy("data_pregao")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        spark.read.table(f"{CAT}.silver.sv_retornos_volatilidade")
        # soma de retornos log = retorno log acumulado; exp(x) - 1 converte para retorno simples
        .withColumn("retorno_acumulado", F.exp(F.sum("retorno_log_1d").over(acumulada)) - 1)
        .join(dim, "ticker")
        .select("ativo_sk", F.col("data_pregao").alias("data_sk"),
                "retorno_log_1d", "retorno_acumulado")
    )
