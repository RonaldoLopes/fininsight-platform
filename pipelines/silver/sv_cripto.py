from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="sv_cripto",
    comment="Ultima cotacao de cada criptoativo por dia (USD)",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("preco_positivo", "preco_usd > 0")
def sv_cripto():
    df = (
        spark.read.table(f"{CAT}.bronze.bz_coingecko_cripto")
        .withColumn("coletado_ts", F.to_timestamp("coletado_em"))
        .withColumn("data_referencia", F.to_date("coletado_ts"))
        .select(
            F.col("id").alias("ativo_id"),
            F.upper("simbolo").alias("simbolo"),
            "data_referencia",
            "coletado_ts",
            F.col("preco_usd").cast("double").alias("preco_usd"),
            F.col("market_cap").cast("double").alias("market_cap"),
            F.col("volume_24h").cast("double").alias("volume_24h"),
        )
    )
    janela = Window.partitionBy("ativo_id", "data_referencia").orderBy(F.col("coletado_ts").desc())
    return df.withColumn("rn", F.row_number().over(janela)).filter("rn = 1").drop("rn", "coletado_ts")
