from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="fato_volatilidade",
    comment="Fato: medias moveis, volatilidade anualizada, RSI e z-score por ativo/dia",
    table_properties={"quality": "gold"},
)
def fato_volatilidade():
    dim = spark.read.table("dim_ativo").select("ticker", "ativo_sk")
    return (
        spark.read.table(f"{CAT}.silver.sv_retornos_volatilidade")
        .join(dim, "ticker")
        .select("ativo_sk", F.col("data_pregao").alias("data_sk"),
                "sma_21d", "sma_63d", "vol_21d_anualizada", "rsi_14d", "zscore_preco_63d")
    )
