from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="fato_precos_diarios",
    comment="Fato: preco de fechamento e volume por ativo/dia",
    table_properties={"quality": "gold"},
)
@dp.expect_or_drop("preco_positivo", "preco_fechamento > 0")
def fato_precos_diarios():
    dim = spark.read.table("dim_ativo").select("ticker", "ativo_sk")
    return (
        spark.read.table(f"{CAT}.silver.sv_precos_ativos")
        .join(dim, "ticker")
        .select("ativo_sk", F.col("data_pregao").alias("data_sk"),
                "preco_fechamento", "volume", "origem")
    )
