from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="dim_moeda",
    comment="Moedas e taxa de conversao para BRL (USD = ultimo dolar disponivel)",
    table_properties={"quality": "gold"},
)
def dim_moeda():
    ultimo_dolar = (
        spark.read.table(f"{CAT}.silver.sv_cambio")
        .orderBy(F.col("data_cotacao").desc())
        .limit(1)
        .select(F.col("ptax_venda").alias("taxa_para_brl"))
        .withColumn("moeda", F.lit("USD"))
        .withColumn("nome", F.lit("Dolar americano"))
    )
    brl = spark.createDataFrame(
        [("BRL", "Real", 1.0)], "moeda string, nome string, taxa_para_brl double"
    )
    return brl.unionByName(ultimo_dolar.select("moeda", "nome", "taxa_para_brl"))
