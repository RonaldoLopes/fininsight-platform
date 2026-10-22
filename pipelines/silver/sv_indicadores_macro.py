from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="sv_indicadores_macro",
    comment="Selic/IPCA (BCB) e Fed Funds/CPI/10Y (FRED) em formato longo",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valor_numerico", "valor IS NOT NULL")
def sv_indicadores_macro():
    bcb = (
        spark.read.table(f"{CAT}.bronze.bz_bcb_series")
        .filter(F.col("serie").isin("selic", "ipca"))
        .select(
            F.col("serie").alias("indicador"),
            F.to_date("data", "dd/MM/yyyy").alias("data_referencia"),
            F.expr("try_cast(valor AS DOUBLE)").alias("valor"),
            F.lit("BCB").alias("fonte"),
        )
    )
    fred = (
        spark.read.table(f"{CAT}.bronze.bz_fred_indicadores")
        .select(
            F.lower(F.col("serie")).alias("indicador"),
            F.to_date("data").alias("data_referencia"),
            F.expr("try_cast(valor AS DOUBLE)").alias("valor"),   # FRED usa "." para dado ausente
            F.lit("FRED").alias("fonte"),
        )
    )
    return bcb.unionByName(fred).dropDuplicates(["indicador", "data_referencia"])
