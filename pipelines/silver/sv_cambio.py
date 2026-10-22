from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="sv_cambio",
    comment="Dolar diario: serie SGS 1 do BCB e fechamento USD/BRL do Alpha Vantage",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("ptax_positivo", "ptax_venda > 0")
def sv_cambio():
    ptax = (
        spark.read.table(f"{CAT}.bronze.bz_bcb_series")
        .filter(F.col("serie") == "ptax_dolar")
        .select(
            F.to_date("data", "dd/MM/yyyy").alias("data_cotacao"),
            F.expr("try_cast(valor AS DOUBLE)").alias("ptax_venda"),
        )
        .dropDuplicates(["data_cotacao"])
    )
    av = (
        spark.read.table(f"{CAT}.bronze.bz_alphavantage_fx")
        .select(
            F.to_date("data").alias("data_cotacao"),
            F.col("close").cast("double").alias("usdbrl_close"),
        )
        .dropDuplicates(["data_cotacao"])
    )
    return ptax.join(av, "data_cotacao", "full_outer").filter("ptax_venda IS NOT NULL")
