from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="sv_precos_com_macro",
    comment="Precos + indicadores tecnicos + dolar (PTAX) + Selic diaria: join multi-fonte",
    table_properties={"quality": "silver"},
)
def sv_precos_com_macro():
    precos = spark.read.table("sv_retornos_volatilidade")

    cambio = spark.read.table("sv_cambio").select(
        F.col("data_cotacao").alias("data_pregao"), "ptax_venda"
    )

    # SGS 11 = Selic em % ao dia (ex.: 0.055 = 0,055% a.d.)
    selic = (
        spark.read.table("sv_indicadores_macro")
        .filter("indicador = 'selic'")
        .select(F.col("data_referencia").alias("data_pregao"), F.col("valor").alias("selic_diaria_pct"))
    )

    df = precos.join(cambio, "data_pregao", "left").join(selic, "data_pregao", "left")

    # O BCB so entrega os ultimos 20 pontos por chamada: onde nao ha Selic do dia usamos a
    # media da Selic disponivel como taxa livre de risco (aproximacao documentada)
    media_selic = F.avg("selic_diaria_pct").over(Window.partitionBy(F.lit(1)))
    return (
        df.withColumn("rf_diaria_pct", F.coalesce("selic_diaria_pct", media_selic))
        .withColumn("preco_fechamento_usd", F.col("preco_fechamento") / F.col("ptax_venda"))
    )
