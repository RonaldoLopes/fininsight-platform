from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="sv_retornos_volatilidade",
    comment="Retorno log, medias moveis, volatilidade anualizada, RSI e z-score por ticker/dia",
    table_properties={"quality": "silver"},
)
def sv_retornos_volatilidade():
    df = spark.read.table("sv_precos_ativos")

    tempo = Window.partitionBy("ticker").orderBy("data_pregao")
    j14 = tempo.rowsBetween(-13, 0)
    j21 = tempo.rowsBetween(-20, 0)
    j63 = tempo.rowsBetween(-62, 0)

    # retorno logaritmico diario: ln(P_t / P_t-1) -- aditivo no tempo, padrao em financas
    df = df.withColumn("preco_anterior", F.lag("preco_fechamento", 1).over(tempo))
    df = df.withColumn(
        "retorno_log_1d",
        F.when(F.col("preco_anterior") > 0, F.log(F.col("preco_fechamento") / F.col("preco_anterior"))),
    )

    # medias moveis simples
    df = df.withColumn("sma_21d", F.avg("preco_fechamento").over(j21))
    df = df.withColumn("sma_63d", F.avg("preco_fechamento").over(j63))

    # volatilidade anualizada: desvio-padrao dos retornos diarios * raiz(252 pregoes)
    df = df.withColumn("vol_21d_anualizada", F.stddev("retorno_log_1d").over(j21) * F.lit(252 ** 0.5))

    # RSI de 14 dias (media simples de ganhos e perdas)
    ret = F.col("retorno_log_1d")
    df = df.withColumn("ganho", F.when(ret > 0, ret).otherwise(0.0))
    df = df.withColumn("perda", F.when(ret < 0, -ret).otherwise(0.0))
    df = df.withColumn("media_ganho_14d", F.avg("ganho").over(j14))
    df = df.withColumn("media_perda_14d", F.avg("perda").over(j14))
    df = df.withColumn(
        "rsi_14d",
        F.when(F.col("media_perda_14d") == 0, F.lit(100.0)).otherwise(
            100 - (100 / (1 + F.col("media_ganho_14d") / F.col("media_perda_14d")))
        ),
    )

    # z-score do preco na janela de 63 dias: (preco - media) / desvio
    df = df.withColumn("std_63d", F.stddev("preco_fechamento").over(j63))
    df = df.withColumn(
        "zscore_preco_63d",
        F.when(F.col("std_63d") > 0, (F.col("preco_fechamento") - F.col("sma_63d")) / F.col("std_63d")),
    )

    return df.drop("ganho", "perda", "preco_anterior", "media_ganho_14d", "media_perda_14d", "std_63d")
