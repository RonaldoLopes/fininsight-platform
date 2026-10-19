from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")
MIN_OBS = 20      # minimo de retornos na janela para as metricas fazerem sentido
Z_95 = 1.645      # quantil da normal padrao para 95% de confianca


@dp.materialized_view(
    name="fato_risco_var_sharpe",
    comment="Fato: retorno/vol anuais, Sharpe, VaR 95% (1 dia) e drawdown maximo (252 pregoes)",
    table_properties={"quality": "gold"},
)
@dp.expect_or_fail("chave_fato_nao_nula", "ativo_sk IS NOT NULL AND data_sk IS NOT NULL")
@dp.expect("sharpe_calculado", "sharpe_ratio IS NOT NULL")      # so mede: nao derruba o pipeline
def fato_risco_var_sharpe():
    dim = spark.read.table("dim_ativo").select("ticker", "ativo_sk")
    j252 = Window.partitionBy("ticker").orderBy("data_pregao").rowsBetween(-251, 0)

    df = spark.read.table(f"{CAT}.silver.sv_precos_com_macro")
    df = df.withColumn("n_obs", F.count("retorno_log_1d").over(j252))
    df = df.withColumn("retorno_medio_anual", F.avg("retorno_log_1d").over(j252) * 252)
    df = df.withColumn("vol_anual", F.stddev("retorno_log_1d").over(j252) * F.lit(252 ** 0.5))

    # Sharpe = (retorno anual - taxa livre de risco anual) / volatilidade anual
    df = df.withColumn("rf_anual", F.col("rf_diaria_pct") / 100 * 252)
    df = df.withColumn(
        "sharpe_ratio", (F.col("retorno_medio_anual") - F.col("rf_anual")) / F.col("vol_anual")
    )

    # VaR parametrico 95% de 1 dia = z * volatilidade diaria (perda maxima esperada em 95% dos dias)
    df = df.withColumn("var_95_1d_pct", F.lit(Z_95) * F.col("vol_anual") / F.lit(252 ** 0.5))

    # drawdown: queda percentual desde o pico da janela; o maximo e o menor valor (mais negativo)
    df = df.withColumn("pico_movel", F.max("preco_fechamento").over(j252))
    df = df.withColumn(
        "drawdown_atual", (F.col("preco_fechamento") - F.col("pico_movel")) / F.col("pico_movel")
    )
    df = df.withColumn("drawdown_maximo_252d", F.min("drawdown_atual").over(j252))

    return (
        df.filter(F.col("n_obs") >= MIN_OBS)
        .join(dim, "ticker")
        .select("ativo_sk", F.col("data_pregao").alias("data_sk"), "retorno_medio_anual", "vol_anual",
                "sharpe_ratio", "var_95_1d_pct", "drawdown_maximo_252d")
    )
