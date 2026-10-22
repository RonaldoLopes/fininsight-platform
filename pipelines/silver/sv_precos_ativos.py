from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")
WATCHLIST = ["PETR4.SA", "VALE3.SA", "ITUB4.SA"]   # tickers da B3 que acompanhamos


@dp.materialized_view(
    name="sv_precos_ativos",
    comment="Precos diarios unificados (B3 + yfinance), deduplicados por ticker/data",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("preco_valido", "preco_fechamento > 0")
@dp.expect_or_drop("ticker_valido", "ticker IS NOT NULL")
def sv_precos_ativos():
    # B3: o COTAHIST traz centenas de papeis; filtramos a watchlist e padronizamos o ticker
    b3 = (
        spark.read.table(f"{CAT}.bronze.bz_b3_cotacoes")
        .withColumn("ticker", F.concat(F.trim(F.col("codigo")), F.lit(".SA")))
        .filter(F.col("ticker").isin(WATCHLIST))
        .select(
            "ticker",
            F.to_date("data_pregao").alias("data_pregao"),
            F.col("preco_fechamento").cast("double").alias("preco_fechamento"),
            F.col("volume").cast("double").alias("volume"),
            F.lit("B3").alias("origem"),
            F.lit(1).alias("prioridade"),
            "_ingested_at",
        )
    )
    yf = (
        spark.read.table(f"{CAT}.bronze.bz_yfinance_precos")
        .select(
            "ticker",
            F.to_date("Date").alias("data_pregao"),
            F.col("Close").cast("double").alias("preco_fechamento"),
            F.col("Volume").cast("double").alias("volume"),
            F.lit("YFINANCE").alias("origem"),
            F.lit(2).alias("prioridade"),
            "_ingested_at",
        )
    )
    # mesma acao/data em duas fontes -> fica a de maior prioridade (B3), depois a mais recente
    janela = Window.partitionBy("ticker", "data_pregao").orderBy(
        F.col("prioridade"), F.col("_ingested_at").desc()
    )
    return (
        b3.unionByName(yf)
        .withColumn("rn", F.row_number().over(janela))
        .filter("rn = 1")
        .drop("rn", "prioridade")
    )
