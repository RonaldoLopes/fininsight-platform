from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")
MODELO = "databricks-meta-llama-3-3-70b-instruct"   # endpoint de Foundation Model API do workspace

PERGUNTA = "De 0 a 100, qual o impacto desta noticia no preco do ativo? Responda so com o numero: "


@dp.materialized_view(
    name="fato_sentiment_noticias",
    comment="Fato: resumo, sentimento e impacto estimado por noticia/ativo (AI Functions)",
    table_properties={"quality": "gold"},
)
def fato_sentiment_noticias():
    dim = spark.read.table("dim_ativo").select("ticker", "ativo_sk")
    noticias = (
        spark.read.table(f"{CAT}.silver.sv_noticias_processadas")
        .filter("ticker_relacionado IS NOT NULL")
    )
    return (
        noticias.join(dim, noticias.ticker_relacionado == dim.ticker)
        .withColumn("resumo", F.expr("ai_summarize(texto_noticia, 40)"))
        .withColumn(
            "sentiment",
            F.expr("ai_classify(texto_noticia, array('positivo','neutro','negativo'))"),
        )
        .withColumn(
            "sentiment_score",
            F.when(F.col("sentiment") == "positivo", 1)
            .when(F.col("sentiment") == "negativo", -1)
            .otherwise(0),
        )
        .withColumn(
            "score_impacto",
            F.expr(f"try_cast(ai_query('{MODELO}', concat('{PERGUNTA}', texto_noticia)) AS INT)"),
        )
        .select("noticia_id", "ativo_sk", F.col("data_publicacao").alias("data_sk"), "titulo",
                "resumo", "sentiment", "sentiment_score", "score_impacto")
    )
