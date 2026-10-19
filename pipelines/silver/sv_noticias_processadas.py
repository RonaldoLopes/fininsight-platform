from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.materialized_view(
    name="sv_noticias_processadas",
    comment="Noticias limpas (sem HTML/espacos extras), deduplicadas por noticia_id",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("texto_presente", "texto_noticia IS NOT NULL AND length(texto_noticia) > 20")
@dp.expect("ticker_identificado", "ticker_relacionado IS NOT NULL")   # apenas mede, nao descarta
def sv_noticias_processadas():
    limpo = F.trim(F.regexp_replace(F.regexp_replace("texto_noticia", "<[^>]*>", " "), "\\s+", " "))
    return (
        spark.read.table(f"{CAT}.bronze.bz_noticias_raw")
        .withColumn("texto_noticia", limpo)
        .withColumn("data_publicacao_ts", F.to_timestamp("data_publicacao"))
        .withColumn("data_publicacao", F.to_date("data_publicacao_ts"))
        .withColumn("qtd_palavras", F.size(F.split("texto_noticia", " ")))
        .select("noticia_id", "titulo", "texto_noticia", "fonte", "data_publicacao",
                "data_publicacao_ts", "url", "ticker_relacionado", "qtd_palavras")
        .dropDuplicates(["noticia_id"])
    )
