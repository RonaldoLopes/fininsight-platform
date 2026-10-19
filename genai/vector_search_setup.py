# Databricks notebook source
# MAGIC %md
# MAGIC # RAG sobre noticias: indice de Vector Search
# MAGIC O indice Delta Sync exige tabela Delta com Change Data Feed. `silver.sv_noticias_processadas`
# MAGIC e gerenciada pelo pipeline, entao copiamos para uma tabela Delta propria.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

ORIGEM = "fininsight_dev.gold.noticias_vs"
ENDPOINT = "fininsight_vs_endpoint"
INDICE = "fininsight_dev.gold.idx_noticias"

(spark.table("fininsight_dev.silver.sv_noticias_processadas")
      .select("noticia_id", "titulo", "texto_noticia", "ticker_relacionado", "data_publicacao")
      .write.mode("overwrite").saveAsTable(ORIGEM))
spark.sql(f"ALTER TABLE {ORIGEM} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

vsc = VectorSearchClient()
if ENDPOINT not in [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]:
    vsc.create_endpoint_and_wait(name=ENDPOINT, endpoint_type="STANDARD")

vsc.create_delta_sync_index_and_wait(
    endpoint_name=ENDPOINT,
    index_name=INDICE,
    source_table_name=ORIGEM,
    pipeline_type="TRIGGERED",
    primary_key="noticia_id",
    embedding_source_column="texto_noticia",
    embedding_model_endpoint_name="databricks-gte-large-en",
)

# COMMAND ----------

indice = vsc.get_index(ENDPOINT, INDICE)
res = indice.similarity_search(query_text="queda da PETR4 hoje",
                               columns=["titulo", "texto_noticia", "data_publicacao"], num_results=5)
for linha in res["result"]["data_array"]:
    print(linha[0], "|", linha[2])
