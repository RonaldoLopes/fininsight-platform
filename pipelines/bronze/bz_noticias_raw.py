# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

@dp.table(
    name="bz_noticias_raw",
    comment="Noticias financeiras brutas (raw)",
    table_properties={"quality": "bronze"},
)
def bz_noticias_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/fininsight_dev/landing/raw/noticias/")
        .withColumn("_ingested_at", current_timestamp())
    )