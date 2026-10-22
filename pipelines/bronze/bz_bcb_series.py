from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


@dp.table(
    name="bz_bcb_series",
    comment="Series temporais do Banco Central (raw)",
    table_properties={"quality": "bronze"},
)
def bz_bcb_series():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/fininsight_dev/landing/raw/bcb/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )
