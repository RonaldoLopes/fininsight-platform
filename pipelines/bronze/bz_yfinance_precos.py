from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


@dp.table(
    name="bz_yfinance_precos",
    comment="Precos diarios via yfinance (raw)",
    table_properties={"quality": "bronze"},
)
def bz_yfinance_precos():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/fininsight_dev/landing/raw/yfinance/")
        .withColumnRenamed("Adj Close", "Adj_Close")  # Delta nao aceita espaco no nome da coluna
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )
