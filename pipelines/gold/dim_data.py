from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="dim_data",
    comment="Calendario 2020-2035 (dia util = segunda a sexta; feriados nao considerados)",
    table_properties={"quality": "gold"},
)
def dim_data():
    dias = spark.sql(
        "SELECT explode(sequence(to_date('2020-01-01'), to_date('2035-12-31'), "
        "interval 1 day)) AS data_sk"
    )
    return (
        dias.withColumn("ano", F.year("data_sk"))
        .withColumn("trimestre", F.quarter("data_sk"))
        .withColumn("mes", F.month("data_sk"))
        .withColumn("nome_mes", F.date_format("data_sk", "MMMM"))
        .withColumn("dia_semana", F.dayofweek("data_sk"))
        .withColumn("dia_util", F.dayofweek("data_sk").isin(2, 3, 4, 5, 6))
    )
