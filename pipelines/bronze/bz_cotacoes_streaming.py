from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

SCOPE = "fininsight"
TOPICO = "cotacoes-tempo-real"


def _opcoes_kafka():
    bootstrap = dbutils.secrets.get(SCOPE, "eventhub-bootstrap")
    conexao = dbutils.secrets.get(SCOPE, "eventhub-connection-string")
    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="$ConnectionString" password="{conexao}";'
        ),
        "subscribe": TOPICO,
        "startingOffsets": "earliest",
    }


@dp.table(
    name="bz_cotacoes_streaming",
    comment="Cotacoes simuladas em tempo real via Event Hubs (raw)",
    table_properties={"quality": "bronze"},
)
def bz_cotacoes_streaming():
    return (
        spark.readStream.format("kafka")
        .options(**_opcoes_kafka())
        .load()
        .selectExpr("CAST(value AS STRING) AS json_payload", "timestamp AS kafka_ts")
        .withColumn("_ingested_at", current_timestamp())
    )
