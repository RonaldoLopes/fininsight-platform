# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Producer de cotações simuladas (Event Hubs via protocolo Kafka)
# MAGIC Envia lotes de cotações sintéticas para o tópico `cotacoes-tempo-real`.

# COMMAND ----------

import json
import random
import time
from datetime import datetime, timezone

SCOPE = "fininsight"
TOPICO = "cotacoes-tempo-real"
RODADAS = 60          # quantidade de lotes
INTERVALO_S = 5       # segundos entre lotes

bootstrap = dbutils.secrets.get(SCOPE, "eventhub-bootstrap")
conexao = dbutils.secrets.get(SCOPE, "eventhub-connection-string")

kafka_opts = {
    "kafka.bootstrap.servers": bootstrap,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{conexao}";'
    ),
}

precos = {"PETR4.SA": 38.0, "VALE3.SA": 62.0, "ITUB4.SA": 33.0, "AAPL": 230.0, "MSFT": 420.0}

# COMMAND ----------

for rodada in range(RODADAS):
    linhas = []
    for ticker, p in precos.items():
        precos[ticker] = round(p * (1 + random.gauss(0, 0.002)), 4)
        evento = {
            "ticker": ticker,
            "preco": precos[ticker],
            "volume": random.randint(100, 5000),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        linhas.append((ticker, json.dumps(evento)))

    (spark.createDataFrame(linhas, ["key", "value"])
          .write.format("kafka")
          .options(**kafka_opts)
          .option("topic", TOPICO)
          .save())
    print(f"lote {rodada + 1}/{RODADAS} enviado")
    time.sleep(INTERVALO_S)
