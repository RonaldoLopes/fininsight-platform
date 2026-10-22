# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configurar secrets (rodar UMA vez)
# MAGIC Cole as chaves abaixo, rode a célula e **apague as chaves do notebook depois**.
# MAGIC Este notebook cria o scope `fininsight` (se ainda não existir) e grava as chaves.
# MAGIC Os secrets do Event Hubs são adicionados depois pelo Terraform (módulo `eventhubs`).

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
SCOPE = "fininsight"

# cria o scope uma unica vez (idempotente)
if SCOPE not in [s.name for s in w.secrets.list_scopes()]:
    w.secrets.create_scope(scope=SCOPE)

CHAVES = {
    "alphavantage-api-key": "COLE_AQUI",   # https://www.alphavantage.co/support/#api-key
    "fred-api-key": "COLE_AQUI",           # https://fred.stlouisfed.org/docs/api/api_key.html
    "newsapi-key": "COLE_AQUI",            # https://newsapi.org/register
}

for chave, valor in CHAVES.items():
    if valor != "COLE_AQUI":
        w.secrets.put_secret(scope=SCOPE, key=chave, string_value=valor)

print(sorted(s.key for s in w.secrets.list_secrets(SCOPE)))
