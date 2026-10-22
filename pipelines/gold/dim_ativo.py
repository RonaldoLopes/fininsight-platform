from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")

# Cadastro de referencia dos ativos (nome, setor, classe, pais, moeda)
CADASTRO = [
    ("PETR4.SA", "Petrobras PN", "Energia", "acao", "BR", "BRL"),
    ("VALE3.SA", "Vale ON", "Mineracao", "acao", "BR", "BRL"),
    ("ITUB4.SA", "Itau Unibanco PN", "Financeiro", "acao", "BR", "BRL"),
    ("^BVSP", "Ibovespa", "Indice", "indice", "BR", "BRL"),
    ("AAPL", "Apple", "Tecnologia", "acao", "US", "USD"),
    ("MSFT", "Microsoft", "Tecnologia", "acao", "US", "USD"),
]


@dp.materialized_view(
    name="dim_ativo",
    comment="Dimensao de ativos. ativo_sk = hash deterministico do ticker (igual em todo ambiente)",
    table_properties={"quality": "gold"},
)
def dim_ativo():
    cadastro = spark.createDataFrame(
        CADASTRO, "ticker string, nome string, setor string, classe string, pais string, moeda string"
    )
    vistos = spark.read.table(f"{CAT}.silver.sv_precos_ativos").select("ticker").distinct()
    return (
        vistos.join(cadastro, "ticker", "left")
        .withColumn("classe", F.coalesce("classe", F.lit("desconhecida")))
        .withColumn("ativo_sk", F.xxhash64("ticker"))
        .select("ativo_sk", "ticker", "nome", "setor", "classe", "pais", "moeda")
    )
