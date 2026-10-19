from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("fininsight.catalog", "fininsight_dev")


@dp.temporary_view(name="vw_carteiras_cdc")
def vw_carteiras_cdc():
    # fonte de eventos de CDC: tabela Delta criada por sql/04_seed_carteiras_cdc.sql
    return spark.readStream.table(f"{CAT}.bronze.bz_composicao_carteiras_cdc")


# Tabela-alvo: historico versionado (SCD Type 2) da composicao das carteiras
dp.create_streaming_table(
    name="sv_carteiras_scd2",
    comment="Composicao historica das carteiras (SCD2: colunas __START_AT / __END_AT)",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="sv_carteiras_scd2",
    source="vw_carteiras_cdc",
    keys=["carteira_id", "ticker"],
    sequence_by=F.col("data_evento"),
    apply_as_deletes=F.expr("operacao = 'DELETE'"),
    except_column_list=["operacao"],
    stored_as_scd_type=2,
)
