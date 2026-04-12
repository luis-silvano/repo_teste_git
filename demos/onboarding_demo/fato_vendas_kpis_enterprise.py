from pyspark.sql import functions as F
from pyspark.sql import Window


RAW_VENDAS_PATH = "/mnt/datalake/bronze/comercial/vendas_pedido_item"
RAW_CLIENTES_PATH = "/mnt/datalake/bronze/cadastro/clientes"
RAW_PRODUTOS_PATH = "/mnt/datalake/bronze/cadastro/produtos"
RAW_CAMPANHAS_PATH = "/mnt/datalake/bronze/marketing/campanhas_ativas"
CURATED_PATH = "/mnt/datalake/silver/comercial/vendas_curated"
GOLD_PATH = "/mnt/datalake/gold/comercial/fato_vendas_kpis"
AUDIT_PATH = "/mnt/datalake/gold/comercial/fato_vendas_kpis_auditoria"


def carregar_fontes():
    vendas = spark.read.format("delta").load(RAW_VENDAS_PATH)
    clientes = spark.read.format("delta").load(RAW_CLIENTES_PATH)
    produtos = spark.read.format("delta").load(RAW_PRODUTOS_PATH)
    campanhas = spark.read.format("delta").load(RAW_CAMPANHAS_PATH)
    return vendas, clientes, produtos, campanhas


def preparar_clientes(clientes_df):
    return (
        clientes_df
        .select(
            "cliente_id",
            "segmento_cliente",
            "canal_preferencial",
            "cidade",
            "estado",
            "data_cadastro",
            "cluster_valor",
            "tier_relacionamento",
        )
        .dropDuplicates(["cliente_id"])
    )


def preparar_produtos(produtos_df):
    return (
        produtos_df
        .select(
            "produto_id",
            "categoria",
            "subcategoria",
            "marca",
            "familia_receita",
            "flag_produto_estrategico",
        )
        .dropDuplicates(["produto_id"])
    )


def preparar_campanhas(campanhas_df):
    return (
        campanhas_df
        .filter(F.col("status_campanha") == F.lit("ATIVA"))
        .select(
            "campaign_id",
            "tipo_campanha",
            "origem_midia",
            "objetivo_comercial",
        )
        .dropDuplicates(["campaign_id"])
    )


def regra_filtro_vendas(vendas_df):
    return (
        vendas_df
        .filter(F.col("status_pedido").isin("FATURADO", "PARCIALMENTE_FATURADO"))
        .filter(F.col("tipo_movimento").isin("VENDA", "UPSELL", "RENOVACAO"))
        .filter(F.col("flag_cancelado") == F.lit(False))
        .filter(F.col("data_competencia").isNotNull())
        .filter(F.col("valor_total").isNotNull())
        .filter(F.col("quantidade").isNotNull())
        .filter(F.col("empresa_origem").isin("B2B", "ENTERPRISE"))
    )


def enriquecer_vendas(vendas_df, clientes_df, produtos_df, campanhas_df):
    clientes_preparados = preparar_clientes(clientes_df)
    produtos_preparados = preparar_produtos(produtos_df)
    campanhas_preparadas = preparar_campanhas(campanhas_df)

    return (
        regra_filtro_vendas(vendas_df)
        .join(clientes_preparados, on="cliente_id", how="left")
        .join(produtos_preparados, on="produto_id", how="left")
        .join(campanhas_preparadas, on="campaign_id", how="left")
        .withColumn("data_pedido", F.to_date("data_pedido"))
        .withColumn("mes_referencia", F.date_format("data_competencia", "yyyy-MM"))
        .withColumn("margem_bruta_item", F.col("valor_total") - F.col("custo_total"))
        .withColumn("desconto_percentual", F.round((F.col("desconto_valor") / F.col("valor_bruto")) * 100, 2))
        .withColumn("pedido_24h", F.when(F.col("tempo_entrega_horas") <= 24, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("pedido_digital", F.when(F.col("canal_venda").isin("E-COMMERCE", "APP", "PORTAL_B2B"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("cliente_ativo_flag", F.when(F.col("valor_total") > 0, F.lit(1)).otherwise(F.lit(0)))
    )


def gerar_curated(vendas_enriquecidas):
    colunas_curated = [
        "pedido_id",
        "item_id",
        "cliente_id",
        "produto_id",
        "campaign_id",
        "data_pedido",
        "data_competencia",
        "mes_referencia",
        "regional",
        "canal_venda",
        "segmento_cliente",
        "cluster_valor",
        "tier_relacionamento",
        "categoria",
        "subcategoria",
        "marca",
        "familia_receita",
        "flag_produto_estrategico",
        "status_pedido",
        "tipo_movimento",
        "quantidade",
        "valor_bruto",
        "desconto_valor",
        "valor_total",
        "custo_total",
        "margem_bruta_item",
        "desconto_percentual",
        "tempo_entrega_horas",
        "pedido_24h",
        "pedido_digital",
        "cliente_ativo_flag",
        "tipo_campanha",
        "origem_midia",
        "objetivo_comercial",
    ]
    return vendas_enriquecidas.select(*colunas_curated)


def gerar_kpis_gold(vendas_curated):
    agrupamento = [
        "mes_referencia",
        "regional",
        "canal_venda",
        "segmento_cliente",
        "cluster_valor",
        "categoria",
        "familia_receita",
    ]

    base_kpi = (
        vendas_curated
        .groupBy(*agrupamento)
        .agg(
            F.sum("valor_total").alias("TOTAL_VENDAS"),
            F.sum("quantidade").alias("QTD_ITENS"),
            F.countDistinct("pedido_id").alias("QTD_PEDIDOS"),
            F.countDistinct("cliente_id").alias("CLIENTES_ATIVOS"),
            F.sum("margem_bruta_item").alias("MARGEM_BRUTA"),
            F.avg("desconto_percentual").alias("DESCONTO_MEDIO_PERC"),
            F.sum("pedido_24h").alias("PEDIDOS_NO_SLA_24H"),
            F.sum("pedido_digital").alias("PEDIDOS_DIGITAIS"),
        )
        .withColumn("TICKET_MEDIO", F.round(F.col("TOTAL_VENDAS") / F.col("QTD_PEDIDOS"), 2))
        .withColumn("ITENS_POR_PEDIDO", F.round(F.col("QTD_ITENS") / F.col("QTD_PEDIDOS"), 2))
        .withColumn("MARGEM_PERC", F.round((F.col("MARGEM_BRUTA") / F.col("TOTAL_VENDAS")) * 100, 2))
        .withColumn("SLA_ENTREGA_24H_PERC", F.round((F.col("PEDIDOS_NO_SLA_24H") / F.col("QTD_PEDIDOS")) * 100, 2))
        .withColumn("DIGITAL_SHARE_PERC", F.round((F.col("PEDIDOS_DIGITAIS") / F.col("QTD_PEDIDOS")) * 100, 2))
    )

    janela_regional = Window.partitionBy("mes_referencia", "regional").orderBy(F.col("TOTAL_VENDAS").desc())

    return (
        base_kpi
        .withColumn("RANK_CATEGORIA_REGIONAL", F.row_number().over(janela_regional))
        .withColumn("KPI_ALERTA_MARGEM", F.when(F.col("MARGEM_PERC") < 18, F.lit("RISCO")).otherwise(F.lit("OK")))
        .withColumn("KPI_ALERTA_SLA", F.when(F.col("SLA_ENTREGA_24H_PERC") < 92, F.lit("RISCO")).otherwise(F.lit("OK")))
        .withColumn("KPI_ALERTA_TICKET", F.when(F.col("TICKET_MEDIO") < 350, F.lit("ATENCAO")).otherwise(F.lit("OK")))
        .withColumn("gerado_em", F.current_timestamp())
    )


def gerar_auditoria(vendas_curated):
    return (
        vendas_curated
        .groupBy("mes_referencia", "regional", "status_pedido", "tipo_movimento")
        .agg(
            F.countDistinct("pedido_id").alias("pedidos"),
            F.sum("valor_total").alias("valor_total"),
            F.sum("margem_bruta_item").alias("margem_bruta"),
        )
        .withColumn("gerado_em", F.current_timestamp())
    )


def persistir(curated_df, gold_df, auditoria_df):
    (
        curated_df
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("mes_referencia")
        .save(CURATED_PATH)
    )

    (
        gold_df
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("mes_referencia", "regional")
        .save(GOLD_PATH)
    )

    (
        auditoria_df
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("mes_referencia")
        .save(AUDIT_PATH)
    )


def main():
    vendas, clientes, produtos, campanhas = carregar_fontes()
    vendas_enriquecidas = enriquecer_vendas(vendas, clientes, produtos, campanhas)
    vendas_curated = gerar_curated(vendas_enriquecidas)
    kpis_gold = gerar_kpis_gold(vendas_curated)
    auditoria = gerar_auditoria(vendas_curated)
    persistir(vendas_curated, kpis_gold, auditoria)


main()
