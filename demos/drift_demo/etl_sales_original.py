from pyspark.sql import functions as F


PATH_SOURCE = "/mnt/consumezone/demo/sales/fato_vendas_tmp"
PATH_TARGET = "/mnt/consumezone/demo/sales/fato_vendas"


df = spark.read.format("delta").load(PATH_SOURCE)

df = (
    df
    .filter(F.col("status") == "OK")
    .filter(F.col("amount") > 100)
    .filter(F.col("tipo_pedido").isin("VENDA", "RENOVACAO"))
    .filter(F.col("data_referencia").isNotNull())
)

resultado = (
    df
    .groupBy("dt_ref", "regional", "canal")
    .agg(
        F.sum("amount").alias("TOTAL_VENDAS"),
        F.countDistinct("order_id").alias("QTD_PEDIDOS"),
        F.countDistinct("customer_id").alias("CLIENTES_ATIVOS"),
        F.sum("gross_margin").alias("MARGEM_BRUTA"),
    )
    .withColumn("TICKET_MEDIO", F.round(F.col("TOTAL_VENDAS") / F.col("QTD_PEDIDOS"), 2))
)

(
    resultado
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("dt_ref")
    .save(PATH_TARGET)
)
