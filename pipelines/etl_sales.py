from pyspark.sql import functions as F

# Simula leitura
source_path = "/mnt/rawzone/demo/sales_teste"
df = spark.read.format("parquet").load(source_path)

# Regras simples
limpo = (
    df
    .filter(F.col("status") == "OK")
    .filter(F.col("amount") > 100)
    .filter(F.col("cliente") == "José da silva")
)

# KPIs
fato = (
    limpo
    .groupBy("sk_unidade", "sk_anomes")
    .agg(
        F.sum("amount").alias("TOTAL_VENDAS"),
        F.count("order_id").alias("QTD_PEDIDOS")
    )
)

# Saída
output_path = "/mnt/consumezone/demo/sales/fato_vendas"
(
    fato
    .write
    .mode("overwrite")
    .format("parquet")
    .save(output_path)
)
