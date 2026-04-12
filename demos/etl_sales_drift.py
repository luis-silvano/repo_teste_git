from pyspark.sql import functions as F

# Simula leitura
source_path = "/mnt/rawzone/demo/sales"
df = spark.read.format("parquet").load(source_path)

# Regras simples
limpo = (
    df
    .filter(F.col("status").isin(["OK", "REPROCESS"]))
    .filter(F.col("amount") >= 0)
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

# Saída (mudança de caminho)
output_path = "/mnt/consumezone/demo/sales/fato_vendas_tmp"
(
    fato
    .write
    .mode("overwrite")
    .format("parquet")
    .save(output_path)
)
