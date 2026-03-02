from pyspark.sql import functions as F

# Leitura da base
df_vendas = spark.read.format("parquet").load("/mnt/bronze/vendas")

# Filtro de escopo
df_vendas = df_vendas.filter(F.col("status") == "ativo")

# Join com dimensao
df_clientes = spark.read.format("parquet").load("/mnt/bronze/clientes")
df_base = df_vendas.join(df_clientes, "id_cliente")

# Agregacao principal
df_kpi = df_base.groupBy("id_cliente").agg(F.sum("valor").alias("total"))

# Saida (gold)
df_kpi.write.mode("overwrite").format("delta").save("/mnt/gold/kpi_vendas")
