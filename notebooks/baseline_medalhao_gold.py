from pyspark.sql.functions import *

silver_path = "/mnt/lake/silver/vendas"
gold_table = "gld.vendas_agregadas"

# leitura silver
vendas = spark.read.parquet(silver_path)

# agregacao
agg = vendas.groupBy("id").agg(sum("valor").alias("total"))

# escrita gold (tabela)
agg.write.saveAsTable(gold_table)
