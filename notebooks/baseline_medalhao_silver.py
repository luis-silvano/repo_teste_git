from pyspark.sql.functions import *

bronze_path = "/mnt/lake/bronze/vendas"
silver_path = "/mnt/lake/silver/vendas"

# leitura bronze
vendas = spark.read.parquet(bronze_path)

# transformacoes simples
vendas_ok = vendas.select("id", "valor").where("valor > 0")

# escrita silver
vendas_ok.write.mode("overwrite").format("delta").save(silver_path)
