from pyspark.sql.functions import *

path_in = "/mnt/lake/bronze/vendas"

vendas = spark.read.parquet(path_in)

# filtro original
vendas_filtradas = vendas.filter(col("valor") > 0)

vendas_filtradas.write.mode("overwrite").format("delta").save("/mnt/lake/silver/vendas")
