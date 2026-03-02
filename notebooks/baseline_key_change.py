from pyspark.sql.functions import *

path_left = "/mnt/lake/bronze/clientes"
path_right = "/mnt/lake/bronze/transacoes"

clientes = spark.read.parquet(path_left)
transacoes = spark.read.parquet(path_right)

# join por chave principal
resultado = transacoes.join(clientes, "id")

resultado.write.mode("overwrite").format("delta").save("/mnt/lake/silver/transacoes")
