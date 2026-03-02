from pyspark.sql.functions import *

path_in = "/mnt/lake/bronze/clientes"

df = spark.read.parquet(path_in)

# cpf mascarado
safe = df.selectExpr("sha2(cpf, 256) as cpf_hash", "nome")

safe.write.mode("overwrite").format("delta").save("/mnt/lake/silver/clientes")
