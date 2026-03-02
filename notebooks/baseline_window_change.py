from pyspark.sql.functions import *
from pyspark.sql.window import Window

path_in = "/mnt/lake/bronze/vendas"

df = spark.read.parquet(path_in)

w = Window.partitionBy("id").orderBy(col("dt").desc())

df2 = df.withColumn("rn", row_number().over(w))

(df2.write
    .mode("overwrite")
    .format("delta")
    .save("/mnt/lake/silver/vendas_rn"))
