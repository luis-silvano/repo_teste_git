from pyspark.sql.functions import *

path_in = "/mnt/lake/bronze/fatos"

df = spark.read.parquet(path_in)

(df.write
    .mode("overwrite")
    .format("delta")
    .save("/mnt/lake/silver/fatos"))
