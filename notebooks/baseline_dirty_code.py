from pyspark.sql.functions import *

path_in = "/mnt/lake/bronze/clientes"
path_out = "/mnt/lake/silver/clientes"

df  = spark.read   .parquet( path_in )

df2 = (df
    .withColumn("flag" , when(col("id") > 0, 1).otherwise(0))
    .join(  dim_clientes ,  "id"  )
)

# escrita com particionamento
(df2
    .write
    .partitionBy("dt")
    .mode("overwrite")
    .format("delta")
    .save(path_out)
)
