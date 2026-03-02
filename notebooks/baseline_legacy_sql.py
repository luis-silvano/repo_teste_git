# SQL legado com triple quotes
sql_query = """
SELECT id, valor
FROM raw.vendas
WHERE valor > 0
"""

df = spark.sql(sql_query)

df.write.saveAsTable("gld.vendas")
