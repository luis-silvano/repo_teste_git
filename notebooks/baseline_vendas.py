# baseline_vendas.py
from pyspark.sql import functions as f

# Configurações de Caminho
path_vendas = "/mnt/datalake/vendas/transacoes_brutas"
path_produtos = "/mnt/datalake/dimensoes/produtos"
path_saida = "/mnt/datalake/gold/vendas_consolidadas"

def processar():
    # 1. Leitura (Linhagem de Entrada)
    vendas = spark.read.parquet(path_vendas)
    produtos = spark.read.table("dw.dim_produtos")

    # 2. Filtro de Negócio
    vendas_ok = vendas.filter(f.col("status") == "FINALIZADO")

    # 3. Join (Lógica)
    df_final = vendas_ok.join(produtos, "id_produto", "inner")

    # 4. Escrita (Linhagem de Saída e Infra)
    df_final.write.mode("overwrite").partitionBy("ano", "mes").save(path_saida)

processar()