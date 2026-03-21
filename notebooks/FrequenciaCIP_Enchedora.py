# Databricks notebook source
# DBTITLE 1,Executa o Read_delta_sharing_tables
# MAGIC %run "/Workspace/Lib/Read delta sharing tables"

# COMMAND ----------

# DBTITLE 1,Instancia método DeltaSharingRead (Brewdat)
delta_sharing_read_tables = DeltaSharingRead()

# COMMAND ----------

# MAGIC %md
# MAGIC # Packaging - Enchedora

# COMMAND ----------

# MAGIC %md
# MAGIC - '**consolidado**': CIPs realizados x previstos por unidade por mês
# MAGIC - '**validade**': últimos CIPs realizados por equipamento por mês
# MAGIC - '**etapas**': etapas de CIP por equipamento

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicialização

# COMMAND ----------

# MAGIC %run /Brasil/Functions/GeneralFunctions

# COMMAND ----------

# DBTITLE 1,Import de funções
from pyspark.sql import functions
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Parametrização do gerenciamento de arquivos
consume_file_type = 'parquet'
history_file_type = 'parquet'
write_mode_cz = 'overwrite'
utc_timezone = 'America/Sao_Paulo'
context = 'Supply'
domain = 'Producao'

sink_path_consolidado = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Fatos/ConsolidadoParciais/FatoConsolidadoParcialEnchedora'
sink_path_validade = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Fatos/ValidadeParciais/FatoValidadeParcialEnchedora'
sink_path_etapas = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Fatos/EtapasParciais/FatoEtapasParcialEnchedora'

# COMMAND ----------

# DBTITLE 1,Criação das variáveis de reprocessamento
dbutils.widgets.text('reprocessamento', '', 'reprocessamento')
dbutils.widgets.text('mes_reprocessamento', '', 'mes_reprocessamento')
dbutils.widgets.text('ano_reprocessamento', '', 'ano_reprocessamento')

reprocessamento = dbutils.widgets.get('reprocessamento').strip().lower() == 'true'
mes_reprocessamento = dbutils.widgets.get('mes_reprocessamento')
ano_reprocessamento = dbutils.widgets.get('ano_reprocessamento')

# COMMAND ----------

# DBTITLE 1,Função para definir quais partições serão lidas
def get_partitions(reprocessamento, mes_reprocessamento, ano_reprocessamento):
    """
    Função que gera partições de dados de acordo com o ano e mês de processamento, com base no parâmetro de reprocessamento.
    
    Parâmetros:
    - reprocessamento: Booleano que indica se o processo é um reprocessamento (True) ou não (False).
    - mes_reprocessamento: Mês de reprocessamento (se o processo for de reprocessamento).
    - ano_reprocessamento: Ano de reprocessamento (se o processo for de reprocessamento).
    
    Retorna:
    - Um dicionário com as seguintes chaves:
        - 'ano_mes_processamento': Valor inteiro no formato YYYYMM representando o ano e mês de processamento.
        - 'ano_mes_dia_processamento': Valor inteiro no formato YYYYMMDD representando o ano, mês e dia de processamento.
        - 'particoes_1_ano': Lista de partições de dados para o último ano (12 meses + o mês atual).
        - 'particoes_3_meses': Lista de partições de dados para os últimos 3 meses.
        OBS: No caso do reprocessamento, 'particoes_1_ano' e 'particoes_3_meses' terão o mesmo valor, 
        cobrindo todo o período a partir de 'mes_processamento' e 'ano_processamento'
    """
    
    if reprocessamento:
        # Se for reprocessamento, usa os parâmetros informados
        ano_mes_processamento = int(ano_reprocessamento) * 100 + int(mes_reprocessamento)
        ano_mes_dia_processamento = int(ano_reprocessamento) * 10000 + int(mes_reprocessamento) * 100
        
        # Define o mês e ano de reprocessamento
        data_referencia_inicio = date(int(ano_reprocessamento), int(mes_reprocessamento), 1)
    else:
        # Caso não seja reprocessamento, pega os últimos 3 meses para o CIP rotina
        data_referencia = date.today() + relativedelta(months=-2)
        ano_mes_processamento = data_referencia.year * 100 + data_referencia.month
        ano_mes_dia_processamento = data_referencia.year * 10000 + data_referencia.month * 100
        
        # Para os CIPs periódicos e opcionais, pega o último ano
        data_referencia_inicio = date.today() - relativedelta(years=1)
        
    # Gera as partições para os meses desde o ano_mes_processamento até o mês atual
    particoes_desde_reprocessamento = [
        f"date_partition={(data_referencia_inicio + relativedelta(months=i)).strftime('%Y%m')}"
        for i in range((date.today().year - data_referencia_inicio.year) * 12 + date.today().month - data_referencia_inicio.month + 1)
    ]
    
    # Para os CIPs periódicos e opcionais, pega o último ano
    data_referencia_1_ano = date.today() - relativedelta(years=1)
    particoes_1_ano = [
        f"date_partition={(data_referencia_1_ano + relativedelta(months=i)).strftime('%Y%m')}"
        for i in range((date.today().year - data_referencia_1_ano.year) * 12 + date.today().month - data_referencia_1_ano.month + 1)
    ]
    
    if not reprocessamento:
        # Gera as partições para os últimos 3 meses (mês atual e dois anteriores)
        data_referencia_3_meses = date.today() + relativedelta(months=-2)
        particoes_3_meses = [
            f"date_partition={(data_referencia_3_meses + relativedelta(months=i)).strftime('%Y%m')}"
            for i in range(3)  # Mês atual e dois meses anteriores
        ]
    else:
        # No caso de reprocessamento, partições_3_meses são iguais a partições_desde_reprocessamento
        particoes_3_meses = particoes_desde_reprocessamento
        
        # Se o período de reprocessamento for maior que 1 ano, as partições_1_ano também serão iguais a partições_desde_reprocessamento
        periodo_maior_que_1_ano = (date.today().year - data_referencia_inicio.year > 1) or \
                                  (date.today().year - data_referencia_inicio.year == 1 and \
                                   date.today().month >= data_referencia_inicio.month)
        if not periodo_maior_que_1_ano:
            # Se não for maior que 1 ano, mantém as partições padrão de 1 ano
            pass
    
    # Determina quais partições de 1 ano retornar
    if reprocessamento:
        periodo_maior_que_1_ano = (date.today().year - data_referencia_inicio.year > 1) or \
                                  (date.today().year - data_referencia_inicio.year == 1 and \
                                   date.today().month >= data_referencia_inicio.month)
        particoes_1_ano_final = particoes_desde_reprocessamento if periodo_maior_que_1_ano else particoes_1_ano
    else:
        particoes_1_ano_final = particoes_1_ano
        
    return {
        "ano_mes_processamento": ano_mes_processamento,
        "ano_mes_dia_processamento": ano_mes_dia_processamento,
        "particoes_1_ano": particoes_1_ano_final,  # Partições de 1 ano
        "particoes_3_meses": particoes_3_meses  # Partições de 3 meses
    }

# COMMAND ----------

# DBTITLE 1,HZ e CZ - Listas de Parâmetros e De/Paras
# Itens de processo
path_history_parametros_ics = '/mnt/historyzone/Brazil/Sharepoint/ParametrosICsFreqCIP'
df_parametros_itens_processo = dataframe_builder(path_history_parametros_ics, history_file_type).filter(
  col('DSC_SUB_AREA') == 'ENCHEDORA')

# Tempos de processo (validade de CIPs, etc)
path_history_parametros_horas = '/mnt/historyzone/Brazil/Sharepoint/ParametrosHorasFreqCIP'
df_parametros_horas = dataframe_builder(path_history_parametros_horas, history_file_type).filter(
  col('DSC_SUB_AREA') == 'ENCHEDORA')

# Trocas de Produto 
path_history_troca_produtos = '/mnt/historyzone/Brazil/Sharepoint/ListaTrocaProdutosFreqCIP'
df_parametros_troca_produtos = dataframe_builder(path_history_troca_produtos, history_file_type)

# Trocas de Sabor - Refri
path_history_troca_sabores = '/mnt/historyzone/Brazil/Sharepoint/ListaTrocaSaboresRefriFreqCIP'
df_parametros_troca_sabores = dataframe_builder(path_history_troca_sabores, history_file_type)

# De/Para Linhas
'''
Campos obrigatórios para que passe corretamente pelo de/para:
- SiglaPlanta
- CodigoLinhaMesAthena (SK_LINHA_PRODUCAO do MES Athena)
- DescricaoLinha (ex: L501, L502)
'''
path_history_depara_linhas = '/mnt/historyzone/Brazil/Sharepoint/DeParaLinhasFreqCIP'
df_de_para_linhas = dataframe_builder(path_history_depara_linhas, history_file_type)

# De/Para Resultantes
path_history_resultantes_trocas = '/mnt/historyzone/Brazil/Sharepoint/ListaDeParaIcsProdutoPackagingFreqCIP'
df_parametros_resultantes_trocas = dataframe_builder(path_history_resultantes_trocas, history_file_type).select(
  col('ResultanteProcesso').cast('int').alias('SK_RESULTANTE'),
  col('MarcaPlanilhaTrocaDeProduto').alias('DESC_MARCA')
)

# COMMAND ----------

# DBTITLE 1,Paths CZ - Leitura de fatos e dimensões
# FatoParadasLms
fato_paradas = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_fato_paradas').\
  withColumn('SK_DATA', col('SK_DATA').cast('int'))

# FatoProducaoLms
fato_producao = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_fato_producao').\
  withColumn('SK_DATA', col('SK_DATA').cast('int'))


# dimTipoEquipamento
path_consume_dim_tipo_equipamento = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Dimensoes/DimTipoEquipamento'
sk_tipo_equipamento = dataframe_builder(path_consume_dim_tipo_equipamento, consume_file_type).filter(col('DESC_TIPO_EQUIPAMENTO') == 'ENCHEDORA').select('SK_TIPO_EQUIPAMENTO').first()


# dimEquipamentoCip
path_consume_dim_equipamento_cip = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Dimensoes/DimEquipamentoCip'
dim_equipamento_cip = dataframe_builder(path_consume_dim_equipamento_cip, consume_file_type)
sk_equipamento_enchedora = dim_equipamento_cip.filter(col('DES_ABREV') == 'ENCHEDORA').select('SK_EQUIPAMENTO').first()

# dimUnidades 
path_consume_dim_unidades = '/mnt/consumezone/Brazil/Supply/Estrutura/DimUnidades'
dim_unidades = dataframe_builder(path_consume_dim_unidades, consume_file_type)

# dimResultanteAthena
path_consume_dim_resultante = '/mnt/consumezone/Brazil/Supply/Producao/DimResultanteMesAthena'
dim_resultante_athena = dataframe_builder(path_consume_dim_resultante, consume_file_type)

# dimLinhaProducaoLMS
dim_linha_producao_lms = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_dim_linha_producao')

# dimCervejariaLMS
dim_cervejaria_lms = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_dim_cervejaria')

# dimFalhaLMS
dim_falha_lms = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_dim_falha')

# dimEquipamentoLMS
dim_equipamento_lms = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_dim_equipamento')

# dimProdutoLMS
dim_produto_lms = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_dim_produto')

# dimEventoLMS
dim_evento_lms = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_dim_evento')

# dimTipoParadaLMS
dim_tipo_parada_lms = delta_sharing_read_tables.read_table(share_name = 'brewdat_uc_saz_scus_iris_supply_prod_ds', schema_name = 'gld_saz_supply_lms', table_name = 'lms_dim_tipo_parada')

# COMMAND ----------

# DBTITLE 1,Manipulação de/para Linhas
df_de_para_linhas = (df_de_para_linhas
    .join(dim_unidades, col('SiglaPlanta') == col('SIGLA_UNIDADE'))
    .join(dim_cervejaria_lms, 'COD_UNIDADE_LAKE')
    .alias('grupo')
    .join(
        dim_linha_producao_lms.alias('linha'), 
        (col('grupo.SK_CERVEJARIA') == col('linha.SK_CERVEJARIA')) & 
        (
            (col('grupo.NomeLinhaLMS') == col('linha.NM_LINHA')) |
            (col('grupo.DescricaoLinha') == col('linha.NM_LINHA'))
        )
    )
    .select(
        col('grupo.COD_UNIDADE_LAKE').alias('SK_UNIDADE'),
        col('grupo.CodigoLinhaMESAthena').alias('SK_LINHA_PRODUCAO_MES'),
        col('linha.SK_LINHA_PRODUCAO').alias('SK_LINHA_PRODUCAO_LMS')
    )
    .distinct()  
).dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coletas MES Athena - FatoColetaItemProcesso

# COMMAND ----------

# DBTITLE 1,Leitura das partições
parametros_processamento = get_partitions(reprocessamento, mes_reprocessamento, ano_reprocessamento)

base_path = "dbfs:/mnt/consumezone/Brazil/Supply/Producao/FatoColetaItemProcessoMesAthena/"
particoes_1_ano = parametros_processamento['particoes_1_ano']

try:
    # Tenta carregar com todas as partições de 1 ano
    fato_coleta_item_processo_athena_1_ano = (
        spark.read.format('parquet')
        .option("basePath", base_path)
        .load([f"{base_path}{particao}" for particao in particoes_1_ano])
    )
except:
    # Se falhar (provavelmente mês atual não existe), tenta sem a última partição
    print("Partição do mês atual não encontrada, carregando sem ela...")
    fato_coleta_item_processo_athena_1_ano = (
        spark.read.format('parquet')
        .option("basePath", base_path)
        .load([f"{base_path}{particao}" for particao in particoes_1_ano[:-1]])
    )

# Processa as partições de 3 meses (o filtro vai funcionar com os dados que foram carregados)
particoes_3_meses = [
    particao.replace("date_partition=", "") 
    for particao in parametros_processamento['particoes_3_meses']
]

fato_coleta_item_processo_athena_3_meses = fato_coleta_item_processo_athena_1_ano.filter(
    col("date_partition").isin(particoes_3_meses)
)

# COMMAND ----------

# DBTITLE 1,Função de processamento de coletas
def processar_coletas_athena(fato_coleta_item_processo_athena, 
                             df_parametros_itens_processo,
                             df_de_para_linhas):
    

    # Filtra a FatoColetaItemProcesso para ter somente os itens de processo maepados
    fato_coleta_item_processo_athena = fato_coleta_item_processo_athena.join(broadcast(df_parametros_itens_processo),
                                                on = 'SK_ITEM_PROCESSO',
                                                how = 'semi')


    df_coletas_athena = (
        fato_coleta_item_processo_athena.select(
            fato_coleta_item_processo_athena.SK_UNIDADE,
            fato_coleta_item_processo_athena.SK_LINHA_PRODUCAO, 
            fato_coleta_item_processo_athena.SK_EQUIPAMENTO,
            fato_coleta_item_processo_athena.SK_RESULTANTE,
            fato_coleta_item_processo_athena.SK_LOTE,
            fato_coleta_item_processo_athena.SK_ETAPA_PROCESSO,
            fato_coleta_item_processo_athena.SK_ITEM_PROCESSO,
            fato_coleta_item_processo_athena.VALOR_TEXTO.alias('NUM_VALOR'),
            fato_coleta_item_processo_athena.NUMERO_LOTE.alias('NUM_EXTERNO_LOTE'),
            fato_coleta_item_processo_athena.SK_DATA_INICIO_LOTE,
            fato_coleta_item_processo_athena.SK_HORA_INICIO_LOTE
        )
    )

   
    df_coletas_athena = (
        df_coletas_athena
        .withColumn("NUM_EXTERNO_LOTE", col("NUM_EXTERNO_LOTE").substr(6, 100).cast("int"))
        .withColumn("NUM_VALOR_ORIGINAL", col("NUM_VALOR"))
        .withColumn("NUM_VALOR", to_timestamp(substring(col("NUM_VALOR_ORIGINAL"), 0, 16), "yyyy-MM-dd HH:mm"))
        .withColumn("NUM_VALOR_VOLUME", when(col("NUM_VALOR").isNull(), col("NUM_VALOR_ORIGINAL").cast("double")))
        .withColumn(
            "NUM_VALOR",
            when(
                col('NUM_VALOR').isNull(),
                to_timestamp(
                    concat(
                        substring(col("SK_DATA_INICIO_LOTE"), 1, 4), lit("-"),
                        substring(col("SK_DATA_INICIO_LOTE"), 5, 2), lit("-"),
                        substring(col("SK_DATA_INICIO_LOTE"), 7, 2), lit(" "),
                        lpad(substring(lpad(col("SK_HORA_INICIO_LOTE").cast("string"), 4, "0"), 1, 2), 2, "0"), lit(":"),
                        lpad(substring(lpad(col("SK_HORA_INICIO_LOTE").cast("string"), 4, "0"), 3, 2), 2, "0")
                    ),
                    "yyyy-MM-dd HH:mm"
                )
            ).otherwise(col("NUM_VALOR"))
        )
        .withColumn("SK_ANOMES", year(col("NUM_VALOR")) * 100 + month(col("NUM_VALOR")))
        .filter(col('NUM_VALOR') < current_timestamp())
    )


    df_coletas_athena = (
        df_coletas_athena
        .select(
            "SK_UNIDADE",
            "SK_LINHA_PRODUCAO",
            "SK_EQUIPAMENTO",
            'SK_ETAPA_PROCESSO',
            'SK_LOTE',
            "NUM_EXTERNO_LOTE",
            'SK_RESULTANTE',
            "SK_ITEM_PROCESSO",
            "NUM_VALOR",
            "NUM_VALOR_VOLUME",
            "SK_ANOMES"
        ).drop_duplicates()
    )

    df_coletas_athena = (
    df_coletas_athena
    .join(
        broadcast(
            df_de_para_linhas.select(
                "SK_UNIDADE",
                col("SK_LINHA_PRODUCAO_MES").alias('SK_LINHA_PRODUCAO'),
                "SK_LINHA_PRODUCAO_LMS"
            )
        ),
        on=["SK_UNIDADE", "SK_LINHA_PRODUCAO"],
        how="inner"
    )
    .select(
        "SK_UNIDADE",
        "SK_LINHA_PRODUCAO",
        "SK_LINHA_PRODUCAO_LMS", 
        "SK_EQUIPAMENTO",
        "SK_ETAPA_PROCESSO",
        "SK_LOTE",
        "NUM_EXTERNO_LOTE",
        "SK_RESULTANTE",
        "SK_ITEM_PROCESSO",
        "NUM_VALOR",
        "NUM_VALOR_VOLUME",
        "SK_ANOMES"
    )
    .drop_duplicates()
    )


    return df_coletas_athena


# COMMAND ----------

# DBTITLE 1,Separação das coletas
df_coletas_athena_1_ano_filtrado = processar_coletas_athena(fato_coleta_item_processo_athena_1_ano, df_parametros_itens_processo, df_de_para_linhas)
df_coletas_athena_3_meses_filtrado = processar_coletas_athena(fato_coleta_item_processo_athena_3_meses, df_parametros_itens_processo, df_de_para_linhas)

fato_producao = fato_producao.filter(col('SK_DATA').cast('int') >= parametros_processamento['ano_mes_dia_processamento'])
fato_paradas = fato_paradas.filter(col('SK_DATA').cast('int') >= parametros_processamento['ano_mes_dia_processamento'])

# COMMAND ----------

df_coletas_athena_1_ano_filtrado = df_coletas_athena_1_ano_filtrado.repartition("SK_LINHA_PRODUCAO")
df_coletas_athena_3_meses_filtrado = df_coletas_athena_3_meses_filtrado.repartition("SK_LINHA_PRODUCAO")
fato_producao = fato_producao.repartition("SK_LINHA_PRODUCAO") 
fato_paradas = fato_paradas.repartition("SK_LINHA_PRODUCAO")

# COMMAND ----------

# MAGIC %md
# MAGIC # Coletas LMS 
# MAGIC ## FatoProdução

# COMMAND ----------

# Aplicando as transformações adaptadas para a nova estrutura:

# Agrupamento por linha e hora inicial
df_fato_producao = fato_producao.withColumn('DATA', to_date(col('SK_DATA'), 'yyyyMMdd'))\
                            .withColumn('HOR_INI_DIG', substring(col('SK_HORA'), 1, 2))\
                            .groupBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'SK_DATA', 'HOR_INI_DIG'])\
                            .agg(sum('VLR_PRODUCAO_BRUTA').alias('VLR_PRODUCAO_BRUTA'))\
                            .withColumn('VLR_PRODUCAO_BRUTA', when(col('VLR_PRODUCAO_BRUTA') <= 6, lit(0)).otherwise(col('VLR_PRODUCAO_BRUTA')))\
                            .sort('SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'SK_DATA', 'HOR_INI_DIG')

# Rolling window para remover outliers isolados
# Para casos em que existem valores de produção bruta isolados, com 8 horas anteriores e posteriores sem produção bruta
windowSpec = Window.partitionBy("SK_LINHA_PRODUCAO").orderBy('SK_DATA', 'HOR_INI_DIG')

df_fato_producao = df_fato_producao.withColumn("IS_ZERO", when((col("VLR_PRODUCAO_BRUTA") == 0), lit(1)).otherwise(lit(0)))

rolling_sum_spec = windowSpec.rowsBetween(-8, 8)
df_fato_producao = df_fato_producao.withColumn("ROLLING_SUM", sum("IS_ZERO").over(rolling_sum_spec))

condition = ((col('IS_ZERO') == 0) & (col("ROLLING_SUM") == 16))
df_fato_producao = df_fato_producao.withColumn("VLR_PRODUCAO_BRUTA", when(condition, lit(0)).otherwise(col("VLR_PRODUCAO_BRUTA")))

df_fato_producao = df_fato_producao.drop("IS_ZERO", "ROLLING_SUM")

# Um novo identificador é atribuído a cada hora com produção bruta diferente de zero. Horas com produção brutas zeradas manterão o mesmo identificador
wd_producao_linha_ord = Window.partitionBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO']).orderBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'SK_DATA', 'HOR_INI_DIG'])

df_fato_producao = df_fato_producao.withColumn('VL_IDENTIFICADOR', concat(
    col('SK_LINHA_PRODUCAO'), 
    lit('_'),
    sum(when(col('VLR_PRODUCAO_BRUTA') != 0, 1).otherwise(0)).over(wd_producao_linha_ord)
))

# Para cada identificador, os registros zerados são contados. Identificadores com mais de 8 registros zerados são marcados como "período sem produção bruta", indicando que por 8 horas seguidas não houve produção.
wd_producao_identificador = Window.partitionBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'VL_IDENTIFICADOR'])
wd_producao_identificador_ord = Window.partitionBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'VL_IDENTIFICADOR']).orderBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'SK_DATA', 'HOR_INI_DIG'])

df_fato_producao = df_fato_producao.withColumn('PERIODO_SEM_PROD', 
    sum((col('VLR_PRODUCAO_BRUTA') == 0).cast('int')).over(wd_producao_identificador) >= 8)\
    .withColumn('RN_ASC', row_number().over(wd_producao_identificador_ord))\
    .withColumn('DATA_REF', to_timestamp(concat(col('SK_DATA'), col('HOR_INI_DIG')), 'yyyyMMddHH'))\
    .withColumn('PERIODO_ANTERIOR_SEM_PROD', lag(col('PERIODO_SEM_PROD'), 1).over(wd_producao_linha_ord))\
    .withColumn('PERIODO_SEM_PROD', (col('PERIODO_SEM_PROD')) | (~col('PERIODO_SEM_PROD') & (col('PERIODO_ANTERIOR_SEM_PROD'))))\
    .filter(col('RN_ASC') == 1)\
    .drop('RN_ASC', 'PERIODO_ANTERIOR_SEM_PROD')\
    .sort(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'SK_DATA', 'HOR_INI_DIG'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## FatoParadas

# COMMAND ----------

def normalizar_texto(col_name):
    return lower(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        col_name, '[áàâãä]', 'a'), '[éèêë]', 'e'), '[íìîï]', 'i'), '[óòôõö]', 'o'))

# Identificar registros para remover
falhas_para_remover = dim_falha_lms.filter(normalizar_texto(col('DSC_FALHA')).contains('velocidade')).select('SK_FALHA')
eventos_para_remover = dim_evento_lms.filter(normalizar_texto(col('DSC_EVENTO')).contains('velocidade')).select('SK_EVENTO')
tipos_para_remover = dim_tipo_parada_lms.filter(normalizar_texto(col('NM_TIPO_PARADA')).contains('inicio')).select('SK_TIPO_PARADA')
equipamentos_para_remover = dim_equipamento_lms.filter(
    normalizar_texto(col('NM_EQUIPAMENTO')).contains('rotuladora') |
    normalizar_texto(col('NM_EQUIPAMENTO')).contains('pasteurizadora') |
    normalizar_texto(col('NM_EQUIPAMENTO')).contains('sopradora') |
    normalizar_texto(col('NM_EQUIPAMENTO')).contains('paletizadora')
).select('SK_EQUIPAMENTO')

# Aplicar filtros com left anti joins
fato_paradas_filtrada = fato_paradas.filter(col('QTD_MINUTOS_PARADA') >= 1)
fato_paradas_filtrada = fato_paradas_filtrada.join(falhas_para_remover, on='SK_FALHA', how='left_anti')
fato_paradas_filtrada = fato_paradas_filtrada.join(eventos_para_remover, on='SK_EVENTO', how='left_anti')
fato_paradas_filtrada = fato_paradas_filtrada.join(equipamentos_para_remover, on='SK_EQUIPAMENTO', how='left_anti')
fato_paradas_filtrada = fato_paradas_filtrada.join(tipos_para_remover, on='SK_TIPO_PARADA', how='left_anti')

fato_paradas_filtrada = fato_paradas_filtrada\
    .withColumn('DAT_INICIO_PARADA', to_timestamp(concat(col('SK_DATA'), col('SK_HORA')), 'yyyyMMddHHmm'))\
    .withColumn('DAT_INICIO_PARADA_DOUBLE', col('DAT_INICIO_PARADA').cast('double'))\
    .withColumn('DAT_FIM_PARADA', expr("timestamp_seconds(DAT_INICIO_PARADA_DOUBLE + QTD_MINUTOS_PARADA * 60)"))\
    .select('SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'DAT_INICIO_PARADA', 'QTD_MINUTOS_PARADA', 'DAT_FIM_PARADA', 'SK_PRODUTO', 'SK_EVENTO')

wd_paradas_linha = Window.partitionBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO']).orderBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'DAT_INICIO_PARADA', 'DAT_FIM_PARADA'])
fato_paradas_filtrada = fato_paradas_filtrada.withColumn('VL_IDENTIFICADOR_PARADA', concat(
      col('SK_LINHA_PRODUCAO'), lit('_'), sum(lit(1)).over(wd_paradas_linha)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integração entre paradas e produção

# COMMAND ----------

# Trunca data de início da parada para a hora
fato_paradas_prod = fato_paradas_filtrada.withColumn('DAT_INICIO_PARADA', date_trunc('hour', col('DAT_INICIO_PARADA')))

# Join entre produção e paradas
df_producao = df_fato_producao.alias('fato_producao').join(
    fato_paradas_prod.alias('fato_paradas'), 
    ((col('fato_producao.SK_CERVEJARIA') == col('fato_paradas.SK_CERVEJARIA')) & 
     (col('fato_producao.SK_LINHA_PRODUCAO') == col('fato_paradas.SK_LINHA_PRODUCAO')) & 
     (col('fato_producao.DATA_REF') >= col('fato_paradas.DAT_INICIO_PARADA')) & 
     (col('fato_producao.DATA_REF') < col('fato_paradas.DAT_FIM_PARADA'))), 
    how='left')\
    .select('fato_producao.*',
            'fato_paradas.VL_IDENTIFICADOR_PARADA')\
    .groupBy(['SK_CERVEJARIA', 'SK_LINHA_PRODUCAO', 'SK_DATA', 'HOR_INI_DIG', 'VLR_PRODUCAO_BRUTA', 'VL_IDENTIFICADOR', 'PERIODO_SEM_PROD', 'DATA_REF']).agg(concat_ws(", ", collect_list(col('VL_IDENTIFICADOR_PARADA'))).alias('VL_IDENTIFICADOR_PARADA'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Parâmetros
# MAGIC
# MAGIC Definido um dicionário de parâmetros para auxílio no cálculo das frequências, ambos com origem no Sharepoint. São duas listas: 
# MAGIC - [Itens de processo](https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/Parametros%20ICs%20%20Frequncia%20de%20CIP/AllItems.aspx) e o que significam (início de CIP, final de CIP, início de assepsia, etc)
# MAGIC - [Parâmetros de tempo](https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/Parametros%20Horas%20%20Frequncia%20de%20CIP/AllItems.aspx) (limite de parada, validade de CIP, etc)

# COMMAND ----------

area = 'ENCHEDORA'


df_parametros_ics_filtered = df_parametros_itens_processo.filter(col("DSC_SUB_AREA") == area).select(col("DSC_ITEM"),
    col("SK_ITEM_PROCESSO").alias("VALOR"),
    col("DSC_EQUIPAMENTO"))
df_parametros_horas_filtered = df_parametros_horas.filter(col("DSC_SUB_AREA") == area).select(col("DSC_ITEM"),
    col("TEMPO_PROCESSO").alias("VALOR"),
    col("DSC_SUB_AREA").alias("DSC_EQUIPAMENTO"))


df_combined = df_parametros_ics_filtered.union(df_parametros_horas_filtered)


df_grouped = df_combined.groupBy("DSC_ITEM", "DSC_EQUIPAMENTO").agg(
    collect_list("VALOR").alias("VALORES")
)

dicionario_dict = df_grouped.collect()
parametros = {}

for row in dicionario_dict:
    item = row['DSC_ITEM'].lower()
    valores = [float(v) for v in row['VALORES']]

    if item in parametros:
        # Se o item já existe, adiciona os valores ao final da lista existente
        parametros[item].extend(valores)
    else:
        # Caso contrário, cria uma nova lista para o item
        parametros[item] = valores


# Agrupamento para equivalência entre CIPs, para caso em que não importa o tempo de CIP realizado para pendência
# Ex: pode ser feito tanto CIP rotina quanto periódico após as 8h sem produção

parametros['inicio_cip_rotina'] = list(set(parametros['inicio_cip_chopp_rotina'] + parametros['inicio_cip_refri_rotina'] + parametros['inicio_cip_cerveja_rotina']))
parametros['inicio_cip_periodico'] = list(set(parametros['inicio_cip_chopp_periodico'] + parametros['inicio_cip_refri_periodico'] + parametros['inicio_cip_cerveja_periodico']))
parametros['inicio_cip_opcional'] = list(set(parametros['inicio_cip_chopp_opcional'] + parametros['inicio_cip_refri_opcional'] + parametros['inicio_cip_cerveja_opcional']))
parametros['inicio_cip'] = list(set(parametros['inicio_cip_rotina'] + parametros['inicio_cip_periodico'] + parametros['inicio_cip_opcional']))

parametros['producao_cerveja'] = list(set(parametros['producao_lata'] + parametros['producao_long_neck'] + parametros['producao_cerv_retorn']))
parametros['producao_refri'] = list(set(parametros['producao_refri_lata'] + parametros['producao_refri_pet']))
parametros['producao'] = list(set(parametros['producao_cerveja'] + parametros['producao_refri']))

parametros['inicio_scrubbing'] = list(set(parametros['inicio_scrubbing_cerveja_01'] + parametros['inicio_scrubbing_cerveja_02'] + parametros['inicio_scrubbing_refri']))
parametros['tempo_cip_trecho_sem_producao'] = 12

parametros['inicio_assepsia_externa'] = list(set(parametros['inicio_assepsia_externa_02'] + parametros['inicio_assepsia_externa_01']))


# COMMAND ----------

# MAGIC %md
# MAGIC # Previsão de CIPs

# COMMAND ----------

# MAGIC %md
# MAGIC https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/Packaging%20DePara%20ICs%20por%20Produto%20%20Frequncia%20de%20CIP/AllItems.aspx
# MAGIC https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/TrocaProdutosFreqCIP/AllItems.aspx
# MAGIC https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/TrocaDeSaboresRefriFreqCIP/AllItems.aspx

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troca de produtos e tipos de produto  
# MAGIC
# MAGIC Trocas de produto são as trocas entre um mesmo tipo de produto mas de marcas diferentes, como **refri para refri** ou **cerveja para cerveja**. O tipo de assepsia definido para cada troca está nos de/paras [Troca Produtos](https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/TrocaProdutosFreqCIP/AllItems.aspx) para cerveja e [Troca Sabores](https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/TrocaDeSaboresRefriFreqCIP/AllItems.aspx) para refri. Para consultar a marca de cada produto é utilizado um de/para de [resultantes](https://anheuserbuschinbev.sharepoint.com/sites/SupplyDatalake/Lists/Packaging%20DePara%20ICs%20por%20Produto%20%20Frequncia%20de%20CIP/AllItems.aspx). _TO DO: tentar pegar a marca direto da dimensão do Athena, tirando a necessidade desse último de/para._
# MAGIC
# MAGIC Trocas de tipo de produto são trocas de cerveja para refri ou vice-versa. Essas trocas sempre devem prever 1 CIP.

# COMMAND ----------

df_horarios_trocas = df_coletas_athena_3_meses_filtrado.alias('coletas').filter(col('SK_ITEM_PROCESSO').isin(parametros['producao'])).join(
  dim_resultante_athena.alias('dim_resultante'), on = (df_coletas_athena_3_meses_filtrado.SK_RESULTANTE == dim_resultante_athena.SK_RESULTANTE_PROCESSO), how = 'left'
).select('coletas.*', col('dim_resultante.DESC_RESULTANTE_PROCESSO').alias('DESC_RESULTANTE'))

wd_producao_linha = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy('NUM_VALOR')

df_horarios_trocas = df_horarios_trocas.\
  withColumn('PROX_SK_RESULTANTE',
             lead(col('SK_RESULTANTE')).over(wd_producao_linha)).\
  withColumn('PROX_DESC_RESULTANTE',
             lead(col('DESC_RESULTANTE')).over(wd_producao_linha)).\
  withColumn('PROX_SK_ITEM_PROCESSO',
             lead(col('SK_ITEM_PROCESSO')).over(wd_producao_linha)).\
  withColumn('PROX_NUM_VALOR',
             lead(col('NUM_VALOR')).over(wd_producao_linha)).\
  withColumn('PROX_NUM_EXTERNO_LOTE',
             lead(col('NUM_EXTERNO_LOTE')).over(wd_producao_linha)).\
  filter(col('SK_RESULTANTE') != col('PROX_SK_RESULTANTE'))

# COMMAND ----------

df_trocas = df_parametros_troca_produtos.select(
  col('Title').alias('ORIGEM'),
  col('DESTINO'),
  col('ASSEPSIA')
).unionByName(df_parametros_troca_sabores.select(
  col('Title').alias('ORIGEM'),
  col('DESTINO'),
  col('ASSEPSIA')
))

# COMMAND ----------

# Trocas de cerveja -> refri ou refri -> cerveja
# Sempre executar 1 CIP

df_horarios_troca_tipo_produto = df_horarios_trocas.filter(
  (
    (col('SK_ITEM_PROCESSO').isin(parametros['producao_cerveja'])) &
    (col('PROX_SK_ITEM_PROCESSO').isin(parametros['producao_refri']))
  ) |
  (
    (col('SK_ITEM_PROCESSO').isin(parametros['producao_refri'])) &
    (col('PROX_SK_ITEM_PROCESSO').isin(parametros['producao_cerveja']))
  )
)

# COMMAND ----------

df_horarios_troca_produto = df_horarios_trocas.alias('df_horarios_trocas').filter(
  (
    (col('SK_ITEM_PROCESSO').isin(parametros['producao_cerveja'])) &
    (col('PROX_SK_ITEM_PROCESSO').isin(parametros['producao_cerveja']))
  ) |
  (
    (col('SK_ITEM_PROCESSO').isin(parametros['producao_refri'])) &
    (col('PROX_SK_ITEM_PROCESSO').isin(parametros['producao_refri']))
  )
).join(
  df_parametros_resultantes_trocas.alias('resultante_origem'), df_horarios_trocas.SK_RESULTANTE == col('resultante_origem.SK_RESULTANTE'), how = 'left'
).join(
  df_parametros_resultantes_trocas.alias('resultante_destino'), df_horarios_trocas.PROX_SK_RESULTANTE == col('resultante_destino.SK_RESULTANTE'), how = 'left'
).join(
  df_trocas.alias('df_trocas'),
  (col('resultante_origem.DESC_MARCA') == col('df_trocas.ORIGEM')) &
  (col('resultante_destino.DESC_MARCA') == col('df_trocas.DESTINO')), how = 'left'
).select('df_horarios_trocas.*', col('resultante_origem.DESC_MARCA').alias('MARCA_ORIGEM'), col('resultante_destino.DESC_MARCA').alias('MARCA_DESTINO'), col('df_trocas.ASSEPSIA').alias('ASSEPSIA'))

# COMMAND ----------

def calcular_timestamps_troca(df_troca, fato_producao):
    """
    Calcula timestamps de início e fim de troca de produto baseado na produção.
    
    Args:
        df_troca: DataFrame com dados de troca de produto
        fato_producao: DataFrame com dados de produção
        
    Returns:
        DataFrame com colunas ts_producao_inicio_troca e ts_producao_final_troca
    """
    
    # Window functions
    w_desc = Window.partitionBy('SK_LOTE').orderBy(desc('SK_DATA'), desc('SK_HORA'))
    w_asc = Window.partitionBy('SK_LOTE').orderBy(asc('SK_DATA'), asc('SK_HORA'))
    
    # Join para pegar timestamp de início (lote atual - maior data/hora)
    df_com_inicio = (
        df_troca.alias('troca')
        .join(
            fato_producao.alias('producao_atual'),
            on=(
                (col('troca.SK_LINHA_PRODUCAO_LMS') == col('producao_atual.SK_LINHA_PRODUCAO')) &
                (concat(year(col('troca.NUM_VALOR')), lit('-'), col('troca.NUM_EXTERNO_LOTE')) == col('producao_atual.IDE_LOTE')) &
                (col('troca.SK_RESULTANTE') == col('producao_atual.SK_RESULTANTE_ATHENA'))
            ),
            how='left'
        )
        .filter(col('VLR_PRODUCAO_BRUTA') > 0)
        .withColumn('row_number_inicio', row_number().over(w_desc))
        .filter(col('row_number_inicio') == 1)
        .withColumn(
            'ts_producao_inicio_troca',
            to_timestamp(
                concat_ws(
                    ' ',
                    col('producao_atual.SK_DATA').cast('string'),
                    concat(
                        lpad(col('producao_atual.SK_HORA').substr(1, 2), 2, '0'),
                        lit(':'),
                        lpad(col('producao_atual.SK_HORA').substr(3, 2), 2, '0')
                    )
                ),
                'yyyyMMdd HH:mm'
            )
        )
        .select('troca.*', 'ts_producao_inicio_troca', col('producao_atual.NM_PRODUTO').alias('NM_PRODUTO_INICIO'))
    )
    
    # Join para pegar timestamp de fim (próximo lote - menor data/hora)
    df_inicio_final = (
        df_com_inicio.alias('troca')
        .join(
            fato_producao.alias('producao_prox'),
            on=(
                (col('troca.SK_LINHA_PRODUCAO_LMS') == col('producao_prox.SK_LINHA_PRODUCAO')) &
                (concat(year(col('troca.PROX_NUM_VALOR')), lit('-'), col('troca.PROX_NUM_EXTERNO_LOTE')) == col('producao_prox.IDE_LOTE'))&
                (col('troca.PROX_SK_RESULTANTE') == col('producao_prox.SK_RESULTANTE_ATHENA'))
            ),
            how='left'
        )
        .filter(col('VLR_PRODUCAO_BRUTA') > 0)
        .withColumn('row_number_final', row_number().over(w_asc))
        .filter(col('row_number_final') == 1)
        .withColumn(
            'ts_producao_final_troca',
            to_timestamp(
                concat_ws(
                    ' ',
                    col('producao_prox.SK_DATA').cast('string'),
                    concat(
                        lpad(col('producao_prox.SK_HORA').substr(1, 2), 2, '0'),
                        lit(':'),
                        lpad(col('producao_prox.SK_HORA').substr(3, 2), 2, '0')
                    )
                ),
                'yyyyMMdd HH:mm'
            )
        )
        .select('troca.*', 'ts_producao_final_troca', col('producao_prox.NM_PRODUTO').alias('NM_PRODUTO_FINAL'))
    )

    df_final = df_inicio_final.withColumn(
        'timestamps_validos',
        col('ts_producao_inicio_troca') < col('ts_producao_final_troca')
    ).filter(col('timestamps_validos')).drop(col('timestamps_validos'))

    
    return df_final

fato_producao_produto = fato_producao.join(dim_produto_lms, on='SK_PRODUTO', how='left')

df_horarios_troca_produto_inicio_final_troca = calcular_timestamps_troca(
    df_horarios_troca_produto, 
    fato_producao_produto
).withColumn(
        'DESC_ETAPA', 
        concat(
            lit('Troca de Produto'), 
            lit(': de '), 
            col('NM_PRODUTO_INICIO'), 
            lit(' para '), 
            col('NM_PRODUTO_FINAL')
        )
    )

df_horarios_troca_tipo_produto_inicio_final_troca = calcular_timestamps_troca(
    df_horarios_troca_tipo_produto, 
    fato_producao_produto
).withColumn(
        'DESC_ETAPA', 
        concat(
            lit('Troca de Tipo de Produto'), 
            lit(': de '), 
            col('NM_PRODUTO_INICIO'), 
            lit(' para '), 
            col('NM_PRODUTO_FINAL')
        )
    )

# COMMAND ----------

# Trocas que requerem Assepsia Externa da Enchedora
df_troca_produto_assepsia = df_horarios_troca_produto_inicio_final_troca.filter(col('ASSEPSIA').contains('Espuma'))

# Trocas que requerem CIP 
df_troca_produto_cip_rotina = df_horarios_troca_produto_inicio_final_troca.filter((col('ASSEPSIA').contains('CIP')) | (col('ASSEPSIA').contains('2')))

# Trocas que requerem CIP Periódico
df_troca_produto_cip_periodico = df_horarios_troca_produto_inicio_final_troca.filter((col('ASSEPSIA').contains('3')))

# Trocas que requerem CIP Clorado
df_troca_produto_cip_opcional = df_horarios_troca_produto_inicio_final_troca.filter((col('ASSEPSIA').contains('4')))

# Trocas que requerem Scrubbing
df_troca_produto_scrubbing = df_horarios_troca_produto_inicio_final_troca.filter((col('ASSEPSIA').contains('scrubbing')))

# Trocas que requerem Desinfecção
df_troca_produto_desin = df_horarios_troca_produto_inicio_final_troca.filter((col('ASSEPSIA').contains('AQ')))

# COMMAND ----------

# MAGIC %md
# MAGIC # CIP Rotina

# COMMAND ----------

wd_producao = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy('SK_LINHA_PRODUCAO', 'DATA_REF')

df_pb_sem_producao = df_producao\
  .filter(df_producao.PERIODO_SEM_PROD)\
  .withColumn('DESC_ETAPA', when(row_number().over(wd_producao) % 2 == 1, 'Inicio de trecho sem producao').otherwise('Fim de trecho sem producao'))\
  .withColumn('DATA_REF_INICIO_TRECHO', lag(col('DATA_REF')).over(wd_producao))



# Identifica linhas com trecho sem produção ativo (número ímpar de registros)
df_trechos_ativos = df_pb_sem_producao.groupBy('SK_LINHA_PRODUCAO').agg(
    count('*').alias('QTD_REGISTROS'),
    max('DATA_REF').alias('ULTIMO_REGISTRO')
).filter(col('QTD_REGISTROS') % 2 == 1)  # Ímpar = trecho ainda ativo

# Cria registros de "fim virtual" para trechos ativos
df_fim_virtual = df_trechos_ativos.select(
    col('SK_LINHA_PRODUCAO'),
    lit(date_format(current_date(), 'yyyyMMdd').cast('int')).alias('SK_DATA'),
    col('ULTIMO_REGISTRO').alias('DATA_REF_INICIO_TRECHO'),
    current_timestamp().alias('NUM_VALOR'),  # Usa timestamp atual como fim
    lit('Fim de trecho sem producao').alias('DESC_ETAPA')
)

# Une com os fins reais
df_producao_final_trechos = df_pb_sem_producao\
  .filter(col('DESC_ETAPA') == 'Fim de trecho sem producao')\
  .select('SK_LINHA_PRODUCAO', 'SK_DATA', 'DATA_REF_INICIO_TRECHO', 
          (col('DATA_REF') + expr("INTERVAL 1 HOUR")).alias('NUM_VALOR'), 'DESC_ETAPA')\
  .unionByName(df_fim_virtual)  # Adiciona os fins virtuais

# COMMAND ----------

# Pendências de CIP por período sem produção bruta > 8h

df_pendencias_producao = df_producao_final_trechos.select(
  col('SK_LINHA_PRODUCAO').alias('SK_LINHA_PRODUCAO_LMS'), 
  col('DATA_REF_INICIO_TRECHO'), 
  col('NUM_VALOR'),
  col('DESC_ETAPA')
)

# COMMAND ----------

# Pendências de CIP por troca de produto ou tipo de produto             
df_troca_cip_rotina = (
    df_horarios_troca_tipo_produto_inicio_final_troca
    .unionByName(df_troca_produto_cip_rotina, allowMissingColumns=True)
)

# COMMAND ----------

# CIPs realizados 

# Define a janela por SK_LOTE, ordenando pelo maior NUM_VALOR
window_lote = Window.partitionBy('SK_LOTE').orderBy(col('NUM_VALOR').desc())
# Isso é pra no caso de 2 enchedoras na linha, considerar somente um horário de CIP

# Filtra e mantém apenas o maior NUM_VALOR por lote
df_cips = df_coletas_athena_3_meses_filtrado \
    .filter(col('SK_ITEM_PROCESSO').isin(parametros['inicio_cip'])) \
    .select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LOTE'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_VALOR'),
        lit('Inicio de CIP').alias('DESC_ETAPA')
    )

# COMMAND ----------

# Previsão de CIPs semanal

validade_cip_rotina_hrs = int(parametros['vencimento_cip_rotina'][0]*24) # Validade do CIP rotina
interval_expr = f"INTERVAL {validade_cip_rotina_hrs} HOURS"
wd_partition = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy('SK_LINHA_PRODUCAO', 'NUM_VALOR') 

df_cips_previsao = df_cips.withColumn(
  'DT_RANGE_PREVISTO',
  # Primeiro cenário: existe próximo CIP
  when((lead(col('NUM_VALOR')).over(wd_partition).isNotNull()),
    sequence(
      least(col("NUM_VALOR"), lead(col("NUM_VALOR")).over(wd_partition)),
      greatest(col("NUM_VALOR"), lead(col("NUM_VALOR")).over(wd_partition)),
      expr(interval_expr)
    )
  ).otherwise(
    # Segundo cenário: último CIP da série e passou da validade
    when(
      ((unix_timestamp(current_timestamp()) - unix_timestamp(col("NUM_VALOR"))) / 3600 > validade_cip_rotina_hrs),
      sequence(
        col('NUM_VALOR'),
        current_timestamp(),
        expr(interval_expr)
      )
    ).otherwise(array(col('NUM_VALOR')))
  )
).withColumn('DT_PREVISTO',
  explode(col('DT_RANGE_PREVISTO'))
).drop('DT_RANGE_PREVISTO').drop_duplicates()


wd_partition = Window.partitionBy('SK_LINHA_PRODUCAO', 'NUM_VALOR').orderBy(asc('DT_PREVISTO')) 
df_calendario_cip_rotina = df_cips_previsao.withColumn('NUM_LINHA', row_number().over(wd_partition)).withColumn(
    'DESC_ETAPA', 
    when(col('NUM_LINHA') == 1, lit('Previsto e Realizado'))
    .otherwise(
        concat(
            lit('Previsto e Não Realizado - Referente ao lote '),
            col('NUM_EXTERNO_LOTE').cast('string'),
            lit(' - '),
            round(((col('NUM_LINHA') - 1) * parametros['vencimento_cip_rotina'][0]), 1).cast('string'),
            lit(' dias desde último CIP')
        )
    )).\
    withColumn('DESC_TIPO_LIMPEZA', lit('CIP')).\
      withColumn('SK_DATA', lit(date_format(col("DT_PREVISTO"), "yyyyMMdd").cast("int"))).\
        withColumn('NUM_VALOR', when(col('NUM_LINHA') > 1, col('DT_PREVISTO')).otherwise(col('NUM_VALOR')))


# COMMAND ----------

df_producao_diaria = df_producao.withColumnRenamed('SK_LINHA_PRODUCAO', 'SK_LINHA_PRODUCAO_LMS')\
    .groupBy('SK_LINHA_PRODUCAO_LMS', 'SK_DATA')\
    .agg(sum('VLR_PRODUCAO_BRUTA').alias('PRODUCAO'))

# COMMAND ----------

# Mantém previsões somente em dias em que houve produção

df_calendario_cip_rotina_filtrado = df_calendario_cip_rotina.join(df_producao_diaria, ['SK_DATA','SK_LINHA_PRODUCAO_LMS'], how = 'left')\
  .withColumn('FLAG_PRODUCAO', 
              when(coalesce(col('PRODUCAO'), lit(0)) > 0, lit(True))
              .otherwise(lit(False)))\
  .\
  filter(~((col('NUM_LINHA') > 1) & ~col('FLAG_PRODUCAO'))) # Filtra os casos em que NUM_LINHA > 1 (é uma previsão) e FLAG_PRODUCAO é false (não tem produção no dia)


  ## TO DO: DEIXAR DE OLHAR SOMENTE O DIA

# COMMAND ----------

# Faz um left_anti join entre trocas de produto que exigem CIP e CIPs realizados, ficando trocas que não tiveram CIP feito
# Faz então união com CIPS previstos e obtém lista final de CIPS e previsões
df_trocas_sem_cip = (
    df_troca_cip_rotina
    .alias('trocas')
    .join(
        df_calendario_cip_rotina_filtrado
        .filter(col('DESC_ETAPA') == 'Previsto e Realizado')
        .alias('previsao'),
        on=[
            col('trocas.SK_LINHA_PRODUCAO') == col('previsao.SK_LINHA_PRODUCAO'),
            col('previsao.NUM_VALOR').cast('timestamp') >= col('trocas.ts_producao_inicio_troca'),
            col('previsao.NUM_VALOR').cast('timestamp') <= col('trocas.ts_producao_final_troca'),
        ],
        how='left_anti'
    )
    .select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LOTE'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('ts_producao_final_troca').alias('NUM_VALOR'), 
        concat(
            col('DESC_ETAPA'),
            lit(' - Requer CIP entre '),
            date_format(col('ts_producao_inicio_troca'), 'dd/MM/yy HH:mm'),
            lit(' e '),
            date_format(col('ts_producao_final_troca'), 'dd/MM/yy HH:mm')
        ).alias('DESC_ETAPA'),  
        lit('CIP').alias('DESC_TIPO_LIMPEZA') 
    )
)


df_trocas_e_realizados = df_trocas_sem_cip.unionByName(df_calendario_cip_rotina_filtrado.select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LOTE'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('NUM_VALOR'), 
        col('DESC_ETAPA'),  
        lit('CIP').alias('DESC_TIPO_LIMPEZA')))

# COMMAND ----------

# Join entre CIPs realizados e pendências por trecho sem produção, se houver CIP realizado marca a coluna FLAG_CIP_FEITO 

tempo_cip_trecho_sem_producao = parametros['tempo_cip_trecho_sem_producao']

df_trecho_sem_pb = df_pendencias_producao.alias('main').join(
    df_cips.alias('cip'), 
    (col('main.SK_LINHA_PRODUCAO_LMS') == col('cip.SK_LINHA_PRODUCAO_LMS')) &
    (col('main.DESC_ETAPA') == 'Fim de trecho sem producao') &
    (col('cip.NUM_VALOR') >= col('main.DATA_REF_INICIO_TRECHO') - expr(f"INTERVAL 2 HOURS")) &
    (col('cip.NUM_VALOR') <= col('main.NUM_VALOR') + expr(f"INTERVAL 2 HOURS")), # tolerância de duas horas para caso de flutuação do LMS
    'left_anti'
).select(
    col('SK_LINHA_PRODUCAO_LMS'),
    col('NUM_VALOR'),
    concat(
        col('DESC_ETAPA'), 
        lit(' - Requer CIP entre '),
        date_format(col('DATA_REF_INICIO_TRECHO'), 'dd/MM/yy HH:mm'),
        lit(' e '),
        date_format(col('NUM_VALOR'), 'dd/MM/yy HH:mm')
    ).alias('DESC_ETAPA'),
    lit('CIP').alias('DESC_TIPO_LIMPEZA')
)

# COMMAND ----------

# CIPs, trocas e pendências por trecho sem produção bruta > 8h (já classificadas se foram atendidas ou não)

df_cip_rotina = df_trecho_sem_pb.unionByName(df_trocas_e_realizados, allowMissingColumns=True)

# COMMAND ----------

window = Window.partitionBy('SK_LINHA_PRODUCAO_LMS')
df_etapas_cip = (
    df_cip_rotina
    .withColumn('SK_UNIDADE', first('SK_UNIDADE', ignorenulls=True).over(window))
    .withColumn('SK_LINHA_PRODUCAO', first('SK_LINHA_PRODUCAO', ignorenulls=True).over(window))
    .fillna(-1)
    .withColumn('SK_ANOMES', date_format(col('NUM_VALOR'), 'yyyyMM').cast('int'))
).filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento'])


# COMMAND ----------

# MAGIC %md
# MAGIC # Scrubbing
# MAGIC Três regras principais para o cálculo de previstos:
# MAGIC - Frequência de 7 dias (tolerância de 12h) [_Se o vencimento ocorrer durante trecho sem produção, deve ser solicitado Scrubbing ao retornar a produção na linha_].
# MAGIC - Trocas de produto que exigem scrubbing
# MAGIC

# COMMAND ----------

# Scrubbings realizados 

# Define a janela por SK_LOTE, ordenando do maior para o menor NUM_VALOR
window_lote = Window.partitionBy('SK_LOTE').orderBy(col('NUM_VALOR').desc())
# Isso é pra no caso de 2 enchedoras na linha, considerar somente um horário de início

# Aplica o filtro, seleciona os campos e classifica
df_scrubbings = df_coletas_athena_3_meses_filtrado \
    .filter(col('SK_ITEM_PROCESSO').isin(parametros['inicio_scrubbing'])) \
    .select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LOTE'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_VALOR'),
        lit('Início de Scrubbing').alias('DESC_ETAPA')
    )

# COMMAND ----------

# Previsão de Scrubbings semanal

validade_scrubbing_hrs = int(parametros['vencimento_scrubbing'][0]*24) # Validade do Scrubbing
interval_expr = f"INTERVAL {validade_scrubbing_hrs} HOURS + INTERVAL 1 SECOND"
wd_partition = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy('SK_LINHA_PRODUCAO', 'NUM_VALOR') 

df_scrubbings_previsao = df_scrubbings.withColumn(
  'DT_RANGE_PREVISTO',
  # Primeiro cenário: existe próximo scrubbing
  when((lead(col('NUM_VALOR')).over(wd_partition).isNotNull()),
    sequence(
      least(col("NUM_VALOR"), lead(col("NUM_VALOR")).over(wd_partition)),
      greatest(col("NUM_VALOR"), lead(col("NUM_VALOR")).over(wd_partition)),
      expr(interval_expr)
    )
  ).otherwise(
    # Segundo cenário: último scrubbing da série e passou da validade
    when(
      ((unix_timestamp(current_timestamp()) - unix_timestamp(col("NUM_VALOR"))) / 3600 > validade_scrubbing_hrs),
      sequence(
        col('NUM_VALOR'),
        current_timestamp(),
        expr(interval_expr)
      )
    ).otherwise(array(col('NUM_VALOR')))
  )
).withColumn('DT_PREVISTO',
  explode(col('DT_RANGE_PREVISTO'))
).drop('DT_RANGE_PREVISTO').drop_duplicates()


wd_partition = Window.partitionBy('SK_LINHA_PRODUCAO', 'NUM_VALOR').orderBy(asc('DT_PREVISTO')) 
df_calendario_scrubbing = df_scrubbings_previsao.withColumn('NUM_LINHA', row_number().over(wd_partition)).\
  withColumn(
    'DESC_ETAPA', 
    when(col('NUM_LINHA') == 1, lit('Previsto e Realizado'))
    .otherwise(
        concat(
            lit('Previsto e Não Realizado - Referente ao lote '),
            col('NUM_EXTERNO_LOTE').cast('string')
        )
    )).\
    withColumn('DESC_TIPO_LIMPEZA', lit('Scrubbing')).\
        withColumn('NUM_VALOR', when(col('NUM_LINHA') > 1, col('DT_PREVISTO')).otherwise(col('NUM_VALOR')))


# COMMAND ----------


scrubbing = df_calendario_scrubbing.alias('scrubbing')
producao = df_producao_final_trechos.alias('producao')

# Realiza o join entre scrubbing e produção
# Isso é feito pra verificar as previsões que ocorrem em períodos sem produção
joined = scrubbing.join(
    producao,
    on=(
        (col('scrubbing.SK_LINHA_PRODUCAO_LMS') == col('producao.SK_LINHA_PRODUCAO')) &
        (col('scrubbing.NUM_VALOR') >= col('producao.DATA_REF_INICIO_TRECHO')) &
        (col('scrubbing.NUM_VALOR') <= col('producao.NUM_VALOR'))
    ),
    how='left'
    )

# Janela para verificar qual NUM_LINHA tem o maior NUM_VALOR por grupo
# Identifica se o scrubbing mais recente (com maior NUM_VALOR) é uma previsão ou um CIP
# Ou seja, identifica a linha teve scrubbing antes de retornar ou não
w_check = Window.partitionBy(
    col('scrubbing.SK_LINHA_PRODUCAO_LMS'),
    col('producao.DATA_REF_INICIO_TRECHO')
).orderBy(col('scrubbing.NUM_VALOR').desc())

# Adiciona NUM_LINHA_MAX
joined_check = joined.withColumn(
    "NUM_LINHA_MAX",
    first("scrubbing.NUM_LINHA").over(w_check)
)

# Cria a flag para indicar se deve ser descartado
df_com_flag = joined_check.withColumn(
    "FLAG_DESCARTAR",
    when(
        isnull(col("producao.DATA_REF_INICIO_TRECHO")),  # sem match na produção >> manter
        False
    ).when(
        (col("scrubbing.NUM_LINHA") > 1) & (col("NUM_LINHA_MAX") != col("scrubbing.NUM_LINHA")),  # scrubbing antes do retorno da linha
        True
    ).otherwise(False)
)

# Cria a coluna NUM_VALOR_FINAL
df_final = df_com_flag.withColumn(
    "NUM_VALOR_FINAL",
    when(
        (col("scrubbing.NUM_LINHA") > 1) & col("producao.NUM_VALOR").isNotNull(),
        col("producao.NUM_VALOR")
    ).otherwise(col("scrubbing.NUM_VALOR"))
).filter(~col('FLAG_DESCARTAR')) # Nessa etapa atualiza o horário da previsão 

# Seleciona apenas as colunas do scrubbing, substituindo NUM_VALOR por NUM_VALOR_FINAL
colunas_scrubbing = [c for c in df_com_flag.columns if c.startswith('scrubbing.')]

df_previsao_scrubbing = df_final.select('SK_UNIDADE',
                'scrubbing.SK_LINHA_PRODUCAO_LMS',
                'scrubbing.SK_LINHA_PRODUCAO',
                'SK_LOTE',
                'NUM_EXTERNO_LOTE',
                'SK_RESULTANTE',
                'SK_ITEM_PROCESSO',
                col('NUM_VALOR_FINAL').alias('NUM_VALOR'),
                'SK_ETAPA_PROCESSO',
                when((col('scrubbing.DESC_ETAPA') == 'Previsto e Não Realizado') & (col('producao.NUM_VALOR').isNotNull()), concat(col('scrubbing.DESC_ETAPA'), lit(' - '), col('producao.DESC_ETAPA'))).otherwise(col('scrubbing.DESC_ETAPA')).alias('DESC_ETAPA'),
                'DESC_TIPO_LIMPEZA'
                )

# COMMAND ----------

# Faz um left_anti join entre trocas de produto que exigem Scrubbing e Scrubbings realizados, ficando trocas que não tiveram Scrubbing feito
# Faz então união com Scrubbing previstos e obtém lista final de Scrubbings e previsões
df_trocas_sem_scrubbing = (
    df_troca_produto_scrubbing
    .alias('trocas')
    .join(
        df_previsao_scrubbing
        .filter(col('DESC_ETAPA') == 'Previsto e Realizado')
        .alias('previsao'),
        on=[
            col('trocas.SK_LINHA_PRODUCAO') == col('previsao.SK_LINHA_PRODUCAO'),
            col('previsao.NUM_VALOR').cast('timestamp') >= col('trocas.ts_producao_inicio_troca'),
            col('previsao.NUM_VALOR').cast('timestamp') <= col('trocas.ts_producao_final_troca'),
        ],
        how='left_anti'
    )
    .select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LOTE'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('ts_producao_final_troca').alias('NUM_VALOR'), 
        concat(
            col('DESC_ETAPA'),
            lit(' - Requer Scrubbing entre '),
            date_format(col('ts_producao_inicio_troca'), 'dd/MM/yy HH:mm'),
            lit(' e '),
            date_format(col('ts_producao_final_troca'), 'dd/MM/yy HH:mm')
        ).alias('DESC_ETAPA'),  
        lit('Scrubbing').alias('DESC_TIPO_LIMPEZA') 
    )
)

df_trocas_e_realizados_scrubbing = df_trocas_sem_scrubbing.unionByName(df_previsao_scrubbing.select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LOTE'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('NUM_VALOR'), 
        col('DESC_ETAPA'),  
        lit('Scrubbing').alias('DESC_TIPO_LIMPEZA')))

# COMMAND ----------

window = Window.partitionBy('SK_LINHA_PRODUCAO_LMS')
df_etapas_scrubbing = (
    df_trocas_e_realizados_scrubbing
    .fillna(-1)
    .withColumn('SK_ANOMES', date_format(col('NUM_VALOR'), 'yyyyMM').cast('int'))
).filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento'])


# COMMAND ----------

# MAGIC %md
# MAGIC # Desinfecção
# MAGIC Duas regras principais para o cálculo de previstos:
# MAGIC - Parada superior a 2h (cerveja) ou 4h (refri)
# MAGIC - Trocas de produto que exigem desinfecção
# MAGIC

# COMMAND ----------

# Classifica por SK_PRODUTO se é refri ou não 
df_indicador_refri = (
    df_coletas_athena_3_meses_filtrado
    .select('SK_RESULTANTE', 'SK_ITEM_PROCESSO')
    .join(
        dim_produto_lms, 
        on=(col('SK_RESULTANTE') == col('SK_RESULTANTE_ATHENA')), 
        how='inner'
    )
    .withColumn(
        'FLAG_REFRI',
        when(col('SK_ITEM_PROCESSO').isin(parametros['producao_refri']), lit(1)).otherwise(lit(0))
    )
    .select('SK_PRODUTO', 'FLAG_REFRI')
    .distinct()
)

# Broadcast join para adicionar FLAG_REFRI na fato_paradas_filtrada
fato_paradas_filtrada_produto = (
    fato_paradas_filtrada
    .join(
        broadcast(df_indicador_refri),
        on='SK_PRODUTO',
        how='left'  # left join para manter todos os registros de paradas
    )
    .fillna({'FLAG_REFRI': 0})  # Preenche com 0 se não encontrar o produto
)


# COMMAND ----------

df_fim_paradas = fato_paradas_filtrada_produto\
    .withColumn('DESC_ETAPA', lit('Fim de parada')).select('SK_LINHA_PRODUCAO', 'FLAG_REFRI', col('DAT_FIM_PARADA').alias('NUM_VALOR'), 'DESC_ETAPA', col('DAT_INICIO_PARADA').alias('DATA_REF_INICIO_TRECHO'))

df_paradas_trechos_desin = df_fim_paradas.unionByName(df_producao_final_trechos.drop('SK_DATA'), allowMissingColumns = True)

wd_paradas = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy('DATA_REF_INICIO_TRECHO')
max_dt_final = max("NUM_VALOR").over(wd_paradas)
aux_grupo = when((unix_timestamp(lag(max_dt_final).over(wd_paradas)) - unix_timestamp(col('DATA_REF_INICIO_TRECHO'))) < 0, 1).otherwise(0)

df_paradas_trechos_desin = (df_paradas_trechos_desin
    .withColumn("AUX_GRUPO", sum(aux_grupo).over(wd_paradas))
    .groupBy('SK_LINHA_PRODUCAO', 'FLAG_REFRI', "AUX_GRUPO")
    .agg(first("DATA_REF_INICIO_TRECHO").alias("DATA_REF_INICIO_TRECHO"), 
         max("NUM_VALOR").alias("NUM_VALOR"), 
         first('DESC_ETAPA').alias('DESC_ETAPA'))
    .drop("AUX_GRUPO")
).withColumn('QTD_MINUTOS_PARADA', (unix_timestamp(col('NUM_VALOR')) - unix_timestamp(col('DATA_REF_INICIO_TRECHO'))) / 60)\
.filter(col('QTD_MINUTOS_PARADA') <= 120)

aux_grupo = when((unix_timestamp(lag(max_dt_final).over(wd_paradas)) - unix_timestamp(col('DATA_REF_INICIO_TRECHO'))) < - parametros['tolerancia_paradas'][0] * 60, 1).otherwise(0)

df_paradas_trechos_desin = (df_paradas_trechos_desin
    .withColumn("AUX_GRUPO", sum(aux_grupo).over(wd_paradas))
    .groupBy('SK_LINHA_PRODUCAO', 'FLAG_REFRI', "AUX_GRUPO")
    .agg(first("DATA_REF_INICIO_TRECHO").alias("DATA_REF_INICIO_TRECHO"), 
         max("NUM_VALOR").alias("NUM_VALOR"), 
         first('DESC_ETAPA').alias('DESC_ETAPA'))
    .drop("AUX_GRUPO")
).withColumn('QTD_MINUTOS_PARADA', (unix_timestamp(col('NUM_VALOR')) - unix_timestamp(col('DATA_REF_INICIO_TRECHO'))) / 60)

pendencias_desin_paradas = df_paradas_trechos_desin.filter(
  ((col('QTD_MINUTOS_PARADA') > 120) & (col('FLAG_REFRI') == 0))    # parada de 120min + e roda cerveja ou chopp
| ((col('QTD_MINUTOS_PARADA') > 240) & (col('FLAG_REFRI') == 1))    # parada de 240min + e roda cerveja ou chopp
).filter(col('QTD_MINUTOS_PARADA') < 480) # acima disso só aceita CIP

# COMMAND ----------

# Desinfecções e CIPs realizados 

# Define a janela por SK_LOTE, ordenando pelo maior NUM_VALOR
window_lote = Window.partitionBy('SK_LOTE').orderBy(col('NUM_VALOR').desc())
# Isso é pra no caso de 2 enchedoras na linha, considerar somente um horário de CIP

# Filtra e mantém apenas o maior NUM_VALOR por lote
df_desin = df_coletas_athena_3_meses_filtrado \
    .filter(col('SK_ITEM_PROCESSO').isin(set(parametros['inicio_cip'] + parametros['inicio_desinfeccao_cerveja']))) \
    .select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LOTE'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_VALOR'),
        lit(when(col('SK_ITEM_PROCESSO').isin(parametros['inicio_cip']), lit('Início de CIP')).otherwise('Previsto e Realizado')).alias('DESC_ETAPA')
    )

# COMMAND ----------

# Filtra por paradas que não tiveram CIP ou Sanitização realizado

df_final_paradas_desin = pendencias_desin_paradas.alias('paradas').join(df_desin.alias('desin'),
                                                       on = (
                                                         (col('paradas.SK_LINHA_PRODUCAO') == col('desin.SK_LINHA_PRODUCAO_LMS')) &
                                                         (col('desin.NUM_VALOR') >= col('paradas.DATA_REF_INICIO_TRECHO') - expr('INTERVAL 1 HOUR')) &
                                                         (col('desin.NUM_VALOR') <= col('paradas.NUM_VALOR') + expr('INTERVAL 1 HOUR'))
                                                       ), how = 'left_anti').select(
                                                         col('paradas.SK_LINHA_PRODUCAO').alias("SK_LINHA_PRODUCAO_LMS"),
                                                         col('NUM_VALOR'),
                                                         concat(col('paradas.DESC_ETAPA'), lit(' - Requer Sanitização entre '), date_format(col('paradas.DATA_REF_INICIO_TRECHO'), 'dd/MM/yy HH:mm'), lit(' e '), date_format(col('paradas.NUM_VALOR'), 'dd/MM/yy HH:mm')
                                                       ).alias('DESC_ETAPA'))


# Filtra por trocas que não tiveram CIP ou Sanitização realizado

df_final_trocas_desin = df_troca_produto_desin.alias('troca').join(df_desin.alias('desin'),
                                                       on = (
                                                         (col('troca.SK_LINHA_PRODUCAO_LMS') == col('desin.SK_LINHA_PRODUCAO_LMS')) &
                                                         (col('desin.NUM_VALOR') >= col('troca.ts_producao_inicio_troca') - expr('INTERVAL 1 HOUR')) &
                                                         (col('desin.NUM_VALOR') <= col('troca.ts_producao_final_troca') + expr('INTERVAL 1 HOUR'))
                                                       ), how = 'left_anti').select(
                                                         col('troca.SK_LINHA_PRODUCAO'),
                                                         col('troca.SK_LINHA_PRODUCAO_LMS'),
                                                         col('troca.ts_producao_final_troca').alias('NUM_VALOR'),
                                                         col('troca.PROX_SK_RESULTANTE').alias('SK_RESULTANTE'),
                                                         col('troca.SK_LOTE'),
                                                         col('troca.SK_ITEM_PROCESSO'),
                                                         col('troca.SK_ETAPA_PROCESSO'), 
                                                         concat(col('troca.DESC_ETAPA'), lit(' - Requer Sanitização entre '), date_format(col('troca.ts_producao_inicio_troca'), 'dd/MM/yy HH:mm'), lit(' e '), date_format(col('troca.ts_producao_final_troca'), 'dd/MM/yy HH:mm')
                                                       ).alias('DESC_ETAPA'))

df_final_desin = df_desin.unionByName(df_final_trocas_desin, allowMissingColumns = True).unionByName(df_final_paradas_desin, allowMissingColumns = True)


# COMMAND ----------

window = Window.partitionBy('SK_LINHA_PRODUCAO_LMS')
df_etapas_desin = (
    df_final_desin
    .withColumn('SK_UNIDADE', first('SK_UNIDADE', ignorenulls=True).over(window))
    .withColumn('SK_LINHA_PRODUCAO', first('SK_LINHA_PRODUCAO', ignorenulls=True).over(window))
    .fillna(-1)
    .withColumn('SK_ANOMES', date_format(col('NUM_VALOR'), 'yyyyMM').cast('int'))
).filter(~col('SK_ITEM_PROCESSO').isin(parametros['inicio_cip']))\
.filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento']).withColumn('DESC_TIPO_LIMPEZA', lit('Sanitização'))


# COMMAND ----------

# MAGIC %md
# MAGIC # Assepsia Externa
# MAGIC Duas regras principais para o cálculo de previstos:
# MAGIC - Parada superior a 40min
# MAGIC - Trocas de produto que exigem desinfecção
# MAGIC

# COMMAND ----------

# Assepias realizadas 

# Define a janela por SK_LOTE, ordenando do maior para o menor NUM_VALOR
window_lote = Window.partitionBy('SK_LOTE').orderBy(col('NUM_VALOR').desc())
# Isso é pra no caso de 2 enchedoras na linha, considerar somente um horário de início

# Aplica o filtro, seleciona os campos e classifica
df_assepsias = df_coletas_athena_3_meses_filtrado \
    .filter(col('SK_ITEM_PROCESSO').isin(set(parametros['inicio_assepsia_externa'] + parametros['inicio_scrubbing']))) \
    .select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LOTE'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_VALOR'),
        lit('Início de Scrubbing').alias('DESC_ETAPA')
    ) 

# COMMAND ----------

# Previsão de Assepaias diárias

validade_assepsia_hrs = int(parametros['vencimento_assepsia'][0]*24) # Validade da assepsia
interval_expr = f"INTERVAL {validade_assepsia_hrs} HOURS"
wd_partition = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy('SK_LINHA_PRODUCAO', 'NUM_VALOR') 

df_assepsias_p = df_assepsias.withColumn(
    'NEXT_NUM_VALOR',
    lead(col('NUM_VALOR')).over(wd_partition)
).withColumn(
    'NEXT_NUM_VALOR_AJUSTADO',
    when(
        col('NEXT_NUM_VALOR') == col('NUM_VALOR'),
        col('NEXT_NUM_VALOR') + expr('INTERVAL 1 SECOND')
    ).otherwise(col('NEXT_NUM_VALOR')))

df_assepsias_previsao = df_assepsias_p.withColumn(
  'DT_RANGE_PREVISTO',
  # Primeiro cenário: existe próxima assepsia
  when(col('NEXT_NUM_VALOR_AJUSTADO').isNotNull(),
    sequence(
      least(col("NUM_VALOR"), col("NEXT_NUM_VALOR_AJUSTADO")),
      greatest(col("NUM_VALOR"), col("NEXT_NUM_VALOR_AJUSTADO")) - expr("INTERVAL 1 SECOND"),
      expr(interval_expr)
    )
  ).otherwise(
    # Segundo cenário: última assepsia da série e passou da validade
    when(
      ((unix_timestamp(current_timestamp()) - unix_timestamp(col("NUM_VALOR"))) / 3600 > validade_assepsia_hrs),
      sequence(
        col('NUM_VALOR'),
        current_timestamp(),
        expr(interval_expr)
      )
    ).otherwise(array(col('NUM_VALOR')))
  )
).withColumn('DT_PREVISTO',
  explode(col('DT_RANGE_PREVISTO'))
).drop('DT_RANGE_PREVISTO').drop_duplicates()


wd_partition = Window.partitionBy('SK_LINHA_PRODUCAO', 'NUM_VALOR').orderBy(asc('DT_PREVISTO')) 
df_calendario_assepsias = df_assepsias_previsao.withColumn('NUM_LINHA', row_number().over(wd_partition)).\
  withColumn(
    'DESC_ETAPA', 
    when(col('NUM_LINHA') == 1, lit('Previsto e Realizado'))
    .otherwise(
        concat(
            lit('Previsto e Não Realizado (Assepsia Diária) - Vencimento do lote '),
            col('NUM_EXTERNO_LOTE').cast('string')
        )
    )).\
    withColumn('DESC_TIPO_LIMPEZA', lit('Assepsia Externa')).\
        withColumn('NUM_VALOR', when(col('NUM_LINHA') > 1, col('DT_PREVISTO')).otherwise(col('NUM_VALOR'))).drop('NEXT_NUM_VALOR','NEXT_NUM_VALOR_AJUSTADO')


# COMMAND ----------

df_fim_paradas = fato_paradas_filtrada_produto\
    .withColumn('DESC_ETAPA', lit('Fim de parada')).select('SK_LINHA_PRODUCAO', 'FLAG_REFRI', col('DAT_FIM_PARADA').alias('NUM_VALOR'), 'DESC_ETAPA', col('DAT_INICIO_PARADA').alias('DATA_REF_INICIO_TRECHO'))

#df_paradas_trechos_assepsia = df_fim_paradas.unionByName(df_producao_final_trechos.drop('SK_DATA'), allowMissingColumns = True)

wd_paradas = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy('DATA_REF_INICIO_TRECHO')
max_dt_final = max("NUM_VALOR").over(wd_paradas)
aux_grupo = when((unix_timestamp(lag(max_dt_final).over(wd_paradas)) - unix_timestamp(col('DATA_REF_INICIO_TRECHO'))) < 0, 1).otherwise(0)

df_paradas_trechos_assepsia = (df_fim_paradas
    .withColumn("AUX_GRUPO", sum(aux_grupo).over(wd_paradas))
    .groupBy('SK_LINHA_PRODUCAO', 'FLAG_REFRI', "AUX_GRUPO")
    .agg(first("DATA_REF_INICIO_TRECHO").alias("DATA_REF_INICIO_TRECHO"), 
         max("NUM_VALOR").alias("NUM_VALOR"), 
         first('DESC_ETAPA').alias('DESC_ETAPA'))
    .drop("AUX_GRUPO")
).withColumn('QTD_MINUTOS_PARADA', (unix_timestamp(col('NUM_VALOR')) - unix_timestamp(col('DATA_REF_INICIO_TRECHO'))) / 60)\
.filter(col('QTD_MINUTOS_PARADA') >= 40)

aux_grupo = when((unix_timestamp(lag(max_dt_final).over(wd_paradas)) - unix_timestamp(col('DATA_REF_INICIO_TRECHO'))) < - parametros['tolerancia_paradas'][0] * 60, 1).otherwise(0)

df_paradas_trechos_assepsia = (df_paradas_trechos_assepsia
    .withColumn("AUX_GRUPO", sum(aux_grupo).over(wd_paradas))
    .groupBy('SK_LINHA_PRODUCAO', 'FLAG_REFRI', "AUX_GRUPO")
    .agg(first("DATA_REF_INICIO_TRECHO").alias("DATA_REF_INICIO_TRECHO"), 
         max("NUM_VALOR").alias("NUM_VALOR"), 
         first('DESC_ETAPA').alias('DESC_ETAPA'))
    .drop("AUX_GRUPO")
)

# COMMAND ----------


assepsia = df_calendario_assepsias.alias('assepsia')
producao = df_producao_final_trechos.alias('producao')

# Realiza o join entre assepsia e produção
# Isso é feito pra verificar as previsões que ocorrem em períodos sem produção
joined = assepsia.join(
    producao,
    on=(
        (col('assepsia.SK_LINHA_PRODUCAO_LMS') == col('producao.SK_LINHA_PRODUCAO')) &
        (col('assepsia.NUM_VALOR') >= col('producao.DATA_REF_INICIO_TRECHO')) &
        (col('assepsia.NUM_VALOR') <= col('producao.NUM_VALOR') + expr(f"INTERVAL 1 HOURS"))
    ),
    how='left'
    )

# Janela para verificar qual NUM_LINHA tem o maior NUM_VALOR por grupo
# Identifica se o assepsia mais recente (com maior NUM_VALOR) é uma previsão ou um CIP
# Ou seja, identifica a linha teve assepsia antes de retornar ou não
w_check = Window.partitionBy(
    col('assepsia.SK_LINHA_PRODUCAO_LMS'),
    col('producao.DATA_REF_INICIO_TRECHO')
).orderBy(col('assepsia.NUM_VALOR').desc())

# Adiciona NUM_LINHA_MAX
joined_check = joined.withColumn(
    "NUM_LINHA_MAX",
    first("assepsia.NUM_LINHA").over(w_check)
)

# Cria a flag para indicar se deve ser descartado
df_com_flag = joined_check.withColumn(
    "FLAG_DESCARTAR",
    when(
        isnull(col("producao.DATA_REF_INICIO_TRECHO")),  # sem match na produção >> manter
        False
    ).when(
        (col("assepsia.NUM_LINHA") > 1) & (col("NUM_LINHA_MAX") != col("assepsia.NUM_LINHA")),  # assepsia antes do retorno da linha
        True
    ).otherwise(False)
)


# Cria a coluna NUM_VALOR_FINAL
df_final = df_com_flag.withColumn(
    "NUM_VALOR_FINAL",
    when(
        ((col("assepsia.NUM_LINHA") > 1) & col("producao.NUM_VALOR").isNotNull()) # quando é uma previsão e cai em trecho sem produção
        ,
        col("producao.NUM_VALOR") # a previsão é agora no final do trecho sem produção, quando a linha retorna
    ).otherwise(col("assepsia.NUM_VALOR"))
).filter(~col('FLAG_DESCARTAR')) # Nessa etapa atualiza o horário da previsão 

# Seleciona apenas as colunas do assepsia, substituindo NUM_VALOR por NUM_VALOR_FINAL
colunas_assepsia = [c for c in df_com_flag.columns if c.startswith('assepsia.')]

df_final_assepsia_diaria_unica = df_final.withColumn('SK_DATA', to_date(col('NUM_VALOR_FINAL')))

# Janela para particionar por SK_DATA e ordenar por NUM_VALOR_FINAL desc
w_assepsia_diaria = Window.partitionBy('assepsia.SK_LINHA_PRODUCAO_LMS', 'SK_DATA').orderBy(col('NUM_VALOR_FINAL').desc())

# Adiciona row_number para identificar o registro com maior NUM_VALOR_FINAL por dia
df_com_rank = df_final_assepsia_diaria_unica.withColumn(
    'rn_assepsia_diaria',
    when(
        col('assepsia.DESC_ETAPA').contains('Assepsia Diária'),
        row_number().over(w_assepsia_diaria)
    ).otherwise(lit(1))  # Para não-assepsia diária, sempre mantém (rn=1)
)

# Filtra: mantém todos os não-assepsia diária + apenas o primeiro (maior NUM_VALOR) da assepsia diária por dia
# Dessa forma só é cobrada a assepsia diária 1x ao dia, independente dos trechos sem produção. Pendências por parada continuam
df_final_filtrado = df_com_rank.filter(col('rn_assepsia_diaria') == 1).drop('rn_assepsia_diaria')


df_previsao_assepsia = df_final_filtrado.select('SK_UNIDADE',
                'assepsia.SK_LINHA_PRODUCAO_LMS',
                'assepsia.SK_LINHA_PRODUCAO',
                'SK_LOTE',
                'NUM_EXTERNO_LOTE',
                'SK_RESULTANTE',
                'SK_ITEM_PROCESSO',
                col('NUM_VALOR_FINAL').alias('NUM_VALOR'),
                'SK_ETAPA_PROCESSO',
                when((col('assepsia.DESC_ETAPA').contains('Previsto e Não Realizado')) & (col('producao.NUM_VALOR').isNotNull()), concat(col('assepsia.DESC_ETAPA'), lit(' - Requer Assepsia Externa antes do retorno da linha em '), date_format(col('producao.NUM_VALOR'), 'dd/MM/yy HH:mm'))).otherwise(col('assepsia.DESC_ETAPA')).alias('DESC_ETAPA'),
                'DESC_TIPO_LIMPEZA'
                )

# COMMAND ----------

df_trocas_sem_assepsia = (
    df_troca_produto_assepsia
    .alias('trocas')
    .join(
        df_previsao_assepsia
        .filter(col('DESC_ETAPA') == 'Previsto e Realizado')
        .alias('previsao'),
        on=[
            col('trocas.SK_LINHA_PRODUCAO') == col('previsao.SK_LINHA_PRODUCAO'),
            col('previsao.NUM_VALOR').cast('timestamp') >= (col('trocas.ts_producao_inicio_troca') - expr('INTERVAL 1 HOUR')),
            col('previsao.NUM_VALOR').cast('timestamp') <= (col('trocas.ts_producao_final_troca') + expr('INTERVAL 1 HOUR'))
        ],
        how='left_anti'
    )
    .select(
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LINHA_PRODUCAO_LMS'),
        col('SK_LOTE'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('ts_producao_final_troca').alias('NUM_VALOR'), 
        concat(
            col('DESC_ETAPA'),
            lit(' - Requer Assepsia Externa entre '),
            date_format(col('ts_producao_inicio_troca'), 'dd/MM/yy HH:mm'),
            lit(' e '),
            date_format(col('ts_producao_final_troca'), 'dd/MM/yy HH:mm')
        ).alias('DESC_ETAPA'),  
        lit('Assepsia Externa').alias('DESC_TIPO_LIMPEZA') 
    )
)

# COMMAND ----------

# Filtra por paradas que não tiveram CIP ou Sanitização realizado

df_final_paradas_assepsia = df_paradas_trechos_assepsia.alias('paradas').join(df_assepsias.alias('assepsia'),
                                                       on = (
                                                         (col('paradas.SK_LINHA_PRODUCAO') == col('assepsia.SK_LINHA_PRODUCAO_LMS')) &
                                                         (col('assepsia.NUM_VALOR') >= col('paradas.DATA_REF_INICIO_TRECHO') - expr('INTERVAL 6 HOUR')) &
                                                         (col('assepsia.NUM_VALOR') <= col('paradas.NUM_VALOR'))
                                                       ), how = 'left_anti').select(
                                                         col('paradas.SK_LINHA_PRODUCAO').alias("SK_LINHA_PRODUCAO_LMS"),
                                                         col('NUM_VALOR'),
                                                         concat(col('paradas.DESC_ETAPA'), lit(' - Requer Assepsia Externa entre '), date_format(col('paradas.DATA_REF_INICIO_TRECHO'), 'dd/MM/yy HH:mm'), lit(' e '), date_format(col('paradas.NUM_VALOR'), 'dd/MM/yy HH:mm')
                                                       ).alias('DESC_ETAPA'))


# Filtra por trocas que não tiveram CIP ou Sanitização realizado

df_final_trocas_assepsia = df_troca_produto_assepsia.alias('troca').join(df_assepsias.alias('assepsia'),
                                                       on = (
                                                         (col('troca.SK_LINHA_PRODUCAO_LMS') == col('assepsia.SK_LINHA_PRODUCAO_LMS')) &
                                                         (col('assepsia.NUM_VALOR') >= col('troca.ts_producao_inicio_troca') - expr('INTERVAL 1 HOUR')) &
                                                         (col('assepsia.NUM_VALOR') <= col('troca.ts_producao_final_troca') + expr('INTERVAL 1 HOUR'))
                                                       ), how = 'left_anti').select(df_assepsias.columns)

df_final_assepsia = df_previsao_assepsia.unionByName(df_final_trocas_assepsia, allowMissingColumns = True).unionByName(df_final_paradas_assepsia, allowMissingColumns = True)


# COMMAND ----------

window = Window.partitionBy('SK_LINHA_PRODUCAO_LMS')
df_etapas_assepsia = (
    df_final_assepsia
    .withColumn('SK_UNIDADE', first('SK_UNIDADE', ignorenulls=True).over(window))
    .withColumn('SK_LINHA_PRODUCAO', first('SK_LINHA_PRODUCAO', ignorenulls=True).over(window))
    .fillna(-1)
    .withColumn('SK_ANOMES', date_format(col('NUM_VALOR'), 'yyyyMM').cast('int'))
    .withColumn('DESC_TIPO_LIMPEZA', lit('Assepsia Externa'))
).filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento'])


# COMMAND ----------

df_etapas = df_etapas_cip.unionByName(df_etapas_desin).unionByName(df_etapas_assepsia).unionByName(df_etapas_scrubbing)

# COMMAND ----------

def consolidador(dados, resultante, parametros):
    # Janela de partição
    wd_partition = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy(asc('NUM_VALOR'))

    # Calcular tempo entre CIPs
    dados_tempo_ultimo_cip = dados.drop_duplicates()\
        .filter(dados.SK_ITEM_PROCESSO.isin(parametros['inicio_cip_' + resultante]))\
        .filter(col('NUM_VALOR').isNotNull())\
        .withColumn('TEMPO_PROX_CIP', (lead(col('NUM_VALOR')).over(wd_partition) - col('NUM_VALOR')).cast('long') / 3600)
    

    # Criar sequência de datas previstas entre os CIPs realizados, 
    # se for o último CIP cria previsão até a data atual
    df_dt_previsto = dados_tempo_ultimo_cip.withColumn(
        "DT_RANGE_PREVISTO",
        when(
            # Primeiro cenário: Há próximo CIP 
            (lead(col("NUM_VALOR")).over(wd_partition).isNotNull()),
            sequence(
                least(col("NUM_VALOR"), lead(col("NUM_VALOR")).over(wd_partition)),
                greatest(col("NUM_VALOR"), lead(col("NUM_VALOR")).over(wd_partition)),
                expr(f"INTERVAL {int(parametros['vencimento_cip_' + resultante][0])} DAYS")
            )
        ).otherwise(
            when(
                # Segundo cenário: Último CIP da série e PASSOU DA VALIDADE
                (datediff(current_date(), col("NUM_VALOR")) > int(parametros[f'vencimento_cip_' + resultante][0])),
                sequence(
                    col("NUM_VALOR"),
                    current_date(),
                    expr(f"INTERVAL {int(parametros['vencimento_cip_' + resultante][0])} DAYS")
                )
            ).otherwise(array(col("NUM_VALOR")))  # Caso o CIP não tenha vencido ainda
        )
    )

    # Explode as datas para várias linhas
    df_calendario = df_dt_previsto.withColumn(
        "DT_PREVISTO",
        explode(col("DT_RANGE_PREVISTO"))
    ).drop("DT_RANGE_PREVISTO").withColumn('SK_ANOMES', 
        year(col("DT_PREVISTO")) * 100 + month(col("DT_PREVISTO"))).drop('NUM_LINHA')



    # Janela de partição
    wd_partition_previsto = Window.partitionBy('SK_LINHA_PRODUCAO').orderBy(asc('DT_PREVISTO'))
    wd_partition_previsto_calendario = Window.partitionBy('SK_LINHA_PRODUCAO', 'NUM_VALOR').orderBy(asc('DT_PREVISTO'))

    # Numeração de linhas para o range de transbordo
    df_calendario = df_calendario.withColumn('NUM_LINHA', row_number().over(wd_partition_previsto_calendario))
    
    wd_partition_por_mes = Window.partitionBy('SK_LINHA_PRODUCAO', 'SK_ANOMES').orderBy(asc('DT_PREVISTO'))
   
    df_etapas = df_calendario

    
    # Calcula o número de CIPs previstos
    df_previstos = df_calendario.groupBy('SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_ANOMES').agg(
        count('SK_LINHA_PRODUCAO').alias(f'QTD_PREVISTO_{resultante.upper()}')
    )

    # Calcula o número de CIPs realizados e CIPs fora do prazo
    df_realizados = dados_tempo_ultimo_cip.withColumn(
        'TEMPO_ANTERIOR_CIP', 
        (col('NUM_VALOR') - lag(col('NUM_VALOR')).over(wd_partition)).cast('long') / 3600
    ).groupBy('SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_ANOMES').agg(
        count('*').alias(f'QTD_REALIZADO_{resultante.upper()}'),
        sum(
            when(
                col('TEMPO_ANTERIOR_CIP') > int(24 * parametros['vencimento_cip_' + resultante][0]), 
                1
            ).otherwise(0)
        ).alias(f'QTD_FORA_DO_PRAZO_{resultante.upper()}')
    )

    # Consolida as informações
    df_consolidado = df_previstos.join(
        df_realizados,
        on=['SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_ANOMES'],
        how='full'
    ).fillna(0).withColumn(
        f'QTD_PREVISTO_{resultante.upper()}',
        when(
            col(f'QTD_REALIZADO_{resultante.upper()}') > col(f'QTD_PREVISTO_{resultante.upper()}'),
            col(f'QTD_REALIZADO_{resultante.upper()}')
        ).otherwise(col(f'QTD_PREVISTO_{resultante.upper()}'))
    )

    return df_consolidado, df_etapas

# COMMAND ----------

consolidado_periodico, etapas_periodico = consolidador(df_coletas_athena_1_ano_filtrado, 'periodico', parametros)
consolidado_opcional, etapas_opcional = consolidador(df_coletas_athena_1_ano_filtrado, 'opcional', parametros)

# COMMAND ----------

condicao = col('NUM_VALOR') == col('DT_PREVISTO')
    
etapas_periodico_final = (
    etapas_periodico
    .withColumn('DESC_ETAPA', 
        when(condicao, lit('Previsto e Realizado'))
        .otherwise(concat(lit('Previsto e Não Realizado - último CIP no lote '), 
                        col('NUM_EXTERNO_LOTE'))))
    .withColumn('NUM_VALOR', 
        when(~condicao, col('DT_PREVISTO'))
        .otherwise(col('NUM_VALOR')))
    .withColumn('DESC_TIPO_LIMPEZA', lit('CIP Periódico - Packaging'))
    .drop('NUM_VALOR_VOLUME', 'RANK_NO_MES', 'TEMPO_PROX_CIP', 'DT_PREVISTO')
    .select(
        col('SK_LINHA_PRODUCAO_LMS'),
        col('NUM_VALOR'),
        col('DESC_ETAPA'),
        col('DESC_TIPO_LIMPEZA'),
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LOTE'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('SK_ANOMES')
    )
)

etapas_opcional_final = (
    etapas_opcional
    .withColumn('DESC_ETAPA', 
        when(condicao, lit('Previsto e Realizado'))
        .otherwise(concat(lit('Previsto e Não Realizado - último CIP no lote '), 
                        col('NUM_EXTERNO_LOTE'))))
    .withColumn('NUM_VALOR', 
        when(~condicao, col('DT_PREVISTO'))
        .otherwise(col('NUM_VALOR')))
    .withColumn('DESC_TIPO_LIMPEZA', lit('CIP Clorado - Packaging'))
    .drop('NUM_VALOR_VOLUME', 'RANK_NO_MES', 'TEMPO_PROX_CIP', 'DT_PREVISTO')
    .select(
        col('SK_LINHA_PRODUCAO_LMS'),
        col('NUM_VALOR'),
        col('DESC_ETAPA'),
        col('DESC_TIPO_LIMPEZA'),
        col('SK_UNIDADE'),
        col('SK_LINHA_PRODUCAO'),
        col('SK_LOTE'),
        col('SK_ETAPA_PROCESSO'),
        col('NUM_EXTERNO_LOTE'),
        col('SK_RESULTANTE'),
        col('SK_ITEM_PROCESSO'),
        col('SK_ANOMES')
    )
)

# COMMAND ----------


# fato_etapas = (
#     etapas_periodico_final
#     .unionByName(etapas_opcional_final)
#     .unionByName(df_etapas)
# )

fato_etapas = df_etapas

window_spec = (
    Window
    .partitionBy("SK_LINHA_PRODUCAO")
    .orderBy("NUM_VALOR")
)

# Cálculo do tempo entre etapas com formatação em 2 casas decimais
fato_etapas_com_tempo = (
    fato_etapas
    .withColumn(
        "PROXIMO_NUM_VALOR", 
        lead("NUM_VALOR", 1).over(window_spec)
    )
    .withColumn(
        "TEMPO_ENTRE_ETAPAS",
        when(
            col("PROXIMO_NUM_VALOR").isNotNull(),
            round(
                (unix_timestamp("PROXIMO_NUM_VALOR") - unix_timestamp("NUM_VALOR")) / 3600,
                2
            )
        ).otherwise(lit(None))
    )
    .drop("PROXIMO_NUM_VALOR", "SK_LINHA_PRODUCAO_LMS")
)

# COMMAND ----------

df_etapas_final = (
    fato_etapas_com_tempo
    .select(
        '*',
        lit(sk_equipamento_enchedora[0]).alias('SK_EQUIPAMENTO'),
        lit(sk_tipo_equipamento[0]).alias('SK_TIPO_EQUIPAMENTO')
    )
    .filter(
        (col('SK_ANOMES') > parametros_processamento['ano_mes_processamento']) &
        (col('SK_LINHA_PRODUCAO') != -1) &
        (col('SK_UNIDADE') != -1)
    )
    .withColumn('CREATED_DATE_CZ', lit(current_timestamp()))
)


spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

(df_etapas_final
 .write
 .partitionBy('SK_ANOMES')
 .mode('overwrite')
 .format(consume_file_type)
 .save(sink_path_etapas)
)

# COMMAND ----------

# Mapeamento dos tipos para nomes de colunas
tipo_map = {
    "CIP": "ROTINA",
    "Assepsia Externa": "ASSEPSIA", 
    "Scrubbing": "SCRUBBING",
    "Sanitização": "DESIN"
}

# Realizado vs tudo resto = Previsto
df_etapas_classificado = df_etapas.withColumn(
    "CATEGORIA",
    when(col("DESC_ETAPA") == "Previsto e Realizado", lit("REALIZADO"))
    .otherwise(lit("PREVISTO"))  # Qualquer coisa que não seja "Realizado" = PREVISTO
).withColumn(
    "TIPO",
    when(col("DESC_TIPO_LIMPEZA") == "CIP", lit("ROTINA"))
    .when(col("DESC_TIPO_LIMPEZA") == "Assepsia Externa", lit("ASSEPSIA"))
    .when(col("DESC_TIPO_LIMPEZA") == "Scrubbing", lit("SCRUBBING"))
    .when(col("DESC_TIPO_LIMPEZA") == "Sanitização", lit("DESIN"))
    .otherwise(lit("OUTROS"))
)

# Monta a coluna final do pivot
df_etapas_classificado = df_etapas_classificado.withColumn(
    "COLUNA",
    concat_ws("_", col("CATEGORIA"), col("TIPO"))
)


df_counts = df_etapas_classificado.groupBy(
    "SK_UNIDADE", "SK_LINHA_PRODUCAO", "SK_ANOMES"
).agg(
    sum(when(col("COLUNA") == "PREVISTO_ROTINA", 1).otherwise(0)).alias("QTD_PREVISTO_ROTINA"),
    sum(when(col("COLUNA") == "REALIZADO_ROTINA", 1).otherwise(0)).alias("QTD_REALIZADO_ROTINA"),
    sum(when(col("COLUNA") == "PREVISTO_ASSEPSIA", 1).otherwise(0)).alias("QTD_PREVISTO_ASSEPSIA"),
    sum(when(col("COLUNA") == "REALIZADO_ASSEPSIA", 1).otherwise(0)).alias("QTD_REALIZADO_ASSEPSIA"),
    sum(when(col("COLUNA") == "PREVISTO_DESIN", 1).otherwise(0)).alias("QTD_PREVISTO_DESIN"),
    sum(when(col("COLUNA") == "REALIZADO_DESIN", 1).otherwise(0)).alias("QTD_REALIZADO_DESIN"),
    sum(when(col("COLUNA") == "PREVISTO_SCRUBBING", 1).otherwise(0)).alias("QTD_PREVISTO_SCRUBBING"),
    sum(when(col("COLUNA") == "REALIZADO_SCRUBBING", 1).otherwise(0)).alias("QTD_REALIZADO_SCRUBBING"),
).withColumn('QTD_PREVISTO_ROTINA', col('QTD_PREVISTO_ROTINA') + col('QTD_REALIZADO_ROTINA')) \
 .withColumn('QTD_PREVISTO_ASSEPSIA', col('QTD_PREVISTO_ASSEPSIA') + col('QTD_REALIZADO_ASSEPSIA')) \
 .withColumn('QTD_PREVISTO_DESIN', col('QTD_PREVISTO_DESIN') + col('QTD_REALIZADO_DESIN')) \
 .withColumn('QTD_PREVISTO_SCRUBBING', col('QTD_PREVISTO_SCRUBBING') + col('QTD_REALIZADO_SCRUBBING'))

# Preenche com 0 onde não existir
df_counts = df_counts.fillna(0)

# Renomeia no schema padrão 
for tipo, nome in tipo_map.items():
    if f"PREVISTO_{nome}" in df_counts.columns:
        df_counts = df_counts.withColumnRenamed(f"PREVISTO_{nome}", f"QTD_PREVISTO_{nome}")
    if f"REALIZADO_{nome}" in df_counts.columns:
        df_counts = df_counts.withColumnRenamed(f"REALIZADO_{nome}", f"QTD_REALIZADO_{nome}")

df_consolidado_rotina = df_counts

# COMMAND ----------

df_consolidado_periodico_opcional = consolidado_periodico.join(consolidado_opcional, on = ['SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_ANOMES'], how = 'full').fillna(0).filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento'])
df_consolidado = df_consolidado_periodico_opcional.join(df_consolidado_rotina, on = ['SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_ANOMES'], how = 'full').withColumn('QTD_PREVISTO_ROTINA', col('QTD_PREVISTO_ROTINA') - col('QTD_REALIZADO_PERIODICO') - col('QTD_REALIZADO_OPCIONAL')).fillna(0)

# COMMAND ----------

consolidado = df_consolidado.select('*',
  lit(sk_equipamento_enchedora[0]).alias('SK_EQUIPAMENTO'),
  lit(sk_tipo_equipamento[0]).alias('SK_TIPO_EQUIPAMENTO')
).withColumn('CREATED_DATE_CZ', lit(current_timestamp()))\
  .withColumn('QTD_PREVISTO_ROTINA', when(col('QTD_REALIZADO_ROTINA') > col('QTD_PREVISTO_ROTINA'), col('QTD_REALIZADO_ROTINA')).otherwise(col('QTD_PREVISTO_ROTINA')))


# COMMAND ----------

df_consolidado_final = consolidado.filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento']).\
  withColumn('CREATED_DATE_CZ', lit(current_timestamp()))

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
df_consolidado_final.write.partitionBy('SK_ANOMES').mode('overwrite').format(consume_file_type).save(sink_path_consolidado)

# COMMAND ----------

df_validade_periodico = df_coletas_athena_1_ano_filtrado.\
  filter(df_coletas_athena_1_ano_filtrado.SK_ITEM_PROCESSO.isin(parametros['inicio_cip_periodico'])).\
  filter(col('NUM_VALOR').isNotNull()).\
  groupBy([df_coletas_athena_1_ano_filtrado.SK_UNIDADE,
          df_coletas_athena_1_ano_filtrado.SK_LINHA_PRODUCAO,
          df_coletas_athena_1_ano_filtrado.SK_RESULTANTE, 
          col('SK_ANOMES')]).\
  agg(max(col('NUM_VALOR')).alias('NUM_VALOR')).\
  withColumn('VENCIMENTO_DIAS', lit(parametros['vencimento_cip_periodico'][0])).\
  withColumn('DH_VALIDADE', expr("NUM_VALOR + make_interval(0, 0, 0, 0, VENCIMENTO_DIAS * 24, 0, 0)")).\
  withColumn('SK_ANOMES_VALIDADE', year(col('DH_VALIDADE'))*100 + month(col('DH_VALIDADE'))).\
  select(df_coletas_athena_1_ano_filtrado.SK_UNIDADE,
        df_coletas_athena_1_ano_filtrado.SK_LINHA_PRODUCAO,
        col('NUM_VALOR'),
        col('SK_ANOMES'),
        col('DH_VALIDADE'),
        col('SK_ANOMES_VALIDADE'), 
        col('SK_RESULTANTE'))
  
df_validade_opcional = df_coletas_athena_1_ano_filtrado.\
  filter(df_coletas_athena_1_ano_filtrado.SK_ITEM_PROCESSO.isin(parametros['inicio_cip_opcional'])).\
  filter(col('NUM_VALOR').isNotNull()).\
  groupBy([df_coletas_athena_1_ano_filtrado.SK_UNIDADE,
          df_coletas_athena_1_ano_filtrado.SK_LINHA_PRODUCAO,
          df_coletas_athena_1_ano_filtrado.SK_RESULTANTE, 
          col('SK_ANOMES')]).\
  agg(max(col('NUM_VALOR')).alias('NUM_VALOR')).\
  withColumn('VENCIMENTO_DIAS', lit(parametros['vencimento_cip_opcional'][0])).\
  withColumn('DH_VALIDADE', expr("NUM_VALOR + make_interval(0, 0, 0, 0, VENCIMENTO_DIAS * 24, 0, 0)")).\
  withColumn('SK_ANOMES_VALIDADE', year(col('DH_VALIDADE'))*100 + month(col('DH_VALIDADE'))).\
  select(df_coletas_athena_1_ano_filtrado.SK_UNIDADE,
        df_coletas_athena_1_ano_filtrado.SK_LINHA_PRODUCAO,
        col('NUM_VALOR'),
        col('SK_ANOMES'),
        col('DH_VALIDADE'),
        col('SK_ANOMES_VALIDADE'), 
        col('SK_RESULTANTE'))

  

# COMMAND ----------

df_validade = df_validade_periodico.unionByName(df_validade_opcional).select(
    'SK_UNIDADE',
    'SK_LINHA_PRODUCAO',
    lit(sk_equipamento_enchedora[0]).alias('SK_EQUIPAMENTO'),
    'SK_ANOMES',
    'SK_ANOMES_VALIDADE',
    'NUM_VALOR',
    'DH_VALIDADE',
    'SK_RESULTANTE',
    lit(sk_tipo_equipamento[0]).alias('SK_TIPO_EQUIPAMENTO'))


# COMMAND ----------

df_validade_final = df_validade.filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento'])

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
df_validade_final.write.partitionBy('SK_ANOMES').mode('overwrite').format(consume_file_type).save(sink_path_validade)
