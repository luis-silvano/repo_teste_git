# Databricks notebook source
# MAGIC %md
# MAGIC # Equipamentos Filtração

# COMMAND ----------

# MAGIC %md
# MAGIC **'Consolidado'**: tabela (aqui chamada de consolidado) contendo as seguintes informações:
# MAGIC
# MAGIC - CIPs realizadas x previstas x fora do prazo **por equipamento** por mês
# MAGIC
# MAGIC **'Validade'**: tabela de validade contendo a validade dos CIPs  **por equipamento**.
# MAGIC
# MAGIC **'Etapas'**: tabela de etapas de CIP contendo um detalhamento dos CIPs **por equipamento**, no caso dos equipamentos de brassagem, os previstos e os relizados.

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

sink_path_consolidado = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Fatos/ConsolidadoParciais/FatoConsolidadoParcialEquipamentosFiltracao'
sink_path_validade = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Fatos/ValidadeParciais/FatoValidadeParcialEquipamentosFiltracao'
sink_path_etapas = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Fatos/EtapasParciais/FatoEtapasParcialEquipamentosFiltracao'

# COMMAND ----------

# DBTITLE 1,Criação das variáveis de reprocessamento
dbutils.widgets.text('reprocessamento', '', 'reprocessamento')
dbutils.widgets.text('mes_reprocessamento', '', 'mes_reprocessamento')
dbutils.widgets.text('ano_reprocessamento', '', 'ano_reprocessamento')

reprocessamento = dbutils.widgets.get('reprocessamento').strip().lower() == 'true'
mes_reprocessamento = dbutils.widgets.get('mes_reprocessamento')
ano_reprocessamento = dbutils.widgets.get('ano_reprocessamento')

# COMMAND ----------

# DBTITLE 1,Função para leitura da FatoColetaItemProcesso
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

# DBTITLE 1,Leitura das fatos e dimensões das coletas do Athena
# fatoColetaItemProcessoAthena 
path_consume_fato_coleta_item_processo_athena = '/mnt/consumezone/Brazil/Supply/Producao/FatoColetaItemProcessoMesAthena'

# dimItemProcessoAthena -> SK_ITEM_PROCESSO
path_consume_dim_item_processo_athena = '/mnt/consumezone/Brazil/Supply/Producao/DimItemProcessoMesAthena'
dim_item_processo_athena = dataframe_builder(path_consume_dim_item_processo_athena, consume_file_type)


# dimLinhaProducaoAthena -> SK_LINHA_PRODUCAO
path_consume_dim_linha_producao_athena = '/mnt/consumezone/Brazil/Supply/Producao/DimLinhaProducaoMesAthena'
dim_linha_producao_athena = dataframe_builder(path_consume_dim_linha_producao_athena, consume_file_type)

# dimEquipamentoAthena -> SK_EQUIPAMENTO
path_consume_dim_equipamento_athena = '/mnt/consumezone/Brazil/Supply/Producao/DimEquipamentoMesAthena'
dim_equipamento_athena = dataframe_builder(path_consume_dim_equipamento_athena, consume_file_type)

# dimTipoEqupamento -> SK_TIPO_EQUIPAMENTO
path_consume_dim_tipo_equipamento = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Dimensoes/DimTipoEquipamento'
dim_tipo_equipamento = dataframe_builder(path_consume_dim_tipo_equipamento, consume_file_type)

# dimEquipamentoCip
path_consume_dim_equipamento_cip = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Dimensoes/DimEquipamentoCip'
dim_equipamento_cip = dataframe_builder(path_consume_dim_equipamento_cip, consume_file_type)

# dimLinhaProducaoCip
path_consume_dim_linha_producao_cip = '/mnt/consumezone/Brazil/Supply/Producao/FrequenciaCIP/Dimensoes/DimLinhaProducaoCip'
dim_linha_producao_cip = dataframe_builder(path_consume_dim_linha_producao_cip, consume_file_type)

# Parâmetros ICs
path_history_parametros_ics = '/mnt/historyzone/Brazil/Sharepoint/ParametrosICsFreqCIP'
df_parametros_ics = dataframe_builder(path_history_parametros_ics, history_file_type)

# Parâmetros horas
path_history_parametros_horas = '/mnt/historyzone/Brazil/Sharepoint/ParametrosHorasFreqCIP'
df_parametros_horas = dataframe_builder(path_history_parametros_horas, history_file_type)


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
    print("⚠️  Partição do mês atual não encontrada, carregando sem ela...")
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

# DBTITLE 1,Listando itens de processo mapeados na lista do Sharepoint e presentes na dim_item_processo_athena
tipo_equipamento = ['FILTRO_CROSS_FLOW', 'FILTRO_KG' 'FILTRO_PVPP', 'LINHA_FILTRACAO', 'PUFFER_CERVEJA_FILTRADA', 'PUFFER_CERVEJA_MATURADA', 'TANQUE_TERRA_INFUSORIA']

lista_item_processo = (
    df_parametros_ics
    .filter((df_parametros_ics.DSC_EQUIPAMENTO.isin(tipo_equipamento)))
    .join(dim_item_processo_athena, df_parametros_ics.SK_ITEM_PROCESSO == dim_item_processo_athena.SK_ITEM_PROCESSO, how='inner')
    .select(dim_item_processo_athena.SK_ITEM_PROCESSO)
    .distinct()
)

lista_item_processo = [int(row['SK_ITEM_PROCESSO']) for row in lista_item_processo.collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção de dicionário de parâmetros

# COMMAND ----------

df_parametros_ics_filtrado = df_parametros_ics.filter(col("DSC_EQUIPAMENTO").isin(tipo_equipamento)).select(col("DSC_ITEM"),
    col("SK_ITEM_PROCESSO").alias("VALOR"),
    col("DSC_EQUIPAMENTO"))
df_parametros_horas_filtrado = df_parametros_horas.filter(col("DSC_SUB_AREA").isin(tipo_equipamento)).select(col("DSC_ITEM"),
    col("TEMPO_PROCESSO").alias("VALOR"),
    col("DSC_SUB_AREA").alias("DSC_EQUIPAMENTO"))


df_parametros = df_parametros_ics_filtrado.union(df_parametros_horas_filtrado)


df_parametros = df_parametros.groupBy("DSC_ITEM", "DSC_EQUIPAMENTO").agg(
    collect_list("VALOR").alias("VALORES")
)

dicionario_dict = df_parametros.collect()
parametros = {}

for row in dicionario_dict:
    item = row['DSC_ITEM'].lower()
    valores = {float(v) for v in row['VALORES']}  # Use um conjunto para evitar duplicatas

    if item in parametros:
        # Atualize com valores únicos usando a união de conjuntos
        parametros[item].update(valores)
    else:
        # Crie o item como um conjunto
        parametros[item] = valores
for chave in parametros:
    parametros[chave] = list(set(parametros[chave]))

# COMMAND ----------

# A partir da dimensão de equipamentos são filtrados os equipamentos por tipo, sendo eles:
# - FILTRO_CROSS_FLOW
# - FILTRO_KG
# - FILTRO_PVPP
# - LINHA_FILTRACAO
# - PUFFER_CERVEJA_FILTRADA
# - TANQUE_TERRA_INFUSORIA

df_equipamentos_filtro_cross_flow = dim_equipamento_athena.filter((col('DESC_EQUIPAMENTO').like('%Cross%')) & (col('DESC_EQUIPAMENTO').like('%Filt%'))).\
  withColumn('DESC_TIPO_EQUIPAMENTO', lit('FILTRO_CROSS_FLOW'))


df_equipamentos_filtro_kg = dim_equipamento_athena.filter(((col('DESC_EQUIPAMENTO').like('%KG%')) & (col('DESC_EQUIPAMENTO').like('%Filt%'))) 
                              | ((col('DESC_EQUIPAMENTO').like('%Terra%')) & (col('DESC_EQUIPAMENTO').like('%Filt%')))).\
  withColumn('DESC_TIPO_EQUIPAMENTO', lit('FILTRO_KG'))


df_equipamentos_filtro_pvpp = dim_equipamento_athena.filter(((col('DESC_EQUIPAMENTO').like('%PVPP%')) & (col('DESC_EQUIPAMENTO').like('%Filt%')))).\
  withColumn('DESC_TIPO_EQUIPAMENTO', lit('FILTRO_PVPP'))


df_equipamentos_linha_filtracao = dim_equipamento_athena.filter(
    (col('DESC_EQUIPAMENTO').like('%Filtração%')) & 
    (~col('DESC_EQUIPAMENTO').like('%Elétr%')) & 
    (~col('DESC_EQUIPAMENTO').like('%CO2%')) & 
    (~col('DESC_EQUIPAMENTO').like('%MP%')) & 
    (~col('DESC_EQUIPAMENTO').like('%não%')) & 
    (~col('DESC_EQUIPAMENTO').like('%Vapor%')) & 
    (~col('DESC_EQUIPAMENTO').like('%Assepsia%')) & 
    (~col('DESC_EQUIPAMENTO').like('%Depósito%')) & 
    (~col('DESC_EQUIPAMENTO').like('%CIP%')) & 
    (~col('DESC_EQUIPAMENTO').like('%Puffer%')) & 
    (~col('DESC_EQUIPAMENTO').like('%PC%')) & 
    (~col('DESC_EQUIPAMENTO').like('%Fermento%')) & 
    (~col('DESC_EQUIPAMENTO').like('%Resfriador%')) & 
    (~col('DESC_EQUIPAMENTO').like('NA%'))).\
  withColumn('DESC_TIPO_EQUIPAMENTO', lit('LINHA_FILTRACAO'))

df_equipamentos_puffer_filtrada = dim_equipamento_athena.filter(((col('DESC_EQUIPAMENTO').like('%Puffer%')) & (col('DESC_EQUIPAMENTO').like('%Filt%')) & (col('DESC_EQUIPAMENTO').like('%Cerv%')))).\
  withColumn('DESC_TIPO_EQUIPAMENTO', lit('PUFFER_CERVEJA_FILTRADA'))

df_equipamentos_tanque_terra = dim_equipamento_athena.filter(((col('DESC_EQUIPAMENTO').like('%Terra%')) & (col('DESC_EQUIPAMENTO').like('%Tanque%')))).\
  withColumn('DESC_TIPO_EQUIPAMENTO', lit('TANQUE_TERRA_INFUSORIA'))

df_equipamentos_mapeados = df_equipamentos_filtro_cross_flow.union(df_equipamentos_filtro_kg).union(df_equipamentos_filtro_pvpp).union(df_equipamentos_linha_filtracao).union(df_equipamentos_puffer_filtrada).union(df_equipamentos_tanque_terra).\
  join(dim_tipo_equipamento, on = 'DESC_TIPO_EQUIPAMENTO', how = 'inner').select('SK_EQUIPAMENTO',  dim_tipo_equipamento.SK_TIPO_EQUIPAMENTO)


# COMMAND ----------

# DBTITLE 1,Função de processamento de coletas
def processar_coletas_athena(fato_coleta_item_processo_athena, 
                             lista_item_processo):
    """
    Processa os dados da FatoColetaItemProcesso aplicando joins com dimensões, 
    criando colunas de data e filtrando os dados conforme parâmetros de itens de processo

    Parâmetros:
    - fato_coleta_item_processo_athena: fato de coletas Athena
    - lista_item_processo: Lista de SK_ITEM_PROCESSO a serem filtrados

    Retorna:
    - df_coletas_athena_filtrado: DataFrame final filtrado
    """

    df_coletas_athena = (
        fato_coleta_item_processo_athena                
        .filter(col("SK_ITEM_PROCESSO").isin(lista_item_processo))
        .select(
            fato_coleta_item_processo_athena.SK_UNIDADE,
            fato_coleta_item_processo_athena.SK_LINHA_PRODUCAO, 
            fato_coleta_item_processo_athena.SK_EQUIPAMENTO,
            fato_coleta_item_processo_athena.SK_RESULTANTE,
            fato_coleta_item_processo_athena.SK_ITEM_PROCESSO,
            fato_coleta_item_processo_athena.VALOR_TEXTO.alias('NUM_VALOR'),
            fato_coleta_item_processo_athena.VALOR.alias('NUM_VALOR_VOLUME'),
            fato_coleta_item_processo_athena.NUMERO_LOTE.alias('NUM_EXTERNO_LOTE'),
            fato_coleta_item_processo_athena.SK_DATA_INICIO_LOTE,
            fato_coleta_item_processo_athena.SK_HORA_INICIO_LOTE,
            fato_coleta_item_processo_athena.SK_DATA_FIM_LOTE,
            fato_coleta_item_processo_athena.SK_HORA_FIM_LOTE,
            fato_coleta_item_processo_athena.SK_LOTE,
            fato_coleta_item_processo_athena.SK_ETAPA_PROCESSO

        )
    )

    df_coletas_athena = (
        df_coletas_athena
        .withColumn("NUM_EXTERNO_LOTE", col("NUM_EXTERNO_LOTE").substr(6, 100).cast("int"))
        .withColumn("NUM_VALOR", to_timestamp(substring(col("NUM_VALOR"), 0, 16), "yyyy-MM-dd HH:mm"))
        # Formatação para garantir 4 dígitos na hora (exemplo: 352 -> 0352 para representar 03:52)
        .withColumn("SK_HORA_INICIO_LOTE_FORMATADA", lpad(col("SK_HORA_INICIO_LOTE").cast("string"), 4, "0"))
        .withColumn("SK_HORA_FIM_LOTE_FORMATADA", lpad(col("SK_HORA_FIM_LOTE").cast("string"), 4, "0"))
        # Criação das strings de data+hora no formato YYYY-MM-DD HH:MM
        .withColumn("DATA_HORA_INICIO_STR", 
                    concat(
                        regexp_replace(col("SK_DATA_INICIO_LOTE").cast("string"), "(\\d{4})(\\d{2})(\\d{2})", "$1-$2-$3"),
                        lit(" "),
                        substring(col("SK_HORA_INICIO_LOTE_FORMATADA"), 1, 2),
                        lit(":"),
                        substring(col("SK_HORA_INICIO_LOTE_FORMATADA"), 3, 2)
                    ))
        .withColumn("DATA_HORA_FIM_STR", 
                    concat(
                        regexp_replace(col("SK_DATA_FIM_LOTE").cast("string"), "(\\d{4})(\\d{2})(\\d{2})", "$1-$2-$3"),
                        lit(" "),
                        substring(col("SK_HORA_FIM_LOTE_FORMATADA"), 1, 2),
                        lit(":"),
                        substring(col("SK_HORA_FIM_LOTE_FORMATADA"), 3, 2)
                    ))
        # Conversão para timestamp
        .withColumn("DATA_INICIO_LOTE", to_timestamp(col("DATA_HORA_INICIO_STR"), "yyyy-MM-dd HH:mm"))
        .withColumn("DATA_FIM", to_timestamp(col("DATA_HORA_FIM_STR"), "yyyy-MM-dd HH:mm"))
    )

    df_coletas_athena = (
        df_coletas_athena
        .withColumn('SK_ANOMES', 
            when(col("NUM_VALOR").isNull(), 
                 (col("SK_DATA_INICIO_LOTE").cast("string").substr(1, 6)).cast("int"))
            .otherwise(year(col("NUM_VALOR"))*100 + month(col("NUM_VALOR")))
        )
    )

    df_coletas_athena = (
        df_coletas_athena
        .select(
            "SK_UNIDADE",
            "SK_LINHA_PRODUCAO",
            "SK_EQUIPAMENTO",
            "NUM_EXTERNO_LOTE",
            'SK_RESULTANTE',
            "SK_ITEM_PROCESSO",
            "NUM_VALOR",
            "NUM_VALOR_VOLUME",
            "DATA_INICIO_LOTE",
            "DATA_FIM",
            "SK_ANOMES",
            'SK_LOTE',
            'SK_ETAPA_PROCESSO'
        ).drop_duplicates()
    )

    return df_coletas_athena

# COMMAND ----------

# DBTITLE 1,Separação das coletas
df_coletas_athena_1_ano_filtrado = processar_coletas_athena(fato_coleta_item_processo_athena_1_ano, lista_item_processo)

df_coletas_athena_3_meses_filtrado = processar_coletas_athena(fato_coleta_item_processo_athena_3_meses, lista_item_processo)

# COMMAND ----------

df_validade_periodico = df_coletas_athena_3_meses_filtrado.\
filter(df_coletas_athena_3_meses_filtrado.SK_ITEM_PROCESSO.isin(parametros['inicio_cip_periodico'])).\
filter(col('NUM_VALOR').isNotNull()).\
groupBy([df_coletas_athena_3_meses_filtrado.SK_UNIDADE, 
        df_coletas_athena_3_meses_filtrado.SK_LINHA_PRODUCAO,
        df_coletas_athena_3_meses_filtrado.SK_EQUIPAMENTO,  
        df_coletas_athena_3_meses_filtrado.SK_RESULTANTE, 
        col('SK_ANOMES')]).\
agg(max(col('NUM_VALOR')).alias('NUM_VALOR')).\
withColumn('DH_VALIDADE', expr("NUM_VALOR + INTERVAL {} DAYS".format(int(parametros['vencimento_cip_periodico'][0])))).\
withColumn('SK_ANOMES_VALIDADE', year(col('DH_VALIDADE'))*100 + month(col('DH_VALIDADE'))).\
select(df_coletas_athena_3_meses_filtrado.SK_UNIDADE,
        df_coletas_athena_3_meses_filtrado.SK_LINHA_PRODUCAO,
        df_coletas_athena_3_meses_filtrado.SK_EQUIPAMENTO,
        col('NUM_VALOR'),
        col('SK_ANOMES'),
        col('DH_VALIDADE'),
        col('SK_ANOMES_VALIDADE'), 
        col('SK_RESULTANTE'))

df_validade_opcional = df_coletas_athena_3_meses_filtrado.\
filter(df_coletas_athena_3_meses_filtrado.SK_ITEM_PROCESSO.isin(parametros['inicio_cip_opcional'])).\
filter(col('NUM_VALOR').isNotNull()).\
groupBy([df_coletas_athena_3_meses_filtrado.SK_UNIDADE, 
        df_coletas_athena_3_meses_filtrado.SK_LINHA_PRODUCAO,
        df_coletas_athena_3_meses_filtrado.SK_EQUIPAMENTO, 
        df_coletas_athena_3_meses_filtrado.SK_RESULTANTE, 
        col('SK_ANOMES')]).\
agg(max(col('NUM_VALOR')).alias('NUM_VALOR')).\
withColumn('DH_VALIDADE', expr("NUM_VALOR + INTERVAL {} DAYS".format(int(parametros['vencimento_cip_opcional'][0])))).\
withColumn('SK_ANOMES_VALIDADE', year(col('DH_VALIDADE'))*100 + month(col('DH_VALIDADE'))).\
select(df_coletas_athena_3_meses_filtrado.SK_UNIDADE,
        df_coletas_athena_3_meses_filtrado.SK_LINHA_PRODUCAO,
        df_coletas_athena_3_meses_filtrado.SK_EQUIPAMENTO,
        col('NUM_VALOR'),
        col('SK_ANOMES'),
        col('DH_VALIDADE'),
        col('SK_ANOMES_VALIDADE'), 
        col('SK_RESULTANTE'))

df_validade_rotina = df_coletas_athena_3_meses_filtrado.\
filter(df_coletas_athena_3_meses_filtrado.SK_ITEM_PROCESSO.isin(parametros['inicio_cip_rotina'])).\
filter(col('NUM_VALOR').isNotNull()).\
groupBy([df_coletas_athena_3_meses_filtrado.SK_UNIDADE, 
        df_coletas_athena_3_meses_filtrado.SK_LINHA_PRODUCAO,
        df_coletas_athena_3_meses_filtrado.SK_EQUIPAMENTO,  
        df_coletas_athena_3_meses_filtrado.SK_RESULTANTE, 
        col('SK_ANOMES')]).\
agg(max(col('NUM_VALOR')).alias('NUM_VALOR')).\
withColumn('DH_VALIDADE', expr("NUM_VALOR + INTERVAL {} HOURS".format(int(parametros['vencimento_cip_rotina'][0]*24)))).\
withColumn('SK_ANOMES_VALIDADE', year(col('DH_VALIDADE'))*100 + month(col('DH_VALIDADE'))).\
select(df_coletas_athena_3_meses_filtrado.SK_UNIDADE,
        df_coletas_athena_3_meses_filtrado.SK_LINHA_PRODUCAO,
        df_coletas_athena_3_meses_filtrado.SK_EQUIPAMENTO,
        col('NUM_VALOR'),
        col('SK_ANOMES'),
        col('DH_VALIDADE'),
        col('SK_ANOMES_VALIDADE'), 
        col('SK_RESULTANTE'))

# COMMAND ----------

df_validade = df_validade_periodico.union(df_validade_opcional).union(df_validade_rotina).select(
    'SK_UNIDADE',
    'SK_LINHA_PRODUCAO',
    'SK_EQUIPAMENTO',
    'SK_ANOMES',
    'SK_ANOMES_VALIDADE',
    'NUM_VALOR',
    'DH_VALIDADE',
    'SK_RESULTANTE')

df_validade = df_validade.alias('validade').join(df_equipamentos_mapeados.alias('classificacao'), on = 'SK_EQUIPAMENTO', how = 'inner')\
.select('validade.*', 'classificacao.SK_TIPO_EQUIPAMENTO').dropna()


# COMMAND ----------

# DBTITLE 1,Escrita na CZ
df_validade_final = df_validade.filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento'])

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
df_validade_final.write.partitionBy('SK_ANOMES').mode('overwrite').format(consume_file_type).save(sink_path_validade)

# COMMAND ----------

# DBTITLE 1,Função Consolidadora para o Periódico e Opcional
def consolidador(dados, resultante, parametros):
    # Janela de partição
    wd_partition = Window.partitionBy('SK_EQUIPAMENTO').orderBy(asc('NUM_VALOR'))
    if resultante != 'rotina':
        resultante_validade = '_' + resultante
        resultante_cip = '_' + resultante
    else:
        resultante_validade = '_' + resultante
        resultante_cip = ''
    # Calcular tempo entre CIPs
    dados_tempo_ultimo_cip = dados.drop_duplicates()\
        .filter(dados.SK_ITEM_PROCESSO.isin(parametros['inicio_cip' + resultante_cip]))\
        .filter(col('NUM_VALOR').isNotNull())\
        .withColumn('TEMPO_PROX_CIP', (lead(col('NUM_VALOR')).over(wd_partition) - col('NUM_VALOR')).cast('long') / 3600)
    
    # Obtém o valor de vencimento e converte para horas
    vencimento_valor = next(iter(parametros['vencimento_cip' + resultante_validade]))
    vencimento_horas = int(vencimento_valor * 24)  # Converte dias para horas
    
    # Cria a expressão de intervalo em horas
    interval_expr = f"INTERVAL {vencimento_horas} HOURS"
    
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
                expr(interval_expr)
            )
        ).otherwise(
            when(
                # Segundo cenário: Último CIP da série e PASSOU DA VALIDADE
                (datediff(current_date(), col("NUM_VALOR")) > vencimento_valor),
                sequence(
                    col("NUM_VALOR"),
                    current_date(),
                    expr(interval_expr)
                )
            ).otherwise(array(col("NUM_VALOR")))  # Caso o CIP não tenha vencido ainda
        )
    )
    # Explode as datas para várias linhas
    df_calendario = df_dt_previsto.withColumn(
        "DT_PREVISTO",
        explode(col("DT_RANGE_PREVISTO"))
    ).drop("DT_RANGE_PREVISTO").withColumn('SK_ANOMES', 
            year(col("DT_PREVISTO")) * 100 + month(col("DT_PREVISTO")))
    # Janela de partição
    wd_partition_previsto = Window.partitionBy('SK_EQUIPAMENTO').orderBy(asc('DT_PREVISTO'))
    wd_partition_previsto_calendario = Window.partitionBy('SK_EQUIPAMENTO', 'NUM_VALOR').orderBy(asc('DT_PREVISTO'))
    
    # Numeração de linhas para o range de transbordo
    df_calendario = df_calendario.withColumn('NUM_LINHA', row_number().over(wd_partition_previsto_calendario))
    df_etapas = df_calendario
    if resultante != 'rotina':
        # Janela de partição
        wd_partition_previsto = Window.partitionBy('SK_EQUIPAMENTO').orderBy(asc('DT_PREVISTO'))
        wd_partition_previsto_calendario = Window.partitionBy('SK_EQUIPAMENTO', 'NUM_VALOR').orderBy(asc('DT_PREVISTO'))
        
        # Numeração de linhas para o range de transbordo
        df_calendario = df_calendario.withColumn('NUM_LINHA', row_number().over(wd_partition_previsto_calendario))
        # Cria range de datas previstas mensais
        # Adicionando verificação de validade para o transbordo mensal
        df_calendario = df_calendario.withColumn(
        'DT_RANGE_PREVISTO',
        when(
            # Se é uma projeção (não um CIP real) E existe próxima projeção
            (col('NUM_LINHA') > 1) & 
            (lead(col("DT_PREVISTO")).over(wd_partition_previsto).isNotNull()),
            when(
                # Se existe pelo menos um mês completo entre a projeção atual e a próxima
                add_months(date_trunc('month', col("DT_PREVISTO")), 1) < date_trunc('month', lead(col("DT_PREVISTO")).over(wd_partition_previsto)),
                array_union(
                    array(col("DT_PREVISTO")),  # Manter a projeção atual
                    slice(
                        sequence(
                            add_months(date_trunc('month', col("DT_PREVISTO")), 1),  # Mês seguinte
                            date_trunc('month', lead(col("DT_PREVISTO")).over(wd_partition_previsto)),  # Até o mês da próxima projeção
                            expr("interval 1 month")
                        ),
                        1, 999
                    )
                )
            ).otherwise(array(col("DT_PREVISTO")))  # Se não há meses entre projeções, manter apenas a atual
        ).otherwise(
            when(
                # Se é uma projeção E é a última (sem próxima projeção)
                (col('NUM_LINHA') > 1) & 
                lead(col("DT_PREVISTO")).over(wd_partition_previsto).isNull(),
                when(
                    # Se existe pelo menos um mês completo entre a projeção atual e o mês atual
                    add_months(date_trunc('month', col("DT_PREVISTO")), 1) <= date_trunc('month', current_date()),
                    array_union(
                        array(col("DT_PREVISTO")),  # Manter a projeção atual
                        slice(
                            sequence(
                                add_months(date_trunc('month', col("DT_PREVISTO")), 1),  # Mês seguinte
                                date_trunc('month', current_date()),  # Até o mês atual
                                expr("interval 1 month")
                            ),
                            1, 999
                        )
                    )
                ).otherwise(array(col("DT_PREVISTO")))  # Se não há meses entre a projeção e hoje, manter apenas a atual
            ).otherwise(array(col("DT_PREVISTO")))  # Se é um CIP real (NUM_LINHA = 1), manter apenas o registro atual
        ))
        
        # Explode as datas para várias linhas
        df_calendario = df_calendario.withColumn(
            "DT_PREVISTO",
            explode(col("DT_RANGE_PREVISTO"))
        ).drop("DT_RANGE_PREVISTO").withColumn('SK_ANOMES', 
            year(col("DT_PREVISTO")) * 100 + month(col("DT_PREVISTO"))).drop('NUM_LINHA')
        
        wd_partition_por_mes = Window.partitionBy('SK_EQUIPAMENTO', 'SK_ANOMES').orderBy(asc('DT_PREVISTO'))
    
    
    # Calcula o número de CIPs previstos
    df_previstos = df_calendario.groupBy('SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_EQUIPAMENTO', 'SK_ANOMES').agg(
        count('SK_EQUIPAMENTO').alias(f'QTD_PREVISTO_{resultante.upper()}')
    )
    # Calcula o número de CIPs realizados e CIPs fora do prazo
    df_realizados = dados_tempo_ultimo_cip.withColumn(
        'TEMPO_ANTERIOR_CIP', 
        (col('NUM_VALOR') - lag(col('NUM_VALOR')).over(wd_partition)).cast('long') / 3600
    ).groupBy('SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_EQUIPAMENTO', 'SK_ANOMES').agg(
        count('*').alias(f'QTD_REALIZADO_{resultante.upper()}'),
        sum(
            when(
                col('TEMPO_ANTERIOR_CIP') > (24 * next(iter(parametros[f'vencimento_cip{resultante_validade}']))), 
                1
            ).otherwise(0)
        ).alias(f'QTD_FORA_DO_PRAZO_{resultante.upper()}')
    )
    # Consolida as informações
    
    if resultante != 'rotina':

        df_consolidado = df_previstos.join(
        df_realizados,
        on=['SK_UNIDADE', 'SK_LINHA_PRODUCAO','SK_EQUIPAMENTO', 'SK_ANOMES'],
        how='full'
            ).fillna(0).withColumn(
                f'QTD_PREVISTO_{resultante.upper()}',
                when(
                    col(f'QTD_REALIZADO_{resultante.upper()}') > col(f'QTD_PREVISTO_{resultante.upper()}'),
                    col(f'QTD_REALIZADO_{resultante.upper()}')
                ).otherwise(col(f'QTD_PREVISTO_{resultante.upper()}'))
            )

    else:

        df_consolidado = df_previstos.join(
            df_realizados,
            on=['SK_UNIDADE', 'SK_LINHA_PRODUCAO','SK_EQUIPAMENTO', 'SK_ANOMES'],
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

def consolidador_desin(dados, parametros):
 
    df_processado = dados

    # Cria uma janela para identificar ciclos baseados na queda do volume e mudança de lote
    wd_partition = Window.partitionBy("SK_UNIDADE", "SK_LINHA_PRODUCAO").orderBy("DATA_INICIO_LOTE", "NUM_EXTERNO_LOTE")
    
    # Identifica os ciclos (FLAG_CICLO = 1 quando inicia um novo ciclo)
    df_com_flag_ciclo = (
        df_processado
        .filter(col("SK_ITEM_PROCESSO").isin(parametros["inicio_processo"]))
        .withColumn("PREV_LOTE", lag(col("NUM_EXTERNO_LOTE")).over(wd_partition))
        .withColumn("PREV_VALOR", lag(col("NUM_VALOR_VOLUME")).over(wd_partition))
        .withColumn("FLAG_CICLO", 
                  when((col("NUM_VALOR_VOLUME") < col("PREV_VALOR")) & 
                       (col("NUM_EXTERNO_LOTE") != col("PREV_LOTE")), 
                       lit(1)).otherwise(lit(0)))
    )
    
    # Filtrar apenas registros que representam início de ciclo (equivalente ao FLG_NOVO_CICLO = True)
    df_ciclos_filtrados = df_com_flag_ciclo.filter(col("FLAG_CICLO") == 1)
    
    # Adicionar número sequencial para cada ciclo por linha/mês
    w_ciclo = Window.partitionBy("SK_UNIDADE", "SK_LINHA_PRODUCAO", "SK_ANOMES").orderBy("DATA_INICIO_LOTE")
    df_ciclos_filtrados = df_ciclos_filtrados.withColumn("NUM_CICLO", row_number().over(w_ciclo))
    
    # Calcula os previstos por unidade, linha e mês
    df_previstos_linha_producao = (
        df_ciclos_filtrados
        .groupby("SK_UNIDADE", "SK_LINHA_PRODUCAO", "SK_ANOMES")
        .agg(
            count("NUM_CICLO").alias("QTD_PREVISTO_DESIN")
        )
    )
    
    # Cálculo dos realizados por linha e equipamento
    df_realizados_desin = (
        dados
        .filter(col("SK_ITEM_PROCESSO").isin(parametros["inicio_sani"]))
        .groupby("SK_UNIDADE", "SK_LINHA_PRODUCAO", "SK_EQUIPAMENTO", "SK_ANOMES")
        .agg(
            count("*").alias("QTD_REALIZADO_DESIN")
        )
    )

    
    # Junta os dataframes de previstos e realizados
    df_consolidado_desin = df_realizados_desin.join(
        df_previstos_linha_producao,
        on=["SK_UNIDADE", "SK_LINHA_PRODUCAO", "SK_ANOMES"],
        how="left"
    )
    
    return df_consolidado_desin

# COMMAND ----------

# Mantém os joins iniciais para filtro
df_coletas_athena_3_meses_filtrado = df_coletas_athena_3_meses_filtrado.join(df_equipamentos_mapeados, on = 'SK_EQUIPAMENTO', how = 'inner').drop('SK_TIPO_EQUIPAMENTO')
df_coletas_athena_1_ano_filtrado = df_coletas_athena_1_ano_filtrado.join(df_equipamentos_mapeados, on = 'SK_EQUIPAMENTO', how = 'inner').drop('SK_TIPO_EQUIPAMENTO')

# Processamento dos consolidadores
df_consolidado_periodico, df_etapas_periodico = consolidador(df_coletas_athena_1_ano_filtrado, 'periodico', parametros)
df_consolidado_rotina, df_etapas_rotina = consolidador(df_coletas_athena_3_meses_filtrado, 'rotina', parametros)
df_consolidado_opcional, df_etapas_opcional = consolidador(df_coletas_athena_1_ano_filtrado, 'opcional', parametros)
df_consolidado_desin = consolidador_desin(df_coletas_athena_3_meses_filtrado, parametros)

# Colunas em comum para os joins
colunas_em_comum = ['SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_ANOMES', 'SK_EQUIPAMENTO']

# Primeiro join: rotina + periodico
df_consolidado = df_consolidado_rotina.join(
    df_consolidado_periodico, 
    on=colunas_em_comum, 
    how='full'
)

# Segundo join: adiciona opcional
df_consolidado = df_consolidado.join(
    df_consolidado_opcional, 
    on=colunas_em_comum, 
    how='full'
)

# Terceiro join: adiciona desin
df_consolidado_com_desin = df_consolidado.join(
    df_consolidado_desin, 
    on=['SK_UNIDADE', 'SK_ANOMES', 'SK_LINHA_PRODUCAO', 'SK_EQUIPAMENTO'], 
    how='full'
).fillna(0).filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento'])


df_consolidado_final = df_consolidado_com_desin.withColumn(
  'QTD_PREVISTO_ROTINA', when((col('QTD_PREVISTO_ROTINA') - col('QTD_REALIZADO_PERIODICO') - col('QTD_REALIZADO_OPCIONAL')) > 0,
                                col('QTD_PREVISTO_ROTINA') - col('QTD_REALIZADO_PERIODICO') - col('QTD_REALIZADO_OPCIONAL')).otherwise(lit(0))
).withColumn(
  'QTD_REALIZADO_ROTINA', when((col('QTD_REALIZADO_ROTINA') - col('QTD_REALIZADO_PERIODICO') - col('QTD_REALIZADO_OPCIONAL')) > 0,
                                col('QTD_REALIZADO_ROTINA') - col('QTD_REALIZADO_PERIODICO') - col('QTD_REALIZADO_OPCIONAL')).otherwise(lit(0))
).withColumn(
  'QTD_PREVISTO_DESIN', when((col('QTD_PREVISTO_DESIN') - col('QTD_REALIZADO_ROTINA')- col('QTD_REALIZADO_PERIODICO') - col('QTD_REALIZADO_OPCIONAL')) > 0,
                              col('QTD_PREVISTO_DESIN') - col('QTD_REALIZADO_ROTINA')- col('QTD_REALIZADO_PERIODICO') - col('QTD_REALIZADO_OPCIONAL')).otherwise(lit(0))
).withColumn(
  'QTD_PREVISTO_DESIN', when(col('QTD_REALIZADO_DESIN') > col('QTD_PREVISTO_DESIN'), col('QTD_REALIZADO_DESIN')).otherwise(col('QTD_PREVISTO_DESIN'))
).join(df_equipamentos_mapeados, on = 'SK_EQUIPAMENTO', how = 'inner')

# Puffer e tanque não fazem desinfecção

df_consolidado_final = df_consolidado_final.withColumn('QTD_REALIZADO_DESIN', when(col('SK_TIPO_EQUIPAMENTO').isin([47, 21]), lit(0)).otherwise(lit(col('QTD_REALIZADO_DESIN'))))\
  .withColumn('QTD_PREVISTO_DESIN', when(col('SK_TIPO_EQUIPAMENTO').isin([47, 21]), lit(0)).otherwise(lit(col('QTD_PREVISTO_DESIN'))))

# COMMAND ----------

df_etapas = df_etapas_rotina.alias('rotina').select(
  'SK_UNIDADE',
  'SK_LINHA_PRODUCAO', 
  'SK_EQUIPAMENTO',
  'NUM_EXTERNO_LOTE',
  'SK_RESULTANTE',
  'SK_ITEM_PROCESSO',
  col('DT_PREVISTO').alias('NUM_VALOR'),
  when(col('NUM_LINHA') == 1, lit('Previsto e Realizado'))
  .otherwise(lit('Previsto e Não Realizado')).alias('DESC_ETAPA'),
  'SK_ANOMES', 'SK_LOTE', 'SK_ETAPA_PROCESSO').withColumn('DESC_TIPO_LIMPEZA', lit('CIP'))

window_spec = Window.partitionBy('SK_UNIDADE', 'SK_LINHA_PRODUCAO', 'SK_EQUIPAMENTO').orderBy('NUM_VALOR')

df_etapas = df_etapas.withColumn(
      'TEMPO_ENTRE_ETAPAS',
      (lead('NUM_VALOR').over(window_spec).cast('long') - col('NUM_VALOR').cast('long')) / 3600
  )
    
df_etapas = df_etapas.join(df_equipamentos_mapeados.alias('mapeados'), on='SK_EQUIPAMENTO', how='inner')


# COMMAND ----------

df_etapas_final = df_etapas.filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento']).\
  withColumn('CREATED_DATE_CZ', lit(current_timestamp()))


spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df_etapas_final.write.partitionBy('SK_ANOMES').mode('overwrite').format(consume_file_type).save(sink_path_etapas)

# COMMAND ----------


df_consolidado_final = df_consolidado_final.filter(col('SK_ANOMES') > parametros_processamento['ano_mes_processamento']).\
  withColumn('CREATED_DATE_CZ', lit(current_timestamp()))

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
df_consolidado_final.write.partitionBy('SK_ANOMES').mode('overwrite').format(consume_file_type).save(sink_path_consolidado)