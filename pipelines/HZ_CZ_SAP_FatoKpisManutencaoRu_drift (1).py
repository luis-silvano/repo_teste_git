# Databricks notebook source
# DBTITLE 1,Executa o General_Functions
# MAGIC %run /Brasil/Functions/GeneralFunctions

# COMMAND ----------

# DBTITLE 1,Bibliotecas
from pyiris.ingestion.config.file_system_config import FileSystemConfig
from pyiris.infrastructure import Spark
from pyiris.ingestion.load import LoadService, FileWriter
from pyiris.ingestion.extract import FileReader
from pyspark.sql.functions import lag, coalesce, lit, col
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# COMMAND ----------

# DBTITLE 1,Iniciando variáveis
_now = date.today()
_spark_iris = Spark()

# COMMAND ----------

# DBTITLE 1,Consulta da DimTipoAtividade
df_tipo_atividade = (spark.read.format('parquet').load('/mnt/consumezone/Brazil/Supply/Producao/Manutencao/DimTipoAtividadeOrdensSap'))

atividades_planejadas = df_tipo_atividade.filter(col('TIPO_PLANO') == 'Plano Manutenção').withColumn('SK_TIPO_ATIVIDADE',col('SK_TIPO_ATIVIDADE').cast(StringType())).select('SK_TIPO_ATIVIDADE')
atividades_corretivas = df_tipo_atividade.filter(col('TIPO_PLANO') == 'Corretiva').withColumn('SK_TIPO_ATIVIDADE',col('SK_TIPO_ATIVIDADE').cast(StringType())).select('SK_TIPO_ATIVIDADE')

# COMMAND ----------

# DBTITLE 1,Criação da lista de tipos de atividade
TIPO_PLANO = atividades_planejadas.rdd.map(lambda x: x[0]).collect()
TIPO_CORRETIVA = atividades_corretivas.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

# DBTITLE 1,Obtendo datasets necessários
#Obtendo os dados do DePara de Area
df_area_hz = FileReader(table_id='df_area', data_lake_zone='historyzone', country='Brazil', path='Sharepoint/DeParasSupply/Areas', format='parquet').consume(spark=_spark_iris)
df_area = select_max_update_date(df_area_hz, 'AREA_SAP', 'CREATED_DATE_HZ')
df_area = df_area.select(col('COD_AREA_LAKE').cast(IntegerType()),
                         'AREA_SAP')

#Obtendo os dados do DePara de SubArea
df_subarea_hz = FileReader(table_id='df_subarea', data_lake_zone='historyzone', country='Brazil', path='Sharepoint/DeParasSupply/SubAreas', format='parquet').consume(spark=_spark_iris)
df_subarea = select_max_update_date(df_subarea_hz, 'SUBAREA_SAP', 'CREATED_DATE_HZ')
df_subarea = df_subarea.select(col('COD_SUBAREA_LAKE').cast(IntegerType()),
                               'SUBAREA_SAP')

#Obtendo os dados do DePara de Unidades
df_unidades_hz = FileReader(table_id='df_unidades', data_lake_zone='historyzone', country='Brazil', path='Sharepoint/DeParasSupply/Unidades', format='parquet').consume(spark=_spark_iris)
df_unidades = select_max_update_date(df_unidades_hz, 'SAP_CODIGO', 'CREATED_DATE_HZ')
df_unidades = df_unidades.select(col('COD_UNIDADE_LAKE').cast(IntegerType()),
                                 col('SAP_CODIGO').alias('Centro_planejamento'),
                                 col('HANA_CODIGO').alias('SAP_CODIGO_HANA'))

#Obtendo os dados de dias úteis da Dim Data
df_dimdata = FileReader(table_id='df_data', data_lake_zone='consumezone', country='Brazil', path='Supply/Estrutura/DimensaoData', format='parquet').consume(spark=_spark_iris)
df_dimdata = df_dimdata.select(col('Data'), col('FlagDiaUtil'))
df_dimdata = df_dimdata.withColumn("AnoMes", date_format(col("Data"), "yyyyMM"))

#Obtendo os dados de ROLLOUT HANA
df_rollouthana = FileReader(table_id='df_rollouthana', data_lake_zone='consumezone', country='Brazil', path='Supply/Producao/RelatorioRolloutSap', format='parquet').consume(spark=_spark_iris)
df_rollouthana = df_rollouthana.withColumn('DATA_ROLLOUT',date_format(col('DATA_ROLLOUT'), "yyyyMMdd").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,SAP ECC
df_iw37n_hz = FileReader(table_id='iw37n', data_lake_zone='historyzone', country='Brazil', path='SAPSupply/IW37N', format='parquet').consume(spark=_spark_iris)
df_iw37n_hz = df_iw37n_hz.withColumn("ORIGEM", lit('ECC'))\
                         .withColumn('SK_TIPO_ATIVIDADE_MANUTENCAO', concat(lit('E'),col('Tipo_ativ_manutencao').cast(IntegerType())))\
                         .withColumn('Data_fim_real_rollout', col('Data_Fim_Real').cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,SAP HANA
#Obtendo os dados da iw37n HANA para gerar a fato
df_iw37n_hz_hana = FileReader(table_id='iw37n', data_lake_zone='historyzone', country='Brazil', path='Aurora/IW37N', format='parquet').consume(spark=_spark_iris)
df_iw37n_hz_hana = df_iw37n_hz_hana.withColumn("ORIGEM", lit('HANA'))

# COMMAND ----------

# SAP HANA Renomenado para PT-Br as colunas para fazer o union:
df_ordens_manutencao_hz_hana_r = df_iw37n_hz_hana.withColumn("Primeiro_hora_inicio", lit(None)) \
    .withColumn("Primeiro_hora_fim", lit(None)) \
    .withColumn("Chave_sobretaxas", lit(None)) \
    .withColumn("Chave_pais", lit(None)) \
    .withColumn("Conjunto", lit(None)) \
    .withColumn("Conjunto_operacao", lit(None)) \
    .withColumn("Criterio_calc", lit(None)) \
    .withColumn("Custo_Tot_Plan_Ordem", lit(None)) \
    .withColumn("Custo_Tot_Plan_Inclusao", lit(None)) \
    .withColumn("Custos_estimados", lit(None)) \
    .withColumn("Custos_tot_reais", lit(None)) \
    .withColumn("Custos_totais_plan", lit(None)) \
    .withColumn("Custos_totais_liquid", lit(None)) \
    .withColumn("Hora_inicio", lit(None)) \
    .withColumn("Data_pedido", lit(None)) \
    .withColumn("Duracao_minima", lit(None)) \
    .withColumn("Duracao_normal", lit(None)) \
    .withColumn("Elemento_PEP", lit(None)) \
    .withColumn("Equipamento", lit(None)) \
    .withColumn("Fornecedor", lit(None)) \
    .withColumn("Fim_real_hora", lit(None)) \
    .withColumn("Item", lit(None)) \
    .withColumn("Item_ReqC", lit(None)) \
    .withColumn("Material", lit(None)) \
    .withColumn("Num_pedido", lit(None)) \
    .withColumn("Overhaul", lit(None)) \
    .withColumn("PEP_cabecalho_ordem", lit(None)) \
    .withColumn("Propriedade", lit(None))\
    .withColumn("Preco", lit(None)) \
    .withColumn("Quantidade", lit(None)) \
    .withColumn("Reserva_Rec_Compras", lit(None)) \
    .withColumn("Subnumero", lit(None)) \
    .withColumn("Texto_breve_material", lit(None)) \
    .withColumn("Unidade_preco", lit(None)) \
    .withColumn("Nome_empregado", lit(None)) \
    .withColumn("Confirmado_por", lit(None))

df_ordens_manutencao_hz_hana_final = df_ordens_manutencao_hz_hana_r.select(
    col('MAINTENANCE_ORDER').alias('Ordem'),
    col('OPERATION_NUMBER_FOR_PM').alias('Operacao'),
    col('SUB_OPERATION_NUMBER_FOR_PM').alias('Sub_Operacao'),
    col('CONTROL_KEY').alias('Chave_Controle'),
    col('FUNCTIONAL_LOCATION_TEXT').alias('Local_Instalacao'),
    col('MAINTENANCE_PLANT').alias('Centro_Planejamento'),
    col('EARLIEST_START_DATE').alias('Primeiro_data_inicio'),
    col('CREATED_ON').alias('Data_Entrada'),
    col('LAST_CHANGED_ON').alias('Data_Modificacao'),
    col('ACTUAL_EXECUTION_END_DATE').alias('Data_fim_real'),
    col('ORDER_TYPE').alias('Tipo_Ordem'),
    col('SUB_OPERATION_NUMBER_FOR_PM_TEXT').alias('Txt_breve_operacao'),
    col('PRIORITY').alias('Prioridade'),
    col('MAINTENANCE_ACTIVITY_TYPE').alias('Tipo_ativ_manutencao'),
    col('OPERATION_SYSTEM_STATUS').alias('Status_Operacao'),
    col('USER_STATUS').alias('Status_usuario'),
    col('SYSTEM_STATUS').alias('Status_sistema'),
    col('MAINTENANCE_PLANNER_GROUP').alias('Grp_Planj_PM'),
    col('PLANNED_WORK_FOR_OPERATION').alias('Trabalho'),
    col('UNIT_FOR_PROCESSING_TIME').alias('Unidade_trabalho'),
    col('WORK_CENTER_RESOURCE').alias('Centro_trab_respons'),
    col('WORK_CENTER_OF_THE_OPERATION').alias('Centro_trab_operacao'),
    col('EARLIEST_FINISH_DATE').alias('Primeiro_data_fim'),
    col('RESPONSIBLE_COST_CENTER').alias('Cen_Cts_Responsavel'),
    col('COST_CENTER').alias('Centro_Custo'),
    col('KEY_FOR_CALCULATION').alias('Chave_calculo'),
    col('STANDARD_TEXT_KEY').alias('Chave_modelo'),
    col('ABC_INDICATOR').alias('Codigo_ABC'),
    col('SYSTEM_CONDITION').alias('Conds_instal'),
    col('SYSTEM_CONDITION_TEXT').alias('Conds_Instal_Operacao'),
    col('CONFIRMATION').alias('Confirmacao'),
    col('CREATED_BY').alias('Criado_por'),
    col('PM_REFERENCE_DATE').alias('Data_referencia'),
    col('BASIC_START_DATE').alias('Inicio_Base'),
    col('MAINTENANCE_ITEM').alias('Item_Manutencao'),
    col('DELETION_FLAG').alias('Marc_eliminacao'),
    col('REAL_CURRENCY').alias('Moeda'),
    col('NOTIFICATION').alias('Nota'),
    col('MAINTENANCE_PLAN').alias('Plano_manutencao'),
    col('PURCHASE_REQUISITION_NUMBER').alias('Requisicao_Compras'),
    col('MAINTENANCE_ITEM_TEXT').alias('Texto_Breve'),
    col('TOTAL_PLAN').alias('Total_planejado'),
    col('TOTAL_REAL').alias('Total_real'),
    col('FORECASTED_WORK').alias('Trabalho_previsto'),
    col('ACTUAL_WORK_FOR_OPERATION').alias('Trabalho_real'),
    col('LAST_CHANGED_ORDER_BY').alias('Modificado_por'),
    col('CONFIRMATION_TEXT').alias('Texto_Confirmacao'),
    col('PERSONNEL_NUMBER').alias('Num_Pessoal'),
    col('POSTING_DATE_IN_THE_DOCUMENT').alias('Data_lancamento'),
    col('ENTRY_DATE_OF_CONFIRMATION').alias('Confirmado_em'),
    col('EARLIEST_TIME_START').alias('Primeiro_hora_inicio'),
    col('EARLIEST_TIME_END').alias('Primeiro_hora_fim'),
    col('CONFIRMED_BY').alias('Confirmado_por'),
    col('EMPLOYEE_NAME').alias('Nome_empregado'),
    col('EARLIEST_TIME').alias('Hora_inicio'),
    col('PLANNED_DATE').alias('Data_planejada'),
    col('DATE_MPA').alias("Data_MPA"),
    col('DAYS_TO_ADD').alias('days_to_add'),
    'CREATED_DATE_HZ',
    'DELETED_AT',
# COLUNAS NAO MAPEADAS                                
    "Chave_sobretaxas",
    "Chave_pais",	
    "Conjunto",	
    "Conjunto_operacao",
    "Criterio_calc",
    "Custo_Tot_Plan_Ordem",
    "Custo_Tot_Plan_Inclusao", 
    "Custos_estimados",        
    "Custos_tot_reais",        
    "Custos_totais_plan",      
    "Custos_totais_liquid",    
    "Data_pedido",	
    "Duracao_minima",
    "Duracao_normal",	
    "Elemento_PEP",	
    "Equipamento",	
    "Fornecedor",	
    "Fim_real_hora",
    "Item",	
    "Item_ReqC",
    "Material",
    "Num_pedido",	
    "Overhaul",	            
    "PEP_cabecalho_ordem",	
    "Propriedade",
    "Preco",	
    "Quantidade",
    "Reserva_Rec_Compras",
    "Subnumero",	
    "Texto_breve_material",
    "Unidade_preco",
    "ORIGEM",
    ).withColumn('SK_TIPO_ATIVIDADE_MANUTENCAO', concat(lit('H'),col('Tipo_ativ_manutencao').cast(IntegerType())))\
     .withColumn('Data_fim_real_rollout', col('Data_Fim_Real').cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Join entre UNIDADES
df_iw37n_ecc = df_iw37n_hz.join(broadcast(df_unidades), ['Centro_planejamento'], how='left')
df_iw37n_hana = df_ordens_manutencao_hz_hana_final.join(broadcast(df_unidades), df_ordens_manutencao_hz_hana_final['Centro_planejamento'] == df_unidades['SAP_CODIGO_HANA'], how='left').drop(df_unidades.Centro_planejamento)

# COMMAND ----------

# DBTITLE 1,JOIN do Rollout com HANA + filtragem
# Retorna as unidades que passaram pelo Rollout:

df_iw37n_hana = df_iw37n_hana\
  .join(
    df_rollouthana,
    (
      df_iw37n_hana['COD_UNIDADE_LAKE'] == df_rollouthana['SK_UNIDADE']
    ) & (
      (df_iw37n_hana['Data_Fim_Real'] >= df_rollouthana['DATA_ROLLOUT'])
    ),
    "inner"
  )\
  .select(
    df_iw37n_hana['*']
  ).drop('DATA_ROLLOUT','rank')

# COMMAND ----------

# DBTITLE 1,JOIN do Rollou com ECC + filtragem
# Join com lista de rollout para determinar o registros que devem ser apresentados
df_iw37n_ecc = df_iw37n_ecc.join(broadcast(df_rollouthana), df_iw37n_ecc['COD_UNIDADE_LAKE'] == df_rollouthana['SK_UNIDADE'], how='left').select(df_iw37n_ecc['*'], df_rollouthana['DATA_ROLLOUT'])

# Filtrando o dataframe principal considerando a lista de rollout
df_iw37n_ecc = df_iw37n_ecc.where(
  (
    (df_iw37n_ecc['Data_Fim_Real'] < df_iw37n_ecc['DATA_ROLLOUT']) |
    (df_iw37n_ecc['DATA_ROLLOUT'].isNull())
  )
).drop('DATA_ROLLOUT','rank')

# COMMAND ----------

# DBTITLE 1,Transformando as bases dos SAPs ECC e HANA em uma base única
df_iw37n_hz = df_iw37n_ecc.unionByName(
    df_iw37n_hana,
    allowMissingColumns = True
    )

# COMMAND ----------

# DBTITLE 1,Filtrar dados da IW37N que não se adequam à analise de RU
df_iw37n_hz = df_iw37n_hz.filter(col('Data_fim_real').isNotNull())\
                         .filter(col('Data_fim_real') != '00000000')\
                         .filter(col('Tipo_Ordem') != 'ZM01')\
                         .filter(col('Trabalho_real') > 0)\
                         .withColumn('Data_fim_final',to_date(col('Data_fim_real')))\
                         .withColumn('Data_fim_real',regexp_replace('Data_fim_real', '-', ''))

# COMMAND ----------

path_history_zone_centro_trabalho = '/mnt/historyzone/Brazil/Sharepoint/DeParasSupply/CentroTrabalho'
df_history_centro_trabalho = dataframe_builder(path_history_zone_centro_trabalho, 'parquet')

# COMMAND ----------

centro_trabalho_filt = ['MEC', 'ELE', 'INS', 'AUT', 'SOL', 'GPA', 'GPM', 'GPI', 'GPE', 'TOR', 'ELEMEC']

df_history_centro_trabalho = df_history_centro_trabalho.withColumn('CT_Filt',substring(col('CentroDeTrabalhoParaConsiderar'),1,3))\
                                                       .filter(col('CT_Filt').isin(centro_trabalho_filt))

# COMMAND ----------

df_history_ct = df_history_centro_trabalho.select("CT_Filt").dropDuplicates()

# Join otimizado com broadcast e deduplicação prévia
df_iw37n_hz = df_iw37n_hz.join(
    broadcast(df_history_ct),
    substring(df_iw37n_hz["Centro_trab_operacao"], 1, 3) == df_history_ct["CT_Filt"],
    "inner"
).select(df_iw37n_hz["*"], df_history_ct["CT_Filt"])

# COMMAND ----------

df_iw37n_hz = df_iw37n_hz.dropDuplicates(["Ordem", "Operacao", "Sub_Operacao", "CREATED_DATE_HZ"])

# COMMAND ----------

# DBTITLE 1,Criação de Área e Subárea - dados BRASIL
# Separando um dataframe com dados somente do Brasil, pois já estão no padrão novo de nomenclatura dos locais de instalação
df_dados_brasil = df_iw37n_hz.where(col('Centro_planejamento').like('BR%'))

# Regra de verificação das AREA_SUBAREA fora do padrão
verifica_local = (rpad(trim(split(col('Local_instalacao'), '-', 3)[1]), 5, '0'))
exist_variance = ( (verifica_local == '00000') & (col('Local_instalacao') != '-') )
concat_area_subarea = ( concat(split(col('Local_instalacao'), '-', 3)[1], split(col('Local_instalacao'), '-', 3)[2]) )
area_subarea_padrao_novo = ( trim(rpad(trim(split(col('Local_instalacao'), '-', 3)[1]), 5, '0')) )

# Criando coluna de Área de Subárea
df_dados_brasil = df_dados_brasil.withColumn('AREA', when(exist_variance, concat_area_subarea).otherwise(area_subarea_padrao_novo))\
                                 .withColumn('SUBAREA', when(exist_variance, concat_area_subarea).otherwise(area_subarea_padrao_novo))

# COMMAND ----------

# DBTITLE 1,Criação de Área e Subárea - dados ABC
# Separando um dataframe com dados somente da ABC pois contém dois padrões (antigo e novo) para a nomenclatura dos locais de instalação
df_dados_abc = df_iw37n_hz.where(~col('Centro_planejamento').like('BR%'))

# Definição das variáveis com quebra do local de instalação para encontrar Área e Subárea no padrão antigo
split_local_instalacao = ( split( col('Local_instalacao'), '-') )
primeiro_grupo_local_instalacao = ( length(split_local_instalacao[0]) )
nomenclatura_padrao_novo = ( primeiro_grupo_local_instalacao > 1 )
nomenclatura_padrao_antigo = ( primeiro_grupo_local_instalacao == 1 )
ultimo_grupo_local_instalacao = ( split_local_instalacao[size(split_local_instalacao)-1] )
existe_subarea = (regexp_extract(ultimo_grupo_local_instalacao, '(^[L][0-9])\w+', 0))
subarea_fora_padrao = (regexp_extract(col('Local_instalacao'), '(-L-)\w+', 0))

# Criando coluna de Área de Subárea com os locais de instalação no padrão novo
df_dados_abc_padrao_novo = df_dados_abc.withColumn('AREA', area_subarea_padrao_novo)\
                                       .withColumn('SUBAREA', area_subarea_padrao_novo)\
                                       .where(nomenclatura_padrao_novo | (col('Local_instalacao').isNull()))

# Criando coluna de Área de Subárea com os locais de instalação no padrão antigo
df_dados_abc_padrao_antigo = df_dados_abc.withColumn('AREA', split_local_instalacao[2])\
                                         .withColumn('SUBAREA', when(ultimo_grupo_local_instalacao.contains('LIN'), ultimo_grupo_local_instalacao)\
                                                               .when(col('Local_instalacao').contains('-L-'), regexp_replace(subarea_fora_padrao, '-', '') )\
                                                               .when(existe_subarea.isNotNull(), existe_subarea)\
                                                               .otherwise(lit(None)))\
                                         .where(nomenclatura_padrao_antigo)

# Unindo os dataframes do Brasil e ABC após tratamento
df_iw37n_inicial = df_dados_abc_padrao_antigo.union(df_dados_abc_padrao_novo)\
                                             .union(df_dados_brasil)

# COMMAND ----------

# DBTITLE 1,Join entre AREA e SUBAREA
df_iw37n = df_iw37n_inicial.join(broadcast(df_area), df_iw37n_inicial['AREA'] == df_area['AREA_SAP'], how='left')
df_iw37n = df_iw37n.join(broadcast(df_subarea), df_iw37n['SUBAREA'] == df_subarea['SUBAREA_SAP'], how='left')

# COMMAND ----------

# DBTITLE 1,Substituindo colunas de 0 para Nulo
colunas_de_horas = ['Primeiro_hora_inicio',
                    'Primeiro_hora_fim',
                    'Hora_inicio',
                    'Fim_real_hora']

colunas_de_data = ['Primeiro_data_inicio',
                   'Data_Entrada',
                   'Data_Modificacao',
                   'Data_fim_real',
                   'Primeiro_data_fim',
                   'Data_referencia',
                   'Data_pedido',
                   'Inicio_Base',
                   'Confirmado_em',
                   'Data_lancamento',
                   'Data_planejada']

df_iw37n = df_iw37n.replace('000000', None, subset=colunas_de_horas)\
                   .replace('00000000', None, subset=colunas_de_data)

# COMMAND ----------

# DBTITLE 1,Criando SK para colunas de horas
hora_cols = [
    ('Primeiro_hora_inicio', 'SK_PRIMEIRA_HORA_INICIO'),
    ('Primeiro_hora_fim', 'SK_PRIMEIRA_HORA_FIM'),
    ('Hora_inicio', 'SK_HORA_INICIO'),
    ('Fim_real_hora', 'SK_HORA_FIM_REAL')
]

df_iw37n = df_iw37n.select(
    '*',
    *[
        when(col(orig).isNull(), '-1').otherwise(substring(col(orig), 0, 4)).alias(new)
        for orig, new in hora_cols
    ]
)

# COMMAND ----------

# DBTITLE 1,Criando SK para colunas de data

# Lista de colunas originais e os nomes das novas colunas que você quer criar
cols = [
    ('Primeiro_data_inicio', 'SK_PRIMEIRA_DATA_INICIO'),
    ('Data_Entrada', 'SK_DATA_ENTRADA'),
    ('Data_Modificacao', 'SK_DATA_MODIFICACAO'),
    ('Data_fim_real', 'SK_DATA_FIM_REAL'),
    ('Primeiro_data_fim', 'SK_PRIMEIRA_DATA_FIM'),
    ('Data_referencia', 'SK_DATA_REFERENCIA'),
    ('Data_pedido', 'SK_DATA_PEDIDO'),
    ('Inicio_Base', 'SK_DATA_INICIO_BASE'),
    ('Confirmado_em', 'SK_DATA_CONFIRMADO_EM'),
    ('Data_lancamento', 'SK_DATA_LANCAMENTO'),
    ('Data_planejada', 'SK_DATA_PLANEJADA'),
]

# Aplica a transformação em uma única operação select
df_iw37n = df_iw37n.select(
    '*',  # mantém as colunas originais
    *[
        when((col(orig).isNull()) | (col(orig) == '0'), '-1')
        .otherwise(col(orig))
        .alias(new)
        for orig, new in cols
    ]
)

# COMMAND ----------

# DBTITLE 1,Formatando colunas de data e hora para os Data Scientists
data_format = 'yyyyMMdd'
string_regex_pattern = r'(\d{2})(\d{2})(\d{2})'
string_regex_replacement = r'$1:$2:$3'

# Monta as expressões mantendo a ordem exata das colunas
exprs = []
for c in df_iw37n.columns:
    if c in colunas_de_data:
        exprs.append(to_date(col(c), data_format).alias(c))
    elif c in colunas_de_horas:
        exprs.append(regexp_replace(col(c), string_regex_pattern, string_regex_replacement).alias(c))
    else:
        exprs.append(col(c))

# Executa todas as transformações de uma só vez
df_iw37n = df_iw37n.select(*exprs)

# COMMAND ----------

# DBTITLE 1,Adicionando colunas necessárias

df_iw37n = df_iw37n.select(
    "*",  # mantém todas as colunas originais
    lit('PM').alias('TIPO_PRIORIDADE'),
    current_timestamp().alias('CREATED_DATE_CZ'),
    when(
        upper(col("Status_Operacao")).rlike("CONF|CNF"),
        1
    ).otherwise(0).alias('STATUS_OPERACAO_FLAG'),
    when(
        upper(col("Status_Operacao")).rlike("MREL|NEXE"),
        1
    ).otherwise(0).alias('MREL_FLAG'),
    when(
        col('SK_TIPO_ATIVIDADE_MANUTENCAO').isin(TIPO_PLANO),
        lit('Plano Manutenção')
    ).when(
        col('SK_TIPO_ATIVIDADE_MANUTENCAO').isin(TIPO_CORRETIVA),
        lit('Corretiva')
    ).otherwise(lit(None)).alias('DSC_TIPO_ATIVIDADE')
)

# COMMAND ----------

# DBTITLE 1,Organizando Colunas Finais
df_iw37n = df_iw37n.select(col('Ordem').cast(LongType()).alias('ORDEM'),
                                 col('Primeiro_data_inicio').alias('PRIMEIRA_DATA_INICIO'),
                                 col('Primeiro_hora_inicio').alias('PRIMEIRA_HORA_INICIO'),
                                 col('Primeiro_data_fim').alias('PRIMEIRA_DATA_FIM'),
                                 col('Primeiro_hora_fim').alias('PRIMEIRA_HORA_FIM'),
                                 col('Hora_inicio').alias('HORA_INICIO'),
                                 col('Data_Entrada').alias('DATA_ENTRADA'),
                                 col('Data_Modificacao').alias('DATA_MODIFICACAO'),
                                 col('Data_fim_real').alias('DATA_FIM_REAL'),
                                 col('Fim_real_hora').alias('HORA_FIM_REAL'),
                                 col('Data_referencia').alias('DATA_REFERENCIA'),
                                 col('Data_pedido').alias('DATA_PEDIDO'),
                                 col('Inicio_Base').alias('DATA_INICIO_BASE'),
                                 col('Confirmado_em').alias('DATA_CONFIRMADO_EM'),
                                 col('Data_lancamento').alias('DATA_LANCAMENTO'),
                                 col('Data_planejada').alias('DATA_PLANEJADA'),
                                 col('Operacao').cast(IntegerType()).alias('OPERACAO'),
                                 col('Chave_Controle').alias('CHAVE_CONTROLE'),
                                 col('Tipo_Ordem').alias('TIPO_ORDEM'),
                                 col('Status_Operacao').alias('STATUS_OPERACAO'),
                                 col('Status_usuario').alias('STATUS_USUARIO'),
                                 col('Status_sistema').alias('STATUS_SISTEMA'),
                                 col('Grp_Planj_PM').alias('GRUPO_PLANEJAMENTO_PM'),
                                 col('Cen_Cts_Responsavel').alias('CENTRO_CUSTO_RESPONSAVEL'),
                                 col('Moeda').alias('MOEDA'),
                                 col('Overhaul').alias('OVERHAUL'),
                                 col('Item').alias('ITEM'),
                                 col('Item_Manutencao').cast(LongType()).alias('ITEM_MANUTENCAO'),
                                 col('PEP_cabecalho_ordem').alias('PEP_CABECALHO_ORDEM'),
                                 col('Requisicao_Compras').alias('REQUISICAO_COMPRAS'),
                                 col('Sub_Operacao').cast(IntegerType()).alias('SUB_OPERACAO'),
                                 col('Codigo_ABC').alias('CODIGO_ABC'),
                                 col('Unidade_trabalho').alias('UNIDADE_TRABALHO'),
                                 col('Confirmacao').cast(LongType()).alias('CONFIRMACAO'),
                                 col('Plano_manutencao').alias('SK_PLANO_MANUTENCAO'),
                                 col('Centro_Custo').alias('CENTRO_CUSTO'),
                                 col('Tipo_ativ_manutencao').cast(IntegerType()).alias('TIPO_ATIVIDADE_MANUTENCAO'),
                                 col('SK_TIPO_ATIVIDADE_MANUTENCAO'),
                                 col('Trabalho').cast(DoubleType()).alias('TRABALHO'),
                                 col('Reserva_Rec_Compras').cast(IntegerType()).alias('RESERVA_REC_COMPRAS'),
                                 col('Conds_instal').cast(IntegerType()).alias('CONDICAO_INSTALACAO'),
                                 col('Conds_Instal_Operacao').cast(StringType()).alias('CONDICAO_INSTALACAO_OPERACAO'),
                                 col('Elemento_PEP').cast(IntegerType()).alias('ELEMENTO_PEP'),
                                 col('Fornecedor').cast(IntegerType()).alias('FORNECEDOR'),
                                 (when(col('Marc_eliminacao').isin('X', 'x'), True).otherwise(False)).cast(BooleanType()).alias('MARCACAO_ELIMINACAO'),
                                 col('Material').cast(IntegerType()).alias('MATERIAL'),
                                 col('Num_pedido').cast(IntegerType()).alias('NUM_PEDIDO'),
                                 col('Nota').cast(LongType()).alias('ID_NOTA'),
                                 col('Custos_tot_reais').cast(DoubleType()).alias('CUSTOS_TOTAIS_REAIS'),
                                 col('Custos_totais_plan').cast(DoubleType()).alias('CUSTOS_TOTAIS_PLANEJADOS'),
                                 col('Duracao_normal').cast(DoubleType()).alias('DURACAO_NORMAL'),
                                 col('Preco').cast(DoubleType()).alias('PRECO'),
                                 col('Quantidade').cast(DoubleType()).alias('QUANTIDADE'),
                                 col('Total_planejado').cast(DoubleType()).alias('TOTAL_PLANEJADO'),
                                 col('Total_real').cast(DoubleType()).alias('TOTAL_REAL'),
                                 col('Trabalho_previsto').cast(DoubleType()).alias('TRABALHO_PREVISTO'),
                                 col('Trabalho_real').cast(DoubleType()).alias('TRABALHO_REAL'),
                                 col('Num_Pessoal').cast(LongType()).alias('NUM_PESSOAL'),
                                 col('SK_PRIMEIRA_DATA_INICIO').cast(IntegerType()),
                                 col('SK_PRIMEIRA_HORA_INICIO').cast(IntegerType()),
                                 col('SK_PRIMEIRA_DATA_FIM').cast(IntegerType()),
                                 col('SK_PRIMEIRA_HORA_FIM').cast(IntegerType()),
                                 col('SK_HORA_INICIO').cast(IntegerType()),
                                 col('SK_DATA_FIM_REAL').cast(IntegerType()),
                                 col('SK_HORA_FIM_REAL').cast(IntegerType()),
                                 col('SK_DATA_ENTRADA').cast(IntegerType()),
                                 col('SK_DATA_MODIFICACAO').cast(IntegerType()),
                                 col('SK_DATA_REFERENCIA').cast(IntegerType()),
                                 col('SK_DATA_PEDIDO').cast(IntegerType()),
                                 col('SK_DATA_INICIO_BASE').cast(IntegerType()),
                                 col('SK_DATA_CONFIRMADO_EM').cast(IntegerType()),
                                 col('SK_DATA_LANCAMENTO').cast(IntegerType()),
                                 col('SK_DATA_PLANEJADA').cast(IntegerType()),
                                 col('CREATED_DATE_HZ'),
                                 col('CREATED_DATE_CZ'),
                                 col('STATUS_OPERACAO_FLAG'),
                                 col('MREL_FLAG'),
                                 col('DSC_TIPO_ATIVIDADE'),
                                 col('Data_MPA'),
                                 substring(date_format(col('Data_fim_real'),'yyyyMM'),1,6).alias('ANO_MES_RU'),
                                 (when(col('Local_Instalacao').isNull(), '-1').otherwise(col('Local_Instalacao'))).alias('SK_LOCAL_INSTALACAO'),
                                 (when(col('Txt_breve_operacao').isNull(), 'Não Informado').otherwise(col('Txt_breve_operacao'))).alias('TEXTO_BREVE_OPERACAO'),
                                 (when(col('Centro_trab_respons').isNull(), -1).otherwise(abs(hash(col('Centro_trab_respons'))))).alias('SK_CENTRO_TRABALHO_RESPONSAVEL').cast(IntegerType()),
                                 (when(col('Centro_trab_operacao').isNull(), -1).otherwise(abs(hash(col('Centro_trab_operacao'))))).alias('SK_CENTRO_TRABALHO_OPERACAO').cast(IntegerType()),
                                 col('CT_Filt'),
                                 col('Centro_trab_operacao'),
                                 (when( (col('TIPO_PRIORIDADE').isNull()) & (col('Propriedade').isNull()), -1).otherwise(abs(hash('TIPO_PRIORIDADE', 'Propriedade')))).alias('SK_PRIORIDADE').cast(IntegerType()),
                                 (when(col('Criado_por').isNull(), 'Não Informado').otherwise(col('Criado_por'))).alias('CRIADO_POR'),
                                 (when(col('Texto_Breve').isNull(), 'Não Informado').otherwise(col('Texto_Breve'))).alias('TEXTO_BREVE'),
                                 (when(col('Modificado_por').isNull(), 'Não Informado').otherwise(col('Modificado_por'))).alias('MODIFICADO_POR'),
                                 (when(col('Confirmado_por').isNull(), 'Não Informado').otherwise(col('Confirmado_por'))).alias('CONFIRMADO_POR'),
                                 (when(col('Texto_breve_material').isNull(), 'Não Informado').otherwise(col('Texto_breve_material'))).alias('TEXTO_BREVE_MATERIAL'),
                                 (when(col('Texto_Confirmacao').isNull(), 'Não Informado').otherwise(col('Texto_Confirmacao'))).alias('TEXTO_CONFIRMACAO'),
                                 (when(col('COD_UNIDADE_LAKE').isNull(), -1).otherwise(col('COD_UNIDADE_LAKE'))).cast(IntegerType()).alias('SK_UNIDADE'),
                                 (when(col('COD_SUBAREA_LAKE').isNull(), -1).otherwise(col('COD_SUBAREA_LAKE'))).cast(IntegerType()).alias('SK_SUBAREA'),
                                 (when(col('COD_AREA_LAKE').isNull(), -1).otherwise(col('COD_AREA_LAKE'))).cast(IntegerType()).alias('SK_AREA'),
                                 (when(col('DATA_MODIFICACAO').isNotNull(), col('DATA_MODIFICACAO')).otherwise(col('DATA_ENTRADA'))).cast(DateType()).alias('INCREMENTAL_DATE'),
                                 concat(col('Ordem').cast(StringType()), col('Operacao').cast(StringType()), when(col('Sub_Operacao').isNull(), '').otherwise(col('Sub_Operacao'))).cast(StringType()).alias('ID_ORDEM_OPERACAO'),
                                 'ORIGEM')                              



# COMMAND ----------

# DBTITLE 1,Criando Dataframe do Status Fechamento
# Guardando um DF específico para ser utilizado na célula de buscar o Status Fechamento 
# Contém os tratamentos acima porém não sendo afetado pelos filtros de Data de Congelamento etc
df_iw37n_status_fechamento = df_iw37n.select(
    col('ID_ORDEM_OPERACAO'),
    col('STATUS_OPERACAO').alias('STATUS_FECHAMENTO'),
    col('CREATED_DATE_HZ').alias('CREATED_DATE_HZ_FECHAMENTO')
).withColumn('STATUS_FECHAMENTO_FLAG',
             when(upper(col("STATUS_FECHAMENTO")).like("%CONF%") |
                  upper(col("STATUS_FECHAMENTO")).like("%CNF%"), 1)
             .otherwise(0))

# COMMAND ----------

# DBTITLE 1,Criando as colunas Data Congelamento Base e Data Fechamento Base 
# Define a janela de partição para classificar as datas dentro de cada ano e mês
window_dia_util = Window.partitionBy("AnoMes").orderBy("Data")

# Seleciona apenas dias úteis
df_dias_uteis = df_dimdata.where(col('FlagDiaUtil') == True).withColumn("rank", row_number().over(window_dia_util))

# Filtra para obter apenas o primeiro dia útil de cada mês de cada ano
primeiro_dia_util = df_dias_uteis.where(col("rank") == 1).drop("rank")

# Adiciona um dia ao primeiro dia útil para DATA_CONGELAMENTO_BASE
dia_seguinte_primeiro_dia_util = primeiro_dia_util.select(
    date_add(col('Data'), 2).alias('DATA_CONGELAMENTO_BASE'),
    col('AnoMes').alias('AnoMesCongelamento')
)

# Calcula o mês seguinte para DATA_FECHAMENTO_BASE
dia_seguinte_proximo_mes = primeiro_dia_util.select(  
    date_add(col('Data'), 2).alias('DATA_FECHAMENTO_BASE'),
    date_format(add_months(col('Data'), -1), 'yyyyMM').alias('AnoMesFechamento')
)

dia_seguinte_proximo_mes = dia_seguinte_proximo_mes.join(primeiro_dia_util, dia_seguinte_proximo_mes['AnoMesFechamento'] == primeiro_dia_util['AnoMes'], how="inner").select(dia_seguinte_proximo_mes['*'])

# Join com o dataset principal para acrescentar a coluna de data de congelamento da base
df_iw37n = df_iw37n.join(dia_seguinte_primeiro_dia_util, df_iw37n["ANO_MES_RU"] == dia_seguinte_primeiro_dia_util["AnoMesCongelamento"], how="left")

# Join com o dataset principal para acrescentar a coluna de data de fechamento da base
df_iw37n = df_iw37n.join(dia_seguinte_proximo_mes, df_iw37n["ANO_MES_RU"] == dia_seguinte_proximo_mes["AnoMesFechamento"], how="left")

# COMMAND ----------

# DBTITLE 1,Filtrando por Data Fechamento Base e recuperando o registro mais recente
# Filtrando pela Data de Congelamento
df_iw37n = df_iw37n.where(col("CREATED_DATE_HZ") <= col("DATA_FECHAMENTO_BASE"))

# Define a janela de partição para classificar as datas dentro de cada ano e mês
window_ultimo_registro = Window.partitionBy("ORDEM", "ANO_MES_RU").orderBy(desc("CREATED_DATE_HZ"))

df_iw37n_final = df_iw37n.withColumn("rank", dense_rank().over(window_ultimo_registro))

# Filtrando a base pela Data de Congelamento
df_iw37n_final = df_iw37n_final.where((col("rank") == 1))

# COMMAND ----------

# DBTITLE 1,Filtrando Dataset final
# Filtrando o dataset com as colunas Mrel e Tipo de Atividade
df_iw37n_final = df_iw37n_final.where(
  (
    (df_iw37n_final['MREL_FLAG'] == 0) &
    ~(df_iw37n_final['SK_TIPO_ATIVIDADE_MANUTENCAO'].isin('E100', 'E210','H209', 'H100'))
  )
)

# COMMAND ----------

# DBTITLE 1,DF de trabalho recebe a função de rank para ordenar periodos da mesma Ordem/Operacao
# Define a janela particionada exatamente como no código original, ordenada pelo mês
w = Window.partitionBy('Ordem', 'Operacao', coalesce(col('Sub_Operacao'), lit(0))).orderBy('ANO_MES_RU')

# Aplica o lag para pegar o Trabalho_real do período anterior e já calcula a diferença
df_iw37n_final_aux = df_iw37n_final.withColumn(
    'Trabalho_real_anterior', 
    lag('Trabalho_real', 1).over(w)
).withColumn(
    'Trabalho_real_calculado', 
    col('Trabalho_real') - coalesce(col('Trabalho_real_anterior'), lit(0))
).drop('Trabalho_real_anterior')

# COMMAND ----------

# DBTITLE 1,Select final completo
df_iw37n_final_aux = df_iw37n_final_aux.select('ORDEM',
                                               'PRIMEIRA_DATA_INICIO',
                                               'PRIMEIRA_HORA_INICIO',
                                               'PRIMEIRA_DATA_FIM',
                                               'PRIMEIRA_HORA_FIM',
                                               'HORA_INICIO',
                                               'DATA_ENTRADA',
                                               'DATA_MODIFICACAO',
                                               'DATA_FIM_REAL',
                                               'HORA_FIM_REAL',
                                               'DATA_REFERENCIA',
                                               'DATA_PEDIDO',
                                               'DATA_INICIO_BASE',
                                               'DATA_CONFIRMADO_EM',
                                               'DATA_LANCAMENTO',
                                               'DATA_PLANEJADA',
                                               col('Data_MPA').alias('DATA_MPA'),
                                               'OPERACAO',
                                               'CHAVE_CONTROLE',
                                               'TIPO_ORDEM',
                                               'STATUS_OPERACAO',
                                               'STATUS_USUARIO',
                                               'STATUS_SISTEMA',
                                               'GRUPO_PLANEJAMENTO_PM',
                                               'CENTRO_CUSTO_RESPONSAVEL',
                                               'MOEDA',
                                               'OVERHAUL',
                                               'ITEM',
                                               'ITEM_MANUTENCAO',
                                               'PEP_CABECALHO_ORDEM',
                                               'REQUISICAO_COMPRAS',
                                               'SUB_OPERACAO',
                                               'CODIGO_ABC',
                                               'UNIDADE_TRABALHO',
                                               'CONFIRMACAO',
                                               'SK_PLANO_MANUTENCAO',
                                               'CENTRO_CUSTO',
                                               'TIPO_ATIVIDADE_MANUTENCAO',
                                               'SK_TIPO_ATIVIDADE_MANUTENCAO',
                                               'TRABALHO',
                                               'RESERVA_REC_COMPRAS',
                                               'CONDICAO_INSTALACAO',
                                               'CONDICAO_INSTALACAO_OPERACAO',
                                               'ELEMENTO_PEP',
                                               'FORNECEDOR',
                                               'MARCACAO_ELIMINACAO',
                                               'MATERIAL',
                                               'NUM_PEDIDO',
                                               'ID_NOTA',
                                               'CUSTOS_TOTAIS_REAIS',
                                               'CUSTOS_TOTAIS_PLANEJADOS',
                                               'DURACAO_NORMAL',
                                               'PRECO',
                                               'QUANTIDADE',
                                               'TOTAL_PLANEJADO',
                                               'TOTAL_REAL',
                                               'TRABALHO_PREVISTO',
                                               'TRABALHO_REAL',
                                               col('Trabalho_real_calculado').alias('TRABALHO_REAL_CALCULADO'),
                                               'NUM_PESSOAL',
                                               'SK_PRIMEIRA_DATA_INICIO',
                                               'SK_PRIMEIRA_HORA_INICIO',
                                               'SK_PRIMEIRA_DATA_FIM',
                                               'SK_PRIMEIRA_HORA_FIM',
                                               'SK_HORA_INICIO',
                                               'SK_DATA_FIM_REAL',
                                               'SK_HORA_FIM_REAL',
                                               'SK_DATA_ENTRADA',
                                               'SK_DATA_MODIFICACAO',
                                               'SK_DATA_REFERENCIA',
                                               'SK_DATA_PEDIDO',
                                               'SK_DATA_INICIO_BASE',
                                               'SK_DATA_CONFIRMADO_EM',
                                               'SK_DATA_LANCAMENTO',
                                               'SK_DATA_PLANEJADA',
                                               'CREATED_DATE_HZ',
                                               'CREATED_DATE_CZ',
                                               'STATUS_OPERACAO_FLAG',
                                               'MREL_FLAG',
                                               'DSC_TIPO_ATIVIDADE',
                                               'ANO_MES_RU',
                                               'SK_LOCAL_INSTALACAO',
                                               'TEXTO_BREVE_OPERACAO',
                                               'SK_CENTRO_TRABALHO_RESPONSAVEL',
                                               'SK_CENTRO_TRABALHO_OPERACAO',
                                               col('CT_Filt').alias('SK_CENTRO_TRABALHO_FAMILIA'),
                                               'SK_PRIORIDADE',
                                               'CRIADO_POR',
                                               'TEXTO_BREVE',
                                               'MODIFICADO_POR',
                                               'CONFIRMADO_POR',
                                               'TEXTO_BREVE_MATERIAL',
                                               'TEXTO_CONFIRMACAO',
                                               'SK_UNIDADE',
                                               'SK_SUBAREA',
                                               'SK_AREA',
                                               'INCREMENTAL_DATE',
                                               'ID_ORDEM_OPERACAO',
                                               'DATA_FECHAMENTO_BASE',
                                               'ORIGEM')


# COMMAND ----------

df_iw37n_final_aux = df_iw37n_final_aux.withColumn('ANO_MES_PARTICAO', substring(col('SK_DATA_FIM_REAL'),1,6))

# Reparticiona baseado na chave de gravação para evitar arquivos pequenos (small files)
df_iw37n_final_aux = df_iw37n_final_aux.repartition('ANO_MES_PARTICAO')

# COMMAND ----------

# DBTITLE 1,Gravando os dados na CZ
path_write = '/mnt/conzumezone/Brazil/Supply/Producao/Manutencao/FatoKpisManutencaoRU'
file_type = 'parquet'
write_mode_cz = 'overwrite'
utc_timezone = 'America/Sao_Paulo'

write_dataframe(df_iw37n_final_aux, write_mode_cz, path_write, file_type, utc_timezone,'ANO_MES_PARTICAO')
