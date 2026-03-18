# Databricks notebook source
# DBTITLE 1,Executa o General_Functions
# MAGIC %run /Brasil/Functions/GeneralFunctions

# COMMAND ----------

# DBTITLE 1,Bibliotecas
from pyiris.ingestion.config.file_system_config import FileSystemConfig
from pyiris.infrastructure import Spark
from pyiris.ingestion.load import LoadService, FileWriter
from pyiris.ingestion.extract import FileReader

# COMMAND ----------

# DBTITLE 1,Iniciando variáveis
_now = date.today()
_first_day_month = trunc(current_date(),'MM')
_last_day_month_2 = last_day(add_months(current_date(),2))
_previous_year = last_day(add_months(current_date(),-12))
_first_day_previous_year = trunc(_previous_year,'MM')
_spark_iris = Spark()

tam_monitoramento = ['H201','E201','H203','E203','H206', 'E207','H207','E208']
tam_inspecao = ['H600','E040','H601','E045','H602','E050','H603','E055','H606','E060','H301','E301','H604','E701','H605','E702','H307','E306','H306','E309','H302','E303','H304','E307']

# COMMAND ----------

# DBTITLE 1,Obtendo datasets
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
df_unidades_hz = df_unidades_hz.select(
    col('COD_UNIDADE_LAKE').cast(IntegerType()),
    col('SAP_CODIGO').alias('Centro_planejamento'),
    col('HANA_CODIGO').alias('SAP_CODIGO_HANA'),
    col('CREATED_DATE_HZ')
)
# Filtrar registros com valores não nulos
df_unidades_hz_hana = df_unidades_hz.where(col('SAP_CODIGO_HANA').isNotNull()).alias("hana")
df_unidades_hz_ecc = df_unidades_hz.where(col('Centro_planejamento').isNotNull()).alias("ecc")
# Realizar o join unidades ecc e hana
df_unidades_hz_join = df_unidades_hz_ecc.join(
    df_unidades_hz_hana,
    on="COD_UNIDADE_LAKE",
    how="left"
).select(
    col("ecc.COD_UNIDADE_LAKE"),
    col("ecc.Centro_planejamento"),
    coalesce(col("hana.SAP_CODIGO_HANA"), col("ecc.SAP_CODIGO_HANA")).alias("SAP_CODIGO_HANA"),
    col("ecc.CREATED_DATE_HZ")
)
df_unidades = select_max_update_date(df_unidades_hz_join, 'Centro_planejamento', 'CREATED_DATE_HZ').drop('CREATED_DATE_HZ')

# Obtendo os dados ddo DePara de Ordem do Hana
df_depara_ordem_hz = FileReader(table_id='depara_ordem_hz', data_lake_zone='historyzone', country='Brazil', path='Supply/Producao/Manutencao/DeparaOrdemSapHana', format='parquet').consume(spark=_spark_iris).select('ORDEM_HANA', 'ORDEM_ECC', 'ITEM_MANUTENCAO_ECC', 'DATA_PLANEJADA_ECC', 'SK_PLANO_MANUTENCAO_ITEM', 'SK_PLANO_MANUTENCAO', 'ITEM', 'ITEM_MANUTENCAO_HANA','NOTA_ECC','DATA_CRIACAO_NOTA')

df_depara_ordem_hz = df_depara_ordem_hz.withColumn('DATA_NOTA_DEPARA',col('DATA_CRIACAO_NOTA')).drop('DATA_CRIACAO_NOTA')

#Obtendo os dados da FatoNotasManutencao
df_notas_cz = FileReader(table_id='df_notas', data_lake_zone='consumezone', country='Brazil', path='Supply/Producao/Manutencao/FatoNotasManutencaoSap', format='parquet').consume(spark=_spark_iris)
df_notas_cz = df_notas_cz.groupBy('ORDEM').agg(min('DATA_CRIACAO_NOTA')).withColumnRenamed('min(DATA_CRIACAO_NOTA)','DATA_CRIACAO_NOTA')

#Obtendo os dados da FatoKpisManutencaoP3m
df_p3m_cz = FileReader(table_id='df_notas', data_lake_zone='consumezone', country='Brazil', path='Supply/Producao/Manutencao/FatoKpisManutencaoP3m', format='parquet').consume(spark=_spark_iris)

# COMMAND ----------

# DBTITLE 1,Obtendo dataset da IW37N
#Obtendo os dados da iw37n para gerar a fato
df_iw37n_hz = FileReader(table_id='iw37n', data_lake_zone='historyzone', country='Brazil', path='SAPSupply/IW37N', format='parquet').consume(spark=_spark_iris)
df_iw37n_hz = select_max_updated_date_multi_columns(df_iw37n_hz, ['Ordem', 'Operacao', 'Sub_Operacao'])

df_iw37n_hz = df_iw37n_hz.dropDuplicates(["Ordem", "Operacao", "Sub_Operacao", "CREATED_DATE_HZ"])

# Cria coluna UPDATE_ORDER para ordenar as ordens pelo registro mais atualizado
df_iw37n_hz = df_iw37n_hz.withColumn('UPDATE_ORDER', split(col("File"), "_").getItem(0))\
                         .withColumn('SK_TIPO_ATIVIDADE_MANUTENCAO', concat(lit('E'),col('Tipo_ativ_manutencao').cast(IntegerType())))

# Determina como os dados serão organizados para realizar o Rank
win = Window.partitionBy(col('Ordem')).orderBy(col('UPDATE_ORDER').desc(), col('CREATED_DATE_HZ').desc())
df_iw37n_hz = df_iw37n_hz.withColumn('Rank', rank().over(win))
df_iw37n_rank = df_iw37n_hz.where(col('Rank') == 1)

# COMMAND ----------


#Obtendo os dados da COBRB 
df_cobrb_hz = FileReader(table_id='cobrb', data_lake_zone='historyzone', country='Brazil', path='Aurora/cobrb', format='parquet').consume(spark=_spark_iris)

# Selecionar apenas as colunas necessárias e filtrar por tipo de objeto OR
df_cobrb_hz = df_cobrb_hz.filter("__operation_type <> 'X'")\
                         .filter(col("OBJNR").startswith("OR"))\
                         .select(
                             "OBJNR",
                             "KOSTL",  # Centro de Custo
                             "PS_PSP_PNR",  # Elemento PEP
                             "CREATED_DATE_HZ"
                         )

# Pegar o registro mais recente por OBJNR
df_cobrb = select_max_update_date(df_cobrb_hz, 'OBJNR', 'CREATED_DATE_HZ').drop('CREATED_DATE_HZ')

# COMMAND ----------

# DBTITLE 1,Obtendo dataset da IP18
df_ip18 = FileReader(table_id='ip18_cz', data_lake_zone='consumezone', country='Brazil', path='Supply/Producao/Manutencao/FatoPlanoManutencaoSap/',format='parquet').consume(spark=_spark_iris)


df_ip18 = df_ip18.select(
  col('DESC_ITEM_MANUTENCAO'),
  col('ITEM_MANUTENCAO').cast(IntegerType()),
  col('ORIGEM'),
  col('SK_RISCO'),
  col('SK_IMPACTO_RISCO')
)

# COMMAND ----------

# DBTITLE 1,Obtendo os dados da IW37N - SAP HANA
#Obtendo os dados da iw37n HANA para gerar a fato
df_iw37n_hz_hana = FileReader(table_id='iw37n', data_lake_zone='historyzone', country='Brazil', path='Aurora/IW37N', format='parquet').consume(spark=_spark_iris)

df_iw37n_hz_hana = df_iw37n_hz_hana.withColumn("Primeiro_hora_inicio", lit(None)) \
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
    .withColumn("Item_ReqC", lit(None)) \
    .withColumn("Material", lit(None)) \
    .withColumn("Num_pedido", lit(None)) \
    .withColumn("Overhaul", lit(None)) \
    .withColumn("PEP_cabecalho_ordem", lit(None)) \
    .withColumn("Preco", lit(None)) \
    .withColumn("Quantidade", lit(None)) \
    .withColumn("Reserva_Rec_Compras", lit(None)) \
    .withColumn("Subnumero", lit(None)) \
    .withColumn("Texto_breve_material", lit(None)) \
    .withColumn("Unidade_preco", lit(None)) \
    .withColumn("Nome_empregado", lit(None)) \
    .withColumn("Confirmado_por", lit(None))

df_iw37n_hz_hana = df_iw37n_hz_hana.select(
    col('MAINTENANCE_ORDER').alias('Ordem'),
    col('OPERATION_NUMBER_FOR_PM').alias('Operacao'),
    col('SUB_OPERATION_NUMBER_FOR_PM').alias('Sub_Operacao'),
    col('CONTROL_KEY').alias('Chave_Controle'),
    col('FUNCTIONAL_LOCATION').alias('Id_Local_Instalacao'),
    col('FUNCTIONAL_LOCATION_TEXT').alias('Local_Instalacao'),
    col('MAINTENANCE_PLANT').alias('Centro_Planejamento'),
    col('EARLIEST_START_DATE').alias('Primeiro_data_inicio'),
    col('CREATED_ON').alias('Data_Entrada'),
    col('LAST_CHANGED_ON').alias('Data_Modificacao'),
    col('ACTUAL_EXECUTION_END_DATE').alias('Data_fim_real'),
    col('ORDER_TYPE').alias('Tipo_Ordem'),
    col('SUB_OPERATION_NUMBER_FOR_PM_TEXT').alias('Txt_breve_operacao'),
    col('PRIORITY').alias('Propriedade'),
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
    col('MAINTENANCE_ORDER_TEXT').alias('Texto_Breve'),
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
    "Item_ReqC",
    "Material",
    "Num_pedido",	
    "Overhaul",	            
    "PEP_cabecalho_ordem",	
    "Preco",	
    "Quantidade",
    "Reserva_Rec_Compras",
    "Subnumero",	
    "Texto_breve_material",
    "Unidade_preco",	      
    ).withColumn('SK_TIPO_ATIVIDADE_MANUTENCAO', concat(lit('H'),col('Tipo_ativ_manutencao').cast(IntegerType())))

df_iw37n_hz_hana = select_max_updated_date_multi_columns(df_iw37n_hz_hana, ['Ordem', 'Operacao', 'Sub_Operacao'])

df_iw37n_hz_hana = df_iw37n_hz_hana.dropDuplicates(["Ordem", "Operacao", "Sub_Operacao", "CREATED_DATE_HZ"])

# COMMAND ----------

# DBTITLE 1,Adaptando colunas do HANA utilizando o depara de Ordens
# Realizando o join entre os dataframes df_iw37n_hz_hana e df_depara_ordem_hz
df_iw37n_hz_hana = df_iw37n_hz_hana.join(df_depara_ordem_hz, df_iw37n_hz_hana['Ordem'] == df_depara_ordem_hz['ORDEM_HANA'], how='left')

# Atualizando as colunas com os valores de df_depara_ordem_hz quando necessário
df_iw37n_hz_hana = df_iw37n_hz_hana.withColumn(
    "Item_Manutencao", 
    when((col('Item_Manutencao').isNull()) | (col('Item_Manutencao') == ''), col('ITEM_MANUTENCAO_HANA')).otherwise(col('Item_Manutencao'))
).withColumn(
    "Data_planejada", 
    when((col('Data_planejada').isNull()) | (col('Data_planejada') == ''), col('DATA_PLANEJADA_ECC')).otherwise(col('Data_planejada'))
).withColumn(
    "Item", 
    col('ITEM')
).withColumn(
    "Plano_manutencao", 
    when((col('Plano_manutencao').isNull()) | (col('Plano_manutencao') == ''), col('SK_PLANO_MANUTENCAO')).otherwise(col('Plano_manutencao'))
).drop(
    'ORDEM_HANA', 'ORDEM_ECC', 'ITEM_MANUTENCAO_ECC', 'DATA_PLANEJADA_ECC', 'SK_PLANO_MANUTENCAO', 'SK_PLANO_MANUTENCAO_ITEM', 'ITEM_MANUTENCAO_HANA','NOTA_ECC'
)

# COMMAND ----------

# DBTITLE 1,Join com Unidades e Union do ECC e HANA
df_iw37n_rank = df_iw37n_rank.join(df_unidades, ['Centro_planejamento'], how='left')

df_iw37n_hz_hana = df_iw37n_hz_hana.join(df_unidades, df_iw37n_hz_hana['Centro_planejamento'] == df_unidades['SAP_CODIGO_HANA'], how='left').drop(df_unidades.Centro_planejamento)

df_iw37n_rank = df_iw37n_rank.withColumn('ORIGEM', lit('ECC'))\
  .unionByName(
    df_iw37n_hz_hana.withColumn('ORIGEM', lit('HANA')),
    allowMissingColumns = True
    )

# COMMAND ----------

# DBTITLE 1,Criação de Área e Subárea - dados BRASIL
# Separando um dataframe com dados somente do Brasil, pois já estão no padrão novo de nomenclatura dos locais de instalação
df_dados_brasil = df_iw37n_rank.where(col('Centro_planejamento').like('BR%'))

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
df_dados_abc = df_iw37n_rank.where(~col('Centro_planejamento').like('BR%'))

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

# DBTITLE 1,Busca Area e Subarea
df_iw37n = df_iw37n_inicial.join(df_area, df_iw37n_inicial['AREA'] == df_area['AREA_SAP'], how='left')
df_iw37n = df_iw37n.join(df_subarea, df_iw37n['SUBAREA'] == df_subarea['SUBAREA_SAP'], how='left')

# COMMAND ----------

# DBTITLE 1,Adicionando novas colunas
df_iw37n = df_iw37n.withColumn('ORDEM',col('Ordem').cast(LongType()))\
                   .withColumn('OPERACAO_CONFIRMADA',when(col('Status_sistema').like('%CONF%'),1).otherwise(0))\
                   .withColumn('ORDEM_ELIMINADA',when((col('Status_sistema').like('%MREL%')) | (col('Status_sistema').like('%NEXE%')),1).otherwise(0))\
                   .withColumn('ORDEM_ENCERRADA',when(col('Status_sistema').like('%ENTE%'),1).otherwise(0))\
                   .withColumn('TIPO_EXECUCAO',when((col('OPERACAO_CONFIRMADA') == 1) & (col('Data_fim_real') > col('Inicio_Base')),0)
                                              .when(col('OPERACAO_CONFIRMADA') == 1,1)
                                              .when(col('Inicio_Base') < _now,2)
                                              .otherwise(3))

# COMMAND ----------

# DBTITLE 1,Join com a tabela de Notas
df_iw37n = df_iw37n.join(df_notas_cz,df_iw37n['ORDEM'] == df_notas_cz['ORDEM'],how='left')\
                   .select(df_iw37n['*'],df_notas_cz['DATA_CRIACAO_NOTA'])\
                   .withColumn('DATA_CRIACAO_NOTA',coalesce(regexp_replace(col('DATA_NOTA_DEPARA'),'-','').cast(StringType())
                                                           ,regexp_replace(col('DATA_CRIACAO_NOTA'),'-','').cast(StringType())))\
                   .withColumn('SK_DATA_CRIACAO_NOTA', when( (col('DATA_CRIACAO_NOTA').isNull()) | (col('DATA_CRIACAO_NOTA') == '0'), '-1').otherwise(col('DATA_CRIACAO_NOTA')))

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
                   'Data_planejada',
                   'Data_MPA',
                   'DATA_CRIACAO_NOTA']

df_iw37n = df_iw37n.replace('000000', None, subset=colunas_de_horas)\
                   .replace('00000000', None, subset=colunas_de_data)

# COMMAND ----------

# DBTITLE 1,Criando SK para colunas de horas
df_iw37n = df_iw37n.withColumn('SK_PRIMEIRA_HORA_INICIO', when( col('Primeiro_hora_inicio').isNull(), '-1').otherwise(substring(col('Primeiro_hora_inicio'), 0, 4)) )\
                   .withColumn('SK_PRIMEIRA_HORA_FIM', when( col('Primeiro_hora_fim').isNull(), '-1').otherwise(substring(col('Primeiro_hora_fim'), 0, 4)) )\
                   .withColumn('SK_HORA_INICIO', when( col('Hora_inicio').isNull(), '-1').otherwise(substring(col('Hora_inicio'), 0, 4)) )\
                   .withColumn('SK_HORA_FIM_REAL', when( col('Fim_real_hora').isNull(), '-1').otherwise(substring(col('Fim_real_hora'), 0, 4)) )

# COMMAND ----------

# DBTITLE 1,Criando SK para colunas de data
df_iw37n = df_iw37n.withColumn('SK_PRIMEIRA_DATA_INICIO', when( (col('Primeiro_data_inicio').isNull()) | (col('Primeiro_data_inicio') == '0'), '-1').otherwise(col('Primeiro_data_inicio'))  )\
                   .withColumn('SK_DATA_ENTRADA', when( (col('Data_Entrada').isNull()) | (col('Data_Entrada') == '0'), '-1').otherwise(col('Data_Entrada'))  )\
                   .withColumn('SK_DATA_MODIFICACAO', when( (col('Data_Modificacao').isNull()) | (col('Data_Modificacao') == '0'), '-1').otherwise(col('Data_Modificacao'))  )\
                   .withColumn('SK_DATA_FIM_REAL', when( (col('Data_fim_real').isNull()) | (col('Data_fim_real') == '0'), '-1').otherwise(col('Data_fim_real'))  )\
                   .withColumn('SK_PRIMEIRA_DATA_FIM', when( (col('Primeiro_data_fim').isNull()) | (col('Primeiro_data_fim') == '0'), '-1').otherwise(col('Primeiro_data_fim'))  )\
                   .withColumn('SK_DATA_REFERENCIA', when( (col('Data_referencia').isNull()) | (col('Data_referencia') == '0'), '-1').otherwise(col('Data_referencia'))  )\
                   .withColumn('SK_DATA_PEDIDO', when( (col('Data_pedido').isNull()) | (col('Data_pedido') == '0'), '-1').otherwise(col('Data_pedido'))  )\
                   .withColumn('SK_DATA_INICIO_BASE', when( (col('Inicio_Base').isNull()) | (col('Inicio_Base') == '0'), '-1').otherwise(col('Inicio_Base'))  )\
                   .withColumn('SK_DATA_CONFIRMADO_EM', when( (col('Confirmado_em').isNull()) | (col('Confirmado_em') == '0'), '-1').otherwise(col('Confirmado_em'))  )\
                   .withColumn('SK_DATA_LANCAMENTO', when( (col('Data_lancamento').isNull()) | (col('Data_lancamento') == '0'), '-1').otherwise(col('Data_lancamento'))  )\
                   .withColumn('SK_DATA_PLANEJADA', when( (col('Data_planejada').isNull()) | (col('Data_planejada') == '0'), '-1').otherwise(col('Data_planejada')))\
                   .withColumn('SK_DATA_MPA', when( (col('Data_MPA').isNull()) | (col('Data_MPA') == '0'), '-1').otherwise(col('Data_MPA')))\
                   .withColumn('SK_DATA_CRIACAO_NOTA', when( (col('DATA_CRIACAO_NOTA').isNull()) | (col('DATA_CRIACAO_NOTA') == '0'), '-1').otherwise(col('DATA_CRIACAO_NOTA')))

# COMMAND ----------

# DBTITLE 1,Formatando colunas de data e hora para os Data Scientists
# Criando variáveis para regex de hora e formato de data
data_format = 'yyyyMMdd'
string_regex_pattern = '(\\d{2})(\\d{2})(\\d{2})'
string_regex_replacement = '$1:$2:$3'

# Formatando as colunas de data e hora a pedido dos Data Scientist
for columnData in colunas_de_data:
  df_iw37n = df_iw37n.withColumn(columnData, when(col(columnData).isNull(), None).otherwise( to_date(col(columnData), data_format ) ))

for columnHour in colunas_de_horas:
  df_iw37n = df_iw37n.withColumn(columnHour, when(col(columnHour).isNull(), None).otherwise(regexp_replace(col(columnHour), string_regex_pattern, string_regex_replacement)))

# COMMAND ----------

# DBTITLE 1,Ajustando a Prioridade
df_iw37n = df_iw37n.withColumn('TIPO_PRIORIDADE', lit('PM'))

# COMMAND ----------

# DBTITLE 1,Criação da flag expurgo
# leitura expurgo
df_expurgo = FileReader(table_id='expurgo_cz', data_lake_zone='consumezone', country='Brazil', path='Supply/Producao/Manutencao/AuxMpaExpurgoSharepoint/',format='parquet').consume(spark=_spark_iris)

# COMMAND ----------

df_expurgo = df_expurgo.select(
  "NUM_ORDEM",
  "DSC_MOTIVO"
)


# COMMAND ----------

# join expurgo

df_iw37n = df_iw37n.join(df_expurgo, df_iw37n["ORDEM"] == df_expurgo["NUM_ORDEM"], "left")


# COMMAND ----------

df_iw37n = df_iw37n.withColumn("FLAG_EXPURGO", when(col("NUM_ORDEM").isNull(), 0).otherwise(1))

# COMMAND ----------

# DBTITLE 1,Criação do campo Status_Planejamento_MPA
df_iw37n = df_iw37n.withColumn('STATUS_PLANEJAMENTO_MPA',
                                 when((col('OPERACAO_CONFIRMADA') == 0) & (col('FLAG_EXPURGO') == 0) & (col('ORDEM_ENCERRADA') == 0) & (col('ORDEM_ELIMINADA') == 0), 
                                     when(col('Data_MPA') < _first_day_month,0)\
                                     .when(col('Inicio_Base') < _now,1)\
                                     .when(col('Data_MPA') < col('Inicio_Base'),2)\
                                     .otherwise(3))
                                 .when((col('OPERACAO_CONFIRMADA') == 1)
                                         & (substring(col('SK_DATA_FIM_REAL'),1,6).cast(IntegerType())>substring(col('SK_DATA_MPA'),1,6).cast(IntegerType())),4)\
                                 .when(col('OPERACAO_CONFIRMADA') == 1,5)\
                                 .when((col('ORDEM_ENCERRADA') == 1) & (col('OPERACAO_CONFIRMADA') == 0),6)\
                                 .when(col('FLAG_EXPURGO') == 1,7)\
                                 .when(col('ORDEM_ELIMINADA') == 1,8)\
                                 .otherwise(9))

# COMMAND ----------

df_p3m_filt = df_p3m_cz.withColumn('Data_inicio_base_p3m',to_date(col('SK_DATA_INICIO_BASE').cast(StringType()),'yyyyMMdd'))\
                     .filter((col('Data_inicio_base_p3m') >= _first_day_month) & (col('Data_inicio_base_p3m') <= _last_day_month_2))\
                     .select('ORDEM').distinct()


# COMMAND ----------

# DBTITLE 1,Join com a tabela P3m
df_iw37n = df_iw37n.join(df_p3m_filt, df_iw37n["ORDEM"] == df_p3m_filt["ORDEM"], "left")\
                   .select(df_iw37n['*'],df_p3m_filt['ORDEM'].alias('Ordem_P3m'))

# COMMAND ----------

# DBTITLE 1,Criação da coluna Fora_P3m
df_iw37n = df_iw37n.withColumn('FORA_P3M',when(col('Ordem_P3m').isNotNull(),0)\
                                         .when(col('Inicio_Base') < _first_day_month,1)\
                                         .when((col('Inicio_Base') >= _first_day_month) & (col('Inicio_Base') <= _last_day_month_2),2)\
                                         .otherwise(3))

# COMMAND ----------

# DBTITLE 1,Criação da coluna Origem_Execucao
df_iw37n = df_iw37n.withColumn('ORIGEM_EXECUCAO',when((col('OPERACAO_CONFIRMADA') == 1) & (col('FORA_P3M') == 0),0)\
                                                .when((col('OPERACAO_CONFIRMADA') == 1) & (col('FORA_P3M') != 0) & (col('Tipo_Ordem') =='ZM01'),1)\
                                                .when((col('OPERACAO_CONFIRMADA') == 1) & (col('FORA_P3M') != 0) 
                                                    & ((col('Data_Entrada') == col('Data_fim_real')) | (col('Data_Entrada') == date_add(col('Data_fim_real'),-1))),2)\
                                                .when((col('OPERACAO_CONFIRMADA') == 1) & (col('FORA_P3M') != 0) 
                                                        & (substring(col('SK_DATA_FIM_REAL'),1,6) == substring(col('SK_DATA_ENTRADA'),1,6)),3)\
                                                .when(col('OPERACAO_CONFIRMADA') == 1,4)\
                                                .otherwise(5))

# COMMAND ----------

# DBTITLE 1,Select no Rollout
history_file_type = 'parquet'
utc_timezone = 'America/Sao_Paulo'

system = 'SAPSupply'
dataset_name = 'ListaRolloutSAP'

path_history_zone = path_builder(layer = 'historyzone', system = system, dataset_name = dataset_name)

rollout_df_history = dataframe_builder(path_history_zone, history_file_type)

# COMMAND ----------

# DBTITLE 1,Filtragem de colunas do Rollout
rollout_df_history = rollout_df_history.select(
  "COD_SAP",
  "SK_UNIDADE",
  "SIG_PLANTA",
  "ANO_ROLLOUT",
  "MES_ROLLOUT",
  "DATA_ROLLOUT",
  "DATA_MODIFICACAO_SHAREPOINT",
  "DATA_CRIACAO_SHAREPOINT",
  "DATA_CRIACAO_PZ",
)

# COMMAND ----------

# DBTITLE 1,Left join da tabela final com o Rollout
df_iw37n = df_iw37n.join(rollout_df_history,((df_iw37n['COD_UNIDADE_LAKE'] == rollout_df_history['SK_UNIDADE']) & (df_iw37n['ORIGEM'] == 'ECC')),'left').select(df_iw37n['*'],rollout_df_history['DATA_ROLLOUT'])

# COMMAND ----------

# DBTITLE 1,Criação da coluna de filtragem do Smart Planning
df_iw37n = df_iw37n.withColumn('FLAG_SMART_PLANNING',when((((~col('Status_sistema').like('%CONF%')) 
                                                            & (~col('Status_sistema').like('%ENCE%')) 
                                                            & (~col ('Status_sistema').like('%ENTE%'))) 
                                                          | (col('Inicio_Base')>= _first_day_previous_year) 
                                                          | (col('Data_MPA')>= _first_day_previous_year)) 
                                                          & (col('Tipo_Ordem')!='ZM05') & (col('Tipo_Ordem')!='ZM09')
                                                          & (~col('Status_sistema').like('%NEXE%')) 
                                                          & (~col('Status_sistema').like('%MREL%'))
                                                          & (col('DATA_ROLLOUT').isNull()),1).otherwise(0))

# COMMAND ----------

df_iw37n = df_iw37n.withColumn('FLAG_MTFS',when(col('DATA_ROLLOUT').isNull(),1)\
                                          .when(((col('ORIGEM') == 'ECC') & (coalesce(col('Confirmado_em'),col('DATA_FIM_REAL')) < col('DATA_ROLLOUT')))
                                               |((col('ORIGEM') == 'HANA') & (coalesce(col('Confirmado_em'),col('DATA_FIM_REAL')) >= col('DATA_ROLLOUT'))),1).otherwise(0))

# COMMAND ----------

df_iw37n = df_iw37n.withColumn('FLAG_AOT',when((col('Trabalho_real') > 0) 
                                             & (coalesce(col('Confirmado_em'),col('DATA_FIM_REAL')) >= _first_day_previous_year),
                                               when(col('DATA_ROLLOUT').isNull(),1)\
                                              .when(((col('ORIGEM') == 'ECC') & (coalesce(col('Confirmado_em'),col('DATA_FIM_REAL')) < col('DATA_ROLLOUT')))
                                                    | ((col('ORIGEM') == 'HANA') & (coalesce(col('Confirmado_em'),col('DATA_FIM_REAL')) >= col('DATA_ROLLOUT'))),1)\
                                                .otherwise(0)).otherwise(0))

# COMMAND ----------

df_iw37n = df_iw37n.withColumn('FLAG_PMPDM',when(col('SK_TIPO_ATIVIDADE_MANUTENCAO').isin(tam_monitoramento),
                                                 when(col('DATA_ROLLOUT').isNull(),1)\
                                                 .when(((col('ORIGEM') == 'ECC') & (col('Data_Entrada') < col('DATA_ROLLOUT')))
                                                    |((col('ORIGEM') == 'HANA') & (col('Data_Entrada') >= col('DATA_ROLLOUT'))),1).otherwise(0))\
                                            .when(col('SK_TIPO_ATIVIDADE_MANUTENCAO').isin(tam_inspecao),
                                                  when(col('DATA_ROLLOUT').isNull(),1)\
                                                  .when(((col('ORIGEM') == 'ECC') & (coalesce(col('Confirmado_em'),col('DATA_FIM_REAL')) < col('DATA_ROLLOUT')))
                                                    |((col('ORIGEM') == 'HANA') & (coalesce(col('Confirmado_em'),col('DATA_FIM_REAL')) >= col('DATA_ROLLOUT'))),1).otherwise(0))\
                                                    .otherwise(0))

# COMMAND ----------

# DBTITLE 1,Select na dim de TAM
df_tipo_atividade = (spark.read.format('parquet').load('/mnt/consumezone/Brazil/Supply/Producao/Manutencao/DimTipoAtividadeOrdensSap'))

atividade_corretiva_lista = df_tipo_atividade.filter(col('TIPO_PLANO') == 'Corretiva').withColumn('SK_TIPO_ATIVIDADE',col('SK_TIPO_ATIVIDADE').cast(StringType())).select('SK_TIPO_ATIVIDADE')

atividade_corretiva_lista = atividade_corretiva_lista.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

# DBTITLE 1,Criação da auxiliar para calcular MTFS
df_mtfs_aux = df_iw37n.filter(col('OPERACAO_CONFIRMADA') == 1)\
                      .filter(col('SK_TIPO_ATIVIDADE_MANUTENCAO').isin(atividade_corretiva_lista))\
                      .groupby('ORDEM').agg( min('DATA_CRIACAO_NOTA').alias('DATA_NOTA_MIN')
                                            , min('DATA_ENTRADA').alias('DATA_ENTRADA_MIN')
                                            , max('Confirmado_em').alias('DATA_CONFIRMADO_EM_MAX')
                                            , max('DATA_FIM_REAL').alias('DATA_FIM_REAL_MAX')
                                            , min('Operacao').alias('OPERACAO_MIN')
                                            , min('Sub_Operacao').alias('SUB_OPERACAO_MIN'))

# COMMAND ----------

# DBTITLE 1,Calculo de MTFS
df_mtfs_aux = df_mtfs_aux.withColumn('MTFS',datediff(coalesce('DATA_CONFIRMADO_EM_MAX','DATA_FIM_REAL_MAX'),coalesce('DATA_NOTA_MIN','DATA_ENTRADA_MIN')))

# COMMAND ----------

# DBTITLE 1,Join do DF final
df_iw37n = df_iw37n.join(df_mtfs_aux,((df_iw37n['ORDEM'] == df_mtfs_aux['ORDEM']) 
                                      & ((df_iw37n['Operacao'] == df_mtfs_aux['OPERACAO_MIN']) 
                                           | (df_iw37n['Operacao'].isNull() & df_mtfs_aux['OPERACAO_MIN'].isNull()))
                                      & ((df_iw37n['Sub_Operacao'] == df_mtfs_aux['SUB_OPERACAO_MIN']) 
                                          | (df_iw37n['Sub_Operacao'].isNull() & df_mtfs_aux['SUB_OPERACAO_MIN'].isNull()))),'left')\
                                      .select(df_iw37n['*'],df_mtfs_aux['MTFS'])

# COMMAND ----------

# Criar coluna OBJNR concatenando OR com número da ordem
df_iw37n = df_iw37n.withColumn("OBJNR", concat(lit("OR"), lpad(col("ORDEM").cast(StringType()), 12, "0")))

# Join com COBRB
df_iw37n = df_iw37n.join(
    df_cobrb,
    on="OBJNR",
    how="left"
)

# Atualizar colunas com valores da COBRB (prioridade para COBRB.KOSTL no Centro de Custo)
df_iw37n = df_iw37n.withColumn(
    "Centro_Custo",
    coalesce(
        when((col("KOSTL") == "") | (col("KOSTL").isNull()), None).otherwise(col("KOSTL")),
        col("Centro_Custo")
    )
).withColumn(
    "Elemento_PEP", 
    coalesce(col("PS_PSP_PNR"), col("Elemento_PEP"))
).drop("OBJNR", "KOSTL", "PS_PSP_PNR")

# COMMAND ----------

# DBTITLE 1,Join DF final e IP18
df_iw37n = df_iw37n.join(df_ip18, ['ITEM_MANUTENCAO', 'ORIGEM'], how='left')

# COMMAND ----------

# DBTITLE 1,Organizando Colunas Finais
df_iw37n_final = df_iw37n.select(col('ORDEM'),
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
                                 col('DATA_CRIACAO_NOTA'),
                                 col('Data_MPA').alias('DATA_MPA'),
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
                                 col('FLAG_EXPURGO'),
                                 col('OPERACAO_CONFIRMADA'),
                                 col('ORDEM_ENCERRADA'),
                                 col('ORDEM_ELIMINADA'),
                                 col('STATUS_PLANEJAMENTO_MPA'),
                                 col('FORA_P3M'),
                                 col('TIPO_EXECUCAO'),
                                 col('ORIGEM_EXECUCAO'),
                                 col('MTFS'),
                                 col('FLAG_SMART_PLANNING'),
                                 col('SK_RISCO').cast(StringType()),
                                 col('SK_IMPACTO_RISCO').cast(IntegerType()),
                                 col('FLAG_MTFS'),
                                 col('FLAG_AOT'),
                                 col('FLAG_PMPDM'),
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
                                 col('SK_DATA_MPA').cast(IntegerType()),
                                 col('SK_DATA_CRIACAO_NOTA').cast(IntegerType()),
                                 (when(col('Id_Local_Instalacao').isNull(), '-1').otherwise(col('Id_Local_Instalacao'))).alias('Id_LOCAL_INSTALACAO'),
                                 (when(col('Local_Instalacao').isNull(), '-1').otherwise(col('Local_Instalacao'))).alias('SK_LOCAL_INSTALACAO'),
                                 (when(col('Txt_breve_operacao').isNull(), 'Não Informado').otherwise(col('Txt_breve_operacao'))).alias('TEXTO_BREVE_OPERACAO'),
                                 (when(col('Centro_trab_respons').isNull(), -1).otherwise(abs(hash(col('Centro_trab_respons'))))).alias('SK_CENTRO_TRABALHO_RESPONSAVEL').cast(IntegerType()),
                                 (when(col('Centro_trab_operacao').isNull(), -1).otherwise(abs(hash(col('Centro_trab_operacao'))))).alias('SK_CENTRO_TRABALHO_OPERACAO').cast(IntegerType()),
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

file_config = FileSystemConfig(
  format = 'parquet', 
  path = 'Supply/Producao/Manutencao/FatoOrdensManutencaoSap', 
  country = 'Brazil', 
  mount_name = 'consumezone', 
  mode = 'overwrite',
  )

writers = [
    FileWriter(config = file_config),
]

load_service = LoadService(writers=writers)
load_service.commit(dataframe=df_iw37n_final) 
