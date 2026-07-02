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
# Resumo das Operações:
# 1. Inicia uma sessão Spark.
# 2. Calcula a data e hora atual com ajuste de fuso horário (subtraindo 3 horas).
# 3. Extrai apenas a data da variável 'current_datetime'.
# 4. Define o período mínimo como o primeiro mês do ano anterior.
# 5. Define o período máximo como 2 meses posteriores ao atual

_spark_iris = Spark()
current_datetime = (datetime.now() - timedelta(hours = 3))
data_atual = current_datetime.date()
periodo_minimo = date(data_atual.year -1, 1, 1).strftime('%Y%m')
periodo_maximo = (data_atual + relativedelta(months=2)).strftime('%Y%m')

# COMMAND ----------

# DBTITLE 1,Parâmetros fixos
TIPO_PLANO_ECC = ['040', '045', '050', '055', '060', '300', '301', '303', '305', '306', '307', '308', '309', '310', '400', '405', '701', '702', '801']
TIPO_CORRETIVA_ECC = ['008', '200', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '600', '601', '602']

TIPO_PLANO_HANA = ["300", "301", "302", "303", "304", "305", "306", "307", "400", "401", "402", "403", "500", "600", "601", "602", "603", "604", "605", "606", "700"]
TIPO_CORRETIVA_HANA = ["100","200", "201", "202", "203", "204","205","206","207","208","209","210","211","212"]

# COMMAND ----------

# MAGIC %md
# MAGIC #DATASETS

# COMMAND ----------

# DBTITLE 1,Obtendo datasets necessários
# Resumo das Operações:
# 1. Carregar e processar dados do DePara de Área da 'historyzone' do Data Lake.
# 2. Carregar e processar dados do DePara de SubÁrea da 'historyzone'.
# 3. Carregar e processar dados do DePara de Unidades da 'historyzone'.
# 4. Carregar dados da Dim Data
#    4.1 Busca o período P3M mínimo e o máximo
#    4.2 Carregar dados de dias úteis da Dim Data da 'consumezone' e adicionar coluna 'AnoMes'.
# 5. Carregar dados de ROLLOUT HANA da 'consumezone'.
# 6. Carregar dados da IP18 da 'consumezone'.

df_area_hz = FileReader(table_id='df_area', data_lake_zone='historyzone', country='Brazil', path='Sharepoint/DeParasSupply/Areas', format='parquet').consume(spark=_spark_iris)
df_area = select_max_update_date(df_area_hz, 'AREA_SAP', 'CREATED_DATE_HZ')
df_area = df_area.select(col('COD_AREA_LAKE').cast(IntegerType()),
                         'AREA_SAP')

df_subarea_hz = FileReader(table_id='df_subarea', data_lake_zone='historyzone', country='Brazil', path='Sharepoint/DeParasSupply/SubAreas', format='parquet').consume(spark=_spark_iris)
df_subarea = select_max_update_date(df_subarea_hz, 'SUBAREA_SAP', 'CREATED_DATE_HZ')
df_subarea = df_subarea.select(col('COD_SUBAREA_LAKE').cast(IntegerType()),
                               'SUBAREA_SAP')

df_unidades_hz = FileReader(table_id='df_unidades', data_lake_zone='historyzone', country='Brazil', path='Sharepoint/DeParasSupply/Unidades', format='parquet').consume(spark=_spark_iris)
df_unidades = select_max_update_date(df_unidades_hz, 'SAP_CODIGO', 'CREATED_DATE_HZ')
df_unidades = df_unidades.select(col('COD_UNIDADE_LAKE').cast(IntegerType()),
                                 col('SAP_CODIGO').alias('Centro_planejamento'),
                                 col('HANA_CODIGO').alias('SAP_CODIGO_HANA'))

df_dimdata = FileReader(table_id='df_data', data_lake_zone='consumezone', country='Brazil', path='Supply/Estrutura/DimensaoData', format='parquet').consume(spark=_spark_iris)
df_periodo = df_dimdata.withColumn("AnoMesP3m",concat(col("AnoP3m"), lpad(col("MesP3m"), 2, "0"))).where((col("AnoMesP3m") >= periodo_minimo) & (col("AnoMesP3m") <= periodo_maximo))
agg_df_periodo = df_periodo.agg(
    min(col("Data")).alias("periodo_p3m_minimo"),
    max(col("Data")).alias("periodo_p3m_maximo")
)
periodo_p3m_minimo, periodo_p3m_maximo = agg_df_periodo.collect()[0]
df_dimdata = df_dimdata.select(col('Data'), col('FlagDiaUtil'))
df_dimdata = df_dimdata.withColumn("AnoMes", date_format(col("Data"), "yyyyMM"))

df_rollouthana = FileReader(table_id='df_rollouthana', data_lake_zone='consumezone', country='Brazil', path='Supply/Producao/RelatorioRolloutSap', format='parquet').consume(spark=_spark_iris)

df_ip18_cz = FileReader(table_id='ip18', data_lake_zone='consumezone', country='Brazil', path='Supply/Producao/Manutencao/FatoPlanoManutencaoSap', format='parquet').consume(spark=_spark_iris)
df_ip18_cz = df_ip18_cz.select(col('DESC_ITEM_MANUTENCAO'), col('ITEM_MANUTENCAO').alias("ITEM_MANUTENCAO_IP18"))

df_ip18_hz_ecc = FileReader(table_id='ip18_ecc', data_lake_zone='historyzone', country='Brazil', path='SAPSupply/IP18', format='parquet').consume(spark=_spark_iris)
df_ip18_hz_ecc = select_max_update_date(df_ip18_hz_ecc, 'Item_manutencao', 'CREATED_DATE_HZ')
df_ip18_hz_ecc = df_ip18_hz_ecc.withColumn("ORIGEM", lit('ECC')).select(col('Descricao_item_manutencao').alias('DESC_ITEM_MANUTENCAO'),
                                                                        col('Item_manutencao').alias('ITEM_MANUTENCAO_IP18'),
                                                                        col('ORIGEM')
                                                                        )

df_ip18_hz_hana = FileReader(table_id='ip18_hana', data_lake_zone='historyzone', country='Brazil', path='Aurora/IP18', format='parquet').consume(spark=_spark_iris)
df_ip18_hz_hana = select_max_update_date(df_ip18_hz_hana, 'Item_manutencao', 'CREATED_DATE_HZ').withColumn("ORIGEM", lit('HANA'))\
                                        .select(col('descricao_item_manutencao').alias('DESC_ITEM_MANUTENCAO'),
                                                col('item_manutencao').cast(IntegerType()).alias('ITEM_MANUTENCAO_IP18'),
                                                col('ORIGEM')
                                                )


# COMMAND ----------

# DBTITLE 1,Obtendo dataset da IW37N
# Resumo das Operações:
# 1. Carrega os dados da tabela 'iw37n' na 'historyzone' do Data Lake.
# 2. Remove duplicatas com base nas colunas 'Ordem', 'Operacao', 'Sub_Operacao' e 'CREATED_DATE_HZ'.
# 3. Filtra os dados para:
#    3.1 Incluir apenas registros com chave de controle 'PM01'.
#    3.2 Incluir apenas atividades de manutenção planejada ou corretiva.
#    3.3 Restringir o conjunto de dados ao período acima do 'periodo_p3m_minimo' e ao final do notebook também pelo 'periodo_p3m_maximo'.
# 4. Remover duplicidades para o contexto necessário

df_iw37n_hz = (
    FileReader(table_id='iw37n', data_lake_zone='historyzone', country='Brazil', path='SAPSupply/IW37N', format='parquet')
    .consume(spark=_spark_iris)
 
    .filter(
        (col("Chave_Controle") == "PM01") &
        (
            col("Tipo_ativ_manutencao").isin(TIPO_PLANO_ECC) |
            col("Tipo_ativ_manutencao").isin(TIPO_CORRETIVA_ECC)
        )
        & ((to_date(col("Inicio_Base"), "yyyyMMdd")) >= lit(periodo_p3m_minimo))
    )
)

df_iw37n_hz = df_iw37n_hz.dropDuplicates(["Ordem", "Operacao", "Sub_Operacao", "CREATED_DATE_HZ"])\
                         .withColumn('SK_TIPO_ATIVIDADE_MANUTENCAO', concat(lit('E'),col('Tipo_ativ_manutencao').cast(IntegerType())))\
                         .withColumn('DSC_TIPO_ATIVIDADE',
                                    when(col('Tipo_ativ_manutencao').isin(TIPO_PLANO_ECC), lit('Plano Manutenção'))
                                    .when(col('Tipo_ativ_manutencao').isin(TIPO_CORRETIVA_ECC), lit('Corretiva'))
                                    .otherwise(None))

# COMMAND ----------

# DBTITLE 1,Obtendo SK_UNIDADE
# Realiza a junção do DataFrame 'df_iw37n_inicial' com o DataFrame 'df_unidades' baseada na coluna 'Centro_planejamento'.
df_iw37n_hz = df_iw37n_hz.join(df_unidades, ['Centro_planejamento'], how='left') \
    .withColumn("SK_UNIDADE", when(col('COD_UNIDADE_LAKE').isNull(), -1).otherwise(col('COD_UNIDADE_LAKE')).cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,HANA - Obtendo dataset da IW37N
# Resumo das Operações:
# 1. Carrega os dados da tabela 'iw37n' na 'historyzone' do Data Lake.
# 2. Remove duplicatas com base nas colunas 'Ordem', 'Operacao', 'Sub_Operacao' e 'CREATED_DATE_HZ'.
# 3. Filtra os dados para:
#    3.1 Incluir apenas registros com chave de controle 'PM01'.
#    3.2 Incluir apenas atividades de manutenção planejada ou corretiva.
#    3.3 Restringir o conjunto de dados ao período acima do 'periodo_p3m_minimo' e ao final do notebook também pelo 'periodo_p3m_maximo'.
# 4. Remover duplicidades para o contexto necessário

#Obtendo os dados da iw37n HANA para gerar a fato
df_iw37n_hz_hana  = FileReader(table_id='iw37n', data_lake_zone='historyzone', country='Brazil', path='Aurora/IW37N', format='parquet').consume(spark=_spark_iris)

df_iw37n_hz_hana = df_iw37n_hz_hana.filter(
        (col("CONTROL_KEY") == "PM01") &
        (
            col("MAINTENANCE_ACTIVITY_TYPE").isin(TIPO_PLANO_HANA) |
            col("MAINTENANCE_ACTIVITY_TYPE").isin(TIPO_CORRETIVA_HANA)
        )
        &
        ((to_date(col("BASIC_START_DATE"), "yyyyMMdd")) >= lit(periodo_p3m_minimo))
    )

df_iw37n_hz_hana = df_iw37n_hz_hana.dropDuplicates(["MAINTENANCE_ORDER", "OPERATION_NUMBER_FOR_PM", "SUB_OPERATION_NUMBER_FOR_PM", "CREATED_DATE_HZ"]).select(
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
    col('PERSONNEL_NUMBER_PLANNED').alias('Num_Pessoal_Planejado'),
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
    'DELETED_AT',).withColumn("Primeiro_hora_inicio", lit(None)) \
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
    .withColumn("Confirmado_por", lit(None))\
    .withColumn('SK_TIPO_ATIVIDADE_MANUTENCAO', concat(lit('H'),col('Tipo_ativ_manutencao').cast(IntegerType())))\
    .withColumn('DSC_TIPO_ATIVIDADE',
                               when(col('Tipo_ativ_manutencao').isin(TIPO_PLANO_HANA), lit('Plano Manutenção'))
                               .when(col('Tipo_ativ_manutencao').isin(TIPO_CORRETIVA_HANA), lit('Corretiva'))
                               .otherwise(None))

# COMMAND ----------

# DBTITLE 1,SAP HANA - Join para trazer o COD_SAP_ECC
# Traz o COD_SAP_ECC do DePara de Unidades, relacionando pelo COD_SAP_HANA:
df_ordens_manutencao_hana_final = df_iw37n_hz_hana\
   .join(
      df_unidades,
      df_iw37n_hz_hana.Centro_Planejamento == df_unidades.SAP_CODIGO_HANA,
      'left'
   )\
  .select(
    df_iw37n_hz_hana['*'],
    df_unidades['Centro_planejamento'].alias("SAP_CODIGO_ECC"),
    df_unidades['COD_UNIDADE_LAKE']
  )

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION HANA/ECC

# COMMAND ----------

# DBTITLE 1,Transformando as bases dos SAPs ECC e HANA em uma base única
df_iw37n_hz = df_iw37n_hz.withColumn('ORIGEM', lit('ECC'))\
  .unionByName(
    df_ordens_manutencao_hana_final.withColumn('ORIGEM', lit('HANA')),
    allowMissingColumns = True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #TRANSFORMAÇÕES

# COMMAND ----------

# DBTITLE 1,Criando SKs de Risco e Impacto
# Resumo das Operações:
# 1. Definindo o padrão de risco e impacto usando expressões regulares.
# 2. Atualizando o DataFrame df_ip18_cz com novas colunas baseadas no padrão definido.
# 3. Join entre IW37N e IP18 para incluir as SKs de Risco e Impacto.
# 4. Preenchendo valores nulos com "XX" na SK_RISCO e -1 na SK_IMPACTO_RISCO.

padrao_risco_impacto = (
    col("DESC_ITEM_MANUTENCAO").rlike("BL-[0-9]-[0-9]{2}") |
    col("DESC_ITEM_MANUTENCAO").rlike("QA-[0-9]-[0-9]{2}") |
    col("DESC_ITEM_MANUTENCAO").rlike("SF-[0-9]-[0-9]{2}") |
    col("DESC_ITEM_MANUTENCAO").rlike("EV-[0-9]-[0-9]{2}")
)

df_ip18_hz = df_ip18_hz_ecc.union(df_ip18_hz_hana.withColumn('ITEM_MANUTENCAO_IP18',col('ITEM_MANUTENCAO_IP18').cast(StringType())))

df_ip18_hz = df_ip18_hz.withColumn(
    "SK_RISCO",
    when(padrao_risco_impacto, col("DESC_ITEM_MANUTENCAO").substr(1, 2))
).withColumn(
    "SK_IMPACTO_RISCO",
    when(padrao_risco_impacto, col("DESC_ITEM_MANUTENCAO").substr(4, 1))
)

df_iw37n_hz = df_iw37n_hz.join(df_ip18_hz, (df_iw37n_hz["Item_Manutencao"].cast("Integer") == df_ip18_hz["ITEM_MANUTENCAO_IP18"])
                                         & (df_iw37n_hz["ORIGEM"] == df_ip18_hz["ORIGEM"]), how="left")\
                         .select(df_iw37n_hz['*'],df_ip18_hz['SK_RISCO'],df_ip18_hz['SK_IMPACTO_RISCO'])

df_iw37n_hz = df_iw37n_hz.fillna({"SK_RISCO": "XX", "SK_IMPACTO_RISCO": -1})

# COMMAND ----------

# DBTITLE 1,Criação de Área e Subárea - dados BRASIL
# Resumo das Operações:
# 1. Filtra os dados para incluir apenas registros do Brasil, identificados pelo prefixo 'BR' no 'Centro_planejamento'.
# 2. Define regras para verificar e processar a nomenclatura das áreas e subáreas:
#    2.1 Define 'verifica_local' para identificar localizações fora do padrão.
#    2.2 Define 'exist_variance' para checar se a localização está fora do padrão.
#    2.3 Define 'concat_area_subarea' para concatenar área e subárea quando fora do padrão.
#    2.4 Define 'area_subarea_padrao_novo' para processar localizações no novo padrão.
# 3. Cria colunas 'AREA' e 'SUBAREA' no DataFrame, aplicando as regras definidas para lidar com variações na nomenclatura.

df_dados_brasil = df_iw37n_hz.where(col('Centro_planejamento').like('BR%'))

verifica_local = (rpad(trim(split(col('Local_instalacao'), '-', 3)[1]), 5, '0'))
exist_variance = ( (verifica_local == '00000') & (col('Local_instalacao') != '-') )
concat_area_subarea = ( concat(split(col('Local_instalacao'), '-', 3)[1], split(col('Local_instalacao'), '-', 3)[2]) )
area_subarea_padrao_novo = ( trim(rpad(trim(split(col('Local_instalacao'), '-', 3)[1]), 5, '0')) )

df_dados_brasil = df_dados_brasil.withColumn('AREA', when(exist_variance, concat_area_subarea).otherwise(area_subarea_padrao_novo))\
                                 .withColumn('SUBAREA', when(exist_variance, concat_area_subarea).otherwise(area_subarea_padrao_novo))

# COMMAND ----------

# DBTITLE 1,Criação de Área e Subárea - dados ABC
# Resumo das Operações:
# 1. Separa um DataFrame com dados da ABC, identificando registros que não seguem o padrão brasileiro 'BR'.
# 2. Define regras e variáveis para processar a nomenclatura dos locais de instalação, diferenciando entre padrões antigos e novos.
#    2.1 'split_local_instalacao': Separa o valor do 'Local_instalacao' em partes usando o hífen '-' como separador.
#    2.2 'primeiro_grupo_local_instalacao': Determina o comprimento do primeiro grupo resultante da separação para identificar o padrão de nomenclatura.
#    2.3 'nomenclatura_padrao_novo': Identifica se a nomenclatura está no novo padrão, que é caracterizado por um primeiro grupo com mais de um caractere.
#    2.4 'nomenclatura_padrao_antigo': Identifica se a nomenclatura está no padrão antigo, que é caracterizado por um primeiro grupo com apenas um caractere.
#    2.5 'ultimo_grupo_local_instalacao': Obtém o último grupo da separação do 'Local_instalacao', útil para identificar a subárea no padrão antigo.
#    2.6 'existe_subarea': Extrai a subárea do 'ultimo_grupo_local_instalacao' se ela estiver no formato específico ('L' seguido de números).
#    2.7 'subarea_fora_padrao': Captura casos onde a subárea está fora do padrão, como indicado pela presença de '-L-' no 'Local_instalacao'.
# 3. Cria um DataFrame 'df_dados_abc_padrao_novo' para registros da ABC com nomenclatura no padrão novo.
# 4. Cria um DataFrame 'df_dados_abc_padrao_antigo' para registros da ABC com nomenclatura no padrão antigo.
# 5. Une os DataFrames processados da ABC com o DataFrame do Brasil, formando 'df_iw37n_inicial'.

df_dados_abc = df_iw37n_hz.where(~col('Centro_planejamento').like('BR%'))

split_local_instalacao = ( split( col('Local_instalacao'), '-') )
primeiro_grupo_local_instalacao = ( length(split_local_instalacao[0]) )
nomenclatura_padrao_novo = ( primeiro_grupo_local_instalacao > 1 )
nomenclatura_padrao_antigo = ( primeiro_grupo_local_instalacao == 1 )
ultimo_grupo_local_instalacao = ( split_local_instalacao[size(split_local_instalacao)-1] )
existe_subarea = (regexp_extract(ultimo_grupo_local_instalacao, '(^[L][0-9])\w+', 0))
subarea_fora_padrao = (regexp_extract(col('Local_instalacao'), '(-L-)\w+', 0))

df_dados_abc_padrao_novo = df_dados_abc.withColumn('AREA', area_subarea_padrao_novo)\
                                       .withColumn('SUBAREA', area_subarea_padrao_novo)\
                                       .where(nomenclatura_padrao_novo | (col('Local_instalacao').isNull()))

df_dados_abc_padrao_antigo = df_dados_abc.withColumn('AREA', split_local_instalacao[2])\
                                         .withColumn('SUBAREA', when(ultimo_grupo_local_instalacao.contains('LIN'), ultimo_grupo_local_instalacao)\
                                                               .when(col('Local_instalacao').contains('-L-'), regexp_replace(subarea_fora_padrao, '-', '') )\
                                                               .when(existe_subarea.isNotNull(), existe_subarea)\
                                                               .otherwise(lit(None)))\
                                         .where(nomenclatura_padrao_antigo)

df_iw37n_inicial = df_dados_abc_padrao_antigo.union(df_dados_abc_padrao_novo)\
                                             .union(df_dados_brasil)

# COMMAND ----------

# DBTITLE 1,Cmd temporário
# Resumo das Operações:
# 1. Este bloco de código é uma solução temporária para um bug identificado (https://dev.azure.com/AMBEV-SA/Analytics%20Supply%20Chain/_workitems/edit/1233900).
# 2. A intenção é ajustar certas colunas com base no status da operação.
# 3. Define condições para identificar registros com os status 'CONF' e 'CNPA'.
# 4. Itera sobre uma lista de colunas específicas, atualizando seus valores para 'None' caso as condições não sejam atendidas.
# 5. Nota: Este código deve ser removido assim que o problema no SAP for resolvido.

cols = ["Texto_Confirmacao", "Num_Pessoal", "Nome_empregado", "Data_lancamento", "Confirmado_em", "Confirmado_por"]
condicao_conf = col("Status_Operacao").rlike(r'\bCONF\b')
condicao_cnpa = col("Status_Operacao").rlike(r'\bCNPA\b')
for coluna in cols:
    df_iw37n_inicial = df_iw37n_inicial.withColumn(coluna, when(condicao_conf | condicao_cnpa, col(coluna)).otherwise(lit(None)))

# COMMAND ----------

# DBTITLE 1,Join entre unidades, area e subarea
# Resumo das Operações:
# 1. Realiza a junção do DataFrame 'df_iw37n_inicial' com o DataFrame 'df_unidades' baseada na coluna 'Centro_planejamento'.
# 2. Realiza uma junção subsequente com o DataFrame 'df_area', combinando as colunas 'AREA' de 'df_iw37n' e 'AREA_SAP' de 'df_area'.
# 3. Realiza outra junção com o DataFrame 'df_subarea', combinando as colunas 'SUBAREA' de 'df_iw37n' e 'SUBAREA_SAP' de 'df_subarea'.

df_iw37n = df_iw37n_inicial.join(df_area, df_iw37n_inicial['AREA'] == df_area['AREA_SAP'], how='left')
df_iw37n = df_iw37n.join(df_subarea, df_iw37n['SUBAREA'] == df_subarea['SUBAREA_SAP'], how='left')

# COMMAND ----------

# DBTITLE 1,Substituindo colunas de 0 para Nulo
# Resumo das Operações:
# 1. Definindo listas de colunas que representam horas e datas.
# 2. Substituindo valores '000000' por 'None' (nulo) nas colunas de horas.
# 3. Substituindo valores '00000000' por 'None' (nulo) nas colunas de datas.

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
# Resumo das Operações:
# 1. Criação de colunas de chave (SK - Surrogate Key) para representar as horas iniciais e finais.
# 2. Utiliza a função 'substring' para extrair os primeiros 4 caracteres das colunas de hora.
# 3. Se a coluna de hora original for nula, a chave é definida como '-1'.

df_iw37n = df_iw37n.withColumn('SK_PRIMEIRA_HORA_INICIO', when( col('Primeiro_hora_inicio').isNull(), '-1').otherwise(substring(col('Primeiro_hora_inicio'), 0, 4)) )\
                   .withColumn('SK_PRIMEIRA_HORA_FIM', when( col('Primeiro_hora_fim').isNull(), '-1').otherwise(substring(col('Primeiro_hora_fim'), 0, 4)) )\
                   .withColumn('SK_HORA_INICIO', when( col('Hora_inicio').isNull(), '-1').otherwise(substring(col('Hora_inicio'), 0, 4)) )\
                   .withColumn('SK_HORA_FIM_REAL', when( col('Fim_real_hora').isNull(), '-1').otherwise(substring(col('Fim_real_hora'), 0, 4)) )

# COMMAND ----------

# DBTITLE 1,Criando SK para colunas de data
# Resumo das Operações:
# 1. Criação de colunas de chave (SK - Surrogate Key) para representar as datas.
# 2. Se a coluna de data original for nula ou igual a '0', a chave é definida como '-1'.
# 3. Caso contrário, a chave mantém o valor original da coluna.

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
                   .withColumn('SK_DATA_PLANEJADA', when( (col('Data_planejada').isNull()) | (col('Data_planejada') == '0'), '-1').otherwise(col('Data_planejada'))  )

# COMMAND ----------

# DBTITLE 1,Formatando colunas de data e hora para os Data Scientists
# Resumo das Operações:
# 1. Definição de variáveis para o formato de data, padrão de regex e substituição de string.
# 2. Formatação das colunas de data e hora a pedido dos Data Scientists.
# 3. Se a coluna for nula, mantém o valor como nulo. Caso contrário, aplica a formatação.
# As colunas de data são convertidas para o formato 'yyyyMMdd'.
# As colunas de hora são formatadas para o formato 'HH:mm:ss'.

data_format = 'yyyyMMdd'
string_regex_pattern = '(\\d{2})(\\d{2})(\\d{2})'
string_regex_replacement = '$1:$2:$3'

for columnData in colunas_de_data:
  df_iw37n = df_iw37n.withColumn(columnData, when(col(columnData).isNull(), None).otherwise( to_date(col(columnData), data_format ) ))

for columnHour in colunas_de_horas:
  df_iw37n = df_iw37n.withColumn(columnHour, when(col(columnHour).isNull(), None).otherwise(regexp_replace(col(columnHour), string_regex_pattern, string_regex_replacement)))

# COMMAND ----------

path_expurgo = '/mnt/consumezone/Brazil/Supply/Producao/Manutencao/AuxMpaExpurgoSharepoint/'

df_expurgo = dataframe_builder(path_expurgo, 'parquet')

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

# DBTITLE 1,Adicionando colunas necessárias
# Resumo das Operações:
# 1. 'TIPO_PRIORIDADE': Adiciona uma coluna constante com o valor 'PM'.
# 2. 'CREATED_DATE_CZ': Adiciona uma coluna com a data e hora atuais.
# 3. 'MREL_FLAG': Adiciona uma coluna indicadora (flag), onde o valor é 1 se o 'Status_sistema' contém CONF|MREL|NEXE|ENTE|ENCE|NEJE|PTBO, e 0 caso contrário.
# 4. 'DSC_TIPO_ATIVIDADE': Adiciona uma coluna com descrições textuais para os tipos de atividade de manutenção, baseadas nos valores de 'Tipo_ativ_manutencao'.
# 5. 'MES_P3M': Adiciona uma coluna AnoMesP3m no formato 'yyyyMM' a partir da dim data com a lógica p3m já aplicada.

MREL_PADRAO = "(CONF|MREL|NEXE|ENTE|ENCE|NEJE|PTBO)"

df_iw37n = df_iw37n.withColumn('TIPO_PRIORIDADE', lit('PM')) \
                   .withColumn('CREATED_DATE_CZ', lit(current_timestamp())) \
                   .withColumn('MREL_FLAG',
                               when(upper(col("Status_sistema")).rlike(MREL_PADRAO), 1)
                               .otherwise(0))
                   
df_iw37n = df_iw37n.join(df_periodo, df_iw37n['SK_DATA_INICIO_BASE'] == df_periodo['CodigoData'], how='left').select(df_iw37n['*'], df_periodo['AnoMesP3m'].alias('MES_P3M'))

# COMMAND ----------

# DBTITLE 1,Organizando Colunas Finais
# Resumo das Operações:
# Esta célula realiza a seleção e renomeação das colunas do DataFrame df_iw37n.
# Algumas colunas são renomeadas e outras são convertidas para tipos de dados específicos.
# Colunas adicionais, como SK (surrogate keys), são criadas para identificação única.
# Os valores nulos são tratados e substituídos quando necessário.

df_iw37n = df_iw37n.select(col('Ordem').cast(LongType()).alias('ORDEM'),
                                 col('Inicio_Base').alias('DATA_INICIO_BASE'),
                                 col('Operacao').cast(IntegerType()).alias('OPERACAO'),
                                 col('Chave_Controle').alias('CHAVE_CONTROLE'),
                                 col('Tipo_Ordem').alias('TIPO_ORDEM'),
                                 col('Status_Operacao').alias('STATUS_OPERACAO'),
                                 col('Status_sistema').alias('STATUS_SISTEMA'),
                                 col('Item_Manutencao').cast(LongType()).alias('ITEM_MANUTENCAO'),
                                 col('Sub_Operacao').cast(IntegerType()).alias('SUB_OPERACAO'),
                                 col('Plano_manutencao').alias('SK_PLANO_MANUTENCAO'),
                                 col('Tipo_ativ_manutencao').cast(IntegerType()).alias('TIPO_ATIVIDADE_MANUTENCAO'),
                                 col('Num_Pessoal_Planejado').alias('NUM_PESSOAL_PLANEJADO'),
                                 col('SK_TIPO_ATIVIDADE_MANUTENCAO'),
                                 col('SK_DATA_FIM_REAL').cast(IntegerType()),
                                 col('SK_HORA_FIM_REAL').cast(IntegerType()),
                                 col('SK_DATA_ENTRADA').cast(IntegerType()),
                                 col('SK_DATA_INICIO_BASE').cast(IntegerType()),
                                 col('SK_DATA_CONFIRMADO_EM').cast(IntegerType()),
                                 col('SK_DATA_PLANEJADA').cast(IntegerType()),
                                 col('CREATED_DATE_HZ'),
                                 col('CREATED_DATE_CZ'),
                                 col('MREL_FLAG'),
                                 col('MES_P3M'),
                                 col('FLAG_EXPURGO'),
                                 col('DSC_TIPO_ATIVIDADE'),
                                 col('SK_RISCO'),
                                 col('SK_IMPACTO_RISCO').cast(IntegerType()),
                                 (when(col('Local_Instalacao').isNull(), '-1').otherwise(col('Local_Instalacao'))).alias('SK_LOCAL_INSTALACAO'),
                                 (when(col('Txt_breve_operacao').isNull(), 'Não Informado').otherwise(col('Txt_breve_operacao'))).alias('TEXTO_BREVE_OPERACAO'),
                                 (when(col('Centro_trab_respons').isNull(), -1).otherwise(abs(hash(col('Centro_trab_respons'))))).alias('SK_CENTRO_TRABALHO_RESPONSAVEL').cast(IntegerType()),
                                 (when(col('Centro_trab_operacao').isNull(), -1).otherwise(abs(hash(col('Centro_trab_operacao'))))).alias('SK_CENTRO_TRABALHO_OPERACAO').cast(IntegerType()),
                                 (when( (col('TIPO_PRIORIDADE').isNull()) & (col('Propriedade').isNull()), -1).otherwise(abs(hash('TIPO_PRIORIDADE', 'Propriedade')))).alias('SK_PRIORIDADE').cast(IntegerType()),
                                 (when(col('Texto_Breve').isNull(), 'Não Informado').otherwise(col('Texto_Breve'))).alias('TEXTO_BREVE'),
                                 (when(col('COD_UNIDADE_LAKE').isNull(), -1).otherwise(col('COD_UNIDADE_LAKE'))).cast(IntegerType()).alias('SK_UNIDADE'),
                                 (when(col('COD_SUBAREA_LAKE').isNull(), -1).otherwise(col('COD_SUBAREA_LAKE'))).cast(IntegerType()).alias('SK_SUBAREA'),
                                 (when(col('COD_AREA_LAKE').isNull(), -1).otherwise(col('COD_AREA_LAKE'))).cast(IntegerType()).alias('SK_AREA'),
                                 (when(col('DATA_MODIFICACAO').isNotNull(), col('DATA_MODIFICACAO')).otherwise(col('DATA_ENTRADA'))).cast(DateType()).alias('INCREMENTAL_DATE'),
                                 concat(col('Ordem').cast(StringType()), col('Operacao').cast(StringType()), when(col('Sub_Operacao').isNull(), '').otherwise(col('Sub_Operacao'))).cast(StringType()).alias('ID_ORDEM_OPERACAO'),
                                 col('ORIGEM'))

# COMMAND ----------

# DBTITLE 1,Criando Dataframe do Status Fechamento
# Resumo das Operações:
# 1. Seleção de Colunas Iniciais:
# 2. Cria a coluna 'STATUS_FECHAMENTO_FLAG' com base nas seguintes condições:
#    - Se 'STATUS_FECHAMENTO' contém "%CONF%" ou "%CNF" (ignorando maiúsculas ou minúsculas), define a flag como 1 (Verdadeiro).
#    - Caso contrário, define a flag como 0 (Falso).
# Nota: Guardando um DF específico para ser utilizado na célula de buscar o Status Fechamento 

df_iw37n_status_fechamento = df_iw37n.select(
    col('ID_ORDEM_OPERACAO'),
    col('STATUS_OPERACAO').alias('STATUS_FECHAMENTO'),
    col('CREATED_DATE_HZ').alias('CREATED_DATE_HZ_FECHAMENTO')
).withColumn('STATUS_FECHAMENTO_FLAG',
             when(upper(col("STATUS_FECHAMENTO")).like("%CONF%") |
                  upper(col("STATUS_FECHAMENTO")).like("%CNF%"), 1)
             .otherwise(0))

# COMMAND ----------

# DBTITLE 1,DF com a lista de datas de congelamento e fechamento
df_datacongelamentop3m = FileReader(table_id='df_data_congelamento', data_lake_zone='historyzone', country='Brazil', path='Sharepoint/ListaDataCongelamentoP3m', format='parquet').consume(spark=_spark_iris)

# COMMAND ----------

# DBTITLE 1,Transformaçoes no DF de data de congelamento para termos os campos de data
#Transformaçoes no DF de data de congelamento para termos os campos de data

df_datacongelamentop3m = df_datacongelamentop3m.withColumn('DATA_CONGELAMENTO_BASE', to_date(col('DATA_CONGELAMENTO_BASE'), 'yyyy-MM-dd'))\
            .withColumn('DATA_FECHAMENTO_BASE', to_date(col('DATA_FECHAMENTO_P3M'), 'yyyy-MM-dd'))

# COMMAND ----------

# DBTITLE 1,Criando as colunas Data Congelamento Base e Data Fechamento Base 
# Join do DF principal com o DF de data de congelamento para buscar as data de congelamento e fechamento do Sharepoint

df_iw37n = df_iw37n.join(df_datacongelamentop3m, df_iw37n["MES_P3M"] == df_datacongelamentop3m["ANO_MES_CONGELAMENTO"], how="left").select(df_iw37n['*'], df_datacongelamentop3m['DATA_CONGELAMENTO_BASE'], df_datacongelamentop3m['ANO_MES_CONGELAMENTO'], df_datacongelamentop3m['DATA_FECHAMENTO_BASE'])

# COMMAND ----------

# DBTITLE 1,Criando Lógica de Congelamento de Base
# Resumo das Operações:
# 1. Filtra o DF principal mantendo apenas as colunas necessárias para realizar a lógica do congelamento
# 2. Self-join: Join pela ORDEM e que a CREATED_DATE_HZ do registro futuro seja anterior à 'DATA_CONGELAMENTO_BASE' do registro atual. Isso permite identificar possíveis alterações de datas de congelamento futuras que ainda estão dentro do critério de congelamento da ORDEM atual
# 3. Determina o registro mais recente para cada ORDEM/DATA_CONGELAMENTO_BASE
# 4. Une ao DF principal para criar a FLAG dos registros que serão mantidos
# 5. Filtra o DF principal mantendo apenas os registros mais recentes para cada DATA_CONGELAMENTO_BASE
# 6. Join com o DF principal para buscar todas as colunas novamente

df_iw37n_congelamento = df_iw37n.select("ORDEM", "OPERACAO", "CREATED_DATE_HZ", "DATA_CONGELAMENTO_BASE").distinct()

df_iw37n_preparacao_congelamento = df_iw37n_congelamento.alias("REGISTRO_ATUAL").join(
    df_iw37n_congelamento.alias("PROXIMO_REGISTRO"),
    (col("REGISTRO_ATUAL.ORDEM") == col("PROXIMO_REGISTRO.ORDEM")) &
    (col("PROXIMO_REGISTRO.CREATED_DATE_HZ") < col("REGISTRO_ATUAL.DATA_CONGELAMENTO_BASE")),
    how="inner"
).select(
    "REGISTRO_ATUAL.ORDEM","REGISTRO_ATUAL.DATA_CONGELAMENTO_BASE",
    col("PROXIMO_REGISTRO.CREATED_DATE_HZ").alias("CREATED_DATE_HZ_MAIS_RECENTE")
)

df_registro_mais_recente = df_iw37n_preparacao_congelamento.groupBy("ORDEM", "DATA_CONGELAMENTO_BASE").agg(
    max("CREATED_DATE_HZ_MAIS_RECENTE").alias("CREATED_DATE_HZ_MAIS_RECENTE")
)

df_iw37n_final = df_iw37n_congelamento.alias("REGISTRO_ATUAL").join(
    df_registro_mais_recente.alias("REGISTRO_MAIS_RECENTE"),
    (col("REGISTRO_ATUAL.ORDEM") == col("REGISTRO_MAIS_RECENTE.ORDEM")) & 
    (col("REGISTRO_ATUAL.DATA_CONGELAMENTO_BASE") == col("REGISTRO_MAIS_RECENTE.DATA_CONGELAMENTO_BASE")),
    how="left"
).select(
    "REGISTRO_ATUAL.*",
    "REGISTRO_MAIS_RECENTE.CREATED_DATE_HZ_MAIS_RECENTE",
    when(col("REGISTRO_MAIS_RECENTE.CREATED_DATE_HZ_MAIS_RECENTE") == col("REGISTRO_ATUAL.CREATED_DATE_HZ"), lit(1)).otherwise(lit(0)).alias("ULTIMO_REGISTRO_CONGELADO_FLAG")
)

df_iw37n_final = df_iw37n_final.where(col("ULTIMO_REGISTRO_CONGELADO_FLAG") == 1)

df_iw37n_final = df_iw37n.join(
    df_iw37n_final,
    (df_iw37n_final["ORDEM"] == df_iw37n["ORDEM"]) &
    (df_iw37n_final["CREATED_DATE_HZ"] == df_iw37n["CREATED_DATE_HZ"]),
    how="inner"
).select(df_iw37n["*"])

# COMMAND ----------

# DBTITLE 1,Filtro rollout ECC
# Remove os registros do ECC unidades que já passaram pelo rollout

df_iw37n_ecc = df_iw37n_final.where(col('ORIGEM') == 'ECC')
                             
df_iw37n_ecc = df_iw37n_ecc.alias('ecc')\
  .join(
    df_rollouthana.alias('rollout'),
    (
        (col('ecc.SK_UNIDADE') == col('rollout.SK_UNIDADE')) &
        (col('DATA_INICIO_BASE') >= col('rollout.DATA_ROLLOUT'))
    ),
    "leftanti"
  )\
  .select(
    df_iw37n_ecc['*']
  )

# COMMAND ----------

# DBTITLE 1,Filtro rollout HANA
# Retorna as unidades que passaram pelo Rollout:

df_iw37n_hana = df_iw37n_final.where(col('ORIGEM') == 'HANA')

df_iw37n_hana = df_iw37n_hana.alias('hana')\
  .join(
    df_rollouthana.alias('rollout'),
    (
      (col('hana.SK_UNIDADE') == col('rollout.SK_UNIDADE')) & 
      (col('DATA_INICIO_BASE') >= col('rollout.DATA_ROLLOUT'))
    ),
    "inner"
  )\
  .select(
    df_iw37n_hana['*']
  )

# COMMAND ----------

# DBTITLE 1,Une dados filtrados do rollout do ECC e HANA
df_iw37n_final = df_iw37n_ecc.unionByName(df_iw37n_hana, allowMissingColumns = True)

# COMMAND ----------

# DBTITLE 1,Buscando o Status Fechamento
# Resumo das Operações:
# 1. Realização de um join entre o DataFrame 'df_iw37n_final' e o DataFrame 'df_iw37n_status_fechamento' com base nas seguintes condições:
#    - 'ID_ORDEM_OPERACAO' deve ser igual em ambos os DataFrames.
#    - 'DATA_FECHAMENTO_BASE' do DataFrame principal deve ser maior ou igual a 'CREATED_DATE_HZ_FECHAMENTO' do DataFrame de datas de fechamento.
#    - 'CREATED_DATE_HZ' do DataFrame principal deve ser menor ou igual a 'CREATED_DATE_HZ_FECHAMENTO' do DataFrame de datas de fechamento.
# 2. Definição da Janela de Partição para a Data Mais Recente:
#    - Definição da janela de partição 'window_status_fechamento' para agrupar os registros pela combinação de 'ID_ORDEM_OPERACAO' e 'MES_P3M', ordenados em ordem decrescente pela coluna 'CREATED_DATE_HZ_FECHAMENTO'.
# 3. Cálculo da Data Mais Recente:
#    - Adição da coluna 'ULTIMO_REGISTRO' ao DataFrame 'df_iw37n_status_fechamento' para calcular o número de registros dentro de cada partição.
# 4. Filtragem para Manter Apenas a Data Mais Recente:

df_iw37n_status_fechamento = df_iw37n_final.join(
    df_iw37n_status_fechamento,
    (df_iw37n_final["ID_ORDEM_OPERACAO"] == df_iw37n_status_fechamento["ID_ORDEM_OPERACAO"]) &
    (df_iw37n_final["DATA_FECHAMENTO_BASE"] >= df_iw37n_status_fechamento["CREATED_DATE_HZ_FECHAMENTO"]) &
    (df_iw37n_final["CREATED_DATE_HZ"] <= df_iw37n_status_fechamento["CREATED_DATE_HZ_FECHAMENTO"]),
    how='left'
).select(df_iw37n_final["*"], df_iw37n_status_fechamento["STATUS_FECHAMENTO"], df_iw37n_status_fechamento["STATUS_FECHAMENTO_FLAG"], df_iw37n_status_fechamento["CREATED_DATE_HZ_FECHAMENTO"])

window_status_fechamento = Window.partitionBy("ID_ORDEM_OPERACAO", "MES_P3M").orderBy(desc("CREATED_DATE_HZ_FECHAMENTO"))

df_iw37n_status_fechamento = df_iw37n_status_fechamento.withColumn("ULTIMO_REGISTRO", row_number().over(window_status_fechamento))

df_iw37n_final = df_iw37n_status_fechamento.where(col("ULTIMO_REGISTRO") == 1).drop("ULTIMO_REGISTRO")

# COMMAND ----------

# DBTITLE 1,Tratamento registros historicos HANA
# Esta célula tem por objetivo tratar alguns registros históricos do hana e adicioná-los ao dataset final.

df_iw37n_final_hana_append = df_iw37n_final.where(
  ((col("DATA_INICIO_BASE")) < lit('2024-06-02')) &
  (col('ORIGEM') == 'HANA') &
  (col('SK_DATA_FIM_REAL') > date_format(col('DATA_CONGELAMENTO_BASE'), "yyyyMMdd").cast("int"))
)

# COMMAND ----------

# DBTITLE 1,Filtrando Dataset final
# Resumo das Operações:
# Filtrando o dataset com dados até o limite do 'periodo_p3m_maximo', as colunas MREL_FLAG = 0 e TIPO_ATIVIDADE_MANUTENCAO não pode ser 100 ou 210
df_iw37n_final = df_iw37n_final.where(
  
  ((col("DATA_INICIO_BASE")) <= lit(periodo_p3m_maximo)) &
  (
    (df_iw37n_final['MREL_FLAG'] == 0) &
    ~(df_iw37n_final['SK_TIPO_ATIVIDADE_MANUTENCAO'].isin('E100', 'E210', 'H100', 'H209'))
  )
).drop('DATA_ROLLOUT','AnoMesCongelamentoP3M','AnoMesFechamentoP3M','DATA_INICIO_BASE','CREATED_DATE_HZ')

# COMMAND ----------

# DBTITLE 1,Unindo Dataset Final aos Registros tratados históricos HANA
df_iw37n_final = df_iw37n_final.unionByName(df_iw37n_final_hana_append, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #GRAVAÇÃO

# COMMAND ----------

# DBTITLE 1,Gravando os dados na CZ
file_config = FileSystemConfig(
  format = 'parquet', 
  path = 'Supply/Producao/Manutencao/FatoKpisManutencaoP3m', 
  country = 'Brazil', 
  mount_name = 'consumezone', 
  mode = 'overwrite',
  )

writers = [
    FileWriter(config = file_config),
]

load_service = LoadService(writers=writers)
load_service.commit(dataframe=df_iw37n_final) 

# COMMAND ----------


