-- Demo genérica para teste de Governança na DALIA
-- Objetivo: expor contratos estruturais claros em SQL/Delta/Unity Catalog
-- Uso sugerido:
-- 1) subir este arquivo no repositório de teste
-- 2) rodar onboarding/documentação
-- 3) aprovar as bases governadas detectadas
-- 4) depois criar um alter_table.sql para validar solicitação de governança

CREATE SCHEMA IF NOT EXISTS uc_demo.silver_generic;
CREATE SCHEMA IF NOT EXISTS uc_demo.gold_generic;

CREATE TABLE IF NOT EXISTS uc_demo.silver_generic.clientes_curados (
    cliente_id STRING NOT NULL COMMENT 'Identificador único do cliente',
    documento_hash STRING COMMENT 'Hash irreversível do documento do cliente',
    nome_fantasia STRING COMMENT 'Nome exibido para análises comerciais',
    segmento STRING COMMENT 'Segmento comercial principal',
    canal_origem STRING COMMENT 'Canal de aquisição do cliente',
    data_cadastro DATE COMMENT 'Data de cadastro no sistema',
    status_cliente STRING COMMENT 'Status atual do cliente no domínio de negócio',
    score_relacionamento DECIMAL(10,2) COMMENT 'Indicador interno de relacionamento',
    cidade STRING COMMENT 'Cidade principal do cliente',
    estado STRING COMMENT 'UF principal do cliente',
    data_ref DATE NOT NULL COMMENT 'Data de referência da carga'
)
USING DELTA
PARTITIONED BY (data_ref)
LOCATION '/mnt/silver/generic/clientes_curados'
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'quality_tier' = 'silver',
    'owner_domain' = 'comercial',
    'data_classification' = 'internal'
);

COMMENT ON TABLE uc_demo.silver_generic.clientes_curados IS
'Tabela curada de clientes usada como base governada para análises comerciais e relacionamento.';


CREATE TABLE IF NOT EXISTS uc_demo.silver_generic.transacoes_curadas (
    transacao_id STRING NOT NULL COMMENT 'Identificador único da transação',
    cliente_id STRING NOT NULL COMMENT 'Chave do cliente associado à transação',
    data_transacao TIMESTAMP NOT NULL COMMENT 'Data e hora em que a transação ocorreu',
    categoria_produto STRING COMMENT 'Categoria macro do item transacionado',
    canal_venda STRING COMMENT 'Canal em que a transação foi realizada',
    status_transacao STRING COMMENT 'Status operacional da transação',
    valor_bruto DECIMAL(18,2) COMMENT 'Valor bruto da transação',
    valor_liquido DECIMAL(18,2) COMMENT 'Valor líquido da transação após ajustes',
    custo_total DECIMAL(18,2) COMMENT 'Custo associado à transação',
    margem_bruta DECIMAL(18,2) COMMENT 'Margem bruta calculada no nível da transação',
    data_ref DATE NOT NULL COMMENT 'Data de referência da partição'
)
USING DELTA
PARTITIONED BY (data_ref)
LOCATION '/mnt/silver/generic/transacoes_curadas'
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.enableChangeDataFeed' = 'true',
    'quality_tier' = 'silver',
    'owner_domain' = 'receita',
    'retention_policy' = '365d'
);

COMMENT ON TABLE uc_demo.silver_generic.transacoes_curadas IS
'Tabela curada de transações com granularidade operacional para servir de base a KPIs e monitoramento de receita.';


CREATE TABLE IF NOT EXISTS uc_demo.gold_generic.kpi_receita_diaria (
    data_ref DATE NOT NULL COMMENT 'Data de referência consolidada',
    regional STRING NOT NULL COMMENT 'Recorte regional da apuração',
    canal_venda STRING NOT NULL COMMENT 'Canal principal do consolidado',
    receita_liquida_total DECIMAL(18,2) COMMENT 'Receita líquida agregada do período',
    quantidade_transacoes BIGINT COMMENT 'Quantidade de transações contabilizadas',
    clientes_ativos BIGINT COMMENT 'Quantidade de clientes únicos ativos',
    margem_bruta_total DECIMAL(18,2) COMMENT 'Margem bruta total consolidada',
    ticket_medio DECIMAL(18,2) COMMENT 'Ticket médio consolidado',
    taxa_conversao DECIMAL(10,4) COMMENT 'Taxa de conversão calculada para o período',
    nivel_confianca STRING COMMENT 'Nível de confiança da apuração'
)
USING DELTA
PARTITIONED BY (data_ref)
LOCATION '/mnt/gold/generic/kpi_receita_diaria'
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'gold_contract' = 'true',
    'owner_domain' = 'executivo',
    'sla_refresh' = 'D+1',
    'criticality' = 'high'
);

COMMENT ON TABLE uc_demo.gold_generic.kpi_receita_diaria IS
'Tabela gold de indicadores executivos de receita diária, destinada a dashboards, reports e acompanhamento gerencial.';
