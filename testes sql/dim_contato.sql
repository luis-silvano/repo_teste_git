-- Databricks notebook source
-- DBTITLE 1,Parametrização utilizada no notebook
-- MAGIC %python
-- MAGIC dbutils.widgets.text("dat_parametro","")
-- MAGIC dbutils.widgets.text("tablename","")
-- MAGIC dbutils.widgets.text("database","")
-- MAGIC dbutils.widgets.text("malha","")
-- MAGIC dbutils.widgets.text("dsc_tabela_pii","")
-- MAGIC dbutils.widgets.text("databricks_catalog","")
-- MAGIC
-- MAGIC dat_parametro = dbutils.widgets.get("dat_parametro")
-- MAGIC tablename = dbutils.widgets.get("tablename")
-- MAGIC database = dbutils.widgets.get("database")
-- MAGIC malha = dbutils.widgets.get("malha")
-- MAGIC dsc_tabela_pii = dbutils.widgets.get("dsc_tabela_pii")
-- MAGIC databricks_catalog = dbutils.widgets.get("databricks_catalog")
-- MAGIC
-- MAGIC # Exemplo de preenchimento
-- MAGIC # dat_parametro = 2026-05-15
-- MAGIC # database = gold_vr_cliente_trabalhador
-- MAGIC # databricks_catalog (DEV) = dev
-- MAGIC # databricks_catalog (PROD) = prod
-- MAGIC # dsc_tabela_pii = dim_contato
-- MAGIC # malha = temp
-- MAGIC # tablename = dim_contato

-- COMMAND ----------

-- DBTITLE 1,Referência de configurações Spark
-- MAGIC %md
-- MAGIC ## Configurações Spark
-- MAGIC
-- MAGIC ### Ambientes
-- MAGIC
-- MAGIC | Ambiente | Workspace | Cluster | RAM/Worker | Workers | Concorrência |
-- MAGIC | --- | --- | --- | --- | --- | --- |
-- MAGIC | PROD | adb-3890842691580589 | Standard_E8ds_v4 (On-Demand) | 64GB | 1–12 | Até 25 jobs |
-- MAGIC | HML | adb-459755612035950 | Standard_D8d_v4 (Spot) | 32GB | 1–10 | Até 25 jobs |
-- MAGIC | DEV | adb-3869081643332278 | Standard_D3_v2 (Spot) | 14GB | 1–6 | Compartilhado |
-- MAGIC
-- MAGIC Todos usam `USER_ISOLATION` (Spark Connect) + DBR 16.4.
-- MAGIC
-- MAGIC ### O que está sendo setado no notebook 
-- MAGIC
-- MAGIC | Config | Valor | O que faz |
-- MAGIC | --- | --- | --- |
-- MAGIC | `spark.sql.shuffle.partitions` | auto | AQE decide quantas partições usar conforme recursos disponíveis. Essencial em cluster compartilhado |
-- MAGIC | `spark.sql.files.maxPartitionBytes` | 64m (PROD) / 128m (HML/DEV) | Tamanho máximo de cada partição na leitura de arquivos. Menor = mais paralelismo |
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Configurações de sessão Spark
-- MAGIC %python
-- MAGIC def get_environment():
-- MAGIC     """Detecta ambiente via workspace URL (independente do catálogo)."""
-- MAGIC     ws_url = ''
-- MAGIC     try:
-- MAGIC         ws_url = spark.conf.get("spark.databricks.workspaceUrl")
-- MAGIC     except Exception:
-- MAGIC         try:
-- MAGIC             ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
-- MAGIC             ws_url = ctx.apiUrl().get()
-- MAGIC         except Exception:
-- MAGIC             pass
-- MAGIC     if '3890842691580589' in ws_url:
-- MAGIC         return 'prod'
-- MAGIC     elif '459755612035950' in ws_url:
-- MAGIC         return 'hml'
-- MAGIC     else:
-- MAGIC         return 'dev'
-- MAGIC
-- MAGIC env = get_environment()
-- MAGIC is_prod = env == 'prod'
-- MAGIC
-- MAGIC # Shuffle partitions: "auto" — AQE decide conforme recursos disponíveis no cluster compartilhado
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions", "auto")
-- MAGIC
-- MAGIC # Leitura: PROD tem mais cores (96) = menor partition = mais paralelismo
-- MAGIC # HML/DEV têm menos cores, partition maior evita overhead de scheduling
-- MAGIC spark.conf.set("spark.sql.files.maxPartitionBytes", "64m" if is_prod else "128m")
-- MAGIC
-- MAGIC env_labels = {'prod': 'PRODUÇÃO', 'hml': 'HOMOLOGAÇÃO', 'dev': 'DESENVOLVIMENTO'}
-- MAGIC print(f"Ambiente: {env_labels[env]} | Catálogo: {databricks_catalog}")
-- MAGIC print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
-- MAGIC print(f"Max partition bytes: {spark.conf.get('spark.sql.files.maxPartitionBytes')}")
-- MAGIC if env == 'dev' and databricks_catalog == 'prod':
-- MAGIC     print("⚠️ ATENÇÃO: Apontando para catálogo PROD em ambiente DEV!")

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC - silver_vr_ms_vr_identidade.ms_vridentidade_login_usuario
-- MAGIC - silver_vr_ms_vr_identidade.ms_vridentidade_usuario
-- MAGIC - silver_audaz.dbo_employeebase
-- MAGIC - silver_audaz.dbo_rechargeorder
-- MAGIC - silver_audaz.dbo_rechargeitem
-- MAGIC - silver_audaz.dbo_schedulequery
-- MAGIC - silver_audaz.dbo_branchs
-- MAGIC - silver_audaz.dbo_accounts
-- MAGIC - silver_audaz.dbo_partners
-- MAGIC - silver_audaz.dbo_orderitems
-- MAGIC - silver_audaz.dbo_orders
-- MAGIC - silver_vr_bi_legado.biadm_snf_relacao_usuario_vt_fct
-- MAGIC - silver_vr_bi_legado.biadm_snf_pedido_venda_vt_fct
-- MAGIC - silver_vr_gente_produto.public_view_employees_datalake
-- MAGIC - silver_vr_admissao.hcmprd_employees

-- COMMAND ----------

-- DBTITLE 1,Função core
-- MAGIC %run ../../../../Utils/Functions/core

-- COMMAND ----------

-- DBTITLE 1,Função segurança
-- MAGIC %run ../../../../Utils/Functions/seguranca_utils

-- COMMAND ----------

-- DBTITLE 1,Definição do catálogo de dados
USE CATALOG IDENTIFIER(:databricks_catalog);
SELECT current_catalog();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Benefícios</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_usuario
create or replace temporary view tmp_usuario as

select ms_vridentidade_usuario.cpf as num_cpf,
       ms_vridentidade_usuario.nome as dsc_nome,
       ms_vridentidade_usuario.email as dsc_email,
       case when trim(ms_vridentidade_usuario.dddtelefone) = '' then null else ms_vridentidade_usuario.dddtelefone end as num_ddd_telefone,
       case when trim(ms_vridentidade_usuario.numtelefone) = '' then null else ms_vridentidade_usuario.numtelefone end as num_telefone,
       ms_vridentidade_usuario.dddcelular as num_ddd_celular,
       ms_vridentidade_usuario.numcelular as num_celular,
       timestamp(ms_vridentidade_usuario.criado_em) as dat_criacao_contato,
       sha2(regexp_replace(regexp_replace(ms_vridentidade_usuario.CPF,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       'VR e Voce' as dsc_origem
  from prod.silver_vr_ms_vr_identidade.ms_vridentidade_usuario
 where ms_vridentidade_usuario.status = 1
;

-- COMMAND ----------

-- DBTITLE 1,tmp_superapp
-- [DESATIVADO] Origem sem atualização desde out/2025. Carga completa executada em 28/05/2026.
-- Manter comentado nas execuções diárias.

-- create or replace temporary view tmp_superapp as
-- select filtro.num_cpf,
--        cast(null as string) as dsc_nome,
--        filtro.dsc_email,
--        cast(null as string) as num_ddd_telefone,
--        cast(null as string) as num_telefone,
--        cast(null as string) as num_ddd_celular,
--        filtro.num_celular,
--        timestamp(filtro.dat_criacao_contato) as dat_criacao_contato,
--        sha2(regexp_replace(regexp_replace(filtro.num_cpf,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
--        'SuperApp' as dsc_origem
--   from (select cpf as num_cpf,
--                usuario as dsc_email,
--                celular as num_celular,
--                max(to_date(data_evento, 'dd/MM/yyyy HH:mm:ss')) as dat_criacao_contato
--           from prod.silver_vr_superapp.prd_ms_superapp_logs
--          group by cpf, usuario, celular
--        ) filtro
-- ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Mobilidade</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_base_mobilidade
create or replace temporary view tmp_contato_base_mobilidade as

select nvl(regexp_replace(dbo_employeebase.Cpf,'[^0-9]',''),-1) as num_cpf,
       dbo_employeebase.Name as dsc_nome,
       dbo_employeebase.Email as dsc_email,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 0,2) as num_ddd_telefone,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 3,11) as num_telefone,
       string(null) as num_ddd_celular,
       string(null) as num_celular,
       timestamp(dbo_employeebase.CreatedOn) as dat_criacao_contato,
       sha2(regexp_replace(regexp_replace(dbo_employeebase.Cpf,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       'VRM - Audaz' as dsc_origem
  from prod.silver_audaz.dbo_employeebase
;

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_base_pedido_mobilidade
create or replace temporary view tmp_contato_base_pedido_mobilidade as

select distinct 
       -1 as num_cpf,
       dbo_employeebase.Name as dsc_nome,
       dbo_employeebase.Email as dsc_email,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 0,2) as num_ddd_telefone,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 3,11) as num_telefone,
       string(null) as num_ddd_celular,
       string(null) as num_celular,
       timestamp(dbo_employeebase.CreatedOn) as dat_criacao_contato,
       sha2(regexp_replace(regexp_replace(nvl(dbo_rechargeitem.Identifier,dbo_rechargeitem.IdentifierERP),'^0+(?!$)|[^0-9a-zA-Z]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       'VRM - Audaz' as dsc_origem
  from prod.silver_audaz.dbo_rechargeorder
  join prod.silver_audaz.dbo_rechargeitem		         on dbo_rechargeorder.RechargeOrderId = dbo_rechargeitem.RechargeOrderId
  join prod.silver_audaz.dbo_schedulequery		       on dbo_rechargeorder.ScheduleQueryId = dbo_schedulequery.ScheduleQueryId
  join prod.silver_audaz.dbo_branchs		             on dbo_schedulequery.BranchId = dbo_branchs.BranchId
  join prod.silver_audaz.dbo_accounts		             on dbo_branchs.AccountId = dbo_accounts.AccountId
  left join prod.silver_audaz.dbo_partners		       on dbo_accounts.PartnerId = dbo_partners.PartnerId
  left join prod.silver_audaz.dbo_employeebase		   on dbo_employeebase.EmployeeBaseId = dbo_rechargeitem.EmployeesBaseId
 where dbo_rechargeorder.STATUS in (1, 2, 4)
		and dbo_rechargeorder.type in (1, 2, 3, 4, 8)
		and dbo_accounts.Name not like ('Teste%')
		and nvl(dbo_partners.Name, 'Audaz') in ('Audaz', 'Mastermaq [Parceiro]')
		and upper(dbo_accounts.accountid) not in ('F813C11D-6A8E-47AD-933B-DCD722BE9996')
    and dbo_employeebase.EmployeeBaseId is null
;

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_base_pedido_item_mobilidade
create or replace temporary view tmp_contato_base_pedido_item_mobilidade as

select distinct 
       nvl(regexp_replace(dbo_employeebase.Cpf,'[^0-9]',''),-1) as num_cpf,
       dbo_employeebase.Name as dsc_nome,
       dbo_employeebase.Email as dsc_email,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 0,2) as num_ddd_telefone,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 3,11) as num_telefone,
       string(null) as num_ddd_celular,
       string(null) as num_celular,
       timestamp(dbo_employeebase.CreatedOn) as dat_criacao_contato,
       case when dbo_employeebase.Cpf is null 
          then sha2(regexp_replace(regexp_replace(nvl(dbo_rechargeitem.Identifier,dbo_rechargeitem.IdentifierERP),'^0+(?!$)|[^0-9a-zA-Z]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512)
          else sha2(regexp_replace(regexp_replace(dbo_employeebase.Cpf,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512)
       end as cod_hash_cpf,
       'VRM - Audaz' as dsc_origem
  from prod.silver_audaz.dbo_rechargeorder
  join prod.silver_audaz.dbo_rechargeitem		         on dbo_rechargeorder.RechargeOrderId = dbo_rechargeitem.RechargeOrderId
  left join prod.silver_audaz.dbo_schedulequery		   on dbo_rechargeorder.ScheduleQueryId = dbo_schedulequery.ScheduleQueryId
	left join prod.silver_audaz.dbo_employeebase		   on dbo_employeebase.EmployeeBaseId = dbo_rechargeitem.EmployeesBaseId
;

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_order_mobilidade
-- [REFATORADO] Leitura única das 4 tabelas. CASE classifica CPF válido vs inválido.

create or replace temporary view tmp_contato_order_mobilidade as

select distinct 
       case when dbo_orderitems.CPF is not null
             and regexp_replace(dbo_orderitems.CPF,'[^0-9]','') <> ''
             and substr(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''), 1, 8) <> '00000000'
            then nvl(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''),'-1')
            else '-1'
       end as num_cpf,
       dbo_orderitems.Name as dsc_nome,
       dbo_employeebase.Email as dsc_email,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 0,2) as num_ddd_telefone,
       substr(regexp_replace(dbo_employeebase.Telephone,'[^0-9]',''), 3,11) as num_telefone,
       string(null) as num_ddd_celular,
       string(null) as num_celular,
       timestamp(dbo_employeebase.CreatedOn) as dat_criacao_contato,
       case when dbo_orderitems.CPF is not null
             and regexp_replace(dbo_orderitems.CPF,'[^0-9]','') <> ''
             and substr(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''), 1, 8) <> '00000000'
            then sha2(regexp_replace(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512)
            else sha2('Audaz - Order Identifier' || nvl(dbo_orderitems.Name,'') || nvl(regexp_replace(dbo_branchs.Cnpj,'[^0-9]',''),'') || nvl(trim(regexp_replace(dbo_orderitems.IdentifierOriginal,'^0+(?!$)|[^0-9a-zA-Z]','')),''),512)
       end as cod_hash_cpf,
       'VRM - Audaz' as dsc_origem
  from prod.silver_audaz.dbo_orderitems
  join prod.silver_audaz.dbo_orders                  on dbo_orderitems.OrderId = dbo_orders.OrderId
  join prod.silver_audaz.dbo_branchs                 on dbo_branchs.BranchId = dbo_orders.BranchId
  left join prod.silver_audaz.dbo_employeebase       on dbo_employeebase.EmployeeBaseId = dbo_orderitems.EmployeesId
 where (dbo_orderitems.CPF is null 
       or regexp_replace(dbo_orderitems.CPF,'[^0-9]','') = ''
       or substr(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''), 1, 8) = '00000000')
    or (dbo_orderitems.CPF is not null 
       and regexp_replace(dbo_orderitems.CPF,'[^0-9]','') <> ''
       and substr(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''), 1, 8) <> '00000000')

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_pagga_mobilidade
-- [DESATIVADO] Sistema Pagga descontinuado com a aquisição da Audaz. Carga completa executada em 28/05/2026.
-- Manter comentado nas execuções diárias.

-- create or replace temporary view tmp_contato_pagga_mobilidade as
-- select distinct
--        nvl(regexp_replace(biadm_snf_relacao_usuario_vt_fct.NR_CPF,'[^0-9]',''),-1) as num_cpf,
--        trim(biadm_snf_relacao_usuario_vt_fct.NM_USUARIO) as dsc_nome,
--        cast(null as string) as dsc_email,
--        cast(null as string) as num_ddd_telefone,
--        cast(null as string) as num_telefone,
--        cast(null as string) as num_ddd_celular,
--        cast(null as string) as num_celular,
--        timestamp(biadm_snf_relacao_usuario_vt_fct.dt_inclusao) as dat_criacao_contato,
--        sha2(regexp_replace(regexp_replace(biadm_snf_relacao_usuario_vt_fct.NR_CPF,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
--        'VRM - Pagga' as dsc_origem
--   from prod.silver_vr_bi_legado.biadm_snf_relacao_usuario_vt_fct
--   join prod.silver_vr_bi_legado.biadm_snf_pedido_venda_vt_fct
--     on biadm_snf_pedido_venda_vt_fct.nr_pedido = biadm_snf_relacao_usuario_vt_fct.nr_pedido_venda
-- ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Gente</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_gente
create or replace temporary view tmp_contato_gente as

select nvl(regexp_replace(produto.cpf,'[^0-9]',''),'-1') as num_cpf,
       produto.first_name || ' ' || produto.last_name as dsc_nome,
       produto.email as dsc_email,
       substr(regexp_replace(produto.main_phone_number,'[^0-9]',''), 0,2) as num_ddd_telefone,
       substr(regexp_replace(produto.main_phone_number,'[^0-9]',''), 3,11) as num_telefone,
       string(null) as num_ddd_celular,
       string(null) as num_celular,
       timestamp(produto.created_at) as dat_criacao_contato,
       case when nvl(produto.cpf,'-1') = '-1'
                 then nvl(produto.cpf, 'HCM-' || sha2(cast(produto.id as string),512)) 
                 else sha2(regexp_replace(regexp_replace(replace(replace(produto.cpf, '.', ''), '-', ''),'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512)
            end as cod_hash_cpf,
       'VR Gente' as dsc_origem
  from prod.silver_vr_gente_produto.public_view_employees_datalake produto
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Admissão</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_admissao
create or replace temporary view tmp_contato_admissao as

select nvl(replace(replace(hcmprd_employees.cpf, '.', ''), '-', ''),-1) as num_cpf,
       upper(hcmprd_employees.name) as dsc_nome,
       hcmprd_employees.email as dsc_email,
       substr(regexp_replace(hcmprd_employees.home_phone,'[^0-9]',''), 0,2) as num_ddd_telefone,
       substr(regexp_replace(hcmprd_employees.home_phone,'[^0-9]',''), 3,11) as num_telefone,
       substr(regexp_replace(hcmprd_employees.phone,'[^0-9]',''), 0,2) as num_ddd_celular,
       substr(regexp_replace(hcmprd_employees.phone,'[^0-9]',''), 3,11) as num_celular,
       timestamp(hcmprd_employees.created_at) as dat_criacao_contato,
       sha2(regexp_replace(regexp_replace(replace(replace(hcmprd_employees.cpf, '.', ''), '-', ''),'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       'VR Admissão' as dsc_origem
  from prod.silver_vr_admissao.hcmprd_employees
;

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_cadastro_trabalhador
create or replace temporary view tmp_contato_cadastro_trabalhador as

select nvl(replace(replace(cadastro_trabalhador.cpf, '.', ''), '-', ''),-1) as num_cpf,
       upper(cadastro_trabalhador.origens_nome) as dsc_nome,
       cadastro_trabalhador.origens_email as dsc_email,
       -- Telefone fixo (len 8, 10, 12)
       case
         when len(origens_celular) = 8  then null
         when len(origens_celular) = 10 then substring(origens_celular, 0, 2)
         when len(origens_celular) = 12 then substring(origens_celular, 2, 2)
         else null
       end as num_ddd_telefone,
       case
         when len(origens_celular) = 8  then origens_celular
         when len(origens_celular) = 10 then substring(origens_celular, 3)
         when len(origens_celular) = 12 then substring(origens_celular, 5)
         else null
       end as num_telefone,
       -- Celular (len 9, 11, 13)
       case
         when len(origens_celular) = 9  then null
         when len(origens_celular) = 11 then substring(origens_celular, 0, 2)
         when len(origens_celular) = 13 then substring(origens_celular, 2, 2)
         else null
       end as num_ddd_celular,
       case
         when len(origens_celular) = 9  then origens_celular
         when len(origens_celular) = 11 then substring(origens_celular, 3)
         when len(origens_celular) = 13 then substring(origens_celular, 5)
         else null
       end as num_celular,
       timestamp(cadastro_trabalhador.datacriacao_date) as dat_criacao_contato,
       sha2(regexp_replace(regexp_replace(replace(replace(cadastro_trabalhador.cpf, '.', ''), '-', ''),'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       'Cadastro Centralizado' as dsc_origem
  from prod.silver_vr_cadastro_centralizado.cadastro_trabalhador
 where cadastro_trabalhador.origens_origem = 'SEM_VR'


-- COMMAND ----------

-- DBTITLE 1,Consolidação
-- MAGIC %md
-- MAGIC <h1>Consolidação + Change Detection</h1>
-- MAGIC
-- MAGIC **Lógica central do notebook:** consolida todas as origens, detecta contatos inativos e calcula hash para change detection em uma única view (`tmp_contato_trabalhador`).
-- MAGIC
-- MAGIC 1. **UNION ALL** — combina os dados de todas as origens ativas
-- MAGIC 2. **Filtro** — exclui registros sem nenhum contato válido (email, telefone ou celular) + regras de exclusão inter-origem (SuperApp/Pagga)
-- MAGIC 3. **Dedup** — remove duplicatas da fonte. Mantém 1 registro por (cpf + email + origem), priorizando o mais recente
-- MAGIC 4. **JOIN dim_cadastro** — associa o `srk_trabalhador_cadastro` usando `cod_hash_cpf`, filtrando pela flag de produto de cada origem
-- MAGIC 5. **FULL OUTER JOIN dim_contato** — leitura única da tabela destino para:
-- MAGIC    * Identificar **contatos inativos** (registros não fornecidos pela origem na última carga → `flg_contato_ativo = 0`)
-- MAGIC    * Capturar **hash destino** para change detection (evita segunda leitura)
-- MAGIC    * Detectar **novos inserts** (source sem match no destino)
-- MAGIC 6. **Ranking** — gera `num_ordenacao` (1, 2, 3...) por pessoa, priorizando data mais recente. Desempate: `flg_contato_ativo DESC` (contato ativo à frente quando datas iguais/NULL)
-- MAGIC 7. **Hash MD5** — calcula hash de todos os campos (incluindo `num_ordenacao` e `flg_contato_ativo`) e compara com hash armazenado

-- COMMAND ----------

-- DBTITLE 1,tmp_contato_consolidado
-- Execução diária (SuperApp e Pagga desativados após carga completa em 28/05/2026).
-- Leitura única de dim_contato via FULL OUTER JOIN (orphan detection + change detection unificados).

create or replace temporary view tmp_contato_trabalhador as

with
tmp_all_sources as (
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_usuario
  union all
  -- [DESATIVADO] SuperApp (carga completa executada em 28/05/2026)
  -- select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
  --   from tmp_superapp
  -- union all
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_contato_base_mobilidade
  union all
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_contato_base_pedido_mobilidade
  union all
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_contato_base_pedido_item_mobilidade
  union all
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_contato_order_mobilidade
  union all
  -- [DESATIVADO] Pagga (carga completa executada em 28/05/2026)
  -- select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
  --   from tmp_contato_pagga_mobilidade
  -- union all
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_contato_gente
  union all
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_contato_admissao
  union all
  select num_cpf, dsc_nome, dsc_email, num_ddd_telefone, num_telefone, num_ddd_celular, num_celular, dat_criacao_contato, cod_hash_cpf, dsc_origem
    from tmp_contato_cadastro_trabalhador
),

-- Origens ativas nesta execução (máx 5 valores após desativação SuperApp/Pagga)
tmp_active_origins as (
  select distinct dsc_origem from tmp_all_sources
),

tmp_filtered as (
  select *
    from tmp_all_sources
   where (nvl(dsc_email,'') <> '' or nvl(num_telefone,'') <> '' or nvl(num_celular,'') <> '')
     -- [DESATIVADO] SuperApp: não incluir se EMAIL já está em VR Benefícios
     -- and not (dsc_origem = 'SuperApp'
     --          and dsc_email in (select dsc_email from tmp_all_sources where dsc_origem = 'VR e Voce'))
     -- [DESATIVADO] Pagga: não incluir se CPF já está em VRM - Audaz
     -- and not (dsc_origem = 'VRM - Pagga'
     --          and num_cpf in (select num_cpf from tmp_all_sources where dsc_origem = 'VRM - Audaz'))
),

-- Dedup: 1 registro por (cpf, email, telefone, celular, origem) — prioriza mais recente
-- Cada combinação distinta de contato é preservada; apenas duplicatas exatas da fonte são eliminadas
tmp_dedup as (
  select *,
         row_number() over(
           partition by cod_hash_cpf, num_cpf, nvl(dsc_email,''), nvl(num_ddd_telefone,''), nvl(num_telefone,''), nvl(num_ddd_celular,''), nvl(num_celular,''), dsc_origem
           order by dat_criacao_contato desc, dsc_nome desc
         ) as rn_dedup
    from tmp_filtered
),

-- Records limpos da fonte (sem duplicatas)
tmp_source_clean as (
  select * from tmp_dedup where rn_dedup = 1
),

-- JOIN com dim_cadastro para obter srk_trabalhador_cadastro
tmp_with_cadastro as (
  select cad.srk_trabalhador_cadastro,
         src.num_cpf,
         upper(src.dsc_nome) as dsc_nome,
         nvl(src.dsc_email, '-1') as dsc_email,
         src.num_ddd_telefone,
         src.num_telefone,
         src.num_ddd_celular,
         src.num_celular,
         src.dat_criacao_contato,
         src.dsc_origem,
         1 as flg_contato_ativo
    from tmp_source_clean src
    join gold_vr_cliente_trabalhador.dim_cadastro cad
      on src.cod_hash_cpf = cad.cod_hash_cpf
     and cad.flg_ativo = 1
     and case
           when src.dsc_origem = 'VR e Voce' then cad.flg_vrb
           -- [DESATIVADO] when src.dsc_origem = 'SuperApp' then cad.flg_vrb
           when src.dsc_origem = 'VRM - Audaz' then cad.flg_vrm
           -- [DESATIVADO] when src.dsc_origem = 'VRM - Pagga' then cad.flg_vrm
           when src.dsc_origem = 'VR Gente' then cad.flg_vrg
           when src.dsc_origem = 'VR Admissão' then cad.flg_admissao
           when src.dsc_origem = 'Cadastro Centralizado' then 1
           else 0
         end = 1
),

-- ============================================================
-- LEITURA ÚNICA de dim_contato: FULL OUTER JOIN com source
-- Resultado:
--   src NOT NULL, dim NOT NULL → contato ativo (flg_contato_ativo=1, pega hash destino)
--   src NOT NULL, dim IS NULL  → novo insert (flg_contato_ativo=1, hash destino NULL)
--   src IS NULL,  dim NOT NULL → contato inativo (flg_contato_ativo=0, usa dados do dim)
-- ============================================================
tmp_full as (
  select
    coalesce(src.srk_trabalhador_cadastro, dim.srk_trabalhador_cadastro) as srk_trabalhador_cadastro,
    coalesce(src.num_cpf, dim.num_cpf) as num_cpf,
    case when src.srk_trabalhador_cadastro is not null then src.dsc_nome else dim.dsc_nome end as dsc_nome,
    coalesce(src.dsc_email, nvl(dim.dsc_email, '-1')) as dsc_email,
    case when src.srk_trabalhador_cadastro is not null then src.num_ddd_telefone else dim.num_ddd_telefone end as num_ddd_telefone,
    case when src.srk_trabalhador_cadastro is not null then src.num_telefone else dim.num_telefone end as num_telefone,
    case when src.srk_trabalhador_cadastro is not null then src.num_ddd_celular else dim.num_ddd_celular end as num_ddd_celular,
    case when src.srk_trabalhador_cadastro is not null then src.num_celular else dim.num_celular end as num_celular,
    case when src.srk_trabalhador_cadastro is not null then src.dat_criacao_contato else dim.dat_criacao_contato end as dat_criacao_contato,
    coalesce(src.dsc_origem, dim.dsc_origem) as dsc_origem,
    case when src.srk_trabalhador_cadastro is not null then 1 else 0 end as flg_contato_ativo,
    dim.cod_hash_md5 as cod_hash_md5_destino
  from tmp_with_cadastro src
  full outer join (
    select srk_trabalhador_cadastro, num_cpf, dsc_nome, dsc_email,
           num_ddd_telefone, num_telefone, num_ddd_celular, num_celular,
           dat_criacao_contato, dsc_origem, cod_hash_md5
      from gold_vr_cliente_trabalhador.dim_contato
     where flg_ativo = 1
       and exists (
             select 1 from tmp_active_origins ao
              where ao.dsc_origem = dsc_origem
           )
  ) dim
    on dim.srk_trabalhador_cadastro = src.srk_trabalhador_cadastro
   and nvl(dim.dsc_email, '-1') = src.dsc_email
   and nvl(dim.num_ddd_telefone,'') = nvl(src.num_ddd_telefone,'')
   and nvl(dim.num_telefone,'') = nvl(src.num_telefone,'')
   and nvl(dim.num_ddd_celular,'') = nvl(src.num_ddd_celular,'')
   and nvl(dim.num_celular,'') = nvl(src.num_celular,'')
   and dim.dsc_origem = src.dsc_origem
),

-- Ranking: contato mais recente = 1, POR ORIGEM. Desempate por flg_contato_ativo (contato ativo primeiro)
tmp_ranked as (
  select *,
         row_number() over(
           partition by srk_trabalhador_cadastro, dsc_origem
           order by dat_criacao_contato desc, flg_contato_ativo desc,
                    dsc_nome desc, dsc_email desc,
                    num_ddd_telefone, num_telefone, num_ddd_celular, num_celular
         ) as num_ordenacao
    from tmp_full
)

-- Cálculo do hash (inclui num_ordenacao e flg_contato_ativo) + hash destino para change detection
select srk_trabalhador_cadastro,
       num_cpf,
       dsc_nome,
       dsc_email,
       num_ddd_telefone,
       num_telefone,
       num_ddd_celular,
       num_celular,
       dat_criacao_contato,
       dsc_origem,
       num_ordenacao,
       flg_contato_ativo,
       md5(upper(
           nvl(cast(srk_trabalhador_cadastro as string),'') ||
           nvl(num_cpf,'') ||
           nvl(dsc_nome,'') ||
           nvl(dsc_email,'') ||
           nvl(num_ddd_telefone,'') ||
           nvl(num_telefone,'') ||
           nvl(num_ddd_celular,'') ||
           nvl(num_celular,'') ||
           nvl(cast(dat_criacao_contato as string),'') ||
           nvl(dsc_origem,'') ||
           nvl(cast(num_ordenacao as string),'') ||
           nvl(cast(flg_contato_ativo as string),'')
       )) as cod_hash_md5,
       cod_hash_md5_destino
  from tmp_ranked
;

-- COMMAND ----------

-- DBTITLE 1,Padroniza campos
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select srk_trabalhador_cadastro,
-- MAGIC        num_cpf,
-- MAGIC        dsc_nome,
-- MAGIC        dsc_email,
-- MAGIC        num_ddd_telefone,
-- MAGIC        num_telefone,
-- MAGIC        num_ddd_celular,
-- MAGIC        num_celular,
-- MAGIC        dat_criacao_contato,
-- MAGIC        dsc_origem,
-- MAGIC        num_ordenacao,
-- MAGIC        flg_contato_ativo,
-- MAGIC        cod_hash_md5,
-- MAGIC        cod_hash_md5_destino
-- MAGIC   from tmp_contato_trabalhador
-- MAGIC  where nvl(cod_hash_md5,'') <> nvl(cod_hash_md5_destino,'')
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_contato_trabalhador_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Merge dim_contato
-- MAGIC %python
-- MAGIC
-- MAGIC df_merge = fn_merge_direto("""
-- MAGIC MERGE INTO gold_vr_cliente_trabalhador_pii.dim_contato as destino
-- MAGIC   USING tmp_contato_trabalhador_criptografado as origem
-- MAGIC      ON destino.srk_trabalhador_cadastro = origem.srk_trabalhador_cadastro
-- MAGIC     and destino.dsc_origem               = origem.dsc_origem
-- MAGIC     and destino.num_ordenacao             = origem.num_ordenacao
-- MAGIC    WHEN MATCHED AND nvl(origem.cod_hash_md5,'') <> nvl(destino.cod_hash_md5,'') THEN UPDATE SET
-- MAGIC         destino.num_cpf             = origem.num_cpf,
-- MAGIC         destino.dsc_nome            = origem.dsc_nome,
-- MAGIC         destino.dsc_email           = origem.dsc_email,
-- MAGIC         destino.num_ddd_telefone    = origem.num_ddd_telefone,
-- MAGIC         destino.num_telefone        = origem.num_telefone,
-- MAGIC         destino.num_ddd_celular     = origem.num_ddd_celular,
-- MAGIC         destino.num_celular         = origem.num_celular,
-- MAGIC         destino.dat_criacao_contato = origem.dat_criacao_contato,
-- MAGIC         destino.dsc_origem          = origem.dsc_origem,
-- MAGIC         destino.num_ordenacao       = origem.num_ordenacao,
-- MAGIC         destino.flg_contato_ativo   = origem.flg_contato_ativo,
-- MAGIC         destino.cod_hash_md5        = origem.cod_hash_md5,
-- MAGIC         destino.dat_alteracao_bi    = current_timestamp()
-- MAGIC    WHEN NOT MATCHED THEN INSERT (
-- MAGIC         destino.srk_trabalhador_cadastro,
-- MAGIC         destino.num_cpf,
-- MAGIC         destino.dsc_nome,
-- MAGIC         destino.dsc_email,
-- MAGIC         destino.num_ddd_telefone,
-- MAGIC         destino.num_telefone,
-- MAGIC         destino.num_ddd_celular,
-- MAGIC         destino.num_celular,
-- MAGIC         destino.dat_criacao_contato,
-- MAGIC         destino.dsc_origem,
-- MAGIC         destino.num_ordenacao,
-- MAGIC         destino.flg_contato_ativo,
-- MAGIC         destino.flg_ativo,
-- MAGIC         destino.cod_hash_md5,
-- MAGIC         destino.dat_inclusao_bi,
-- MAGIC         destino.dat_alteracao_bi
-- MAGIC       ) VALUES (
-- MAGIC         origem.srk_trabalhador_cadastro,
-- MAGIC         origem.num_cpf,
-- MAGIC         origem.dsc_nome,
-- MAGIC         origem.dsc_email,
-- MAGIC         origem.num_ddd_telefone,
-- MAGIC         origem.num_telefone,
-- MAGIC         origem.num_ddd_celular,
-- MAGIC         origem.num_celular,
-- MAGIC         origem.dat_criacao_contato,
-- MAGIC         origem.dsc_origem,
-- MAGIC         origem.num_ordenacao,
-- MAGIC         origem.flg_contato_ativo,
-- MAGIC         1,
-- MAGIC         origem.cod_hash_md5,
-- MAGIC         current_timestamp(),
-- MAGIC         current_timestamp()
-- MAGIC     )"""
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,View Segura
-- MAGIC %python
-- MAGIC
-- MAGIC fn_cria_view_segura(
-- MAGIC             spark=spark,
-- MAGIC             nm_catalogo=databricks_catalog,
-- MAGIC             nm_schema_origem=database + '_pii',
-- MAGIC             nm_tabela_origem=dsc_tabela_pii,
-- MAGIC             nm_schema_destino=database,
-- MAGIC             nm_view_destino=dsc_tabela_pii,
-- MAGIC             fl_cria_view=True
-- MAGIC         )
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Exit notebook
-- MAGIC %python
-- MAGIC
-- MAGIC fn_gera_arquivo_bastao(malha,database,tablename)
-- MAGIC fn_exit_notebook(df_merge)