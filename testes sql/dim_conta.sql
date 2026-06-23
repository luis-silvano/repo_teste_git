-- Databricks notebook source
-- DBTITLE 1,Parametrização utilizada no notebook
-- MAGIC %python
-- MAGIC dbutils.widgets.text("dat_parametro","")
-- MAGIC dbutils.widgets.text("tablename","")
-- MAGIC dbutils.widgets.text("database","")
-- MAGIC dbutils.widgets.text("malha","")
-- MAGIC dbutils.widgets.text("dsc_tabela_pii","")
-- MAGIC dbutils.widgets.text("databricks_catalog","")
-- MAGIC dbutils.widgets.text("location_catalogo_temp","")
-- MAGIC
-- MAGIC dat_parametro = dbutils.widgets.get("dat_parametro")
-- MAGIC tablename = dbutils.widgets.get("tablename")
-- MAGIC database = dbutils.widgets.get("database")
-- MAGIC malha = dbutils.widgets.get("malha")
-- MAGIC dsc_tabela_pii = dbutils.widgets.get("dsc_tabela_pii")
-- MAGIC databricks_catalog = dbutils.widgets.get("databricks_catalog")
-- MAGIC location_catalogo_temp = dbutils.widgets.get("location_catalogo_temp")
-- MAGIC
-- MAGIC # Exemplo de preenchimento
-- MAGIC # dat_parametro = 2026-03-02
-- MAGIC # database = gold_vr_cliente_trabalhador
-- MAGIC # databricks_catalog (DEV) = dev
-- MAGIC # databricks_catalog (PROD) = prod
-- MAGIC # dsc_tabela_pii = dim_conta
-- MAGIC # location_catalogo_temp (DEV) = abfss://gold@adlsvrbeneficiosdev01.dfs.core.windows.net
-- MAGIC # location_catalogo_temp (PROD) = abfss://gold@adlsvrbeneficiosprd.dfs.core.windows.net
-- MAGIC # malha = temp
-- MAGIC # tablename = dim_conta

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC - silver_vr_autorizador.sncoreon_snccu
-- MAGIC - silver_vr_autorizador.sncoreon_snusc
-- MAGIC - silver_vr_autorizador.sncoreon_sncrtcfg
-- MAGIC - gold_vr_cliente_rh.dim_produto
-- MAGIC - gold_vr_cliente_rh.dim_cadastro
-- MAGIC - gold_vr_cliente_trabalhador.dim_cadastro
-- MAGIC - gold_vr_cliente_trabalhador.dim_conta
-- MAGIC - gold_vr_cliente_rh.fat_transacao_credito (não obrigatório)
-- MAGIC - silver_audaz.dbo_employeebase
-- MAGIC - silver_audaz.dbo_branchs
-- MAGIC - silver_audaz.dbo_status
-- MAGIC - silver_audaz.dbo_rechargeorder
-- MAGIC - silver_audaz.dbo_rechargeitem
-- MAGIC - silver_audaz.dbo_schedulequery
-- MAGIC - silver_audaz.dbo_accounts
-- MAGIC - silver_audaz.dbo_partners
-- MAGIC - silver_audaz.dbo_orderitems
-- MAGIC - silver_audaz.dbo_orders
-- MAGIC - silver_vr_bi_legado.biadm_snf_relacao_usuario_vt_fct
-- MAGIC - silver_vr_bi_legado.biadm_snf_pedido_venda_vt_fct    
-- MAGIC - silver_vr_gente_produto.public_view_employees_datalake
-- MAGIC - silver_vr_gente_produto.public_view_clients_datalake
-- MAGIC - silver_vr_admissao.hcmprd_employees
-- MAGIC - silver_vr_admissao.hcmprd_employments
-- MAGIC - silver_vr_admissao.hcmprd_business_units

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

-- DBTITLE 1,tmp_transacao_credito_ordenada
create or replace temporary view tmp_transacao_credito_ordenada as

Select fat_transacao_credito.num_conta,
       max(cast(fat_transacao_credito.val_transacao_credito as decimal(11, 2))) as val_transacao,
       min(fat_transacao_credito_min_dat.dat_transacao) as dat_transacao
  from gold_vr_cliente_rh.fat_transacao_credito
 inner join (Select fat_transacao_credito.num_conta,
               min(fat_transacao_credito.dat_transacao) as dat_transacao
          from gold_vr_cliente_rh.fat_transacao_credito
         group by fat_transacao_credito.num_conta) as fat_transacao_credito_min_dat
    on fat_transacao_credito.num_conta = fat_transacao_credito_min_dat.num_conta
   and fat_transacao_credito.dat_transacao = fat_transacao_credito_min_dat.dat_transacao
 group by fat_transacao_credito.num_conta


-- COMMAND ----------

-- DBTITLE 1,tmp_num_contrato
create or replace temporary view tmp_num_contrato as

select distinct
       numero as num_conta_plataforma,
       contratodb_id as num_contrato
  from prod.silver_vr_plataforma_de_pedidos.app_snped_conta


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_vrb
create or replace temporary view tmp_trabalhador_conta_vrb as

select int(sncoreon_snccu.ccunum) as num_conta,
       sncoreon_snccu.ccunumemi as num_conta_plataforma,
       sncoreon_snccu.aplcod as cod_aplicacao,
       dim_produto.cod_sdprod,
       dim_produto.cod_produto,
       dim_produto.dsc_produto,
       dim_produto.cod_emissor,
       sncoreon_snccu.cfjseq as cod_autorizador_matriz,
       sncoreon_snccu.jurcgccmp as cod_autorizador_filial,
       dim_trabalhador_cadastro.srk_trabalhador_cadastro,
       int(sncoreon_snccu.usccod) as cod_usuario,
       sncoreon_snusc.uscnum as cod_matricula,
       cast(sncoreon_sncrtcfg.depcod as int) as cod_centro_custo,
       dim_rh_cadastro.srk_rh_cadastro,
       sncoreon_snccu.ccuabedatemi as dat_abertura,
       cast(sncoreon_snccu.ccusld as decimal(11,2)) as val_saldo,
       cast(sncoreon_snccu.ccusldblq as decimal(11,2)) as val_saldo_bloqueado,
       int(sncoreon_snccu.ccusit) as cod_situacao,
       case sncoreon_snccu.ccusit
            when 0 then 'Ativo'
            when 1 then 'Inativo'
            when 3 then 'Expurgado'
       end as dsc_situacao,
       --sncoreon_snccu.ccupridat as dat_primeiro_consumo,
       sncoreon_snccu.ccupridat as dat_primeiro_credito,
       tmp_transacao_credito_ordenada.val_transacao as val_primeiro_credito,
       sncoreon_snccu.ccuultconsumodat as dat_ultimo_consumo,
       cast(sncoreon_snccu.ccucmprultvlr as decimal(11,2)) as val_ultima_compra,
       sncoreon_snccu.ccucrdultdat as dat_ultimo_credito,
       cast(sncoreon_snccu.ccucrdultvlr as decimal(11,2)) as val_ultimo_credito,
       sncoreon_snccu.ccuincdat as dat_criacao_conta,
       sncoreon_snccu.ccuatudat as dat_atualizacao,
       cast(sncoreon_snccu.ccursv as decimal(11,2)) as val_saldo_reserva,
       cast(sncoreon_snccu.ccursvblq as decimal(11, 2)) as val_saldo_reserva_bloqueado,
       sncoreon_snccu.id_config_conta_externa as cod_conta_externa,
       sncoreon_sncpr.dat_ultimo_pedido,
       tmp_num_contrato.num_contrato,
       dim_contrato.srk_rh_contrato,
       'VR Benefícios' as dsc_origem
  from prod.silver_vr_autorizador.sncoreon_snccu
  left join gold_vr_cliente_rh.dim_produto
    on sncoreon_snccu.aplcod = dim_produto.cod_aplicacao
  left join prod.silver_vr_autorizador.sncoreon_snusc
    on int(sncoreon_snccu.usccod) = int(sncoreon_snusc.usccod)
  left join tmp_transacao_credito_ordenada
    on sncoreon_snccu.ccunum = tmp_transacao_credito_ordenada.num_conta
  left join gold_vr_cliente_trabalhador.dim_cadastro as dim_trabalhador_cadastro
    on dim_trabalhador_cadastro.num_cpf = lpad( cast(sncoreon_snusc.usccpf as int) || lpad(cast(sncoreon_snusc.usccpfdig as int),2,'0') ,11,'0')
   and dim_trabalhador_cadastro.flg_ativo = 1
   and dim_trabalhador_cadastro.flg_vrb = 1
  left join gold_vr_cliente_rh.dim_cadastro as dim_rh_cadastro
    on sncoreon_snccu.cfjseq = dim_rh_cadastro.cod_autorizador_matriz
   and sncoreon_snccu.jurcgccmp = dim_rh_cadastro.cod_autorizador_filial
   and dim_rh_cadastro.flg_ativo = 1
   and dim_rh_cadastro.flg_vrb = 1
  left join prod.silver_vr_autorizador.sncoreon_sncrtcfg
    on sncoreon_snccu.ccunum = sncoreon_sncrtcfg.ccunum
   and sncoreon_snccu.usccod = sncoreon_sncrtcfg.usccod
  left join (select max(cprincdat) as dat_ultimo_pedido, ccunumemi
               from prod.silver_vr_autorizador.sncoreon_sncpr
              group by ccunumemi) as sncoreon_sncpr
    on sncoreon_snccu.ccunumemi = sncoreon_sncpr.ccunumemi
  left join tmp_num_contrato
    on sncoreon_snccu.ccunumemi = tmp_num_contrato.num_conta_plataforma
  left join gold_vr_cliente_rh.dim_contrato
    on tmp_num_contrato.num_contrato = dim_contrato.num_contrato
   and dim_contrato.flg_ativo = '1'
   and dim_contrato.dsc_origem = 'VR Benefícios'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Mobilidade</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_base_audaz
create or replace table vr_temporaria.tmp_trabalhador_conta_base_audaz as

Select dbo_employeebase.EmployeeBaseId as cod_trabalhador,
       regexp_replace(dbo_employeebase.Cpf,'[^0-9]','') as num_cpf,
       regexp_replace(dbo_branchs.Cnpj,'[^0-9]','') as num_cnpj,
       dbo_status.StatusText as dsc_status,
       dbo_employeebase.CreatedOn as dat_criacao,
       sha2(regexp_replace(regexp_replace(dbo_employeebase.Cpf,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       dbo_branchs.BranchId as cod_contrato
  from prod.silver_audaz.dbo_employeebase
 inner join prod.silver_audaz.dbo_branchs
    on dbo_branchs.BranchId = dbo_employeebase.BranchId
  left join prod.silver_audaz.dbo_status
    on dbo_status.TableName = 'EmployeeBase'
   and dbo_status.FieldName = 'Status'
   and dbo_status.StatusCode = dbo_employeebase.Status


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_base_pedido_audaz
create or replace table vr_temporaria.tmp_trabalhador_conta_base_pedido_audaz as

Select distinct
       dbo_rechargeitem.EmployeesBaseId as cod_trabalhador, 
       string(null) as num_cpf,
       regexp_replace(dbo_branchs.Cnpj,'[^0-9]','') as num_cnpj,
       string(null) as dsc_status,
       timestamp(null) as dat_criacao,
       sha2(regexp_replace(regexp_replace(nvl(dbo_rechargeitem.Identifier,dbo_rechargeitem.IdentifierERP),'^0+(?!$)|[^0-9a-zA-Z]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       dbo_branchs.BranchId as cod_contrato
  from prod.silver_audaz.dbo_rechargeorder
 inner join prod.silver_audaz.dbo_rechargeitem
    on dbo_rechargeorder.RechargeOrderId = dbo_rechargeitem.RechargeOrderId
 inner join prod.silver_audaz.dbo_schedulequery
    on dbo_rechargeorder.ScheduleQueryId = dbo_schedulequery.ScheduleQueryId
 inner join prod.silver_audaz.dbo_branchs
    on dbo_schedulequery.BranchId = dbo_branchs.BranchId
 inner join prod.silver_audaz.dbo_accounts
    on dbo_branchs.AccountId = dbo_accounts.AccountId
  left join prod.silver_audaz.dbo_partners
    on dbo_accounts.PartnerId = dbo_partners.PartnerId
  left join prod.silver_audaz.dbo_employeebase
    on dbo_employeebase.EmployeeBaseId = dbo_rechargeitem.EmployeesBaseId
where 1=1
  and dbo_rechargeorder.STATUS in (1, 2, 4)
  and dbo_rechargeorder.type in (1, 2, 3, 4, 8)
  and dbo_accounts.Name not like ('Teste%')
  and nvl(dbo_partners.Name, 'Audaz') in ('Audaz', 'Mastermaq [Parceiro]')
  and upper(dbo_accounts.accountid) not in ('F813C11D-6A8E-47AD-933B-DCD722BE9996')
  and dbo_employeebase.EmployeeBaseId is null


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_base_pedido_item_audaz
create or replace table vr_temporaria.tmp_trabalhador_conta_base_pedido_item_audaz as

Select distinct
       dbo_rechargeitem.EmployeesBaseId as cod_trabalhador,
       regexp_replace(dbo_employeebase.Cpf,'[^0-9]','') as num_cpf,
       regexp_replace(dbo_branchs.Cnpj,'[^0-9]','') as num_cnpj,
       dbo_status.StatusText as dsc_status,
       dbo_employeebase.CreatedOn as dat_criacao,
       case when dbo_employeebase.Cpf is null
          then sha2(regexp_replace(regexp_replace(nvl(dbo_rechargeitem.Identifier,dbo_rechargeitem.IdentifierERP),'^0+(?!$)|[^0-9a-zA-Z]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512)
          else sha2(regexp_replace(regexp_replace(dbo_employeebase.Cpf,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512)
       end as cod_hash_cpf,
       dbo_branchs.BranchId as cod_contrato
  from prod.silver_audaz.dbo_rechargeorder
 inner join prod.silver_audaz.dbo_rechargeitem
    on dbo_rechargeorder.RechargeOrderId = dbo_rechargeitem.RechargeOrderId
  left join prod.silver_audaz.dbo_schedulequery
    on dbo_rechargeorder.ScheduleQueryId = dbo_schedulequery.ScheduleQueryId
  left join prod.silver_audaz.dbo_branchs
    on dbo_schedulequery.BranchId = dbo_branchs.BranchId
  left join prod.silver_audaz.dbo_employeebase
    on dbo_employeebase.EmployeeBaseId = dbo_rechargeitem.EmployeesBaseId
  left join prod.silver_audaz.dbo_status
    on dbo_status.TableName = 'EmployeeBase'
   and dbo_status.FieldName = 'Status'
   and dbo_status.StatusCode = dbo_employeebase.Status


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_order_audaz
create or replace table vr_temporaria.tmp_trabalhador_conta_order_audaz as

Select distinct
       sha2(regexp_replace(dbo_orderitems.CPF,'[^0-9]','') || regexp_replace(dbo_branchs.Cnpj,'[^0-9]',''),512) as cod_trabalhador,
       regexp_replace(dbo_orderitems.CPF,'[^0-9]','') as num_cpf,
       regexp_replace(dbo_branchs.Cnpj,'[^0-9]','') as num_cnpj,
       string(null) as dsc_status,
       date(dbo_orders.CreatedOn) as dat_criacao,
       sha2(regexp_replace(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})', '\$1.\$2.\$3-\$4'),512) as cod_hash_cpf,
       dbo_branchs.BranchId as cod_contrato
  from prod.silver_audaz.dbo_orderitems
 inner join prod.silver_audaz.dbo_orders
    on dbo_orderitems.OrderId = dbo_orders.OrderId
 inner join prod.silver_audaz.dbo_branchs
    on dbo_branchs.BranchId = dbo_orders.BranchId
 where dbo_orderitems.CPF is not null 
   and regexp_replace(dbo_orderitems.CPF,'[^0-9]','') <> ''
   and substr(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''), 1, 8) <> '00000000'

 union all

--Trabalhadores sem cpf preenchidos
Select distinct
       sha2('Audaz - Order Identifier' || nvl(dbo_orderitems.Name,'') || nvl(regexp_replace(dbo_branchs.Cnpj,'[^0-9]',''),'') || nvl(trim(regexp_replace(dbo_orderitems.IdentifierOriginal,'^0+(?!$)|[^0-9a-zA-Z]','')),''),512) as cod_trabalhador,
       '-1' as num_cpf,
       regexp_replace(dbo_branchs.Cnpj,'[^0-9]','') as num_cnpj,
       string(null) as dsc_status,
       string(null) as dat_criacao,
       sha2('Audaz - Order Identifier' || nvl(dbo_orderitems.Name,'') || nvl(regexp_replace(dbo_branchs.Cnpj,'[^0-9]',''),'') || nvl(trim(regexp_replace(dbo_orderitems.IdentifierOriginal,'^0+(?!$)|[^0-9a-zA-Z]','')),''),512) as cod_hash_cpf,
       dbo_branchs.BranchId as cod_contrato
  from prod.silver_audaz.dbo_orderitems
 inner join prod.silver_audaz.dbo_orders
    on dbo_orderitems.OrderId = dbo_orders.OrderId
 inner join prod.silver_audaz.dbo_branchs
    on dbo_branchs.BranchId = dbo_orders.BranchId
 where dbo_orderitems.CPF is null
    or regexp_replace(dbo_orderitems.CPF,'[^0-9]','') = ''
    or substr(regexp_replace(dbo_orderitems.CPF,'[^0-9]',''), 1, 8) = '00000000'


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_audaz
create or replace temporary view tmp_trabalhador_conta_audaz as

with tmp_union as (
  select tmp_trabalhador_conta_base_audaz.*,
         'VRM - Audaz' as dsc_origem
    from vr_temporaria.tmp_trabalhador_conta_base_audaz
  union 
  select tmp_trabalhador_conta_base_pedido_audaz.*,
         'VRM - Audaz' as dsc_origem
    from vr_temporaria.tmp_trabalhador_conta_base_pedido_audaz
  union 
  select tmp_trabalhador_conta_base_pedido_item_audaz.*,
         'VRM - Audaz' as dsc_origem
    from vr_temporaria.tmp_trabalhador_conta_base_pedido_item_audaz
  union
  select tmp_trabalhador_conta_order_audaz.*,
         'VRM - Audaz' as dsc_origem
    from vr_temporaria.tmp_trabalhador_conta_order_audaz
)

, tmp_max_union (
  select cod_hash_cpf,
         max(dat_criacao) as dat_criacao
    from tmp_union
   group by cod_hash_cpf
) 

, tmp_audaz as (
  select tmp_union.cod_trabalhador as num_conta,
         nvl(dim_produto.cod_aplicacao,'-1') as cod_aplicacao,
         nvl(dim_contrato.cod_sdprod,'-1') as cod_sdprod,
         nvl(dim_contrato.cod_produto,'-1') as cod_produto,
         nvl(dim_produto.dsc_produto,'-1') as dsc_produto,
         nvl(dim_produto.cod_emissor,'-1') as cod_emissor,
         nvl(dim_trabalhador_cadastro.srk_trabalhador_cadastro,'-1') as srk_trabalhador_cadastro,
         nvl(dim_rh_cadastro.srk_rh_cadastro,'-1') as srk_rh_cadastro,
         tmp_union.dat_criacao as dat_criacao_conta,
         tmp_union.cod_contrato as num_contrato,
         nvl(dim_contrato.srk_rh_contrato,'-1') as srk_rh_contrato,
         tmp_union.dsc_origem
    from tmp_union
   inner join tmp_max_union
      on tmp_union.cod_hash_cpf = tmp_max_union.cod_hash_cpf
     and nvl(tmp_union.dat_criacao,'') = nvl(tmp_max_union.dat_criacao,'')
    left join gold_vr_cliente_trabalhador.dim_cadastro as dim_trabalhador_cadastro
      on tmp_union.cod_hash_cpf = dim_trabalhador_cadastro.cod_hash_cpf
     and dim_trabalhador_cadastro.flg_ativo = 1
     and dim_trabalhador_cadastro.flg_vrm = 1
    left join gold_vr_cliente_rh.dim_cadastro as dim_rh_cadastro
      on tmp_union.num_cnpj = dim_rh_cadastro.num_cnpj
     and dim_rh_cadastro.flg_ativo = 1
     and dim_rh_cadastro.flg_vrm = 1
    left join gold_vr_cliente_rh.dim_contrato
      on tmp_union.cod_contrato = dim_contrato.num_contrato
     and dim_contrato.flg_ativo = '1'
     and dim_contrato.dsc_origem in ('VR Mobilidade', 'VR Mobilidade - Dynamics')
    left join gold_vr_cliente_rh.dim_produto
      on dim_contrato.cod_sdprod = dim_produto.cod_sdprod
     and dim_contrato.cod_produto = dim_produto.cod_produto
     and dim_produto.flg_ativo = 1
)

select tmp_audaz.num_conta,
       max(tmp_audaz.cod_aplicacao) as cod_aplicacao,
       max(tmp_audaz.cod_sdprod) as cod_sdprod,
       max(tmp_audaz.cod_produto) as cod_produto,
       max(tmp_audaz.dsc_produto) as dsc_produto,
       max(tmp_audaz.cod_emissor) as cod_emissor,
       max(tmp_audaz.srk_trabalhador_cadastro) as srk_trabalhador_cadastro,
       max(tmp_audaz.srk_rh_cadastro) as srk_rh_cadastro,
       max(tmp_audaz.dat_criacao_conta) as dat_criacao_conta,
       max(tmp_audaz.num_contrato) as num_contrato,
       max(tmp_audaz.srk_rh_contrato) as srk_rh_contrato,
       tmp_audaz.dsc_origem
  from tmp_audaz
 group by tmp_audaz.num_conta,
       tmp_audaz.dsc_origem


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_pagga
create or replace table vr_temporaria.tmp_trabalhador_conta_pagga as

with tmp_pagga as (
  select distinct 
         sha2('Pagga' || regexp_replace(biadm_snf_relacao_usuario_vt_fct.NR_CPF,'[^0-9]','') || biadm_snf_pedido_venda_vt_fct.nr_cnpj, 512) as num_conta,
         nvl(dim_trabalhador_cadastro.srk_trabalhador_cadastro,'-1') as srk_trabalhador_cadastro,
         nvl(dim_rh_cadastro.srk_rh_cadastro,'-1') as srk_rh_cadastro,
         biadm_snf_pedido_venda_vt_fct.DT_EMISSAO as dat_criacao_conta
    from prod.silver_vr_bi_legado.biadm_snf_relacao_usuario_vt_fct
   inner join prod.silver_vr_bi_legado.biadm_snf_pedido_venda_vt_fct       
      on biadm_snf_pedido_venda_vt_fct.nr_pedido = biadm_snf_relacao_usuario_vt_fct.nr_pedido_venda
    left join gold_vr_cliente_trabalhador.dim_cadastro dim_trabalhador_cadastro
      on sha2(regexp_replace(regexp_replace(biadm_snf_relacao_usuario_vt_fct.NR_CPF,'[^0-9]',''),'(\\d{3})(\\d{3})(\\d{3})(\\d{2})',   '\$1.\$2.\$3-\$4'),512) = dim_trabalhador_cadastro.cod_hash_cpf
     and dim_trabalhador_cadastro.flg_ativo = 1
     and dim_trabalhador_cadastro.flg_vrm = 1
    left join gold_vr_cliente_rh.dim_cadastro dim_rh_cadastro
      on biadm_snf_pedido_venda_vt_fct.nr_cnpj = dim_rh_cadastro.num_cnpj
     and dim_rh_cadastro.flg_ativo = 1
     and dim_rh_cadastro.flg_vrm = 1
)

, tmp_pagga_max_criacao as (
  select num_conta,
         max(dat_criacao_conta) as dat_criacao_conta
    from tmp_pagga
   group by num_conta
)

Select tmp_pagga.*,
       'VRM - Pagga' as dsc_origem
  from tmp_pagga
  join tmp_pagga_max_criacao
       on tmp_pagga.num_conta = tmp_pagga_max_criacao.num_conta
      and tmp_pagga.dat_criacao_conta = tmp_pagga_max_criacao.dat_criacao_conta
 where not exists (select srk_trabalhador_cadastro from tmp_trabalhador_conta_audaz where tmp_trabalhador_conta_audaz.srk_trabalhador_cadastro = tmp_pagga.srk_trabalhador_cadastro)


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_mobilidade
create or replace table vr_temporaria.tmp_trabalhador_conta_mobilidade as

select num_conta,
       cod_aplicacao,
       cod_sdprod,
       cod_produto,
       dsc_produto,
       cod_emissor,
       srk_trabalhador_cadastro,
       srk_rh_cadastro,
       dat_criacao_conta,
       num_contrato,
       srk_rh_contrato,
       dsc_origem
  from tmp_trabalhador_conta_audaz
union
select num_conta,
       '-1' as cod_aplicacao,
       '-1' as cod_sdprod,
       '-1' as cod_produto,
       '-1' as dsc_produto,
       '-1' as cod_emissor,
       srk_trabalhador_cadastro,
       srk_rh_cadastro,
       dat_criacao_conta,
       '-1' as num_contrato,
       '-1' as srk_rh_contrato,
       dsc_origem
  from vr_temporaria.tmp_trabalhador_conta_pagga


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Gente</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_gente
create or replace temporary view tmp_trabalhador_conta_gente as

with tmp_base_cpf as (
  Select id as num_conta,
         client_id,
         upper(first_name) || ' ' || upper(last_name) as dsc_nome,
         case
           when cpf is null then null
           when trim(cpf) = '' then null
           else regexp_replace(cpf,'[^0-9]','')
         end as num_cpf,
         case active when 'true' then 'Ativo' else 'Inativo' end as dsc_status_vrg,
         case has_time_cards when 'true' then 1 else 0 end as flg_bate_ponto,
         created_at as dat_criacao,
         deleted_at as dat_desativacao_vrg,
         case when nvl(configuration_id,0) = 0 then 0 else 1 end as flg_configuracao_ferias_vrg,
         case
              when hierarchy_level = 1 then 'administrador'
              when hierarchy_level = 2 then 'gestor'
          else 'Trabalhador' end as dsc_cargo
    from prod.silver_vr_gente_produto.public_view_employees_datalake
)

, tmp_menor_num_conta (
  Select min(num_conta) as num_conta,
         dsc_nome
    from tmp_base_cpf
   where num_cpf is null
   group by dsc_nome
)

, tmp_base_cpf_pronta as (
  Select num_conta,
         client_id,
         dsc_nome,
         num_cpf,
         dsc_status_vrg,
         flg_bate_ponto,
         dat_criacao,
         dat_desativacao_vrg,
         flg_configuracao_ferias_vrg,
         dsc_cargo
    from tmp_base_cpf
   where num_cpf is not null
  union
  Select tmp_base_cpf.num_conta,
         tmp_base_cpf.client_id,
         tmp_base_cpf.dsc_nome,
         'VRG-' || tmp_menor_num_conta.num_conta as num_cpf,
         tmp_base_cpf.dsc_status_vrg,
         tmp_base_cpf.flg_bate_ponto,
         tmp_base_cpf.dat_criacao,
         tmp_base_cpf.dat_desativacao_vrg,
         tmp_base_cpf.flg_configuracao_ferias_vrg,
         tmp_base_cpf.dsc_cargo
    from tmp_base_cpf
   inner join tmp_menor_num_conta
      on tmp_base_cpf.dsc_nome = tmp_menor_num_conta.dsc_nome
     and tmp_base_cpf.num_cpf is null
)

select tmp_base_cpf_pronta.num_conta,
       dim_produto.cod_aplicacao,
       dim_contrato.cod_sdprod,
       dim_contrato.cod_produto,
       dim_produto.dsc_produto,
       dim_produto.cod_emissor,
       dim_cadastro.srk_trabalhador_cadastro,
       dim_contrato.srk_rh_cadastro,
       tmp_base_cpf_pronta.dat_criacao,
       tmp_base_cpf_pronta.dsc_status_vrg,
       tmp_base_cpf_pronta.flg_bate_ponto,
       tmp_base_cpf_pronta.dat_desativacao_vrg,
       tmp_base_cpf_pronta.flg_configuracao_ferias_vrg,
       tmp_base_cpf_pronta.dsc_cargo,
       dim_contrato.num_contrato,
       max(dim_contrato.srk_rh_contrato) as srk_rh_contrato,
       'VR Gente' as dsc_origem
  from tmp_base_cpf_pronta
 inner join gold_vr_cliente_rh.dim_contrato
    on dim_contrato.num_contrato = tmp_base_cpf_pronta.client_id
   and dim_contrato.cod_produto = 'BI-VR-GENTE'
   and dim_contrato.flg_ativo = 1
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.num_cpf = tmp_base_cpf_pronta.num_cpf
   and dim_cadastro.flg_ativo = 1
 inner join gold_vr_cliente_rh.dim_produto
    on dim_contrato.cod_sdprod = dim_produto.cod_sdprod
   and dim_contrato.cod_produto = dim_produto.cod_produto
   and dim_produto.flg_ativo = 1
 group by all


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Admissão</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_admissao
create or replace temporary view tmp_trabalhador_conta_admissao as

Select hcmprd_employees.id as num_conta,
       dim_produto.cod_aplicacao,
       dim_contrato.cod_sdprod,
       dim_contrato.cod_produto,
       dim_produto.dsc_produto,
       dim_produto.cod_emissor,
       dim_cadastro.srk_trabalhador_cadastro,
       dim_contrato.srk_rh_cadastro,
       dim_contrato.num_contrato,
       dim_contrato.srk_rh_contrato,
       'VR Admissão' as dsc_origem
  from prod.silver_vr_admissao.hcmprd_employees
 inner join gold_vr_cliente_rh.dim_contrato
    on hcmprd_employees.contract_id = dim_contrato.num_contrato
   and dim_contrato.cod_produto = 'BI-VR-GENTE-HCM'
   and dim_contrato.flg_ativo = 1
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.num_cpf = regexp_replace(hcmprd_employees.cpf,'[^0-9]','')
   and dim_cadastro.flg_ativo = 1
 inner join gold_vr_cliente_rh.dim_produto
    on dim_contrato.cod_sdprod = dim_produto.cod_sdprod
   and dim_contrato.cod_produto = dim_produto.cod_produto
   and dim_produto.flg_ativo = 1


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>Consolidação</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_consolidado
create or replace table vr_temporaria.tmp_trabalhador_conta_consolidado as

with tmp_consolidacao as (
  select num_conta,
         num_conta_plataforma,
         cod_aplicacao,
         cod_sdprod,
         cod_produto,
         dsc_produto,
         cod_emissor,
         cod_autorizador_matriz,
         cod_autorizador_filial,
         srk_trabalhador_cadastro,
         cod_usuario,
         cod_matricula,
         cod_centro_custo,
         srk_rh_cadastro,
         dat_abertura,
         val_saldo,
         val_saldo_bloqueado,
         cod_situacao,
         dsc_situacao,
        --  dat_primeiro_consumo,
         dat_primeiro_credito,
         val_primeiro_credito,
         dat_ultimo_consumo,
         val_ultima_compra,
         dat_ultimo_credito,
         val_ultimo_credito,
         dat_criacao_conta,
         dat_atualizacao,
         val_saldo_reserva,
         val_saldo_reserva_bloqueado,
         cod_conta_externa,
         dat_ultimo_pedido,
         string(null) as dsc_status_vrg,
         int(null) as flg_bate_ponto,
         timestamp(null) as dat_desativacao_vrg,
         int(null) as flg_configuracao_ferias_vrg,
         num_contrato,
         srk_rh_contrato,
         dsc_origem,
         null as dsc_cargo
    from tmp_trabalhador_conta_vrb
  union
  select num_conta,
         '-1' as num_conta_plataforma,
         cod_aplicacao,
         cod_sdprod,
         cod_produto,
         dsc_produto,
         cod_emissor,
         '-1' as cod_autorizador_matriz,
         '-1' as cod_autorizador_filial,
         srk_trabalhador_cadastro,
         '-1' as cod_usuario,
         '-1' as cod_matricula,
         '-1' as cod_centro_custo,
         srk_rh_cadastro,
         string(null) as dat_abertura,
         string(null) as val_saldo,
         string(null) as val_saldo_bloqueado,
         '-1' as cod_situacao,
         '-1' as dsc_situacao,
        --  string(null) as dat_primeiro_consumo,
         string(null) as dat_primeiro_credito, 
         string(null) as val_primeiro_credito,
         string(null) as dat_ultimo_consumo,
         string(null) as val_ultima_compra,
         string(null) as dat_ultimo_credito,
         string(null) as val_ultimo_credito,
         dat_criacao_conta,
         string(null) as dat_atualizacao,
         string(null) as val_saldo_reserva,
         string(null) as val_saldo_reserva_bloqueado,
         '-1' as cod_conta_externa,
         string(null) as dat_ultimo_pedido,
         string(null) as dsc_status_vrg,
         int(null) as flg_bate_ponto,
         timestamp(null) as dat_desativacao_vrg,
         int(null) as flg_configuracao_ferias_vrg,
         num_contrato,
         srk_rh_contrato,
         dsc_origem,
         null as dsc_cargo
    from vr_temporaria.tmp_trabalhador_conta_mobilidade
  union
  select num_conta,
           '-1' as num_conta_plataforma,
         cod_aplicacao,
         cod_sdprod,
         cod_produto,
         dsc_produto,
         cod_emissor,
         '-1' as cod_autorizador_matriz,
         '-1' as cod_autorizador_filial,
         srk_trabalhador_cadastro,
         '-1' as cod_usuario,
         '-1' as cod_matricula,
         '-1' as cod_centro_custo,
         srk_rh_cadastro,
         string(null) as dat_abertura,
         string(null) as val_saldo,
         string(null) as val_saldo_bloqueado,
         '-1' as cod_situacao,
         '-1' as dsc_situacao,
        --  string(null) as dat_primeiro_consumo,
         string(null) as dat_primeiro_credito, 
         string(null) as val_primeiro_credito,
         string(null) as dat_ultimo_consumo,
         string(null) as val_ultima_compra,
         string(null) as dat_ultimo_credito,
         string(null) as val_ultimo_credito,
         dat_criacao,
         string(null) as dat_atualizacao,
         string(null) as val_saldo_reserva,
         string(null) as val_saldo_reserva_bloqueado,
         '-1' as cod_conta_externa,
         string(null) as dat_ultimo_pedido,
         dsc_status_vrg,
         flg_bate_ponto,
         dat_desativacao_vrg,
         flg_configuracao_ferias_vrg,
         num_contrato,
         srk_rh_contrato,
         dsc_origem,
         dsc_cargo
    from tmp_trabalhador_conta_gente
  union
  select num_conta,
         '-1' as num_conta_plataforma,
         cod_aplicacao,
         cod_sdprod,
         cod_produto,
         dsc_produto,
         cod_emissor,
         '-1' as cod_autorizador_matriz,
         '-1' as cod_autorizador_filial,
         srk_trabalhador_cadastro,
         '-1' as cod_usuario,
         string(null) as cod_matricula,
         '-1' as cod_centro_custo,
         srk_rh_cadastro,
         string(null) as dat_abertura,
         string(null) as val_saldo,
         string(null) as val_saldo_bloqueado,
         '-1' as cod_situacao,
         '-1' as dsc_situacao,
        --  string(null) as dat_primeiro_consumo,
         string(null) as dat_primeiro_credito, 
         string(null) as val_primeiro_credito,
         string(null) as dat_ultimo_consumo,
         string(null) as val_ultima_compra,
         string(null) as dat_ultimo_credito,
         string(null) as val_ultimo_credito,
         string(null) as dat_criacao_conta,
         string(null) as dat_atualizacao,
         string(null) as val_saldo_reserva,
         string(null) as val_saldo_reserva_bloqueado,
         '-1' as cod_conta_externa,
         string(null) as dat_ultimo_pedido,
         string(null) as dsc_status_vrg,
         int(null) as flg_bate_ponto,
         timestamp(null) as dat_desativacao_vrg,
         int(null) as flg_configuracao_ferias_vrg,
         num_contrato,
         srk_rh_contrato,
         dsc_origem,
         null as dsc_cargo
    from tmp_trabalhador_conta_admissao
)

select tmp_consolidacao.*,
       case
         when dim_conta.srk_trabalhador_conta is null then 'inserir'
         when nvl(tmp_consolidacao.dsc_situacao,'') <> nvl(dim_conta.dsc_situacao,'') then 'versionar'
         when nvl(tmp_consolidacao.dsc_status_vrg,'') <> nvl(dim_conta.dsc_status_vrg,'') then 'versionar'
         when nvl(tmp_consolidacao.flg_bate_ponto,'') <> nvl(dim_conta.flg_bate_ponto,'') then 'versionar'
         when nvl(tmp_consolidacao.dat_desativacao_vrg,'') <> nvl(dim_conta.dat_desativacao_vrg,'') then 'versionar'
         when nvl(tmp_consolidacao.flg_configuracao_ferias_vrg,'') <> nvl(dim_conta.flg_configuracao_ferias_vrg,'') then 'versionar'
       else 'atualizar'
       end as dsc_acao,
       --Criação do código hash MD5 do registro
       md5(upper(
           nvl(tmp_consolidacao.num_conta,'') ||
           nvl(tmp_consolidacao.num_conta_plataforma,'') ||
           nvl(tmp_consolidacao.cod_aplicacao,'') ||
           nvl(tmp_consolidacao.cod_sdprod,'') ||
           nvl(tmp_consolidacao.cod_produto,'') ||
           nvl(tmp_consolidacao.dsc_produto,'') ||
           nvl(tmp_consolidacao.cod_emissor,'') ||
           nvl(tmp_consolidacao.srk_trabalhador_cadastro,'') ||
           nvl(tmp_consolidacao.cod_usuario,'') ||
           nvl(tmp_consolidacao.cod_matricula,'') ||
           nvl(tmp_consolidacao.cod_centro_custo,'') ||
           nvl(tmp_consolidacao.srk_rh_cadastro,'') ||
           nvl(tmp_consolidacao.dat_abertura,'') ||
           nvl(tmp_consolidacao.val_saldo,'') ||
           nvl(tmp_consolidacao.val_saldo_bloqueado,'') ||
           nvl(tmp_consolidacao.cod_situacao,'') ||
           nvl(tmp_consolidacao.dsc_situacao,'') ||
           nvl(tmp_consolidacao.cod_autorizador_matriz,'') ||
           nvl(tmp_consolidacao.cod_autorizador_filial,'') ||
           nvl(tmp_consolidacao.dat_criacao_conta,'') ||    
           nvl(tmp_consolidacao.dat_atualizacao,'') ||
           nvl(tmp_consolidacao.dat_ultimo_consumo,'') ||
           -- nvl(tmp_consolidacao.dat_primeiro_consumo,'') ||
           nvl(tmp_consolidacao.dat_primeiro_credito,'') ||
           nvl(tmp_consolidacao.val_ultima_compra,'') ||
           nvl(tmp_consolidacao.dat_ultimo_credito,'') ||
           nvl(tmp_consolidacao.val_ultimo_credito,'') ||
           nvl(tmp_consolidacao.val_saldo_reserva,'') ||
           nvl(tmp_consolidacao.val_saldo_reserva_bloqueado,'') ||
           nvl(tmp_consolidacao.cod_conta_externa,'') ||
           nvl(tmp_consolidacao.dat_ultimo_pedido,'') ||
           nvl(tmp_consolidacao.num_contrato,'') ||
           nvl(tmp_consolidacao.dsc_status_vrg,'') ||
           nvl(tmp_consolidacao.flg_bate_ponto,'') ||
           nvl(tmp_consolidacao.dat_desativacao_vrg,'') ||
           nvl(tmp_consolidacao.flg_configuracao_ferias_vrg,'') ||
           nvl(tmp_consolidacao.srk_rh_contrato,'') ||
           nvl(tmp_consolidacao.dsc_origem,'')||
           nvl(tmp_consolidacao.dsc_cargo,'')
       )) as cod_hash_md5,
       dim_conta.srk_trabalhador_conta as srk_trabalhador_conta_destino,
       dim_conta.cod_hash_md5 as cod_hash_md5_destino 
  from tmp_consolidacao
  left join gold_vr_cliente_trabalhador.dim_conta
    on nvl(tmp_consolidacao.num_conta,'') = nvl(dim_conta.num_conta,'')
   and nvl(tmp_consolidacao.dsc_origem,'') = nvl(dim_conta.dsc_origem,'')
   and nvl(tmp_consolidacao.cod_sdprod,'') = nvl(dim_conta.cod_sdprod,'')
   and nvl(tmp_consolidacao.cod_produto,'') = nvl(dim_conta.cod_produto,'')
   and dim_conta.flg_ativo = 1
 order by dim_conta.srk_trabalhador_conta nulls first


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_conta_merge
create or replace temporary view tmp_trabalhador_conta_merge as

with tmp_union_conta (
  Select num_conta,
         num_conta_plataforma,
         cod_aplicacao,
         cod_sdprod,
         cod_produto,
         dsc_produto,
         cod_emissor,
         cod_autorizador_matriz,
         cod_autorizador_filial,
         srk_trabalhador_cadastro,
         cod_usuario,
         cod_matricula,
         cod_centro_custo,
         srk_rh_cadastro,
         dat_abertura,
         val_saldo,
         val_saldo_bloqueado,
         cod_situacao,
         dsc_situacao,
         dat_primeiro_credito,
         val_primeiro_credito,
         dat_ultimo_consumo,
         val_ultima_compra,
         dat_ultimo_credito,
         val_ultimo_credito,
         dat_criacao_conta,
         dat_atualizacao,
         val_saldo_reserva,
         val_saldo_reserva_bloqueado,
         cod_conta_externa,
         dat_ultimo_pedido,
         dsc_status_vrg,
         flg_bate_ponto,
         dat_desativacao_vrg,
         flg_configuracao_ferias_vrg,
         num_contrato,
         srk_rh_contrato,
         dsc_origem,
         dsc_cargo,
         dsc_acao,
         cod_hash_md5,
         srk_trabalhador_conta_destino,
         cod_hash_md5_destino,
         case dsc_acao
           when 'inserir' then -1
           when 'versionar' then -1
           when 'atualizar' then 1
         end as flg_ativo
    from vr_temporaria.tmp_trabalhador_conta_consolidado
  union
  
  Select num_conta,
         num_conta_plataforma,
         cod_aplicacao,
         cod_sdprod,
         cod_produto,
         dsc_produto,
         cod_emissor,
         cod_autorizador_matriz,
         cod_autorizador_filial,
         srk_trabalhador_cadastro,
         cod_usuario,
         cod_matricula,
         cod_centro_custo,
         srk_rh_cadastro,
         dat_abertura,
         val_saldo,
         val_saldo_bloqueado,
         cod_situacao,
         dsc_situacao,
         dat_primeiro_credito,
         val_primeiro_credito,
         dat_ultimo_consumo,
         val_ultima_compra,
         dat_ultimo_credito,
         val_ultimo_credito,
         dat_criacao_conta,
         dat_atualizacao,
         val_saldo_reserva,
         val_saldo_reserva_bloqueado,
         cod_conta_externa,
         dat_ultimo_pedido,
         dsc_status_vrg,
         flg_bate_ponto,
         dat_desativacao_vrg,
         flg_configuracao_ferias_vrg,
         num_contrato,
         srk_rh_contrato,
         dsc_origem,
         dsc_cargo,
         'versionar_flag' as dsc_acao,
         cod_hash_md5,
         srk_trabalhador_conta_destino,
         cod_hash_md5_destino,
         1 flg_ativo
    from vr_temporaria.tmp_trabalhador_conta_consolidado
   where dsc_acao = 'versionar'
)

Select *
  from tmp_union_conta
 where nvl(cod_hash_md5,'') <> nvl(cod_hash_md5_destino,'')
 order by srk_trabalhador_conta_destino nulls first	

-- COMMAND ----------

-- DBTITLE 1,--Validação de duplicidades
--Validação de duplicidades
-- select num_conta, dsc_origem, cod_sdprod, cod_produto, flg_ativo, dsc_acao, count(*) as total 
--   from tmp_trabalhador_conta_merge 
--  group by num_conta, dsc_origem, cod_sdprod, cod_produto, flg_ativo, dsc_acao having count(*) > 1;

-- COMMAND ----------

-- DBTITLE 1,Padroniza campos
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select num_conta,
-- MAGIC        num_conta_plataforma,
-- MAGIC        cod_aplicacao,
-- MAGIC        cod_sdprod,
-- MAGIC        cod_produto,
-- MAGIC        dsc_produto,
-- MAGIC        cod_emissor,
-- MAGIC        cod_autorizador_matriz,
-- MAGIC        cod_autorizador_filial,
-- MAGIC        srk_trabalhador_cadastro,
-- MAGIC        cod_usuario,
-- MAGIC        cod_matricula,
-- MAGIC        cod_centro_custo,
-- MAGIC        srk_rh_cadastro,
-- MAGIC        dat_abertura,
-- MAGIC        val_saldo,
-- MAGIC        val_saldo_bloqueado,
-- MAGIC        cod_situacao,
-- MAGIC        dsc_situacao,
-- MAGIC        dat_primeiro_credito,
-- MAGIC        val_primeiro_credito,
-- MAGIC        dat_ultimo_consumo,
-- MAGIC        val_ultima_compra,
-- MAGIC        dat_ultimo_credito,
-- MAGIC        val_ultimo_credito,
-- MAGIC        dat_criacao_conta,
-- MAGIC        dat_atualizacao,
-- MAGIC        val_saldo_reserva,
-- MAGIC        val_saldo_reserva_bloqueado,
-- MAGIC        cod_conta_externa,
-- MAGIC        dat_ultimo_pedido,
-- MAGIC        dsc_status_vrg,
-- MAGIC        flg_bate_ponto,
-- MAGIC        dat_desativacao_vrg,
-- MAGIC        flg_configuracao_ferias_vrg,
-- MAGIC        num_contrato,
-- MAGIC        srk_rh_contrato,
-- MAGIC        dsc_origem,
-- MAGIC        dsc_cargo,
-- MAGIC        dsc_acao,
-- MAGIC        cod_hash_md5,
-- MAGIC        srk_trabalhador_conta_destino,
-- MAGIC        cod_hash_md5_destino,
-- MAGIC        flg_ativo
-- MAGIC   from tmp_trabalhador_conta_merge
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_trabalhador_conta_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Merge dim_conta
-- MAGIC %python
-- MAGIC
-- MAGIC df_merge = fn_merge_direto("""
-- MAGIC  MERGE INTO gold_vr_cliente_trabalhador_pii.dim_conta as destino
-- MAGIC  USING tmp_trabalhador_conta_criptografado as origem
-- MAGIC     ON nvl(origem.num_conta,'') = nvl(destino.num_conta,'')
-- MAGIC    AND nvl(origem.dsc_origem,'') = nvl(destino.dsc_origem,'')
-- MAGIC    AND nvl(origem.cod_sdprod,'') = nvl(destino.cod_sdprod,'')
-- MAGIC    AND nvl(origem.cod_produto,'') = nvl(destino.cod_produto,'')
-- MAGIC    AND nvl(origem.flg_ativo,'') = nvl(destino.flg_ativo,'')
-- MAGIC   WHEN MATCHED AND origem.dsc_acao = 'versionar_flag' THEN UPDATE SET
-- MAGIC        destino.flg_ativo = 0,
-- MAGIC        destino.dat_fim_vigencia = timestampadd(DAY,-1,current_timestamp()),
-- MAGIC        destino.dat_alteracao_bi = current_timestamp()
-- MAGIC   WHEN MATCHED AND origem.dsc_acao = 'atualizar' THEN UPDATE SET
-- MAGIC        destino.num_conta_plataforma        = origem.num_conta_plataforma,
-- MAGIC        destino.cod_aplicacao               = origem.cod_aplicacao,
-- MAGIC        destino.dsc_produto                 = origem.dsc_produto,
-- MAGIC        destino.cod_emissor                 = origem.cod_emissor,
-- MAGIC        destino.srk_trabalhador_cadastro    = origem.srk_trabalhador_cadastro,
-- MAGIC        destino.cod_usuario                 = origem.cod_usuario,
-- MAGIC        destino.cod_matricula               = origem.cod_matricula,
-- MAGIC        destino.cod_centro_custo            = origem.cod_centro_custo,
-- MAGIC        destino.srk_rh_cadastro             = origem.srk_rh_cadastro,
-- MAGIC        destino.dat_abertura                = origem.dat_abertura,
-- MAGIC        destino.val_saldo                   = origem.val_saldo,
-- MAGIC        destino.val_saldo_bloqueado         = origem.val_saldo_bloqueado,
-- MAGIC        destino.cod_situacao                = origem.cod_situacao,
-- MAGIC        destino.dsc_situacao                = origem.dsc_situacao,
-- MAGIC        destino.dat_criacao_conta           = origem.dat_criacao_conta,
-- MAGIC        destino.dat_atualizacao             = origem.dat_atualizacao,
-- MAGIC        destino.cod_autorizador_matriz      = origem.cod_autorizador_matriz,
-- MAGIC        destino.cod_autorizador_filial      = origem.cod_autorizador_filial,
-- MAGIC        destino.dat_ultimo_consumo          = origem.dat_ultimo_consumo,
-- MAGIC        destino.dat_primeiro_credito        = origem.dat_primeiro_credito,
-- MAGIC        destino.val_ultima_compra           = origem.val_ultima_compra,
-- MAGIC        destino.dat_ultimo_credito          = origem.dat_ultimo_credito,
-- MAGIC        destino.val_ultimo_credito          = origem.val_ultimo_credito,
-- MAGIC        destino.val_saldo_reserva           = origem.val_saldo_reserva,
-- MAGIC        destino.val_saldo_reserva_bloqueado = origem.val_saldo_reserva_bloqueado,
-- MAGIC        destino.cod_conta_externa           = origem.cod_conta_externa,
-- MAGIC        destino.dat_ultimo_pedido           = origem.dat_ultimo_pedido,
-- MAGIC        destino.num_contrato                = origem.num_contrato,
-- MAGIC        destino.dsc_status_vrg              = origem.dsc_status_vrg,
-- MAGIC        destino.flg_bate_ponto              = origem.flg_bate_ponto,
-- MAGIC        destino.dat_desativacao_vrg         = origem.dat_desativacao_vrg,
-- MAGIC        destino.flg_configuracao_ferias_vrg = origem.flg_configuracao_ferias_vrg,
-- MAGIC        destino.srk_rh_contrato             = origem.srk_rh_contrato,
-- MAGIC        destino.dsc_cargo                   = origem.dsc_cargo,
-- MAGIC        destino.cod_hash_md5                = origem.cod_hash_md5,
-- MAGIC        destino.dat_alteracao_bi            = current_timestamp()
-- MAGIC   WHEN NOT MATCHED AND origem.dsc_acao = 'versionar' THEN INSERT (
-- MAGIC        destino.srk_trabalhador_conta,
-- MAGIC        destino.num_conta,
-- MAGIC        destino.num_conta_plataforma,
-- MAGIC        destino.cod_aplicacao,
-- MAGIC        destino.cod_sdprod,
-- MAGIC        destino.cod_produto,
-- MAGIC        destino.dsc_produto,
-- MAGIC        destino.cod_emissor,
-- MAGIC        destino.srk_trabalhador_cadastro,
-- MAGIC        destino.cod_usuario,
-- MAGIC        destino.cod_matricula,
-- MAGIC        destino.cod_centro_custo,
-- MAGIC        destino.srk_rh_cadastro,
-- MAGIC        destino.dat_abertura,
-- MAGIC        destino.val_saldo,
-- MAGIC        destino.val_saldo_bloqueado,
-- MAGIC        destino.cod_situacao,
-- MAGIC        destino.dsc_situacao,
-- MAGIC        destino.cod_autorizador_matriz,
-- MAGIC        destino.cod_autorizador_filial,
-- MAGIC        destino.dat_criacao_conta,
-- MAGIC        destino.dat_atualizacao,
-- MAGIC        destino.dat_ultimo_consumo,
-- MAGIC        destino.dat_primeiro_credito,
-- MAGIC        destino.val_ultima_compra,
-- MAGIC        destino.dat_ultimo_credito,
-- MAGIC        destino.val_ultimo_credito,
-- MAGIC        destino.val_saldo_reserva,
-- MAGIC        destino.val_saldo_reserva_bloqueado,
-- MAGIC        destino.cod_conta_externa,
-- MAGIC        destino.dat_ultimo_pedido,
-- MAGIC        destino.num_contrato,
-- MAGIC        destino.dsc_status_vrg,
-- MAGIC        destino.flg_bate_ponto,
-- MAGIC        destino.dat_desativacao_vrg,
-- MAGIC        destino.flg_configuracao_ferias_vrg,
-- MAGIC        destino.srk_rh_contrato,
-- MAGIC        destino.dsc_cargo,
-- MAGIC        destino.dsc_origem,
-- MAGIC        destino.cod_hash_md5,
-- MAGIC        destino.flg_ativo,       
-- MAGIC        destino.dat_inicio_vigencia,
-- MAGIC        destino.dat_inclusao_bi,
-- MAGIC        destino.dat_alteracao_bi
-- MAGIC     ) VALUES (
-- MAGIC        origem.srk_trabalhador_conta_destino,
-- MAGIC        origem.num_conta,
-- MAGIC        origem.num_conta_plataforma,
-- MAGIC        origem.cod_aplicacao,
-- MAGIC        origem.cod_sdprod,
-- MAGIC        origem.cod_produto,
-- MAGIC        origem.dsc_produto,
-- MAGIC        origem.cod_emissor,
-- MAGIC        origem.srk_trabalhador_cadastro,
-- MAGIC        origem.cod_usuario,
-- MAGIC        origem.cod_matricula,
-- MAGIC        origem.cod_centro_custo,
-- MAGIC        origem.srk_rh_cadastro,
-- MAGIC        origem.dat_abertura,
-- MAGIC        origem.val_saldo,
-- MAGIC        origem.val_saldo_bloqueado,
-- MAGIC        origem.cod_situacao,
-- MAGIC        origem.dsc_situacao,
-- MAGIC        origem.cod_autorizador_matriz,
-- MAGIC        origem.cod_autorizador_filial,
-- MAGIC        origem.dat_criacao_conta,
-- MAGIC        origem.dat_atualizacao,
-- MAGIC        origem.dat_ultimo_consumo,
-- MAGIC        origem.dat_primeiro_credito,
-- MAGIC        origem.val_ultima_compra,
-- MAGIC        origem.dat_ultimo_credito,
-- MAGIC        origem.val_ultimo_credito,
-- MAGIC        origem.val_saldo_reserva,
-- MAGIC        origem.val_saldo_reserva_bloqueado,
-- MAGIC        origem.cod_conta_externa,
-- MAGIC        origem.dat_ultimo_pedido,
-- MAGIC        origem.num_contrato,
-- MAGIC        origem.dsc_status_vrg,
-- MAGIC        origem.flg_bate_ponto,
-- MAGIC        origem.dat_desativacao_vrg,
-- MAGIC        origem.flg_configuracao_ferias_vrg,
-- MAGIC        origem.srk_rh_contrato,
-- MAGIC        origem.dsc_cargo,
-- MAGIC        origem.dsc_origem,
-- MAGIC        origem.cod_hash_md5,
-- MAGIC        1,
-- MAGIC        timestampadd(DAY,-1,current_timestamp()),
-- MAGIC        current_timestamp(),
-- MAGIC        current_timestamp()
-- MAGIC ) WHEN NOT MATCHED AND origem.dsc_acao = 'inserir' THEN INSERT (
-- MAGIC        destino.num_conta,
-- MAGIC        destino.num_conta_plataforma,
-- MAGIC        destino.cod_aplicacao,
-- MAGIC        destino.cod_sdprod,
-- MAGIC        destino.cod_produto,
-- MAGIC        destino.dsc_produto,
-- MAGIC        destino.cod_emissor,
-- MAGIC        destino.srk_trabalhador_cadastro,
-- MAGIC        destino.cod_usuario,
-- MAGIC        destino.cod_matricula,
-- MAGIC        destino.cod_centro_custo,
-- MAGIC        destino.srk_rh_cadastro,
-- MAGIC        destino.dat_abertura,
-- MAGIC        destino.val_saldo,
-- MAGIC        destino.val_saldo_bloqueado,
-- MAGIC        destino.cod_situacao,
-- MAGIC        destino.dsc_situacao,
-- MAGIC        destino.cod_autorizador_matriz,
-- MAGIC        destino.cod_autorizador_filial,
-- MAGIC        destino.dat_criacao_conta,
-- MAGIC        destino.dat_atualizacao,
-- MAGIC        destino.dat_ultimo_consumo,
-- MAGIC        destino.dat_primeiro_credito,
-- MAGIC        destino.val_ultima_compra,
-- MAGIC        destino.dat_ultimo_credito,
-- MAGIC        destino.val_ultimo_credito,
-- MAGIC        destino.val_saldo_reserva,
-- MAGIC        destino.val_saldo_reserva_bloqueado,
-- MAGIC        destino.cod_conta_externa,
-- MAGIC        destino.dat_ultimo_pedido,
-- MAGIC        destino.num_contrato,
-- MAGIC        destino.dsc_status_vrg,
-- MAGIC        destino.flg_bate_ponto,
-- MAGIC        destino.dat_desativacao_vrg,
-- MAGIC        destino.flg_configuracao_ferias_vrg,
-- MAGIC        destino.srk_rh_contrato,
-- MAGIC        destino.dsc_cargo,
-- MAGIC        destino.dsc_origem,
-- MAGIC        destino.cod_hash_md5,
-- MAGIC        destino.flg_ativo,
-- MAGIC        destino.dat_inicio_vigencia,
-- MAGIC        destino.dat_inclusao_bi,
-- MAGIC        destino.dat_alteracao_bi
-- MAGIC     ) VALUES (
-- MAGIC        origem.num_conta,
-- MAGIC        origem.num_conta_plataforma,
-- MAGIC        origem.cod_aplicacao,
-- MAGIC        origem.cod_sdprod,
-- MAGIC        origem.cod_produto,
-- MAGIC        origem.dsc_produto,
-- MAGIC        origem.cod_emissor,
-- MAGIC        origem.srk_trabalhador_cadastro,
-- MAGIC        origem.cod_usuario,
-- MAGIC        origem.cod_matricula,
-- MAGIC        origem.cod_centro_custo,
-- MAGIC        origem.srk_rh_cadastro,
-- MAGIC        origem.dat_abertura,
-- MAGIC        origem.val_saldo,
-- MAGIC        origem.val_saldo_bloqueado,
-- MAGIC        origem.cod_situacao,
-- MAGIC        origem.dsc_situacao,
-- MAGIC        origem.cod_autorizador_matriz,
-- MAGIC        origem.cod_autorizador_filial,
-- MAGIC        origem.dat_criacao_conta,
-- MAGIC        origem.dat_atualizacao,
-- MAGIC        origem.dat_ultimo_consumo,
-- MAGIC        origem.dat_primeiro_credito,
-- MAGIC        origem.val_ultima_compra,
-- MAGIC        origem.dat_ultimo_credito,
-- MAGIC        origem.val_ultimo_credito,
-- MAGIC        origem.val_saldo_reserva,
-- MAGIC        origem.val_saldo_reserva_bloqueado,
-- MAGIC        origem.cod_conta_externa,
-- MAGIC        origem.dat_ultimo_pedido,
-- MAGIC        origem.num_contrato,
-- MAGIC        origem.dsc_status_vrg,
-- MAGIC        origem.flg_bate_ponto,
-- MAGIC        origem.dat_desativacao_vrg,
-- MAGIC        origem.flg_configuracao_ferias_vrg,
-- MAGIC        origem.srk_rh_contrato,
-- MAGIC        origem.dsc_cargo,
-- MAGIC        origem.dsc_origem,
-- MAGIC        origem.cod_hash_md5,
-- MAGIC        1,
-- MAGIC        timestampadd(DAY,-1,current_timestamp()),
-- MAGIC        current_timestamp(),
-- MAGIC        current_timestamp()
-- MAGIC    )
-- MAGIC """
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

-- DBTITLE 1,Drop temporária

drop table if exists vr_temporaria.tmp_trabalhador_conta_base_audaz;
drop table if exists vr_temporaria.tmp_trabalhador_conta_base_pedido_audaz;
drop table if exists vr_temporaria.tmp_trabalhador_conta_base_pedido_item_audaz;
drop table if exists vr_temporaria.tmp_trabalhador_conta_order_audaz;
drop table if exists vr_temporaria.tmp_trabalhador_conta_pagga;
drop table if exists vr_temporaria.tmp_trabalhador_conta_mobilidade;
drop table if exists vr_temporaria.vr_temporaria;

-- COMMAND ----------

-- DBTITLE 1,Exit notebook
-- MAGIC %python
-- MAGIC
-- MAGIC fn_gera_arquivo_bastao(malha,database,tablename)
-- MAGIC fn_exit_notebook(df_merge)
