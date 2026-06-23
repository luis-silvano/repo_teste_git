-- Databricks notebook source
-- DBTITLE 1,Parametrização utilizada no notebook
-- MAGIC %python
-- MAGIC dbutils.widgets.text("dat_parametro","")
-- MAGIC dbutils.widgets.text("database","")
-- MAGIC dbutils.widgets.text("databricks_catalog","")
-- MAGIC dbutils.widgets.text("dsc_tabela_pii","")
-- MAGIC dbutils.widgets.text("malha","")
-- MAGIC dbutils.widgets.text("tablename","")
-- MAGIC
-- MAGIC dat_parametro = dbutils.widgets.get("dat_parametro")
-- MAGIC database = dbutils.widgets.get("database")
-- MAGIC databricks_catalog = dbutils.widgets.get("databricks_catalog")
-- MAGIC dsc_tabela_pii = dbutils.widgets.get("dsc_tabela_pii")
-- MAGIC malha = dbutils.widgets.get("malha")
-- MAGIC tablename = dbutils.widgets.get("tablename")
-- MAGIC
-- MAGIC # Exemplo de preenchimento
-- MAGIC # dat_parametro = 2026-02-23
-- MAGIC # database = gold_vr_cliente_rh
-- MAGIC # databricks_catalog (DEV) = dev
-- MAGIC # databricks_catalog (PROD) = prod
-- MAGIC # dsc_tabela_pii = dim_cadastro
-- MAGIC # malha = temp
-- MAGIC # tablename = dim_cadastro

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC - silver_vr_autorizador.sncorecomum_fjjur
-- MAGIC - silver_vr_autorizador.sncoreon_fjcfj
-- MAGIC - silver_vr_autorizador.sncoreon_sncde
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_rd_company
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_bo_name
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_rd_comp_asso_sn
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_rd_companydb_sn
-- MAGIC - silver_vr_salesforce.dbo_account
-- MAGIC - silver_vr_salesforce.dbo_user
-- MAGIC - silver_audaz.dbo_branchs
-- MAGIC - silver_audaz.dbo_accounts
-- MAGIC - silver_audaz.dbo_Affiliate
-- MAGIC - silver_audaz.dbo_partners
-- MAGIC - silver_vr_bi_legado.biadm_snf_pedido_venda_vt_fct
-- MAGIC - silver_vr_gente_serverless.activation_client_billings
-- MAGIC - silver_vr_gente_produto.public_view_clients_datalake
-- MAGIC - silver_vr_vr_admissao_hcm.hcmprd_business_units
-- MAGIC - mlops_resultado.mlr_persona_rh

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

-- MAGIC %md
-- MAGIC Origem das informações do RH no sistema do autorizador
-- MAGIC
-- MAGIC Utilizado o filtro acotip =7, esse filtro é usado para identificar quando o cadastro no autorizador for um RH.

-- COMMAND ----------

-- DBTITLE 1,tmp_cadastro_autorizador
create or replace temporary view tmp_cadastro_autorizador as

select sncoreon_fjcfj.cfjseq as cod_autorizador_matriz,
       sncorecomum_fjjur.jurcgccmp as cod_autorizador_filial,
       coalesce(sncorecomum_fj_doc_empresa.documento,
         lpad(sncoreon_fjcfj.cfjcpfcgc || 
         lpad(sncorecomum_fjjur.jurcgccmp,4,'0') || 
         lpad(sncorecomum_fjjur.jurcgcdig,2,'0') ,14,'0')) as num_cnpj,
       sncorecomum_fj_doc_empresa.tipo_documento as dsc_tipo_documento,
       sncoreon_fjcfj.cfjnom as dsc_razao_social,
       sncoreon_fjcfj.cfjincdat as dat_criacao_registro,
       sncoreon_fjcfj.cfjatudat as dat_alteracao_registro
  from prod.silver_vr_autorizador.sncorecomum_fjjur
 inner join prod.silver_vr_autorizador.sncoreon_fjcfj
    on sncorecomum_fjjur.cfjseq = sncoreon_fjcfj.cfjseq  
 inner join (Select distinct acocfjseq, acocgccmp from prod.silver_vr_autorizador.sncoreon_sncde where acotip = 7) as sncoreon_sncde
    on sncorecomum_fjjur.cfjseq = sncoreon_sncde.acocfjseq
   and sncorecomum_fjjur.jurcgccmp = sncoreon_sncde.acocgccmp
  left join prod.silver_vr_autorizador.sncorecomum_fj_doc_empresa
    on sncorecomum_fj_doc_empresa.cfjseq = sncoreon_fjcfj.cfjseq
   and sncorecomum_fj_doc_empresa.jurcgccmp = sncorecomum_fjjur.jurcgccmp


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Origem das informações do RH no sistema do crm people soft, os registros que estão na tabela ps_rd_companydb_sn são os cadastros de empresa RH

-- COMMAND ----------

-- DBTITLE 1,tmp_cadastro_crm_people_soft
create or replace temporary view tmp_cadastro_crm_people_soft as

select sysadm_ps_rd_company.setid as cod_setid,
       sysadm_ps_rd_company.companyid as cod_companyid, 
       sysadm_ps_rd_company.bo_id as cod_registro_peoplesoft,
       sysadm_ps_rd_company.duns_number as num_cnpj,
       sysadm_ps_bo_name.bo_name_display as dsc_razao_social,
       sysadm_ps_rd_company.inscr_est_sn as num_inscricao_estadual,
       sysadm_ps_rd_company.inscr_municip_sn as num_inscricao_municipal,
       sysadm_ps_rd_company.legal_structure as cod_tipo_sociedade,
       decode(sysadm_ps_rd_company.legal_structure
              ,'1','1 - Sociedade Anônima'
              ,'2','2 - Sociedade Limitada'
              ,'3','3 - Firma Individual'
              ,'4','4 - Não informado'
              ,'5','5 - Microempresa'
              ,'6','6 - Microempreendedor individual'
             ) as dsc_tipo_sociedade,
       sysadm_ps_rd_comp_asso_sn.setid as cod_setid_associado, 
       sysadm_ps_rd_comp_asso_sn.companyid as cod_companyid_associado,
       sysadm_ps_rd_company.row_added_dttm as dat_criacao_registro,
       sysadm_ps_rd_company.row_lastmant_dttm as dat_alteracao_registro
  from prod.silver_vr_crm_peoplesoft.sysadm_ps_rd_company
  join (Select sysadm_ps_rd_company.duns_number, 
               max(sysadm_ps_rd_company.companyid) as companyid
          from prod.silver_vr_crm_peoplesoft.sysadm_ps_rd_company
         group by sysadm_ps_rd_company.duns_number
       ) as max_companyid
    on sysadm_ps_rd_company.companyid = max_companyid.companyid
  join prod.silver_vr_crm_peoplesoft.sysadm_ps_bo_name
    on cast(sysadm_ps_rd_company.bo_id as string) = cast(sysadm_ps_bo_name.bo_id as string)
   and sysadm_ps_bo_name.primary_ind = 'Y' 
  --Consulta editada para resolver duplicidade do CRM, já alinhado com Yang que é bug e tem que ser corrigido, no aguardo da correção
  left join (select distinct comp_asso.setid, comp_asso.companyid, comp_asso.duns_number
               from prod.silver_vr_crm_peoplesoft.sysadm_ps_rd_comp_asso_sn comp_asso
              where comp_asso.status_comp_ass_sn = 2
                and companyid = (select max(max_comp_asso.companyid)
                                   from prod.silver_vr_crm_peoplesoft.sysadm_ps_rd_comp_asso_sn max_comp_asso
                                  where max_comp_asso.status_comp_ass_sn = 2
                                    and max_comp_asso.duns_number = comp_asso.duns_number)
            ) as sysadm_ps_rd_comp_asso_sn
    on sysadm_ps_rd_company.duns_number = sysadm_ps_rd_comp_asso_sn.duns_number     
 where nvl(sysadm_ps_rd_comp_asso_sn.companyid, sysadm_ps_rd_company.companyid) in (Select companyid from prod.silver_vr_crm_peoplesoft.sysadm_ps_rd_companydb_sn)
   and sysadm_ps_rd_company.duns_number not like ('DUP%')
   and sysadm_ps_rd_company.duns_number not like ('9-%')


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Origem das informações do RH no sistema do crm sales force

-- COMMAND ----------

-- DBTITLE 1,tmp_cadastro_crm_salesforce
create or replace temporary view tmp_cadastro_crm_salesforce as

with tmp_filtro_criacao as (
select CNPJ__c,
       max(CreatedDate) as dat_criacao
 from (
   select dbo_account.CNPJ__c,
          dbo_account.CreatedDate,
          case
            when dbo_opportunity.ID is not null then 1
            when dbo_contrato__c.ID is not null then 1
            when dim_proposta.cod_proposta is not null then 1
            when dim_contrato.num_contrato is not null then 1
            else 0
          end as flg_existe
     from prod.silver_vr_salesforce.dbo_account
     left join prod.silver_vr_salesforce.dbo_opportunity
       on dbo_opportunity.AccountId = dbo_account.Id
     left join prod.silver_vr_salesforce.dbo_contrato__c
       on dbo_contrato__c.Conta__c = dbo_account.Id
     left join gold_vr_cliente_rh.dim_cadastro
       on regexp_replace(UPPER(dbo_account.CNPJ__c),'[^0-9A-Z]','') = UPPER(dim_cadastro.num_cnpj)
      and dim_cadastro.flg_ativo = 1
     left join gold_vr_cliente_rh.dim_proposta
       on dim_proposta.srk_rh_cadastro = dim_cadastro.srk_rh_cadastro
      and dim_proposta.flg_ativo = 1
     left join gold_vr_cliente_rh.dim_contrato
       on regexp_replace(UPPER(dbo_account.CNPJ__c),'[^0-9A-Z]','') = UPPER(dim_contrato.num_cnpj)
      and dim_contrato.flg_ativo = 1
   ) as dbo_account
   where flg_existe = 1
   group by CNPJ__c
)

, tmp_account_grupo as (
  select Grupo_SF_ID__c,
         Name
    from prod.silver_vr_salesforce.dbo_account
   where parentid is null
),

----Criado posteriormente para classificar os farmers que tiveram suas contas migradas para o supercrm, porém ainda não tiveram as propostas e os contratos migrados.
tmp_farmer as (
  select dbo_account.id,
         acc_sales.id as id_sales,
         dbo_user.name,
         dbo_user.ClassificacaoAtendimento__c,
         row_number() over (partition by dbo_account.id order by dbo_accountteammember.CreatedDate desc) as row_number
    from prod.silver_vr_salesforce_super_crm.dbo_account
    inner join prod.silver_vr_salesforce.dbo_account acc_sales
       on trim(regexp_replace(UPPER(dbo_account.NumeroDocumento__c),'[^0-9A-Z]','')) = UPPER(acc_sales.CNPJ__c)
    inner join prod.silver_vr_salesforce_super_crm.dbo_accountteammember
      on dbo_accountteammember.accountid = dbo_account.id
     and dbo_accountteammember.TeamMemberRole = 'CSResponsavel'
    inner join prod.silver_vr_salesforce_super_crm.dbo_user
      on dbo_accountteammember.userid = dbo_user.id
     and dbo_user.isActive is true
)

Select dbo_account.Id as cod_registro_salesforce,
       dbo_account.CNPJ__c as num_cnpj,
       dbo_account.Name as dsc_razao_social,
      --  nvl(dbo_account.Name, dbo_account.Name) as dsc_grupo_economico,
       nvl(dbo_account.Grupo_SF_ID__c, dbo_account.CNPJ__c) as cod_grupo_economico,
       nvl(tmp_account_grupo.Name, dbo_account.Name) as dsc_grupo_economico,
       dbo_cnae__c.name as cod_cnae_principal_salesforce,
       dbo_cnae__c.descricao__c as dsc_cnae_principal_salesforce,
       case dbo_account.ClientePremium__c
         when 'Sim' then 1
         when 'Não' then 0
         else dbo_account.ClientePremium__c
       end as flg_cliente_premium,
       dbo_account.Data_de_Fundacao_da_Empresa__c as dat_fundacao,
       dbo_account.CreatedDate as dat_criacao_registro,
       dbo_account.LastModifiedDate as dat_alteracao_registro,
       case 
         when tmp_farmer.name is not null then 1
         when dbo_user.isActive = true and dbo_account.Farmer_respons_vel__c is not null then 1
         else 0
       end as flg_cliente_farmer,
       case 
         when tmp_farmer.name is not null then tmp_farmer.name
         when dbo_user.isActive = true then dbo_user.Name
         else null
       end as dsc_farmer_responsavel,
       tmp_farmer.ClassificacaoAtendimento__c as dsc_classificacao_farmer,
       dbo_account.Type as dsc_tipo_cliente
  from prod.silver_vr_salesforce.dbo_account
  join tmp_filtro_criacao
    on dbo_account.CNPJ__c = tmp_filtro_criacao.CNPJ__c
   and dbo_account.CreatedDate = tmp_filtro_criacao.dat_criacao
  join tmp_account_grupo
    on dbo_account.Grupo_SF_ID__c = tmp_account_grupo.Grupo_SF_ID__c
  left join prod.silver_vr_salesforce.dbo_user
    on dbo_account.Farmer_respons_vel__c = dbo_user.id
  left join prod.silver_vr_salesforce.dbo_cnae__c
    on dbo_account.CNAE__c = dbo_cnae__c.id
  left join tmp_farmer
    on dbo_account.Id = tmp_farmer.id_sales
   and tmp_farmer.row_number = 1
 where dbo_account.CNPJ__c is not null
   and (dbo_account.Type in ('Cliente RH', 'Matriz', 'Filial', 'Grupo de Negociação', 'Grupo Econômico', 'Grupo e Coligados')
    or exists (Select distinct 1 from gold_vr_cliente_rh.dim_contrato where regexp_replace(UPPER(dbo_account.CNPJ__c),'[^0-9A-Z]','') = UPPER(dim_contrato.num_cnpj)) )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Origem das informações do RH no sistema do crm sales forces super crm

-- COMMAND ----------

-- DBTITLE 1,tmp_cadastro_supercrm
create or replace temporary view tmp_cadastro_supercrm as

with tmp_filtro_criacao as (
select NumeroDocumento__c, max(CreatedDate) as dat_criacao from (
   select dbo_account.NumeroDocumento__c,
          dbo_account.CreatedDate,
          case
            when dbo_opportunity.ID is not null then 1
            when dbo_contract.ID is not null then 1
            when dim_proposta.cod_proposta is not null then 1
            when dim_contrato.num_contrato is not null then 1
            else 0
          end as flg_existe
     from prod.silver_vr_salesforce_super_crm.dbo_account
     left join prod.silver_vr_salesforce_super_crm.dbo_opportunity
       on dbo_opportunity.AccountId = dbo_account.Id
     left join prod.silver_vr_salesforce_super_crm.dbo_contract
       on dbo_contract.AccountId = dbo_account.Id
     left join gold_vr_cliente_rh.dim_cadastro
       on regexp_replace(UPPER(dbo_account.NumeroDocumento__c),'[^0-9A-Z]','') = UPPER(dim_cadastro.num_cnpj)
      and dim_cadastro.flg_ativo = 1
     left join gold_vr_cliente_rh.dim_proposta
       on dim_proposta.srk_rh_cadastro = dim_cadastro.srk_rh_cadastro
      and dim_proposta.flg_ativo = 1
     left join gold_vr_cliente_rh.dim_contrato
       on regexp_replace(UPPER(dbo_account.NumeroDocumento__c),'[^0-9A-Z]','') = UPPER(dim_contrato.num_cnpj)
      and dim_contrato.flg_ativo = 1
   ) as dbo_account
   where flg_existe = 1
   group by NumeroDocumento__c
)

, tmp_farmer as (
  select dbo_account.id,
         dbo_user.name,
         dbo_user.ClassificacaoAtendimento__c,
         row_number() over (partition by dbo_account.id order by dbo_accountteammember.CreatedDate desc) as row_number
    from prod.silver_vr_salesforce_super_crm.dbo_account
    inner join prod.silver_vr_salesforce_super_crm.dbo_accountteammember
      on dbo_accountteammember.accountid = dbo_account.id
     and dbo_accountteammember.TeamMemberRole = 'CSResponsavel'
    inner join prod.silver_vr_salesforce_super_crm.dbo_user
      on dbo_accountteammember.userid = dbo_user.id
     and dbo_user.isActive is true
)

, tmp_associado as (
  Select dbo_associacaocontas__c.ContaAssociada__c as Id,
         dim_cadastro.srk_rh_cadastro as srk_rh_cadastro_associado
    from prod.silver_vr_salesforce_super_crm.dbo_associacaocontas__c
   inner join prod.silver_vr_salesforce_super_crm.dbo_account
      on dbo_associacaocontas__c.ContaPrincipal__c = dbo_account.Id
   inner join gold_vr_cliente_rh.dim_cadastro
      on regexp_replace(UPPER(dbo_account.NumeroDocumento__c),'[^0-9A-Z]','') = UPPER(dim_cadastro.num_cnpj)
     and dim_cadastro.flg_ativo = 1
     and dim_cadastro.flg_vrb = 1
 qualify row_number() over (partition by dbo_associacaocontas__c.ContaAssociada__c order by dbo_associacaocontas__c.CreatedDate desc) = 1
)

Select dbo_account.Id as cod_registro_supercrm,
       dbo_account.AccountIdLegado__c as cod_registro_salesforce,
       trim(regexp_replace(UPPER(dbo_account.NumeroDocumento__c),'[^0-9A-Z]','')) as num_cnpj,
       dbo_account.tipodocumento__c as dsc_tipo_documento,
       dbo_account.RazaoSocial__c as dsc_razao_social,
       nvl(dbo_account.Grupo__c, trim(regexp_replace(UPPER(dbo_account.NumeroDocumento__c),'[^0-9A-Z]',''))) as cod_grupo_economico_supercrm,
       nvl(dbo_grupo__c.Name, dbo_account.RazaoSocial__c) as dsc_grupo_economico_supercrm,
       dbo_cnae__c.name as cod_cnae_principal_salesforce,
       dbo_cnae__c.descricao__c as dsc_cnae_principal_salesforce,
       case dbo_account.ClientePremium__c
         when 'Sim' then 1
         when 'Não' then 0
         else dbo_account.ClientePremium__c
       end as flg_cliente_premium,
      case dbo_account.ClientePremiumVRGente__c
         when 'Sim' then 1
         when 'Não' then 0
         else dbo_account.ClientePremiumVRGente__c
       end as flg_cliente_premium_vrg,
       dbo_account.DataFundacao__c as dat_fundacao,
       dbo_account.CreatedDate as dat_criacao_registro,
       dbo_account.LastModifiedDate as dat_alteracao_registro,
       case when tmp_farmer.name is null then 0 else 1 end as flg_cliente_farmer,
       tmp_farmer.name as dsc_farmer_responsavel,
       tmp_farmer.ClassificacaoAtendimento__c as dsc_classificacao_farmer,
       tmp_associado.srk_rh_cadastro_associado,
       'Cliente RH' as dsc_tipo_cliente
  from prod.silver_vr_salesforce_super_crm.dbo_account
 inner join tmp_filtro_criacao
    on dbo_account.NumeroDocumento__c = tmp_filtro_criacao.NumeroDocumento__c
   and dbo_account.CreatedDate = tmp_filtro_criacao.dat_criacao
  left join prod.silver_vr_salesforce_super_crm.dbo_grupo__c
    on dbo_account.Grupo__c = dbo_grupo__c.ID
  left join tmp_farmer
    on dbo_account.id = tmp_farmer.id
   and tmp_farmer.row_number = 1
  left join prod.silver_vr_salesforce.dbo_cnae__c
    on dbo_account.CNAE__c = dbo_cnae__c.id
  left join tmp_associado
    on dbo_account.Id = tmp_associado.Id
 where dbo_account.NumeroDocumento__c is not null


-- COMMAND ----------

-- DBTITLE 1,vr_temporaria.tmp_rh_cadastro_vrb
create or replace table vr_temporaria.tmp_rh_cadastro_vrb

with dim_cadastro_associado as (
  Select srk_rh_cadastro,
         cod_setid,
         cod_companyid
    from gold_vr_cliente_rh.dim_cadastro
   where flg_ativo = 1
     and flg_vrb = 1
)

, tmp_cnpj as (
  Select num_cnpj
    from tmp_cadastro_autorizador
  union
    Select num_cnpj
    from tmp_cadastro_crm_people_soft
  union
    Select num_cnpj
    from tmp_cadastro_crm_salesforce
  union
    Select num_cnpj
    from tmp_cadastro_supercrm
)

select tmp_cnpj.num_cnpj as num_cnpj,
       coalesce(tmp_cadastro_supercrm.dsc_tipo_documento,
                tmp_cadastro_autorizador.dsc_tipo_documento
       ) as dsc_tipo_documento,
       substr(tmp_cnpj.num_cnpj,1,8) as num_raiz_cnpj,
       coalesce(tmp_cadastro_supercrm.dsc_razao_social,
                tmp_cadastro_crm_salesforce.dsc_razao_social,
                tmp_cadastro_crm_people_soft.dsc_razao_social,
                tmp_cadastro_autorizador.dsc_razao_social
       ) as dsc_razao_social,
       tmp_cadastro_crm_people_soft.num_inscricao_estadual,
       tmp_cadastro_crm_people_soft.num_inscricao_municipal,
       tmp_cadastro_crm_people_soft.cod_tipo_sociedade,
       tmp_cadastro_crm_people_soft.dsc_tipo_sociedade,
       tmp_cadastro_crm_salesforce.dsc_grupo_economico,
       tmp_cadastro_supercrm.dsc_grupo_economico_supercrm,
       tmp_cadastro_crm_salesforce.cod_grupo_economico,
       tmp_cadastro_supercrm.cod_grupo_economico_supercrm,
       coalesce(tmp_cadastro_supercrm.cod_cnae_principal_salesforce,
                tmp_cadastro_crm_salesforce.cod_cnae_principal_salesforce
       ) as cod_cnae_principal_salesforce,
       coalesce(tmp_cadastro_supercrm.dsc_cnae_principal_salesforce,
                tmp_cadastro_crm_salesforce.dsc_cnae_principal_salesforce
       ) as dsc_cnae_principal_salesforce,
       coalesce(tmp_cadastro_supercrm.flg_cliente_premium,
                tmp_cadastro_crm_salesforce.flg_cliente_premium
       ) as flg_cliente_premium,
       --  Priorizar Super CRM como fonte da verdade para farmer
       -- Quando a account existe no Super CRM, usar somente o valor do Super CRM (mesmo que NULL)
       -- Só usar legado quando a account NÃO existe no Super CRM
       case 
         when tmp_cadastro_supercrm.num_cnpj is not null then tmp_cadastro_supercrm.flg_cliente_farmer
         else tmp_cadastro_crm_salesforce.flg_cliente_farmer
       end as flg_cliente_farmer,
       case 
         when tmp_cadastro_supercrm.num_cnpj is not null then tmp_cadastro_supercrm.dsc_farmer_responsavel
         else tmp_cadastro_crm_salesforce.dsc_farmer_responsavel
       end as dsc_farmer_responsavel,
       case 
         when tmp_cadastro_supercrm.num_cnpj is not null then tmp_cadastro_supercrm.dsc_classificacao_farmer
         else tmp_cadastro_crm_salesforce.dsc_classificacao_farmer
       end as dsc_classificacao_farmer,
       coalesce(tmp_cadastro_supercrm.dsc_tipo_cliente,
                tmp_cadastro_crm_salesforce.dsc_tipo_cliente
       ) as dsc_tipo_cliente,
       coalesce(tmp_cadastro_supercrm.dat_fundacao,
                tmp_cadastro_crm_salesforce.dat_fundacao
       ) as dat_fundacao,
       least(tmp_cadastro_supercrm.dat_criacao_registro,
             tmp_cadastro_autorizador.dat_criacao_registro,
             tmp_cadastro_crm_people_soft.dat_criacao_registro,
             tmp_cadastro_crm_salesforce.dat_criacao_registro
       ) as dat_criacao_registro,
       greatest(tmp_cadastro_supercrm.dat_alteracao_registro,
                tmp_cadastro_autorizador.dat_alteracao_registro,
                tmp_cadastro_crm_people_soft.dat_alteracao_registro,
                tmp_cadastro_crm_salesforce.dat_alteracao_registro
       ) as dat_alteracao_registro,
       tmp_cadastro_autorizador.cod_autorizador_matriz,
       tmp_cadastro_autorizador.cod_autorizador_filial,
       tmp_cadastro_crm_people_soft.cod_setid,
       tmp_cadastro_crm_people_soft.cod_companyid,
       tmp_cadastro_crm_people_soft.cod_registro_peoplesoft,
       coalesce(tmp_cadastro_crm_salesforce.cod_registro_salesforce,
                tmp_cadastro_supercrm.cod_registro_salesforce
       ) as cod_registro_salesforce,
       tmp_cadastro_supercrm.cod_registro_supercrm,
       nvl(dim_cadastro_associado.srk_rh_cadastro, tmp_cadastro_supercrm.srk_rh_cadastro_associado) as srk_rh_cadastro_associado,
       tmp_cadastro_crm_people_soft.cod_setid_associado,
       tmp_cadastro_crm_people_soft.cod_companyid_associado,
       tmp_cadastro_supercrm.flg_cliente_premium_vrg
  from tmp_cnpj
  left join tmp_cadastro_autorizador
    on tmp_cnpj.num_cnpj = tmp_cadastro_autorizador.num_cnpj
  left join tmp_cadastro_crm_people_soft
    on tmp_cnpj.num_cnpj = tmp_cadastro_crm_people_soft.num_cnpj
  left join tmp_cadastro_crm_salesforce
    on tmp_cnpj.num_cnpj = tmp_cadastro_crm_salesforce.num_cnpj
  left join tmp_cadastro_supercrm
    on tmp_cnpj.num_cnpj = tmp_cadastro_supercrm.num_cnpj
  left join dim_cadastro_associado
    on dim_cadastro_associado.cod_setid = tmp_cadastro_crm_people_soft.cod_setid_associado
   and dim_cadastro_associado.cod_companyid = tmp_cadastro_crm_people_soft.cod_companyid_associado


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Mobilidade</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_empresa_audaz
create or replace temporary view tmp_empresa_audaz as

with tmp_todos as (
  select dbo_branchs.BranchId as cod_empresa, 
         regexp_replace(UPPER(dbo_branchs.Cnpj),'[^0-9A-Z]','') as num_cnpj,
         nvl(regexp_replace(UPPER(dbo_accounts.Cnpj),'[^0-9A-Z]',''),concat('VRM-',dbo_accounts.AccountId)) as num_cnpj_grupo_vrm, 
         dbo_branchs.SocialReason as dsc_razao_social,
         dbo_branchs.IE as dsc_inscricao_estadual,
         dbo_branchs.IM as dsc_inscricao_municipal,
         case dbo_branchs.STATUS when 'true' then 'Ativo' else 'Inativo' end as dsc_status,
         cast(dbo_branchs.ModifiedOn as timestamp) as dat_alteracao,
         dbo_branchs.AccountId as cod_grupo,
         dbo_accounts.Name as dsc_nome_grupo,
         dbo_Affiliate.AffiliateId as cod_afiliacao,
         regexp_replace(UPPER(dbo_Affiliate.Cnpj),'[^0-9A-Z]','') as num_cnpj_afiliacao,
         dbo_Affiliate.Name as dsc_nome_afiliacao,
         dbo_accounts.PartnerId as cod_parceiro,
         dbo_partners.Name as dsc_nome_parceiro,
         'VRM - Audaz' as dsc_origem
    from prod.silver_audaz.dbo_branchs --dbo_branchs: Tabela de Unidades (Filiais)
   inner join prod.silver_audaz.dbo_accounts --dbo_accounts: Tabela de Grupos Economicos (Matriz)
      on dbo_branchs.AccountId = dbo_accounts.AccountId
    left join prod.silver_audaz.dbo_Affiliate --dbo_Affiliate: base dos escritórios de contabilidade (Afiliados)
      on dbo_accounts.AffiliateId = dbo_Affiliate.AffiliateId
    left join prod.silver_audaz.dbo_partners --dbo_partners: base dos parceiros de negócio (Parceiros)
      on dbo_accounts.PartnerId = dbo_partners.PartnerId
   where upper(dbo_accounts.Name) not like ('TESTE%') --Retira Testes
     and upper(dbo_accounts.accountid) not in ('F813C11D-6A8E-47AD-933B-DCD722BE9996') --Retira bases de Implantação/Demonstração
)

, tmp_status_ativo as (
  select tmp_todos.* 
    from tmp_todos
   inner join (Select num_cnpj, 
                 min(dsc_status) as dsc_status
            from tmp_todos
           group by num_cnpj) as tmp_ativo
      on tmp_todos.num_cnpj = tmp_ativo.num_cnpj
     and tmp_todos.dsc_status = tmp_ativo.dsc_status   
)

select tmp_status_ativo.* 
  from tmp_status_ativo
 inner join (Select num_cnpj, 
                    max(dat_alteracao) as dat_alteracao
               from tmp_status_ativo
           group by num_cnpj) as tmp_ativo
    on tmp_status_ativo.num_cnpj = tmp_ativo.num_cnpj
   and nvl(tmp_status_ativo.dat_alteracao,current_timestamp()) = nvl(tmp_ativo.dat_alteracao,current_timestamp())


-- COMMAND ----------

-- DBTITLE 1,tmp_empresa_audaz_grupo
create or replace temporary view tmp_empresa_audaz_grupo as

with tmp_todos as (
  select distinct dbo_accounts.AccountId as cod_empresa, 
         coalesce(regexp_replace(UPPER(dbo_accounts.Cnpj),'[^0-9A-Z]',''),concat('VRM-',dbo_accounts.AccountId)) as num_cnpj,
         coalesce(regexp_replace(UPPER(dbo_accounts.Cnpj),'[^0-9A-Z]',''),concat('VRM-',dbo_accounts.AccountId)) as num_cnpj_grupo_vrm, 
         dbo_accounts.Name as dsc_razao_social,
         dbo_accounts.StateRegistration as dsc_inscricao_estadual,
         dbo_accounts.MunicipalRegistration as dsc_inscricao_municipal,
         case when dbo_accountlicense.Status = 'true' then 'Ativo' else 'Inativo' end as dsc_status,
         cast(dbo_accounts.ModifiedOn as timestamp) as dat_alteracao,
         dbo_accounts.AccountId as cod_grupo,
         dbo_accounts.Name as dsc_nome_grupo,
         dbo_Affiliate.AffiliateId as cod_afiliacao,
         regexp_replace(UPPER(dbo_Affiliate.Cnpj),'[^0-9A-Z]','') as num_cnpj_afiliacao,
         dbo_Affiliate.Name as dsc_nome_afiliacao,
         dbo_accounts.PartnerId as cod_parceiro,
         dbo_partners.Name as dsc_nome_parceiro,
         'VRM - Audaz' as dsc_origem
    from prod.silver_audaz.dbo_accounts --dbo_accounts: Tabela de Grupos Economicos (Matriz)
    inner join prod.silver_audaz.dbo_branchs --dbo_branchs: Tabela de Unidades (Filiais)
      on dbo_branchs.AccountId = dbo_accounts.AccountId
    left join prod.silver_audaz.dbo_Affiliate --dbo_Affiliate: base dos escritórios de contabilidade (Afiliados)
      on dbo_accounts.AffiliateId = dbo_Affiliate.AffiliateId
    left join prod.silver_audaz.dbo_partners --dbo_partners: base dos parceiros de negócio (Parceiros)
      on dbo_accounts.PartnerId = dbo_partners.PartnerId
    left join prod.silver_audaz.dbo_accountlicense
      on dbo_accountlicense.AccountId = dbo_accounts.AccountId
   where upper(dbo_accounts.Name) not like ('TESTE%') --Retira Testes
     and upper(dbo_accounts.accountid) not in ('F813C11D-6A8E-47AD-933B-DCD722BE9996') --Retira bases de Implantação/Demonstração
     and regexp_replace(UPPER(dbo_accounts.Cnpj),'[^0-9A-Z]','') is null
     
)    

, tmp_status_ativo as (
  select tmp_todos.* 
    from tmp_todos
   inner join (Select num_cnpj, 
                 min(dsc_status) as dsc_status
            from tmp_todos
           group by num_cnpj) as tmp_ativo
      on tmp_todos.num_cnpj = tmp_ativo.num_cnpj
     and tmp_todos.dsc_status = tmp_ativo.dsc_status   
)

select tmp_status_ativo.* 
  from tmp_status_ativo
 inner join (Select num_cnpj, 
                    max(dat_alteracao) as dat_alteracao
               from tmp_status_ativo
           group by num_cnpj) as tmp_ativo
    on tmp_status_ativo.num_cnpj = tmp_ativo.num_cnpj
   and nvl(tmp_status_ativo.dat_alteracao,current_timestamp()) = nvl(tmp_ativo.dat_alteracao,current_timestamp())

-- COMMAND ----------

-- DBTITLE 1,tmp_empresa_pagga
create or replace temporary view tmp_empresa_pagga as

with tmp_pedido as (
  select distinct 
         sha(biadm_snf_pedido_venda_vt_fct.nr_cnpj) as cod_empresa,
         biadm_snf_pedido_venda_vt_fct.nr_cnpj as num_cnpj,
         biadm_snf_pedido_venda_vt_fct.nr_cnpj_grupo as num_cnpj_grupo_vrm,
         biadm_snf_pedido_venda_vt_fct.nm_empresa as dsc_razao_social,
         biadm_snf_pedido_venda_vt_fct.nm_grupo_economico as dsc_nome_grupo,
         biadm_snf_pedido_venda_vt_fct.tp_empresa as dsc_licenca,
         biadm_snf_pedido_venda_vt_fct.DT_EMISSAO_NF
    from prod.silver_vr_bi_legado.biadm_snf_pedido_venda_vt_fct
)
select  cod_empresa,
        num_cnpj,
        num_cnpj_grupo_vrm,
        dsc_razao_social,
        dsc_nome_grupo,
        dsc_licenca,
        dsc_origem
    from
        (Select cod_empresa,
              num_cnpj,
              num_cnpj_grupo_vrm,
              dsc_razao_social,
              dsc_nome_grupo,
              dsc_licenca,
              'VRM - Pagga' as dsc_origem,
              row_number() over (partition by num_cnpj order by DT_EMISSAO_NF desc) as ordenacao_recente
          from tmp_pedido
        where not exists (select num_cnpj
                            from tmp_empresa_audaz
                            where tmp_empresa_audaz.num_cnpj = tmp_pedido.num_cnpj))
      where ordenacao_recente = 1


-- COMMAND ----------

-- DBTITLE 1,tmp_migracao
create or replace temporary view tmp_migracao as
select
  dim_cadastro_antigo.srk_rh_cadastro as srk_rh_cadastro_antigo
  , regexp_replace(UPPER(migrationcnpj),'[^0-9A-Z]','') as num_cnpj_antigo
  , dim_cadastro_antigo.dsc_nome_grupo_vrm as dsc_nome_grupo_vrm_antigo
  , dim_cadastro.srk_rh_cadastro
  , regexp_replace(UPPER(originalcnpj),'[^0-9A-Z]','') as num_cnpj
  , dim_cadastro.dsc_nome_grupo_vrm
  , migratedon as dat_migracao
from prod.silver_audaz.dbo_migratedcompany
  inner join gold_vr_cliente_rh.dim_cadastro
     on UPPER(dim_cadastro.num_cnpj) = regexp_replace(UPPER(originalcnpj),'[^0-9A-Z]','')
    and dim_cadastro.flg_ativo = 1
  inner join gold_vr_cliente_rh.dim_cadastro dim_cadastro_antigo
     on UPPER(dim_cadastro_antigo.num_cnpj) = regexp_replace(UPPER(migrationcnpj),'[^0-9A-Z]','')
    and dim_cadastro_antigo.flg_ativo = 1
where isaccount is false



-- COMMAND ----------

-- DBTITLE 1,vr_temporaria.tmp_rh_cadastro_vrm
create or replace table vr_temporaria.tmp_rh_cadastro_vrm

with tmp_empresa as (
  select cod_empresa,
         num_cnpj,
         dsc_razao_social,
         dsc_inscricao_estadual,
         dsc_inscricao_municipal,
         cod_grupo,
         num_cnpj_grupo_vrm,
         dsc_nome_grupo,
         cod_afiliacao,
         num_cnpj_afiliacao,
         dsc_nome_afiliacao,
         dsc_origem
    from tmp_empresa_audaz
   union
    select cod_empresa,
         num_cnpj,
         dsc_razao_social,
         dsc_inscricao_estadual,
         dsc_inscricao_municipal,
         cod_grupo,
         num_cnpj_grupo_vrm,
         dsc_nome_grupo,
         cod_afiliacao,
         num_cnpj_afiliacao,
         dsc_nome_afiliacao,
         dsc_origem
    from tmp_empresa_audaz_grupo
   union
  select cod_empresa,
         num_cnpj,
         dsc_razao_social,
         null as dsc_inscricao_estadual,
         null as dsc_inscricao_municipal,
         null as cod_grupo,
         num_cnpj_grupo_vrm,
         dsc_nome_grupo,
         null as cod_afiliacao,
         null as num_cnpj_afiliacao,
         null as dsc_nome_afiliacao,
         dsc_origem
    from tmp_empresa_pagga
)

, tmp_pedido as (
  select num_cnpj,
         min(dat_criacao_pedido) as dat_primeiro_pedido
    from gold_vr_cliente_rh.fat_pedido_item
   group by num_cnpj
)

select tmp_empresa.cod_empresa,
       tmp_empresa.num_cnpj,
       tmp_empresa.dsc_razao_social,
       tmp_empresa.dsc_inscricao_estadual,
       tmp_empresa.dsc_inscricao_municipal,
       tmp_empresa.cod_grupo as cod_grupo_vrm,
       tmp_empresa.num_cnpj_grupo_vrm,
       tmp_empresa.dsc_nome_grupo as dsc_nome_grupo_vrm,
       tmp_pedido.dat_primeiro_pedido as dat_primeiro_pedido_vrm,
       tmp_empresa.cod_afiliacao as cod_afiliacao_vrm,
       tmp_empresa.num_cnpj_afiliacao as num_cnpj_afiliacao_vrm,
       tmp_empresa.dsc_nome_afiliacao as dsc_nome_afiliacao_vrm,
       tmp_empresa.dsc_origem as dsc_origem_vrm,
       tmp_migracao.srk_rh_cadastro as srk_rh_cadastro_migrado_vrm,
       tmp_migracao.num_cnpj as num_cnpj_migrado_vrm
  from tmp_empresa
  left join tmp_pedido
    on tmp_empresa.num_cnpj = tmp_pedido.num_cnpj
  left join tmp_migracao
    on tmp_migracao.num_cnpj_antigo = tmp_pedido.num_cnpj



-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Gente</h1>

-- COMMAND ----------

-- DBTITLE 1,vr_temporaria.tmp_rh_cadastro_vrg
create or replace table vr_temporaria.tmp_rh_cadastro_vrg

with tmp_pagamento as (
Select distinct client_id
    from 
      (Select distinct activation_clients.id as client_id
          from prod.silver_vr_gente_serverless.activation_clients
          where activation_clients.first_payment is not null
      union
      Select distinct activation_client_billings.client_id
          from prod.silver_vr_gente_serverless.activation_client_billings
          where activation_client_billings.payment_date is not null)
)

, tmp_empresas (
select distinct activation_clients.id,
        nvl(public_view_clients_datalake.corporate_name, activation_clients.name) as dsc_razao_social,
        activation_clients.created_at as dat_criacao_vrg,
        activation_clients.updated_at as dat_atualizacao_vrg,
        regexp_replace(UPPER(nvl(activation_clients.cnpj,public_view_clients_datalake.cnpj)),'[^0-9A-Z]','') as num_cnpj,
        case
            when activation_clients.is_internal_account is true then true
            when activation_clients.email ilike '%@sememail%' and tmp_pagamento.client_id is null
                then true
            when activation_clients.email ilike '%pontomais.c%' and tmp_pagamento.client_id is null
                then true
            when activation_clients.email ilike '%uorak%' and tmp_pagamento.client_id is null
                then true
            when lower(activation_clients.name) like '%demonstração%' and tmp_pagamento.client_id is null
                then true
            when lower(activation_clients.name) like '%test%' and tmp_pagamento.client_id is null
                then true
            when lower(activation_clients.name) like '%empresa modelo%' and tmp_pagamento.client_id is null
                then true
            when lower(activation_clients.name) in ('hahaha','fer fer','nome empresa','nome da empresa','fantasia minha empresa') and tmp_pagamento.client_id is null
                then true
            when activation_clients.cnpj in ('03.371.411/0001-81', '34.895.579/0001-10', '16.838.202/0001-47', '45.092.615/0001-23', '08.967347/0001-10', '97.514.312/0001-55', '56.212.493/0001-88', '00.000.000/0001-91', '02.239.287/0001-32', '16.461.316/0001-10', '51.244.861/0001-56', '78.595352/0001-24', '31.511.696/0001-44', '93.960.861/0001-00', '67.637.653/0001-57')
                then true
            when activation_clients.email in ('companuy.pedro@test.com.br')
                then true
        else false
        end as status_final_cliente
    from prod.silver_vr_gente_serverless.activation_clients
    left join tmp_pagamento
            on tmp_pagamento.client_id = activation_clients.id
    left join prod.silver_vr_gente_produto.public_view_clients_datalake
            on public_view_clients_datalake.id = activation_clients.external_client_id
)

select num_cnpj,
      dsc_razao_social,
      dat_criacao_vrg,
      dat_atualizacao_vrg
    from
        (select  *,
                row_number() over(partition by num_cnpj order by dat_atualizacao_vrg desc, dat_criacao_vrg asc, dsc_razao_social) as ordenacao_atual
            from
                (select num_cnpj,
                        dsc_razao_social,
                        dat_criacao_vrg,
                        dat_atualizacao_vrg
                    from tmp_empresas
                    where status_final_cliente = false
                    and num_cnpj is not null
                union
                select 'VRG-' || min(id) as num_cnpj,
                        dsc_razao_social,
                        dat_criacao_vrg,
                        dat_atualizacao_vrg
                    from tmp_empresas
                    where status_final_cliente = false
                    and num_cnpj is null
                    group by dsc_razao_social,
                        dat_criacao_vrg,
                        dat_atualizacao_vrg))
    where ordenacao_atual = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>EPS</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_rh_eps
create or replace temporary view tmp_rh_eps as

Select lpad(regexp_replace(UPPER(biadm_eps_retorno_lig_rpt.CNPJ),'[^0-9A-Z]',''),14,'0') as num_cnpj,
       max(biadm_eps_retorno_lig_rpt.RAZAO_SOCIAL) as dsc_razao_social,
       least(min(biadm_eps_retorno_lig_rpt.DT_INCLUSAO),min(biadm_eps_retorno_lig_rpt.dt_ingestao)) as dat_criacao_registro
  from prod.silver_vr_bi_legado.biadm_eps_retorno_lig_rpt
 inner join prod.silver_vr_receita_federal.empresas
    on left(lpad(regexp_replace(UPPER(biadm_eps_retorno_lig_rpt.CNPJ),'[^0-9A-Z]',''),14,'0'), 8) = empresas.cnpj_basico 
 where len(biadm_eps_retorno_lig_rpt.CNPJ) <= 18
 group by all


-- COMMAND ----------

-- DBTITLE 1,tmp_agrupamento_empregador
create or replace temporary view tmp_agrupamento_empregador as

select num_cnpj,
       cast(cluster as int) as cod_agrupamento_empregador
  from prod.mlops_resultado.mlr_persona_rh
qualify row_number() over (partition by num_cnpj order by dat_referencia desc) = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>Consolidação</h1>

-- COMMAND ----------

-- DBTITLE 1,vr_temporaria.tmp_rh_vr_cadastro
create or replace table vr_temporaria.tmp_rh_vr_cadastro 

with 
tmp_cnpj as (
select distinct num_cnpj,
        concat_ws(", ", sort_array(collect_set(distinct dsc_origem))) as dsc_origem
     from
  (select num_cnpj,
          'VR Benefícios' as dsc_origem
         from vr_temporaria.tmp_rh_cadastro_vrb
    union
  select num_cnpj,
         'VR Mobilidade' as dsc_origem
         from vr_temporaria.tmp_rh_cadastro_vrm
    union
  select num_cnpj,
         'VR Gente' as dsc_origem
         from vr_temporaria.tmp_rh_cadastro_vrg
    union
  select num_cnpj,
         'EPS' as dsc_origem
         from tmp_rh_eps)
group by num_cnpj
)

, tmp_consolidacao as (
select 
      tmp_cnpj.num_cnpj,
      vrb.dsc_tipo_documento,
      case when tmp_cnpj.num_cnpj like 'VRG-%' then '-1' else left(tmp_cnpj.num_cnpj, 8) end as num_raiz_cnpj,
      tmp_cnpj.dsc_origem,
      vrb.num_inscricao_estadual,
      vrb.num_inscricao_municipal,
      vrb.cod_tipo_sociedade,
      vrb.dsc_tipo_sociedade,
      coalesce(vrb.dsc_razao_social, vrm.dsc_razao_social, vrg.dsc_razao_social, tmp_rh_eps.dsc_razao_social) as dsc_razao_social_cadastro,
      vrb.dsc_grupo_economico,
      vrb.dsc_grupo_economico_supercrm,
      vrb.cod_grupo_economico,
      vrb.cod_grupo_economico_supercrm,
      vrb.cod_cnae_principal_salesforce,
      vrb.dsc_cnae_principal_salesforce,
      vrb.flg_cliente_premium,
      vrb.flg_cliente_farmer,
      vrb.dsc_farmer_responsavel,
      vrb.dsc_classificacao_farmer,
      vrb.dsc_tipo_cliente,
      vrb.dat_fundacao,
      nvl(vrb.dat_criacao_registro, tmp_rh_eps.dat_criacao_registro) as dat_criacao_registro,
      vrb.dat_alteracao_registro,
      vrb.cod_autorizador_matriz,
      vrb.cod_autorizador_filial,
      vrb.cod_setid,
      vrb.cod_companyid,
      vrb.cod_registro_peoplesoft,
      vrb.cod_registro_salesforce,
      vrb.cod_registro_supercrm,
      vrb.srk_rh_cadastro_associado,
      vrb.cod_setid_associado,
      vrb.cod_companyid_associado,
      vrm.num_cnpj_grupo_vrm,
      vrm.cod_grupo_vrm,    
      vrm.dsc_nome_grupo_vrm,
      vrm.dat_primeiro_pedido_vrm,
      vrm.cod_afiliacao_vrm,
      vrm.num_cnpj_afiliacao_vrm,
      vrm.dsc_nome_afiliacao_vrm,
      vrm.srk_rh_cadastro_migrado_vrm,
      vrm.num_cnpj_migrado_vrm,
      vrm.dsc_origem_vrm,
      vrb.flg_cliente_premium_vrg,
      vrg.dat_criacao_vrg,
      vrg.dat_atualizacao_vrg
    from tmp_cnpj
    left join vr_temporaria.tmp_rh_cadastro_vrb vrb
      on tmp_cnpj.num_cnpj = vrb.num_cnpj
    left join vr_temporaria.tmp_rh_cadastro_vrm vrm
      on tmp_cnpj.num_cnpj = vrm.num_cnpj
    left join vr_temporaria.tmp_rh_cadastro_vrg vrg
      on tmp_cnpj.num_cnpj = vrg.num_cnpj
    left join tmp_rh_eps
      on tmp_cnpj.num_cnpj = tmp_rh_eps.num_cnpj
),

tmp_primeiro_credito as (
  Select dim_conta.srk_rh_cadastro,
         cast(min(dim_conta.dat_primeiro_credito) as date) as dat_primeiro_credito
    from gold_vr_cliente_trabalhador.dim_conta
   where dim_conta.flg_ativo = 1
     and dim_conta.dat_primeiro_credito is not null
     and cast(dim_conta.dat_primeiro_credito as date) not in ('0001-01-03','0001-01-01')
     and cast(dim_conta.dat_primeiro_credito as date) >= '2013-01-01'
   group by dim_conta.srk_rh_cadastro
),

tmp_valor_limite_gpa as (
  select distinct sysadm_ps_cust_add_in_brl.STD_ID_NUM num_cnpj,
         cliente.gvr_limite_credito num_valor_limite
    from prod.silver_vr_erp_peoplesoft.sysadm_ps_gvr_lim_cliente cliente
   inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_cust_add_in_brl
      on cliente.setid = sysadm_ps_cust_add_in_brl.setid
     and cliente.cust_id = sysadm_ps_cust_add_in_brl.cust_id
   where cliente.SETID='SNBRL'
     and cliente.BUSINESS_UNIT in ('VRGPA')
     and sysadm_ps_cust_add_in_brl.STD_ID_NUM_QUAL='CNJ'
     and cliente.gvr_limite_ativo = 'Y'
),

tmp_final as (
  select 
        case when tmp_consolidacao.num_cnpj not like 'VRG-%' then regexp_replace(tmp_consolidacao.num_cnpj,'[^0-9A-Z]','') 
            else tmp_consolidacao.num_cnpj end as num_cnpj,
        tmp_consolidacao.dsc_tipo_documento,
        case when tmp_consolidacao.num_raiz_cnpj <> '-1' then regexp_replace(tmp_consolidacao.num_raiz_cnpj,'[^0-9A-Z]','')
            else tmp_consolidacao.num_raiz_cnpj end as num_raiz_cnpj,
        case when (tmp_consolidacao.num_cnpj_grupo_vrm is null or tmp_consolidacao.num_cnpj_grupo_vrm = '-1') then '-1' 
            else regexp_replace(tmp_consolidacao.num_cnpj_grupo_vrm,'[^0-9A-Z]','') end as num_cnpj_grupo_vrm,
        upper(translate(ltrim(empresas.razao_social),'ŠŽšžŸÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy')) as dsc_razao_social,
        upper(translate(ltrim(estabelecimentos.nome_fantasia),'ŠŽšžŸÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy')) as dsc_nome_fantasia,
        upper(translate(ltrim(tmp_consolidacao.dsc_razao_social_cadastro),'ŠŽšžŸÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ','SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy')) as dsc_razao_social_cadastro,
        tmp_consolidacao.num_inscricao_estadual,
        tmp_consolidacao.num_inscricao_municipal,
        estabelecimentos.situacao_cadastral as cod_situacao_cadastral,
        tmp_consolidacao.cod_tipo_sociedade,
        tmp_consolidacao.dsc_tipo_sociedade,
        estabelecimentos.cnae_fiscal_principal as cod_cnae_principal,
        cnaes.descricao as dsc_cnae_principal,
        estabelecimentos.cnae_fiscal_secundaria as cod_cnae_secundario,
        tmp_consolidacao.srk_rh_cadastro_associado,
        tmp_consolidacao.cod_setid_associado,
        tmp_consolidacao.cod_companyid_associado,
        tmp_consolidacao.dsc_grupo_economico,
        tmp_consolidacao.dsc_grupo_economico_supercrm,
        tmp_consolidacao.cod_grupo_economico,
        tmp_consolidacao.cod_grupo_economico_supercrm,
        tmp_consolidacao.cod_cnae_principal_salesforce,
        tmp_consolidacao.dsc_cnae_principal_salesforce,
        (to_date(concat(substr(estabelecimentos.data_inicio_atividade, 1, 4), '-', substr(estabelecimentos.data_inicio_atividade, 5, 2),'-', substr(estabelecimentos.data_inicio_atividade, 7, 2)))) as dat_abertura,
        initcap(agrupamento_cnae.dsc_setor) as dsc_setor,
        initcap(agrupamento_cnae.dsc_secao) as dsc_ramo_atividade,
        app_dtm_f_export_neoway.QTD_FUNCIONARIOS as qtd_funcionario,
        tmp_valor_limite_gpa.num_valor_limite,
        tmp_consolidacao.flg_cliente_premium,
        tmp_consolidacao.flg_cliente_farmer,
        tmp_consolidacao.dsc_farmer_responsavel,
        tmp_consolidacao.dsc_classificacao_farmer,
        tmp_consolidacao.dsc_tipo_cliente,
        tmp_consolidacao.dat_fundacao,
        tmp_consolidacao.dat_criacao_registro,
        tmp_consolidacao.dat_alteracao_registro,
        tmp_consolidacao.cod_autorizador_matriz,
        tmp_consolidacao.cod_autorizador_filial,
        tmp_consolidacao.cod_setid,
        tmp_consolidacao.cod_companyid,
        tmp_consolidacao.cod_registro_peoplesoft,
        tmp_consolidacao.cod_registro_salesforce,
        tmp_consolidacao.cod_registro_supercrm,
        tmp_consolidacao.cod_grupo_vrm,    
        tmp_consolidacao.dsc_nome_grupo_vrm,
        tmp_consolidacao.dat_primeiro_pedido_vrm,
        tmp_consolidacao.cod_afiliacao_vrm,
        tmp_consolidacao.num_cnpj_afiliacao_vrm,
        tmp_consolidacao.dsc_nome_afiliacao_vrm,
        tmp_consolidacao.srk_rh_cadastro_migrado_vrm,
        tmp_consolidacao.num_cnpj_migrado_vrm,
        tmp_consolidacao.dsc_origem_vrm,
        tmp_consolidacao.flg_cliente_premium_vrg,
        tmp_consolidacao.dat_criacao_vrg,
        tmp_consolidacao.dat_atualizacao_vrg,
        tmp_agrupamento_empregador.cod_agrupamento_empregador,
        max(case when tmp_consolidacao.dsc_origem like '%VR Benefícios%' then 1 else 0 end) as flg_vrb,
        max(case when tmp_consolidacao.dsc_origem like '%VR Mobilidade%' then 1 else 0 end) as flg_vrm, 
        max(case when tmp_consolidacao.dsc_origem like '%VR Gente%'      then 1 else 0 end) as flg_vrg,
        max(case when tmp_consolidacao.dsc_origem like '%EPS%'           then 1 else 0 end) as flg_eps
    from tmp_consolidacao
    left join prod.silver_vr_receita_federal.empresas
      on left(tmp_consolidacao.num_cnpj, 8) = empresas.cnpj_basico 
    left join prod.silver_vr_receita_federal.estabelecimentos
      on tmp_consolidacao.num_cnpj = estabelecimentos.cnpj_basico || estabelecimentos.cnpj_ordem || estabelecimentos.cnpj_dv
    left join prod.silver_vr_receita_federal.cnaes
      on estabelecimentos.cnae_fiscal_principal = cnaes.id
    left join prod.silver_ibge_outros.agrupamento_cnae
      on agrupamento_cnae.id_subclasse = estabelecimentos.cnae_fiscal_principal
    left join prod.silver_vr_neoway.app_dtm_f_export_neoway
      on tmp_consolidacao.num_cnpj = app_dtm_f_export_neoway.CNPJ
    left join tmp_valor_limite_gpa
      on tmp_consolidacao.num_cnpj = tmp_valor_limite_gpa.num_cnpj
    left join tmp_agrupamento_empregador
      on tmp_consolidacao.num_cnpj = tmp_agrupamento_empregador.num_cnpj
    group by all
)

select tmp_final.num_cnpj,
       tmp_final.dsc_tipo_documento,
       tmp_final.num_raiz_cnpj,
       tmp_final.num_cnpj_grupo_vrm,
       tmp_final.dsc_razao_social,
       tmp_final.dsc_nome_fantasia,
       tmp_final.dsc_razao_social_cadastro,
       tmp_final.num_inscricao_estadual,
       tmp_final.num_inscricao_municipal,
       tmp_final.cod_situacao_cadastral,
       tmp_final.cod_tipo_sociedade,
       tmp_final.dsc_tipo_sociedade,
       tmp_final.cod_cnae_principal,
       tmp_final.dsc_cnae_principal,
       tmp_final.cod_cnae_secundario,
       tmp_final.srk_rh_cadastro_associado,
       tmp_final.cod_setid_associado,
       tmp_final.cod_companyid_associado,
       tmp_final.dsc_grupo_economico,
       tmp_final.dsc_grupo_economico_supercrm,
       tmp_final.cod_grupo_economico,
       tmp_final.cod_grupo_economico_supercrm,
       tmp_final.cod_cnae_principal_salesforce,
       tmp_final.dsc_cnae_principal_salesforce,
       tmp_final.dat_abertura,
       tmp_final.dsc_setor,
       tmp_final.dsc_ramo_atividade,
       tmp_final.qtd_funcionario,
       tmp_final.flg_cliente_premium,
       tmp_final.flg_cliente_farmer,
       tmp_final.dsc_farmer_responsavel,
       tmp_final.dsc_classificacao_farmer,
       tmp_final.dsc_tipo_cliente,
       tmp_final.dat_fundacao,
       tmp_final.dat_criacao_registro,
       tmp_final.dat_alteracao_registro,
       tmp_final.cod_autorizador_matriz,
       tmp_final.cod_autorizador_filial,
       tmp_final.cod_setid,
       tmp_final.cod_companyid,
       tmp_final.cod_registro_peoplesoft,
       tmp_final.cod_registro_salesforce,
       tmp_final.cod_registro_supercrm,
       tmp_final.cod_grupo_vrm,
       tmp_final.dsc_nome_grupo_vrm,
       tmp_final.dat_primeiro_pedido_vrm,
       tmp_final.cod_afiliacao_vrm,
       tmp_final.num_cnpj_afiliacao_vrm,
       tmp_final.dsc_nome_afiliacao_vrm,
       nvl(dim_cadastro.srk_rh_cadastro_migrado_vrm, tmp_final.srk_rh_cadastro_migrado_vrm) as srk_rh_cadastro_migrado_vrm,
       nvl(dim_cadastro.num_cnpj_migrado_vrm, tmp_final.num_cnpj_migrado_vrm) as num_cnpj_migrado_vrm,
       tmp_final.dsc_origem_vrm,
       tmp_final.flg_cliente_premium_vrg,
       tmp_final.dat_criacao_vrg,
       tmp_final.dat_atualizacao_vrg,
       tmp_final.cod_agrupamento_empregador,
       tmp_final.flg_vrb,
       tmp_final.flg_vrm,
       tmp_final.flg_vrg,
       tmp_final.flg_eps,
       tmp_final.num_valor_limite,
       tmp_primeiro_credito.dat_primeiro_credito,
       case
         when dim_cadastro.srk_rh_cadastro is null then 'inserir'
         when nvl(tmp_final.dsc_grupo_economico,'')         <> nvl(dim_cadastro.dsc_grupo_economico,'') then 'versionar'
         when nvl(tmp_final.flg_cliente_farmer,'')          <> nvl(dim_cadastro.flg_cliente_farmer,'') then 'versionar'
         when nvl(tmp_final.dsc_setor,'')                   <> nvl(dim_cadastro.dsc_setor,'') then 'versionar'
         when nvl(tmp_final.dsc_ramo_atividade,'')          <> nvl(dim_cadastro.dsc_ramo_atividade,'') then 'versionar'
         when nvl(tmp_final.dsc_classificacao_farmer,'')    <> nvl(dim_cadastro.dsc_classificacao_farmer,'') then 'versionar'
         when nvl(tmp_final.cod_agrupamento_empregador,'')  <> nvl(dim_cadastro.cod_agrupamento_empregador,'') then 'versionar'
       else 'atualizar'
       end as dsc_acao,
       --Criação do código hash MD5 do registro
       MD5(nvl(tmp_final.num_cnpj,'') ||
           nvl(tmp_final.dsc_tipo_documento,'') ||
           nvl(tmp_final.num_raiz_cnpj,'') ||
           nvl(tmp_final.num_cnpj_grupo_vrm,'') ||
           nvl(tmp_final.dsc_razao_social,'') ||
           nvl(tmp_final.dsc_nome_fantasia,'') ||
           nvl(tmp_final.dsc_razao_social_cadastro,'') ||
           nvl(tmp_final.num_inscricao_estadual,'') ||
           nvl(tmp_final.num_inscricao_municipal,'') ||
           nvl(tmp_final.cod_situacao_cadastral,'') ||
           nvl(tmp_final.cod_tipo_sociedade,'') ||
           nvl(tmp_final.dsc_tipo_sociedade,'') ||
           nvl(tmp_final.cod_cnae_principal,'') ||
           nvl(tmp_final.dsc_cnae_principal,'')||
           nvl(tmp_final.cod_cnae_secundario,'') ||
           nvl(tmp_final.srk_rh_cadastro_associado,'') ||
           nvl(tmp_final.cod_setid_associado,'') ||
           nvl(tmp_final.cod_companyid_associado,'') ||
           nvl(tmp_final.dsc_grupo_economico,'') ||
           nvl(tmp_final.dsc_grupo_economico_supercrm,'') ||
           nvl(tmp_final.cod_grupo_economico,'') ||
           nvl(tmp_final.cod_grupo_economico_supercrm,'') ||
           nvl(tmp_final.cod_cnae_principal_salesforce,'') ||
           nvl(tmp_final.dsc_cnae_principal_salesforce,'') ||
           nvl(tmp_final.dat_abertura,'') ||
           nvl(tmp_final.dsc_setor,'') ||
           nvl(tmp_final.dsc_ramo_atividade,'') ||
           nvl(tmp_final.qtd_funcionario,'') ||
           nvl(tmp_primeiro_credito.dat_primeiro_credito,'') ||
           nvl(tmp_final.num_valor_limite,'') ||
           nvl(tmp_final.flg_cliente_premium,'') ||
           nvl(tmp_final.flg_cliente_farmer,'') ||
           nvl(tmp_final.dsc_farmer_responsavel,'') ||
           nvl(tmp_final.dsc_classificacao_farmer,'') ||
           nvl(tmp_final.dsc_tipo_cliente,'') ||
           nvl(tmp_final.dat_fundacao,'') ||
           nvl(tmp_final.dat_criacao_registro,'') ||
           nvl(tmp_final.dat_alteracao_registro,'') ||
           nvl(tmp_final.cod_autorizador_matriz,'') ||
           nvl(tmp_final.cod_autorizador_filial,'') ||
           nvl(tmp_final.cod_setid,'') ||
           nvl(tmp_final.cod_companyid,'') || 
           nvl(tmp_final.cod_registro_peoplesoft,'') ||
           nvl(tmp_final.cod_registro_salesforce,'') ||
           nvl(tmp_final.cod_registro_supercrm,'') ||
           nvl(tmp_final.flg_vrb,'') ||
           nvl(tmp_final.flg_vrm,'') ||
           nvl(tmp_final.flg_vrg,'') ||
           nvl(tmp_final.flg_eps,'') ||
           nvl(tmp_final.cod_grupo_vrm,'') ||
           nvl(tmp_final.dsc_nome_grupo_vrm,'') ||
           nvl(tmp_final.dat_primeiro_pedido_vrm,'') ||
           nvl(tmp_final.cod_afiliacao_vrm,'') ||
           nvl(tmp_final.num_cnpj_afiliacao_vrm,'') ||
           nvl(tmp_final.dsc_nome_afiliacao_vrm,'') ||
           nvl(nvl(dim_cadastro.srk_rh_cadastro_migrado_vrm, tmp_final.srk_rh_cadastro_migrado_vrm),'') ||
           nvl(nvl(dim_cadastro.num_cnpj_migrado_vrm, tmp_final.num_cnpj_migrado_vrm),'') ||
           nvl(tmp_final.dsc_origem_vrm,'') ||
           nvl(tmp_final.flg_cliente_premium_vrg,'') ||
           nvl(tmp_final.dat_criacao_vrg,'') ||
           nvl(tmp_final.dat_atualizacao_vrg,'') ||
           nvl(tmp_final.cod_agrupamento_empregador,'')
       ) as cod_hash_md5,
       dim_cadastro.srk_rh_cadastro as srk_rh_cadastro_dimensao,
       dim_cadastro.cod_hash_md5 as cod_hash_md5_destino
  from tmp_final
  left join gold_vr_cliente_rh.dim_cadastro
    on tmp_final.num_cnpj = dim_cadastro.num_cnpj
   and dim_cadastro.flg_ativo = 1
  left join tmp_primeiro_credito
    on dim_cadastro.srk_rh_cadastro = tmp_primeiro_credito.srk_rh_cadastro
    
 

-- COMMAND ----------

-- DBTITLE 1,tmp_rh_cadastro_merge
create or replace temporary view tmp_rh_cadastro_merge as

with tmp_union_rh_cadastro (
  select num_cnpj,
         dsc_tipo_documento,
         num_raiz_cnpj,
         num_cnpj_grupo_vrm,
         dsc_razao_social,
         dsc_nome_fantasia,
         dsc_razao_social_cadastro,
         num_inscricao_estadual,
         num_inscricao_municipal,
         cod_situacao_cadastral,
         cod_tipo_sociedade,
         dsc_tipo_sociedade,
         cod_cnae_principal,
         dsc_cnae_principal,
         cod_cnae_secundario,
         srk_rh_cadastro_associado,
         cod_setid_associado,
         cod_companyid_associado,
         dsc_grupo_economico,
         dsc_grupo_economico_supercrm,
         cod_grupo_economico,
         cod_grupo_economico_supercrm,
         cod_cnae_principal_salesforce,
         dsc_cnae_principal_salesforce,
         dat_abertura,
         dsc_setor,
         dsc_ramo_atividade,
         qtd_funcionario,
         flg_cliente_premium,
         flg_cliente_farmer,
         dsc_farmer_responsavel,
         dsc_classificacao_farmer,
         dsc_tipo_cliente,
         dat_fundacao,
         dat_criacao_registro,
         dat_alteracao_registro,
         cod_autorizador_matriz,
         cod_autorizador_filial,
         cod_setid,
         cod_companyid,
         cod_registro_peoplesoft,
         cod_registro_salesforce,
         cod_registro_supercrm,
         cod_grupo_vrm,
         dsc_nome_grupo_vrm,
         dat_primeiro_pedido_vrm,
         cod_afiliacao_vrm,
         num_cnpj_afiliacao_vrm,
         dsc_nome_afiliacao_vrm,
         srk_rh_cadastro_migrado_vrm,
         num_cnpj_migrado_vrm,
         dsc_origem_vrm,
         flg_cliente_premium_vrg,
         dat_criacao_vrg,
         dat_atualizacao_vrg,
         cod_agrupamento_empregador,
         flg_vrb,
         flg_vrm,
         flg_vrg,
         flg_eps,
         dat_primeiro_credito,
         num_valor_limite,
         dsc_acao,
         cod_hash_md5,
         srk_rh_cadastro_dimensao,
         cod_hash_md5_destino,
         case dsc_acao
           when 'inserir' then -1
           when 'versionar' then -1
           when 'atualizar' then 1
         end as flg_ativo
    from vr_temporaria.tmp_rh_vr_cadastro
  union
  select num_cnpj,
         dsc_tipo_documento,
         num_raiz_cnpj,
         num_cnpj_grupo_vrm,
         dsc_razao_social,
         dsc_nome_fantasia,
         dsc_razao_social_cadastro,
         num_inscricao_estadual,
         num_inscricao_municipal,
         cod_situacao_cadastral,
         cod_tipo_sociedade,
         dsc_tipo_sociedade,
         cod_cnae_principal,
         dsc_cnae_principal,
         cod_cnae_secundario,
         srk_rh_cadastro_associado,
         cod_setid_associado,
         cod_companyid_associado,
         dsc_grupo_economico,
         dsc_grupo_economico_supercrm,
         cod_grupo_economico,
         cod_grupo_economico_supercrm,
         cod_cnae_principal_salesforce,
         dsc_cnae_principal_salesforce,
         dat_abertura,
         dsc_setor,
         dsc_ramo_atividade,
         qtd_funcionario,
         flg_cliente_premium,
         flg_cliente_farmer,
         dsc_farmer_responsavel,
         dsc_classificacao_farmer,
         dsc_tipo_cliente,
         dat_fundacao,
         dat_criacao_registro,
         dat_alteracao_registro,
         cod_autorizador_matriz,
         cod_autorizador_filial,
         cod_setid,
         cod_companyid,
         cod_registro_peoplesoft,
         cod_registro_salesforce,
         cod_registro_supercrm,
         cod_grupo_vrm,
         dsc_nome_grupo_vrm,
         dat_primeiro_pedido_vrm,
         cod_afiliacao_vrm,
         num_cnpj_afiliacao_vrm,
         dsc_nome_afiliacao_vrm,
         srk_rh_cadastro_migrado_vrm,
         num_cnpj_migrado_vrm,
         dsc_origem_vrm,
         flg_cliente_premium_vrg,
         dat_criacao_vrg,
         dat_atualizacao_vrg,
         cod_agrupamento_empregador,
         flg_vrb,
         flg_vrm,
         flg_vrg,
         flg_eps,
         dat_primeiro_credito,
         num_valor_limite,
         'versionar_flag' as dsc_acao,
         cod_hash_md5,
         srk_rh_cadastro_dimensao,
         cod_hash_md5_destino,
         1 as flg_ativo
    from vr_temporaria.tmp_rh_vr_cadastro
   where dsc_acao = 'versionar'
)

Select *
  from tmp_union_rh_cadastro
 where nvl(cod_hash_md5_destino,'') <> nvl(cod_hash_md5,'')
 order by srk_rh_cadastro_dimensao nulls first

-- COMMAND ----------

-- DBTITLE 1,Exibe duplicidade no processo
-- Select *
--   from tmp_rh_cadastro_merge 
--  where srk_rh_cadastro_dimensao in (Select srk_rh_cadastro_dimensao
--                                       from tmp_rh_cadastro_merge
--                                      where srk_rh_cadastro_dimensao is not null
--                                      group by srk_rh_cadastro_dimensao, flg_ativo
--                                     having count(1) > 1)

-- COMMAND ----------

-- DBTITLE 1,Padroniza campos
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select num_cnpj,
-- MAGIC        dsc_tipo_documento,
-- MAGIC        num_raiz_cnpj,
-- MAGIC        num_cnpj_grupo_vrm,
-- MAGIC        dsc_razao_social,
-- MAGIC        dsc_nome_fantasia,
-- MAGIC        dsc_razao_social_cadastro,
-- MAGIC        num_inscricao_estadual,
-- MAGIC        num_inscricao_municipal,
-- MAGIC        cod_situacao_cadastral,
-- MAGIC        cod_tipo_sociedade,
-- MAGIC        dsc_tipo_sociedade,
-- MAGIC        cod_cnae_principal,
-- MAGIC        dsc_cnae_principal,
-- MAGIC        cod_cnae_secundario,
-- MAGIC        srk_rh_cadastro_associado,
-- MAGIC        cod_setid_associado,
-- MAGIC        cod_companyid_associado,
-- MAGIC        dsc_grupo_economico,
-- MAGIC        dsc_grupo_economico_supercrm,
-- MAGIC        cod_grupo_economico,
-- MAGIC        cod_grupo_economico_supercrm,
-- MAGIC        cod_cnae_principal_salesforce,
-- MAGIC        dsc_cnae_principal_salesforce,
-- MAGIC        dat_abertura,
-- MAGIC        dsc_setor,
-- MAGIC        dsc_ramo_atividade,
-- MAGIC        qtd_funcionario,
-- MAGIC        flg_cliente_premium,
-- MAGIC        flg_cliente_farmer,
-- MAGIC        dsc_farmer_responsavel,
-- MAGIC        dsc_classificacao_farmer,
-- MAGIC        dsc_tipo_cliente,
-- MAGIC        dat_fundacao,
-- MAGIC        dat_criacao_registro,
-- MAGIC        dat_alteracao_registro,
-- MAGIC        cod_autorizador_matriz,
-- MAGIC        cod_autorizador_filial,
-- MAGIC        cod_setid,
-- MAGIC        cod_companyid,
-- MAGIC        cod_registro_peoplesoft,
-- MAGIC        cod_registro_salesforce,
-- MAGIC        cod_registro_supercrm,
-- MAGIC        cod_grupo_vrm,
-- MAGIC        dsc_nome_grupo_vrm,
-- MAGIC        dat_primeiro_pedido_vrm,
-- MAGIC        cod_afiliacao_vrm,
-- MAGIC        num_cnpj_afiliacao_vrm,
-- MAGIC        dsc_nome_afiliacao_vrm,
-- MAGIC        srk_rh_cadastro_migrado_vrm,
-- MAGIC        num_cnpj_migrado_vrm,
-- MAGIC        dsc_origem_vrm,
-- MAGIC        flg_cliente_premium_vrg,
-- MAGIC        dat_criacao_vrg,
-- MAGIC        dat_atualizacao_vrg,
-- MAGIC        cod_agrupamento_empregador,
-- MAGIC        flg_vrb,
-- MAGIC        flg_vrm,
-- MAGIC        flg_vrg,
-- MAGIC        flg_eps,
-- MAGIC        dat_primeiro_credito,
-- MAGIC        num_valor_limite,
-- MAGIC        dsc_acao,
-- MAGIC        cod_hash_md5,
-- MAGIC        srk_rh_cadastro_dimensao,
-- MAGIC        cod_hash_md5_destino,
-- MAGIC        flg_ativo
-- MAGIC   from tmp_rh_cadastro_merge
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_rh_cadastro_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Merge dim_cadastro
-- MAGIC %python
-- MAGIC
-- MAGIC df_merge = fn_merge_direto("""
-- MAGIC MERGE INTO gold_vr_cliente_rh_pii.dim_cadastro as destino
-- MAGIC      USING tmp_rh_cadastro_criptografado as origem
-- MAGIC         ON destino.srk_rh_cadastro = origem.srk_rh_cadastro_dimensao
-- MAGIC        AND destino.flg_ativo = origem.flg_ativo
-- MAGIC       WHEN MATCHED AND origem.dsc_acao = 'versionar_flag' THEN UPDATE SET
-- MAGIC            destino.flg_ativo = 0,
-- MAGIC            destino.dat_fim_vigencia = timestampadd(DAY,-1,current_timestamp()),
-- MAGIC            destino.dat_alteracao_bi = current_timestamp()
-- MAGIC       WHEN MATCHED AND origem.dsc_acao = 'atualizar' THEN UPDATE SET
-- MAGIC            destino.dsc_tipo_documento                       = origem.dsc_tipo_documento,
-- MAGIC            destino.num_raiz_cnpj                            = origem.num_raiz_cnpj,
-- MAGIC            destino.num_cnpj_grupo_vrm                       = origem.num_cnpj_grupo_vrm,
-- MAGIC            destino.dsc_razao_social                         = origem.dsc_razao_social,
-- MAGIC            destino.dsc_nome_fantasia                        = origem.dsc_nome_fantasia,
-- MAGIC            destino.dsc_razao_social_cadastro                = origem.dsc_razao_social_cadastro,
-- MAGIC            destino.num_inscricao_estadual                   = origem.num_inscricao_estadual,
-- MAGIC            destino.num_inscricao_municipal                  = origem.num_inscricao_municipal,
-- MAGIC            destino.cod_situacao_cadastral                   = origem.cod_situacao_cadastral,
-- MAGIC            destino.cod_tipo_sociedade                       = origem.cod_tipo_sociedade,
-- MAGIC            destino.dsc_tipo_sociedade                       = origem.dsc_tipo_sociedade,
-- MAGIC            destino.cod_cnae_principal                       = origem.cod_cnae_principal,
-- MAGIC            destino.dsc_cnae_principal                       = origem.dsc_cnae_principal,
-- MAGIC            destino.cod_cnae_secundario                      = origem.cod_cnae_secundario,
-- MAGIC            destino.srk_rh_cadastro_associado                = origem.srk_rh_cadastro_associado,
-- MAGIC            destino.cod_setid_associado                      = origem.cod_setid_associado,
-- MAGIC            destino.cod_companyid_associado                  = origem.cod_companyid_associado,
-- MAGIC            destino.dsc_grupo_economico                      = origem.dsc_grupo_economico,
-- MAGIC            destino.dsc_grupo_economico_supercrm             = origem.dsc_grupo_economico_supercrm,
-- MAGIC            destino.cod_grupo_economico                      = origem.cod_grupo_economico,
-- MAGIC            destino.cod_grupo_economico_supercrm             = origem.cod_grupo_economico_supercrm,
-- MAGIC            destino.cod_cnae_principal_salesforce            = origem.cod_cnae_principal_salesforce,
-- MAGIC            destino.dsc_cnae_principal_salesforce            = origem.dsc_cnae_principal_salesforce,
-- MAGIC            destino.dat_abertura                             = origem.dat_abertura,
-- MAGIC            destino.dsc_setor                                = origem.dsc_setor,
-- MAGIC            destino.dsc_ramo_atividade                       = origem.dsc_ramo_atividade,
-- MAGIC            destino.qtd_funcionario                          = origem.qtd_funcionario,
-- MAGIC            destino.dat_primeiro_credito                     = origem.dat_primeiro_credito,
-- MAGIC            destino.num_valor_limite                         = origem.num_valor_limite,
-- MAGIC            destino.flg_cliente_premium                      = origem.flg_cliente_premium,
-- MAGIC            destino.flg_cliente_farmer                       = origem.flg_cliente_farmer,
-- MAGIC            destino.dsc_farmer_responsavel                   = origem.dsc_farmer_responsavel,
-- MAGIC            destino.dsc_classificacao_farmer                 = origem.dsc_classificacao_farmer,
-- MAGIC            destino.dsc_tipo_cliente                         = origem.dsc_tipo_cliente,
-- MAGIC            destino.dat_fundacao                             = origem.dat_fundacao,
-- MAGIC            destino.dat_criacao_registro                     = origem.dat_criacao_registro,
-- MAGIC            destino.dat_alteracao_registro                   = origem.dat_alteracao_registro,
-- MAGIC            destino.cod_autorizador_matriz                   = origem.cod_autorizador_matriz,
-- MAGIC            destino.cod_autorizador_filial                   = origem.cod_autorizador_filial,
-- MAGIC            destino.cod_setid                                = origem.cod_setid,
-- MAGIC            destino.cod_companyid                            = origem.cod_companyid,
-- MAGIC            destino.cod_registro_peoplesoft                  = origem.cod_registro_peoplesoft,
-- MAGIC            destino.cod_registro_salesforce                  = origem.cod_registro_salesforce,
-- MAGIC            destino.cod_registro_supercrm                    = origem.cod_registro_supercrm,
-- MAGIC            destino.flg_vrb                                  = origem.flg_vrb,
-- MAGIC            destino.flg_vrm                                  = origem.flg_vrm,
-- MAGIC            destino.flg_vrg                                  = origem.flg_vrg,
-- MAGIC            destino.flg_eps                                  = origem.flg_eps,
-- MAGIC            destino.cod_grupo_vrm                            = origem.cod_grupo_vrm,
-- MAGIC            destino.dsc_nome_grupo_vrm                       = origem.dsc_nome_grupo_vrm,
-- MAGIC            destino.dat_primeiro_pedido_vrm                  = origem.dat_primeiro_pedido_vrm,
-- MAGIC            destino.cod_afiliacao_vrm                        = origem.cod_afiliacao_vrm,
-- MAGIC            destino.num_cnpj_afiliacao_vrm                   = origem.num_cnpj_afiliacao_vrm,
-- MAGIC            destino.dsc_nome_afiliacao_vrm                   = origem.dsc_nome_afiliacao_vrm,
-- MAGIC            destino.srk_rh_cadastro_migrado_vrm              = origem.srk_rh_cadastro_migrado_vrm,
-- MAGIC            destino.num_cnpj_migrado_vrm                     = origem.num_cnpj_migrado_vrm,
-- MAGIC            destino.dsc_origem_vrm                           = origem.dsc_origem_vrm,
-- MAGIC            destino.flg_cliente_premium_vrg                  = origem.flg_cliente_premium_vrg,
-- MAGIC            destino.dat_criacao_vrg                          = origem.dat_criacao_vrg,
-- MAGIC            destino.dat_atualizacao_vrg                      = origem.dat_atualizacao_vrg,
-- MAGIC            destino.cod_agrupamento_empregador               = origem.cod_agrupamento_empregador,
-- MAGIC            destino.cod_hash_md5                             = origem.cod_hash_md5,
-- MAGIC            destino.dat_alteracao_bi                         = current_timestamp()
-- MAGIC       WHEN NOT MATCHED AND origem.dsc_acao = 'versionar' THEN INSERT (
-- MAGIC            destino.srk_rh_cadastro,
-- MAGIC            destino.num_cnpj,
-- MAGIC            destino.dsc_tipo_documento,
-- MAGIC            destino.num_raiz_cnpj,
-- MAGIC            destino.num_cnpj_grupo_vrm,
-- MAGIC            destino.dsc_razao_social,
-- MAGIC            destino.dsc_nome_fantasia,
-- MAGIC            destino.dsc_razao_social_cadastro,
-- MAGIC            destino.num_inscricao_estadual,
-- MAGIC            destino.num_inscricao_municipal,
-- MAGIC            destino.cod_situacao_cadastral,
-- MAGIC            destino.cod_tipo_sociedade,
-- MAGIC            destino.dsc_tipo_sociedade,
-- MAGIC            destino.cod_cnae_principal,
-- MAGIC            destino.dsc_cnae_principal,
-- MAGIC            destino.cod_cnae_secundario,
-- MAGIC            destino.srk_rh_cadastro_associado,
-- MAGIC            destino.cod_setid_associado,
-- MAGIC            destino.cod_companyid_associado,
-- MAGIC            destino.dsc_grupo_economico,
-- MAGIC            destino.dsc_grupo_economico_supercrm,
-- MAGIC            destino.cod_grupo_economico,
-- MAGIC            destino.cod_grupo_economico_supercrm,
-- MAGIC            destino.cod_cnae_principal_salesforce,
-- MAGIC            destino.dsc_cnae_principal_salesforce,
-- MAGIC            destino.dat_abertura,
-- MAGIC            destino.dsc_setor,
-- MAGIC            destino.dsc_ramo_atividade,
-- MAGIC            destino.qtd_funcionario,
-- MAGIC            destino.dat_primeiro_credito,
-- MAGIC            destino.num_valor_limite,
-- MAGIC            destino.flg_cliente_premium,
-- MAGIC            destino.flg_cliente_farmer,
-- MAGIC            destino.dsc_farmer_responsavel,
-- MAGIC            destino.dsc_classificacao_farmer,
-- MAGIC            destino.dsc_tipo_cliente,
-- MAGIC            destino.dat_fundacao,
-- MAGIC            destino.dat_criacao_registro,
-- MAGIC            destino.dat_alteracao_registro,
-- MAGIC            destino.cod_autorizador_matriz,
-- MAGIC            destino.cod_autorizador_filial,
-- MAGIC            destino.cod_setid,
-- MAGIC            destino.cod_companyid,
-- MAGIC            destino.cod_registro_peoplesoft,
-- MAGIC            destino.cod_registro_salesforce,
-- MAGIC            destino.cod_registro_supercrm,
-- MAGIC            destino.flg_vrb,
-- MAGIC            destino.flg_vrm,
-- MAGIC            destino.flg_vrg,
-- MAGIC            destino.flg_eps,
-- MAGIC            destino.cod_grupo_vrm,
-- MAGIC            destino.dsc_nome_grupo_vrm,
-- MAGIC            destino.dat_primeiro_pedido_vrm,
-- MAGIC            destino.cod_afiliacao_vrm,
-- MAGIC            destino.num_cnpj_afiliacao_vrm,
-- MAGIC            destino.dsc_nome_afiliacao_vrm,
-- MAGIC            destino.srk_rh_cadastro_migrado_vrm,
-- MAGIC            destino.num_cnpj_migrado_vrm,
-- MAGIC            destino.dsc_origem_vrm,
-- MAGIC            destino.flg_cliente_premium_vrg,
-- MAGIC            destino.dat_criacao_vrg,
-- MAGIC            destino.dat_atualizacao_vrg,
-- MAGIC            destino.cod_agrupamento_empregador,
-- MAGIC            destino.cod_hash_md5,
-- MAGIC            destino.flg_ativo,
-- MAGIC            destino.dat_inicio_vigencia,
-- MAGIC            destino.dat_inclusao_bi,
-- MAGIC            destino.dat_alteracao_bi
-- MAGIC       ) VALUES (
-- MAGIC            origem.srk_rh_cadastro_dimensao,
-- MAGIC            origem.num_cnpj,
-- MAGIC            origem.dsc_tipo_documento,
-- MAGIC            origem.num_raiz_cnpj,
-- MAGIC            origem.num_cnpj_grupo_vrm,
-- MAGIC            origem.dsc_razao_social,
-- MAGIC            origem.dsc_nome_fantasia,
-- MAGIC            origem.dsc_razao_social_cadastro,
-- MAGIC            origem.num_inscricao_estadual,
-- MAGIC            origem.num_inscricao_municipal,
-- MAGIC            origem.cod_situacao_cadastral,
-- MAGIC            origem.cod_tipo_sociedade,
-- MAGIC            origem.dsc_tipo_sociedade,
-- MAGIC            origem.cod_cnae_principal,
-- MAGIC            origem.dsc_cnae_principal,
-- MAGIC            origem.cod_cnae_secundario,
-- MAGIC            origem.srk_rh_cadastro_associado,
-- MAGIC            origem.cod_setid_associado,
-- MAGIC            origem.cod_companyid_associado,
-- MAGIC            origem.dsc_grupo_economico,
-- MAGIC            origem.dsc_grupo_economico_supercrm,
-- MAGIC            origem.cod_grupo_economico,
-- MAGIC            origem.cod_grupo_economico_supercrm,
-- MAGIC            origem.cod_cnae_principal_salesforce,
-- MAGIC            origem.dsc_cnae_principal_salesforce,
-- MAGIC            origem.dat_abertura,
-- MAGIC            origem.dsc_setor,
-- MAGIC            origem.dsc_ramo_atividade,
-- MAGIC            origem.qtd_funcionario,
-- MAGIC            origem.dat_primeiro_credito,
-- MAGIC            origem.num_valor_limite,
-- MAGIC            origem.flg_cliente_premium,
-- MAGIC            origem.flg_cliente_farmer,
-- MAGIC            origem.dsc_farmer_responsavel,
-- MAGIC            origem.dsc_classificacao_farmer,
-- MAGIC            origem.dsc_tipo_cliente,
-- MAGIC            origem.dat_fundacao,
-- MAGIC            origem.dat_criacao_registro,
-- MAGIC            origem.dat_alteracao_registro,
-- MAGIC            origem.cod_autorizador_matriz,
-- MAGIC            origem.cod_autorizador_filial,
-- MAGIC            origem.cod_setid,
-- MAGIC            origem.cod_companyid,
-- MAGIC            origem.cod_registro_peoplesoft,
-- MAGIC            origem.cod_registro_salesforce,
-- MAGIC            origem.cod_registro_supercrm,
-- MAGIC            origem.flg_vrb,
-- MAGIC            origem.flg_vrm,
-- MAGIC            origem.flg_vrg,
-- MAGIC            origem.flg_eps,
-- MAGIC            origem.cod_grupo_vrm,
-- MAGIC            origem.dsc_nome_grupo_vrm,
-- MAGIC            origem.dat_primeiro_pedido_vrm,
-- MAGIC            origem.cod_afiliacao_vrm,
-- MAGIC            origem.num_cnpj_afiliacao_vrm,
-- MAGIC            origem.dsc_nome_afiliacao_vrm,
-- MAGIC            origem.srk_rh_cadastro_migrado_vrm,
-- MAGIC            origem.num_cnpj_migrado_vrm,
-- MAGIC            origem.dsc_origem_vrm,
-- MAGIC            origem.flg_cliente_premium_vrg,
-- MAGIC            origem.dat_criacao_vrg,
-- MAGIC            origem.dat_atualizacao_vrg,
-- MAGIC            origem.cod_agrupamento_empregador,
-- MAGIC            origem.cod_hash_md5,
-- MAGIC            1,
-- MAGIC            timestampadd(DAY,-1,current_timestamp()),
-- MAGIC            current_timestamp(),
-- MAGIC            current_timestamp()           
-- MAGIC       )
-- MAGIC       WHEN NOT MATCHED AND origem.dsc_acao = 'inserir' THEN INSERT (
-- MAGIC            destino.num_cnpj,
-- MAGIC            destino.dsc_tipo_documento,
-- MAGIC            destino.num_raiz_cnpj,
-- MAGIC            destino.num_cnpj_grupo_vrm,
-- MAGIC            destino.dsc_razao_social,
-- MAGIC            destino.dsc_nome_fantasia,
-- MAGIC            destino.dsc_razao_social_cadastro,
-- MAGIC            destino.num_inscricao_estadual,
-- MAGIC            destino.num_inscricao_municipal,
-- MAGIC            destino.cod_situacao_cadastral,
-- MAGIC            destino.cod_tipo_sociedade,
-- MAGIC            destino.dsc_tipo_sociedade,
-- MAGIC            destino.cod_cnae_principal,
-- MAGIC            destino.dsc_cnae_principal,
-- MAGIC            destino.cod_cnae_secundario,
-- MAGIC            destino.srk_rh_cadastro_associado,
-- MAGIC            destino.cod_setid_associado,
-- MAGIC            destino.cod_companyid_associado,
-- MAGIC            destino.dsc_grupo_economico,
-- MAGIC            destino.dsc_grupo_economico_supercrm,
-- MAGIC            destino.cod_grupo_economico,
-- MAGIC            destino.cod_grupo_economico_supercrm,
-- MAGIC            destino.cod_cnae_principal_salesforce,
-- MAGIC            destino.dsc_cnae_principal_salesforce,
-- MAGIC            destino.dat_abertura,
-- MAGIC            destino.dsc_setor,
-- MAGIC            destino.dsc_ramo_atividade,
-- MAGIC            destino.qtd_funcionario,
-- MAGIC            destino.dat_primeiro_credito,
-- MAGIC            destino.num_valor_limite,
-- MAGIC            destino.flg_cliente_premium,
-- MAGIC            destino.flg_cliente_farmer,
-- MAGIC            destino.dsc_farmer_responsavel,
-- MAGIC            destino.dsc_classificacao_farmer,
-- MAGIC            destino.dsc_tipo_cliente,
-- MAGIC            destino.dat_fundacao,
-- MAGIC            destino.dat_criacao_registro,
-- MAGIC            destino.dat_alteracao_registro,
-- MAGIC            destino.cod_autorizador_matriz,
-- MAGIC            destino.cod_autorizador_filial,
-- MAGIC            destino.cod_setid,
-- MAGIC            destino.cod_companyid,
-- MAGIC            destino.cod_registro_peoplesoft,
-- MAGIC            destino.cod_registro_salesforce,
-- MAGIC            destino.cod_registro_supercrm,
-- MAGIC            destino.flg_vrb,
-- MAGIC            destino.flg_vrm,
-- MAGIC            destino.flg_vrg,
-- MAGIC            destino.flg_eps,
-- MAGIC            destino.cod_grupo_vrm,
-- MAGIC            destino.dsc_nome_grupo_vrm,
-- MAGIC            destino.dat_primeiro_pedido_vrm,
-- MAGIC            destino.cod_afiliacao_vrm,
-- MAGIC            destino.num_cnpj_afiliacao_vrm,
-- MAGIC            destino.dsc_nome_afiliacao_vrm,
-- MAGIC            destino.srk_rh_cadastro_migrado_vrm,
-- MAGIC            destino.num_cnpj_migrado_vrm,
-- MAGIC            destino.dsc_origem_vrm,
-- MAGIC            destino.flg_cliente_premium_vrg,
-- MAGIC            destino.dat_criacao_vrg,
-- MAGIC            destino.dat_atualizacao_vrg,
-- MAGIC            destino.cod_agrupamento_empregador,
-- MAGIC            destino.cod_hash_md5,
-- MAGIC            destino.flg_ativo,
-- MAGIC            destino.dat_inicio_vigencia,
-- MAGIC            destino.dat_inclusao_bi,
-- MAGIC            destino.dat_alteracao_bi
-- MAGIC       ) VALUES (
-- MAGIC            origem.num_cnpj,
-- MAGIC            origem.dsc_tipo_documento,
-- MAGIC            origem.num_raiz_cnpj,
-- MAGIC            origem.num_cnpj_grupo_vrm,
-- MAGIC            origem.dsc_razao_social,
-- MAGIC            origem.dsc_nome_fantasia,
-- MAGIC            origem.dsc_razao_social_cadastro,
-- MAGIC            origem.num_inscricao_estadual,
-- MAGIC            origem.num_inscricao_municipal,
-- MAGIC            origem.cod_situacao_cadastral,
-- MAGIC            origem.cod_tipo_sociedade,
-- MAGIC            origem.dsc_tipo_sociedade,
-- MAGIC            origem.cod_cnae_principal,
-- MAGIC            origem.dsc_cnae_principal,
-- MAGIC            origem.cod_cnae_secundario,
-- MAGIC            origem.srk_rh_cadastro_associado,
-- MAGIC            origem.cod_setid_associado,
-- MAGIC            origem.cod_companyid_associado,
-- MAGIC            origem.dsc_grupo_economico,
-- MAGIC            origem.dsc_grupo_economico_supercrm,
-- MAGIC            origem.cod_grupo_economico,
-- MAGIC            origem.cod_grupo_economico_supercrm,
-- MAGIC            origem.cod_cnae_principal_salesforce,
-- MAGIC            origem.dsc_cnae_principal_salesforce,
-- MAGIC            origem.dat_abertura,
-- MAGIC            origem.dsc_setor,
-- MAGIC            origem.dsc_ramo_atividade,
-- MAGIC            origem.qtd_funcionario,
-- MAGIC            origem.dat_primeiro_credito,
-- MAGIC            origem.num_valor_limite,
-- MAGIC            origem.flg_cliente_premium,
-- MAGIC            origem.flg_cliente_farmer,
-- MAGIC            origem.dsc_farmer_responsavel,
-- MAGIC            origem.dsc_classificacao_farmer,
-- MAGIC            origem.dsc_tipo_cliente,
-- MAGIC            origem.dat_fundacao,
-- MAGIC            origem.dat_criacao_registro,
-- MAGIC            origem.dat_alteracao_registro,
-- MAGIC            origem.cod_autorizador_matriz,
-- MAGIC            origem.cod_autorizador_filial,
-- MAGIC            origem.cod_setid,
-- MAGIC            origem.cod_companyid,
-- MAGIC            origem.cod_registro_peoplesoft,
-- MAGIC            origem.cod_registro_salesforce,
-- MAGIC            origem.cod_registro_supercrm,
-- MAGIC            origem.flg_vrb,
-- MAGIC            origem.flg_vrm,
-- MAGIC            origem.flg_vrg,
-- MAGIC            origem.flg_eps,
-- MAGIC            origem.cod_grupo_vrm,
-- MAGIC            origem.dsc_nome_grupo_vrm,
-- MAGIC            origem.dat_primeiro_pedido_vrm,
-- MAGIC            origem.cod_afiliacao_vrm,
-- MAGIC            origem.num_cnpj_afiliacao_vrm,
-- MAGIC            origem.dsc_nome_afiliacao_vrm,
-- MAGIC            origem.srk_rh_cadastro_migrado_vrm,
-- MAGIC            origem.num_cnpj_migrado_vrm,
-- MAGIC            origem.dsc_origem_vrm,
-- MAGIC            origem.flg_cliente_premium_vrg,
-- MAGIC            origem.dat_criacao_vrg,
-- MAGIC            origem.dat_atualizacao_vrg,
-- MAGIC            origem.cod_agrupamento_empregador,
-- MAGIC            origem.cod_hash_md5,
-- MAGIC            1,
-- MAGIC            timestampadd(DAY,-1,current_timestamp()),
-- MAGIC            current_timestamp(),
-- MAGIC            current_timestamp()
-- MAGIC       );"""
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
drop table if exists vr_temporaria.tmp_rh_cadastro_vrb;
drop table if exists vr_temporaria.tmp_rh_cadastro_vrm;
drop table if exists vr_temporaria.tmp_rh_cadastro_vrg;
drop table if exists vr_temporaria.tmp_rh_vr_cadastro;

-- COMMAND ----------

-- DBTITLE 1,Exit notebook
-- MAGIC %python
-- MAGIC
-- MAGIC fn_gera_arquivo_bastao(malha,database,tablename)
-- MAGIC fn_exit_notebook(df_merge)
