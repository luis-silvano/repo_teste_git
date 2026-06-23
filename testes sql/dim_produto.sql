-- Databricks notebook source
-- DBTITLE 1,Parametrização utilizada no notebook
-- MAGIC %python
-- MAGIC dbutils.widgets.text("dat_parametro","")
-- MAGIC dbutils.widgets.text("database","")
-- MAGIC dbutils.widgets.text("databricks_catalog","")
-- MAGIC dbutils.widgets.text("dsc_tabela_pii","")
-- MAGIC dbutils.widgets.text("location_catalogo_temp","")
-- MAGIC dbutils.widgets.text("malha","")
-- MAGIC dbutils.widgets.text("tablename","")
-- MAGIC
-- MAGIC dat_parametro = dbutils.widgets.get("dat_parametro")
-- MAGIC database = dbutils.widgets.get("database")
-- MAGIC databricks_catalog = dbutils.widgets.get("databricks_catalog")
-- MAGIC dsc_tabela_pii = dbutils.widgets.get("dsc_tabela_pii")
-- MAGIC location_catalogo_temp = dbutils.widgets.get("location_catalogo_temp")
-- MAGIC malha = dbutils.widgets.get("malha")
-- MAGIC tablename = dbutils.widgets.get("tablename")
-- MAGIC
-- MAGIC # Exemplo de preenchimento
-- MAGIC # dat_parametro = 2026-02-25
-- MAGIC # database = gold_vr_cliente_rh
-- MAGIC # databricks_catalog (DEV) = dev
-- MAGIC # databricks_catalog (PROD) = prod
-- MAGIC # dsc_tabela_pii = dim_produto
-- MAGIC # location_catalogo_temp (DEV) = abfss://temp@adlsvrbeneficiosdev01.dfs.core.windows.net
-- MAGIC # location_catalogo_temp (PROD) = abfss://temp@adlsvrbeneficiosprd.dfs.core.windows.net
-- MAGIC # malha = temp
-- MAGIC # tablename = dim_produto

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_prod_item
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_prod_attr
-- MAGIC - silver_vr_autorizador.sncoreon_snapl 
-- MAGIC - silver_vr_salesforce_super_crm.dbo_product2

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

-- DBTITLE 1,tmp_produtos_saleforce
create or replace temporary view tmp_produtos_saleforce as

with 
tmp_rank_produto as (
Select dbo_product2.*,
       rank () over (partition by dbo_product2.productcode order by nvl(dbo_product2.Data_de_Desativacao__c, '2099-01-01') desc) as flg_ativo
  from prod.silver_vr_salesforce.dbo_product2
 where dbo_product2.Sub_Familia__c in ('Produto', 'SVA') 
       and dbo_product2.Produto_Pai__c is null
)

Select tmp_rank_produto.*
  from tmp_rank_produto
 where tmp_rank_produto.flg_ativo = 1
;  


-- COMMAND ----------

-- DBTITLE 1,tmp_produto
create or replace temporary view tmp_produto as

select sysadm_ps_prod_item.setid as cod_sdprod,
       sysadm_ps_prod_item.product_id as cod_produto,
       sysadm_ps_prod_item.descr as dsc_produto,
       sysadm_ps_prod_attr_sigla.attribute_value as dsc_sigla_produto,
       sysadm_ps_prod_item.prod_brand as cod_emissor,
       case
         when trim(sysadm_ps_prod_attr.attribute_value) = ''
         then null
         else sysadm_ps_prod_attr.attribute_value
       end as cod_aplicacao,
       case when sysadm_ps_prod_attr.attribute_value = '50000' then null else sncoreon_snapl.aplcodpai end as cod_aplicacao_pai,
       tmp_produtos_saleforce.id as cod_identificador_salesforce,
       case when tmp_produtos_saleforce.family is null and  upper(tmp_produtos_saleforce.sub_familia__c) = 'SVA' then 'SVA'
            when (tmp_produtos_saleforce.family is null and upper(tmp_produtos_saleforce.sub_familia__c) = 'PRODUTO' and upper(sysadm_ps_prod_item.prod_brand) = 'VRGPA') then 'GPA'
            when tmp_produtos_saleforce.family is not null and upper(tmp_produtos_saleforce.sub_familia__c) = 'PRODUTO' then tmp_produtos_saleforce.family else null 
        end as dsc_familia_produto,
       cast(tmp_produtos_saleforce.isactive as string) as dsc_situacao_ativo,
       sncoreon_snapl.flag_multisaldo as flg_multisaldo
  from prod.silver_vr_crm_peoplesoft.sysadm_ps_prod_item
  join prod.silver_vr_crm_peoplesoft.sysadm_ps_prod_attr
            on sysadm_ps_prod_item.setid = sysadm_ps_prod_attr.setid
           and sysadm_ps_prod_item.product_id = sysadm_ps_prod_attr.product_id
           and (sysadm_ps_prod_attr.attribute_id = 'SN-IDACORDO' or (sysadm_ps_prod_attr.attribute_id = 'SVA-PROD-FLG' and sysadm_ps_prod_attr.attr_item_id = 'Y'))
  join prod.silver_vr_crm_peoplesoft.sysadm_ps_prod_attr as sysadm_ps_prod_attr_sigla
            on sysadm_ps_prod_item.setid = sysadm_ps_prod_attr_sigla.setid
           and sysadm_ps_prod_item.product_id = sysadm_ps_prod_attr_sigla.product_id
           and sysadm_ps_prod_attr_sigla.attribute_id = 'SN-SIGLA'
  left join tmp_produtos_saleforce
            on sysadm_ps_prod_attr_sigla.attribute_value = tmp_produtos_saleforce.productcode
  left join prod.silver_vr_autorizador.sncoreon_snapl
            on sysadm_ps_prod_attr.attribute_value = sncoreon_snapl.aplcod
where sysadm_ps_prod_item.prod_type = 'SVC'
      and ((sysadm_ps_prod_item.setid = 'SNFS1' and sysadm_ps_prod_item.prod_brand = 'VRPAT') or 
           (sysadm_ps_prod_item.setid = 'VRGPA' and sysadm_ps_prod_item.prod_brand = 'VRGPA') or 
           (sysadm_ps_prod_item.product_id like '%TRANSPORTE%') or 
           (sysadm_ps_prod_item.product_id like '%ADICIONAL%') or 
           (sysadm_ps_prod_item.product_id like '%NATAL%') or 
           (sysadm_ps_prod_item.product_id like '%SAUDE%') or 
           (sysadm_ps_prod_item.setid = 'VRPAT' and ((sysadm_ps_prod_item.product_id like '%CESTA%') or 
                                                     (sysadm_ps_prod_item.product_id like '%COMPRAS%') or 
                                                     (sysadm_ps_prod_item.product_id like '%SVA%') or 
                                                     (sysadm_ps_prod_item.product_id like '%AXA%') or 
                                                     (sysadm_ps_prod_item.product_id like '%AXR%') or 
                                                     (sysadm_ps_prod_item.product_id like '%MULT%') or 
                                                     (sysadm_ps_prod_item.product_id like '%FLEX%') or 
                                                     (sysadm_ps_prod_item.product_id like '%MSALDO%') or 
                                                     (sysadm_ps_prod_item.product_id like '%MSALFLEX%') or 
                                                     (sysadm_ps_prod_item.product_id like '%HOMMULT%') or 
                                                     (sysadm_ps_prod_item.product_id like '%MSALPAT%') or 
                                                     (sysadm_ps_prod_item.product_id like '%MSALTO%') or 
                                                     (sysadm_ps_prod_item.product_id like '%HCM%') or 
                                                     (sysadm_ps_prod_item.product_id like '%AUB-AUX-MOBI-SRV%')
                                                    )
           )
          )
      and sysadm_ps_prod_item.product_id not like 'TST%'
      and (sysadm_ps_prod_item.setid || '-' || sysadm_ps_prod_item.product_id) <> 'SNFS1-COM-COMPRAS-SRV'
      and (sysadm_ps_prod_item.setid || '-' || sysadm_ps_prod_item.product_id) <> 'SNFS1-033-CESTAVR-I-SRV'
      and (sysadm_ps_prod_item.setid || '-' || sysadm_ps_prod_item.product_id) <> 'SNFS1-034-MAMAEVR-I-SRV'
 order by sysadm_ps_prod_item.product_id


-- COMMAND ----------

-- DBTITLE 1,tmp_produto_vrb
create or replace temporary view tmp_produto_vrb as
 
Select tmp_produto.cod_sdprod,
       tmp_produto.cod_produto,
       tmp_produto.dsc_produto,
       tmp_produto.dsc_sigla_produto,
       tmp_produto.cod_emissor,
       tmp_produto.cod_aplicacao,
       tmp_produto.cod_identificador_salesforce,
       tmp_produto_pai.cod_sdprod as cod_sdprod_pai,
       tmp_produto_pai.cod_produto as cod_produto_pai,
       tmp_produto.dsc_familia_produto,
       tmp_produto.dsc_situacao_ativo,
       tmp_produto.flg_multisaldo,
       case when (tmp_produto.cod_emissor = 'VRPAT' and upper(tmp_produto.dsc_sigla_produto) in ('VBA','VRO','VCA','RAD','VAM','VRM', 'ALP', 'REP'))
       then 'PAT'
       when (tmp_produto.cod_emissor = 'VRPAT' and upper(tmp_produto.dsc_sigla_produto) not in ('VBA','VRO','VCA','RAD','VAM','VRM', 'ALP', 'REP'))
       then 'AUXILIO'
       else null end as dsc_tipo_beneficio
 from tmp_produto
 left join tmp_produto as tmp_produto_pai
           on tmp_produto.cod_aplicacao_pai = tmp_produto_pai.cod_aplicacao
 left join prod.silver_vr_crm_peoplesoft.sysadm_ps_prod_attr
           on tmp_produto.cod_sdprod = sysadm_ps_prod_attr.setid
          and tmp_produto.cod_produto = sysadm_ps_prod_attr.product_id
          and sysadm_ps_prod_attr.attribute_id in ('SN-PRD-PAT')
;

-- COMMAND ----------

-- DBTITLE 1,tmp_produto_super_crm
create or replace temporary view tmp_produto_super_crm as
 
select dbo_product2.Emissor__c               as cod_sdprod,
       dbo_product2.ProductCode              as cod_produto,
       dbo_product2.name                     as dsc_produto,
       dbo_product2.ProductCode              as dsc_sigla_produto,
       dbo_product2.Emissor__c               as cod_emissor,
       dbo_product2.IdAplicacao__c           as cod_aplicacao,
       dbo_product2.id                       as cod_identificador_super_crm,
       dbo_product2.family                   as dsc_familia_produto,
       cast(dbo_product2.IsActive as string) as dsc_situacao_ativo,
       case when (dbo_product2.Emissor__c = 'VRPAT' and upper(dbo_product2.ProductCode) in ('VBA','VRO','VCA','RAD','VAM','VRM', 'ALP', 'REP'))    
                 then 'PAT'
            when (dbo_product2.Emissor__c = 'VRPAT' and upper(dbo_product2.ProductCode) not in ('VBA','VRO','VCA','RAD','VAM','VRM', 'ALP', 'REP'))
                 then 'AUXILIO'
                 else null
        end as dsc_tipo_beneficio,
        TipoProduto__c as dsc_tipo_produto,
        BU__c as dsc_unidade_negocio
  from prod.silver_vr_salesforce_super_crm.dbo_product2

-- COMMAND ----------

-- DBTITLE 1,tmp_produto_final_super_crm
create or replace temporary view tmp_produto_final_super_crm as

select distinct coalesce(tmp_produto_vrb.cod_sdprod, tmp_produto_super_crm.cod_sdprod) as cod_sdprod, 
       coalesce(tmp_produto_vrb.cod_produto, tmp_produto_super_crm.cod_produto) as cod_produto, 
       case
         when coalesce(tmp_produto_vrb.cod_produto, tmp_produto_super_crm.cod_produto) = 'VBA-ALIMENT-SRV' 
              or coalesce(tmp_produto_vrb.cod_produto, tmp_produto_super_crm.cod_produto) = 'VRO-REFEICAO-SRV' 
         then tmp_produto_super_crm.dsc_produto 
         else coalesce(tmp_produto_vrb.dsc_produto, tmp_produto_super_crm.dsc_produto) 
       end as dsc_produto,
       coalesce(tmp_produto_vrb.dsc_sigla_produto, tmp_produto_super_crm.dsc_sigla_produto) as dsc_sigla_produto, 
       tmp_produto_vrb.flg_multisaldo, 
       coalesce(tmp_produto_vrb.cod_emissor, tmp_produto_super_crm.cod_emissor) as cod_emissor, 
       coalesce(tmp_produto_vrb.cod_aplicacao, tmp_produto_super_crm.cod_aplicacao) as cod_aplicacao, 
       tmp_produto_vrb.cod_identificador_salesforce,
       tmp_produto_super_crm.cod_identificador_super_crm,
       coalesce(tmp_produto_vrb.dsc_tipo_beneficio, tmp_produto_super_crm.dsc_tipo_beneficio) as dsc_tipo_beneficio, 
       tmp_produto_vrb.cod_sdprod_pai,
       tmp_produto_vrb.cod_produto_pai,
       coalesce(tmp_produto_super_crm.dsc_familia_produto, tmp_produto_vrb.dsc_familia_produto) as dsc_familia_produto, 
       coalesce(tmp_produto_super_crm.dsc_situacao_ativo, tmp_produto_vrb.dsc_situacao_ativo) as dsc_situacao_ativo, 
       tmp_produto_super_crm.dsc_tipo_produto,
       tmp_produto_super_crm.dsc_unidade_negocio
  from tmp_produto_vrb
  full outer join tmp_produto_super_crm
    on tmp_produto_vrb.cod_aplicacao = tmp_produto_super_crm.cod_aplicacao


-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>VR Gente</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_produto_vrg
create or replace temporary view tmp_produto_vrg as

Select 'VRG' as cod_sdprod,
       'BI-VR-GENTE' as cod_produto,
       'VR Gente' as dsc_produto,
       'VRG' as cod_emissor
union
Select 'VRG' as cod_sdprod,
       'BI-VR-GENTE-HCM' as cod_produto,
       'VR Gente HCM' as dsc_produto,
       'VRG' as cod_emissor
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1>Consolidação</h1>

-- COMMAND ----------

-- DBTITLE 1,tmp_produto_final
create or replace temporary view tmp_produto_final as

with tmp_produto_consolidado as (
  Select tmp_produto_final_super_crm.cod_sdprod,
         tmp_produto_final_super_crm.cod_produto,
         tmp_produto_final_super_crm.dsc_produto,
         tmp_produto_final_super_crm.dsc_sigla_produto,
         tmp_produto_final_super_crm.cod_emissor,
         tmp_produto_final_super_crm.cod_aplicacao,
         tmp_produto_final_super_crm.cod_identificador_salesforce,
         tmp_produto_final_super_crm.cod_identificador_super_crm,
         tmp_produto_final_super_crm.cod_sdprod_pai,
         tmp_produto_final_super_crm.cod_produto_pai,
         tmp_produto_final_super_crm.dsc_familia_produto,
         tmp_produto_final_super_crm.dsc_situacao_ativo,
         tmp_produto_final_super_crm.flg_multisaldo,
         tmp_produto_final_super_crm.dsc_tipo_beneficio,
         tmp_produto_final_super_crm.dsc_tipo_produto,
         tmp_produto_final_super_crm.dsc_unidade_negocio
    from tmp_produto_final_super_crm
union
  Select tmp_produto_vrg.cod_sdprod,
         tmp_produto_vrg.cod_produto,
         tmp_produto_vrg.dsc_produto,
         null as dsc_sigla_produto,
         tmp_produto_vrg.cod_emissor,
         null as cod_aplicacao,
         null as cod_identificador_salesforce,
         null as cod_identificador_super_crm,
         null as cod_sdprod_pai,
         null as cod_produto_pai,
         null as dsc_familia_produto,
         null as dsc_situacao_ativo,
         null as flg_multisaldo,
         null as dsc_tipo_beneficio,
         null as dsc_tipo_produto,
         null as dsc_unidade_negocio
    from tmp_produto_vrg
)

Select tmp_produto_consolidado.cod_sdprod,
       tmp_produto_consolidado.cod_produto,
       tmp_produto_consolidado.dsc_produto,
       tmp_produto_consolidado.dsc_sigla_produto,
       tmp_produto_consolidado.flg_multisaldo,
       tmp_produto_consolidado.cod_emissor,
       tmp_produto_consolidado.cod_aplicacao,
       tmp_produto_consolidado.cod_identificador_salesforce,
       tmp_produto_consolidado.cod_identificador_super_crm,
       tmp_produto_consolidado.cod_sdprod_pai,
       tmp_produto_consolidado.cod_produto_pai,
       tmp_produto_consolidado.dsc_familia_produto,
       tmp_produto_consolidado.dsc_situacao_ativo,
       tmp_produto_consolidado.dsc_tipo_beneficio,
       tmp_produto_consolidado.dsc_tipo_produto,
       tmp_produto_consolidado.dsc_unidade_negocio,
       md5(upper(
           nvl(tmp_produto_consolidado.cod_sdprod,'') ||
           nvl(tmp_produto_consolidado.cod_produto,'') ||
           nvl(tmp_produto_consolidado.dsc_produto,'') ||
           nvl(tmp_produto_consolidado.dsc_sigla_produto,'') ||
           nvl(tmp_produto_consolidado.flg_multisaldo,'')||
           nvl(tmp_produto_consolidado.cod_emissor,'') ||
           nvl(tmp_produto_consolidado.cod_aplicacao,'') ||
           nvl(tmp_produto_consolidado.cod_identificador_salesforce,'') ||
           nvl(tmp_produto_consolidado.cod_identificador_super_crm,'') ||
           nvl(tmp_produto_consolidado.cod_sdprod_pai,'') ||
           nvl(tmp_produto_consolidado.cod_produto_pai,'') ||
           nvl(tmp_produto_consolidado.dsc_familia_produto, '') ||
           nvl(tmp_produto_consolidado.dsc_situacao_ativo, '') ||
           nvl(tmp_produto_consolidado.dsc_tipo_beneficio, '') ||
           nvl(tmp_produto_consolidado.dsc_tipo_produto, '') ||
           nvl(tmp_produto_consolidado.dsc_unidade_negocio, '')
       )) as cod_hash_md5
 from tmp_produto_consolidado


-- COMMAND ----------

-- DBTITLE 1,Padroniza campos
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select cod_sdprod,
-- MAGIC        cod_produto,
-- MAGIC        dsc_produto,
-- MAGIC        dsc_sigla_produto,
-- MAGIC        flg_multisaldo,
-- MAGIC        cod_emissor,
-- MAGIC        cod_aplicacao,
-- MAGIC        cod_identificador_salesforce,
-- MAGIC        cod_identificador_super_crm,
-- MAGIC        cod_sdprod_pai,
-- MAGIC        cod_produto_pai,
-- MAGIC        dsc_familia_produto,
-- MAGIC        dsc_situacao_ativo,
-- MAGIC        dsc_tipo_beneficio,
-- MAGIC        dsc_tipo_produto,
-- MAGIC        dsc_unidade_negocio,
-- MAGIC        cod_hash_md5
-- MAGIC   from tmp_produto_final
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_produto_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Merge dim_produto
-- MAGIC %python
-- MAGIC
-- MAGIC df_merge = fn_merge_direto("""
-- MAGIC MERGE INTO gold_vr_cliente_rh_pii.dim_produto as destino
-- MAGIC USING tmp_produto_criptografado as origem
-- MAGIC    ON destino.cod_sdprod = origem.cod_sdprod
-- MAGIC   AND destino.cod_produto = origem.cod_produto
-- MAGIC  WHEN MATCHED AND nvl(origem.cod_hash_md5,'') <> nvl(destino.cod_hash_md5,'') THEN UPDATE SET
-- MAGIC       destino.dsc_produto = origem.dsc_produto,
-- MAGIC       destino.dsc_sigla_produto = origem.dsc_sigla_produto,
-- MAGIC       destino.flg_multisaldo = origem.flg_multisaldo,
-- MAGIC       destino.cod_emissor = origem.cod_emissor,
-- MAGIC       destino.cod_aplicacao = origem.cod_aplicacao,
-- MAGIC       destino.cod_identificador_salesforce = origem.cod_identificador_salesforce,
-- MAGIC       destino.cod_identificador_super_crm = origem.cod_identificador_super_crm,
-- MAGIC       destino.dsc_tipo_beneficio = origem.dsc_tipo_beneficio,
-- MAGIC       destino.dsc_familia_produto = origem.dsc_familia_produto,
-- MAGIC       destino.dsc_situacao_ativo = origem.dsc_situacao_ativo,
-- MAGIC       destino.cod_sdprod_pai = origem.cod_sdprod_pai,
-- MAGIC       destino.cod_produto_pai = origem.cod_produto_pai,
-- MAGIC       destino.dsc_tipo_produto = origem.dsc_tipo_produto,
-- MAGIC       destino.dsc_unidade_negocio = origem.dsc_unidade_negocio,
-- MAGIC       destino.cod_hash_md5 = origem.cod_hash_md5,
-- MAGIC       destino.dat_alteracao_bi = current_timestamp()
-- MAGIC  WHEN NOT MATCHED THEN INSERT (
-- MAGIC       destino.cod_sdprod,
-- MAGIC       destino.cod_produto,
-- MAGIC       destino.dsc_produto,
-- MAGIC       destino.dsc_sigla_produto,
-- MAGIC       destino.flg_multisaldo,
-- MAGIC       destino.cod_emissor,
-- MAGIC       destino.cod_aplicacao,
-- MAGIC       destino.cod_identificador_salesforce,
-- MAGIC       destino.cod_identificador_super_crm,
-- MAGIC       destino.dsc_tipo_beneficio,
-- MAGIC       destino.cod_sdprod_pai,
-- MAGIC       destino.cod_produto_pai,
-- MAGIC       destino.dsc_familia_produto,
-- MAGIC       destino.dsc_situacao_ativo,
-- MAGIC       destino.dsc_tipo_produto,
-- MAGIC       destino.dsc_unidade_negocio,
-- MAGIC       destino.cod_hash_md5,
-- MAGIC       destino.flg_ativo,
-- MAGIC       destino.dat_inclusao_bi,
-- MAGIC       destino.dat_alteracao_bi
-- MAGIC     ) VALUES (
-- MAGIC       origem.cod_sdprod,
-- MAGIC       origem.cod_produto,
-- MAGIC       origem.dsc_produto,
-- MAGIC       origem.dsc_sigla_produto,
-- MAGIC       origem.flg_multisaldo,
-- MAGIC       origem.cod_emissor,
-- MAGIC       origem.cod_aplicacao,
-- MAGIC       origem.cod_identificador_salesforce,
-- MAGIC       origem.cod_identificador_super_crm,
-- MAGIC       origem.dsc_tipo_beneficio,
-- MAGIC       origem.cod_sdprod_pai,
-- MAGIC       origem.cod_produto_pai,
-- MAGIC       origem.dsc_familia_produto,
-- MAGIC       origem.dsc_situacao_ativo,
-- MAGIC       origem.dsc_tipo_produto,
-- MAGIC       origem.dsc_unidade_negocio,
-- MAGIC       origem.cod_hash_md5,
-- MAGIC       1,
-- MAGIC       current_timestamp(),
-- MAGIC       current_timestamp()
-- MAGIC )""")

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
