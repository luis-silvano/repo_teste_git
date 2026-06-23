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
-- MAGIC # database = gold_vr_cliente_ec
-- MAGIC # databricks_catalog (DEV) = dev
-- MAGIC # databricks_catalog (PRD) = prod
-- MAGIC # dsc_tabela_pii = fat_voucher
-- MAGIC # location_catalogo_temp (DEV) = abfss://temp@adlsvrbeneficiosdev01.dfs.core.windows.net
-- MAGIC # location_catalogo_temp (PRD) = abfss://temp@adlsvrbeneficiosprd.dfs.core.windows.net
-- MAGIC # malha = temp
-- MAGIC # tablename = fat_voucher

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC - Arquivos necessários para passagem de bastão:
-- MAGIC - silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load
-- MAGIC - silver_vr_erp_peoplesoft.sysadm_ps_voucher
-- MAGIC - silver_jvcef_erp_peoplesoft.sysadm_ps_voucher
-- MAGIC - silver_vr_erp_peoplesoft.sysadm_ps_distrib_line
-- MAGIC - silver_vr_kenan.arbor_smt_extrato_fatura_tbl
-- MAGIC - silver_vr_kenan.arbor_cmf
-- MAGIC - silver_vr_kenan.arbor_payment_profile
-- MAGIC - silver_vr_erp_peoplesoft.sysadm_ps_gvr_vchr_relac
-- MAGIC - silver_vr_kenan.arbor_payment_trans
-- MAGIC - gold_vr_cliente_rh.dim_produto
-- MAGIC - silver_vr_erp_peoplesoft.sysadm_ps_ke_xref_sn
-- MAGIC - silver_vr_kenan.arbor_smt_voucher_reembolso

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
-- MAGIC - Na query abaixo foram incluídos alguns filtros para que sejam obtidas as informações de maneira precisa.
-- MAGIC - São eles:
-- MAGIC   - Account Category: 101 = GPA Mamãe;
-- MAGIC   - 63 = Alimentação;
-- MAGIC   - 65 = Refeição;
-- MAGIC   - 64 = Auto;
-- MAGIC   - 66 = Cultura.
-- MAGIC - Prep_Status: 1 significa que a Guia é efetiva, não proforma (rascunho).
-- MAGIC - Backout_Status: 0 significa que a guia não sofreu um rollback, ou seja, retorna sempre a mais atual.

-- COMMAND ----------

-- DBTITLE 1,tmp_extrato_fatura
create or replace temporary view tmp_extrato_fatura as

select arbor_smt_extrato_fatura_tbl.bill_ref_no,
       arbor_smt_extrato_fatura_tbl.account_no,
       arbor_smt_extrato_fatura_tbl.bill_ref_resets,
       string(LPAD(string(int(arbor_payment_trans.tracking_id_serv)), 2, 0) ||
              LPAD(string(int(arbor_payment_trans.tracking_id)), 10, 0) ||
              LPAD(string(int(arbor_payment_trans.counter)), 3, 0)) as vr_ke_trans_id
  from prod.silver_vr_kenan.arbor_smt_extrato_fatura_tbl
  left join prod.silver_vr_kenan.arbor_payment_trans
    on arbor_smt_extrato_fatura_tbl.bill_ref_no = arbor_payment_trans.bill_ref_no
   and arbor_smt_extrato_fatura_tbl.bill_ref_resets = arbor_payment_trans.bill_ref_resets
   and arbor_smt_extrato_fatura_tbl.account_category in (63, 64, 65, 66, 67, 127, 131)
   and arbor_smt_extrato_fatura_tbl.prep_status = 1
   and arbor_smt_extrato_fatura_tbl.backout_status = 0

 union

select arbor_smt_extrato_fatura_tbl.bill_ref_no,
       arbor_smt_extrato_fatura_tbl.account_no,
       arbor_smt_extrato_fatura_tbl.bill_ref_resets,
       string(LPAD(string(int(arbor_payment_trans.tracking_id_serv)), 2, 0) ||
              LPAD(string(int(arbor_payment_trans.tracking_id)), 10, 0) ||
              LPAD(string(int(arbor_payment_trans.counter)), 3, 0)) as vr_ke_trans_id
  from prod.silver_vr_kenan.arbor_smt_extrato_fatura_tbl
  left join prod.silver_vr_kenan.arbor_payment_trans
    on arbor_smt_extrato_fatura_tbl.bill_ref_no = arbor_payment_trans.bill_ref_no
   and arbor_smt_extrato_fatura_tbl.bill_ref_resets = arbor_payment_trans.bill_ref_resets
 inner join prod.silver_vr_kenan.arbor_cmf
    on arbor_smt_extrato_fatura_tbl.account_no = arbor_cmf.account_no
   and arbor_cmf.account_category = 101
   and arbor_smt_extrato_fatura_tbl.account_category in (63, 64, 65, 66, 67, 127, 131)
   and arbor_smt_extrato_fatura_tbl.prep_status = 1
   and arbor_smt_extrato_fatura_tbl.backout_status = 0
 inner join prod.silver_vr_kenan.arbor_payment_profile
    on arbor_cmf.payment_profile_id = arbor_payment_profile.profile_id
   and arbor_payment_profile.clearing_house_id = 'VRP'



-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Método para encontrar a descrição de uma Categoria de Conta (account_category):
-- MAGIC - select * from silver_vr_kenan.arbor_smt_charges_knn where charge_type = 'COD_APLICACAO' and account_category = 101

-- COMMAND ----------

-- DBTITLE 1,tmp_antecipacao
create or replace temporary view tmp_antecipacao as

with tmp_antecipacao_parcial (
  select sysadm_ps_gvr_vchr_relac.business_unit,
         sysadm_ps_gvr_vchr_relac.voucher_id,
         sysadm_ps_gvr_vchr_relac.business_unit2,
         sysadm_ps_gvr_vchr_relac.voucher_id2,
         sysadm_ps_gvr_vchr_relac.gvr_tipo_vchr
    from prod.silver_vr_erp_peoplesoft.sysadm_ps_gvr_vchr_relac 
   where sysadm_ps_gvr_vchr_relac.gvr_tipo_vchr = 'ANT' -- Antecipação
),

tmp_compras_parcial (
  select sysadm_ps_gvr_vchr_relac.business_unit,
         sysadm_ps_gvr_vchr_relac.voucher_id,
         sysadm_ps_gvr_vchr_relac.business_unit2,
         sysadm_ps_gvr_vchr_relac.voucher_id2,
         sysadm_ps_gvr_vchr_relac.gvr_tipo_vchr
    from prod.silver_vr_erp_peoplesoft.sysadm_ps_gvr_vchr_relac
   where sysadm_ps_gvr_vchr_relac.gvr_tipo_vchr = 'CMP' -- Compras
),

tmp_antecipacao_compras_parcial (
  select tmp_antecipacao_parcial.business_unit,
         tmp_antecipacao_parcial.voucher_id,
         tmp_antecipacao_parcial.business_unit2 as cod_unidade_negocio_antecipacao,
         tmp_antecipacao_parcial.voucher_id2 as num_voucher_antecipacao,
         tmp_compras_parcial.business_unit2 as cod_unidade_negocio_compras,
         tmp_compras_parcial.voucher_id2 as num_voucher_compras
    from tmp_antecipacao_parcial
    left join tmp_compras_parcial
      on tmp_antecipacao_parcial.business_unit = tmp_compras_parcial.business_unit
     and tmp_antecipacao_parcial.voucher_id = tmp_compras_parcial.voucher_id
)

select sysadm_ps_vr_voucher_load.voucher_id as num_voucher,
       sysadm_ps_vr_voucher_load.business_unit as cod_unidade_negocio,
       sysadm_ps_vr_voucher_load.gvr_status_antvchr as cod_tipo_antecipacao,
       voucher_vr_jvcef.accounting_dt as dat_contabil_normal,
       voucher_vr_jvcef_antecipacao.accounting_dt as dat_contabil_antecipacao,
       vchr_relac.cod_unidade_negocio_antecipacao,
       vchr_relac.num_voucher_antecipacao,
       vchr_relac.cod_unidade_negocio_compras,
       vchr_relac.num_voucher_compras,
       sysadm_ps_vr_voucher_load.vr_ke_trans_id
  from prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load
  left join 
            (select voucher_id, business_unit, accounting_dt from prod.silver_vr_erp_peoplesoft.sysadm_ps_voucher sysadm_ps_voucher 
              union 
             select voucher_id, business_unit, accounting_dt from prod.silver_jvcef_erp_peoplesoft.sysadm_ps_voucher sysadm_ps_voucher
            ) voucher_vr_jvcef
    on sysadm_ps_vr_voucher_load.business_unit = voucher_vr_jvcef.business_unit
   and sysadm_ps_vr_voucher_load.voucher_id = voucher_vr_jvcef.voucher_id
  left join tmp_antecipacao_compras_parcial vchr_relac 
    on voucher_vr_jvcef.business_unit = vchr_relac.business_unit
   and voucher_vr_jvcef.voucher_id = vchr_relac.voucher_id
  left join 
            (select voucher_id, business_unit, accounting_dt from prod.silver_vr_erp_peoplesoft.sysadm_ps_voucher sysadm_ps_voucher 
              union 
             select voucher_id, business_unit, accounting_dt from prod.silver_jvcef_erp_peoplesoft.sysadm_ps_voucher sysadm_ps_voucher
            ) voucher_vr_jvcef_antecipacao
    on vchr_relac.cod_unidade_negocio_antecipacao = voucher_vr_jvcef_antecipacao.business_unit
   and vchr_relac.num_voucher_antecipacao = voucher_vr_jvcef_antecipacao.voucher_id 
 where exists (select 1
                 from tmp_extrato_fatura
                where tmp_extrato_fatura.vr_ke_trans_id = sysadm_ps_vr_voucher_load.vr_ke_trans_id)

-- COMMAND ----------

-- DBTITLE 1,tmp_pagamento
create or replace temporary view tmp_pagamento as

select sysadm_ps_distrib_line.voucher_id as num_voucher,
       sysadm_ps_distrib_line.accounting_dt as dat_pagamento_contabil,
       sum(sysadm_ps_distrib_line.monetary_amount) as val_pagamento
  from prod.silver_vr_erp_peoplesoft.sysadm_ps_distrib_line
 inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load
    on sysadm_ps_distrib_line.voucher_id = sysadm_ps_vr_voucher_load.voucher_id
    left join (select distinct cod_aplicacao from gold_vr_cliente_rh.dim_produto where dim_produto.flg_ativo = 1) as dim_produto
    on sysadm_ps_vr_voucher_load.gvr_produto = dim_produto.cod_aplicacao
 group by sysadm_ps_distrib_line.voucher_id,
          sysadm_ps_distrib_line.accounting_dt

-- COMMAND ----------

-- DBTITLE 1,tmp_voucher
create or replace temporary view tmp_voucher as

select distinct sysadm_ps_vr_voucher_load.voucher_id as num_voucher,
       case sysadm_ps_vr_voucher_load.ke_bill_ref_no_sn
            when 0 then tmp_extrato_fatura.bill_ref_no
            else sysadm_ps_vr_voucher_load.ke_bill_ref_no_sn end as num_guia,
       case sysadm_ps_vr_voucher_load.ke_bill_ref_no_sn
            when 0 then tmp_extrato_fatura.bill_ref_resets
            else sysadm_ps_vr_voucher_load.ke_bill_rf_rst_sn end as num_alteracoes_guia,
       dim_produto.cod_sdprod as cod_sdprod,
       dim_produto.cod_produto as cod_produto,
       sysadm_ps_vr_voucher_load.business_unit as cod_unidade_negocio,
       sysadm_ps_vr_voucher_load.invoice_id as num_sequencial_erp,
       tmp_pagamento.dat_pagamento_contabil as dat_pagamento_contabil,
       nvl(tmp_antecipacao.dat_contabil_antecipacao, tmp_antecipacao.dat_contabil_normal) as dat_primeiro_pagamento,
       tmp_antecipacao.cod_tipo_antecipacao as cod_tipo_antecipacao,
       case tmp_antecipacao.cod_tipo_antecipacao
         when 0 then 'Não Antecipado'
         when 1 then 'Eventual'
         when 2 then 'Automática'
         else 'NA'
       end as dsc_tipo_antecipacao,
       tmp_antecipacao.cod_unidade_negocio_antecipacao,
       tmp_antecipacao.num_voucher_antecipacao,
       tmp_antecipacao.cod_unidade_negocio_compras,
       tmp_antecipacao.num_voucher_compras,   
       sysadm_ps_ke_xref_sn.nf_brl_id as num_nota_fiscal,
       nvl(tmp_pagamento.val_pagamento, 0) as val_pagamento,
       cast(sysadm_ps_vr_voucher_load.gvr_valor_tarifa as decimal(10, 2)) as val_tarifa,
       case sysadm_ps_vr_voucher_load.gvr_status_antvchr
         when 2 then cast(sysadm_ps_vr_voucher_load.gvr_valor_tarifa as decimal(10, 2))
         else 0
       end as val_tarifa_adiantamento_automatico,
       case sysadm_ps_vr_voucher_load.gvr_status_antvchr
         when 1 then cast(sysadm_ps_vr_voucher_load.gvr_valor_tarifa as decimal(10, 2))
         else 0
       end as val_tarifa_adiantamento_eventual,
       sysadm_ps_vr_voucher_load.vr_ke_trans_id,
       arbor_smt_voucher_reembolso.aceita_pgto_avulso as dsc_pagamento_avulso
  from prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load
  left join tmp_pagamento
    on sysadm_ps_vr_voucher_load.voucher_id = tmp_pagamento.num_voucher
  left join gold_vr_cliente_rh.dim_produto
    on sysadm_ps_vr_voucher_load.gvr_produto = dim_produto.cod_aplicacao
  left join tmp_antecipacao
    on sysadm_ps_vr_voucher_load.voucher_id = tmp_antecipacao.num_voucher
   and sysadm_ps_vr_voucher_load.business_unit = tmp_antecipacao.cod_unidade_negocio
  left join prod.silver_vr_erp_peoplesoft.sysadm_ps_ke_xref_sn 
    on sysadm_ps_vr_voucher_load.ke_bill_ref_no_sn = sysadm_ps_ke_xref_sn.ke_bill_ref_no_sn
   and sysadm_ps_vr_voucher_load.ke_bill_rf_rst_sn = sysadm_ps_ke_xref_sn.ke_bill_rf_rst_sn
  left join tmp_extrato_fatura
    on sysadm_ps_vr_voucher_load.vr_ke_trans_id = tmp_extrato_fatura.vr_ke_trans_id
  left join prod.silver_vr_kenan.arbor_smt_voucher_reembolso
    on tmp_extrato_fatura.account_no = arbor_smt_voucher_reembolso.account_no
   and tmp_extrato_fatura.bill_ref_no = arbor_smt_voucher_reembolso.bill_ref_no


-- COMMAND ----------

-- DBTITLE 1,tmp_md5_voucher
create or replace temporary view tmp_md5_voucher as
 SELECT tmp_voucher.*,
       trunc(tmp_voucher.dat_primeiro_pagamento, 'MM') as dat_referencia,
        MD5(UPPER(
          nvl(tmp_voucher.num_voucher                        ,'')||
          nvl(tmp_voucher.num_guia                           ,'')||
          nvl(tmp_voucher.num_alteracoes_guia                ,'')||
          nvl(tmp_voucher.cod_sdprod                         ,'')||
          nvl(tmp_voucher.cod_produto                        ,'')||
          nvl(tmp_voucher.cod_unidade_negocio                ,'')||
          nvl(tmp_voucher.num_sequencial_erp                 ,'')||
          nvl(tmp_voucher.dat_pagamento_contabil             ,'')||
          nvl(tmp_voucher.dat_primeiro_pagamento             ,'')||
          nvl(tmp_voucher.cod_tipo_antecipacao               ,'')||
          nvl(tmp_voucher.dsc_tipo_antecipacao               ,'')||
          nvl(tmp_voucher.cod_unidade_negocio_antecipacao    ,'')||
          nvl(tmp_voucher.num_voucher_antecipacao            ,'')||
          nvl(tmp_voucher.cod_unidade_negocio_compras        ,'')||
          nvl(tmp_voucher.num_voucher_compras                ,'')||
          nvl(tmp_voucher.num_nota_fiscal                    ,'')||
          nvl(tmp_voucher.val_pagamento                      ,'')||
          nvl(tmp_voucher.val_tarifa                         ,'')||
          nvl(tmp_voucher.val_tarifa_adiantamento_automatico ,'')||
          nvl(tmp_voucher.val_tarifa_adiantamento_eventual   ,'')||
          nvl(tmp_voucher.dsc_pagamento_avulso        ,'') 
        )) as cod_hash_md5
   from tmp_voucher
  where trunc(tmp_voucher.dat_primeiro_pagamento, 'MM') >= add_months(trunc(cast('${dat_parametro}' as date) -1, 'MM'), -1) 
;

-- COMMAND ----------

-- DBTITLE 1,Padroniza campos
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select num_voucher,
-- MAGIC        num_guia,
-- MAGIC        num_alteracoes_guia,
-- MAGIC        cod_sdprod,
-- MAGIC        cod_produto,
-- MAGIC        cod_unidade_negocio,
-- MAGIC        num_sequencial_erp,
-- MAGIC        dat_pagamento_contabil,
-- MAGIC        dat_primeiro_pagamento,
-- MAGIC        cod_tipo_antecipacao,
-- MAGIC        dsc_tipo_antecipacao,
-- MAGIC        cod_unidade_negocio_antecipacao,
-- MAGIC        num_voucher_antecipacao,
-- MAGIC        cod_unidade_negocio_compras,
-- MAGIC        num_voucher_compras,
-- MAGIC        num_nota_fiscal,
-- MAGIC        val_pagamento,
-- MAGIC        val_tarifa,
-- MAGIC        val_tarifa_adiantamento_automatico,
-- MAGIC        val_tarifa_adiantamento_eventual,
-- MAGIC        vr_ke_trans_id,
-- MAGIC        dsc_pagamento_avulso,
-- MAGIC        dat_referencia,
-- MAGIC        cod_hash_md5
-- MAGIC   from tmp_md5_voucher
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_voucher_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Delete fat_voucher
DELETE FROM gold_vr_cliente_ec_pii.fat_voucher where dat_referencia >= add_months(trunc(cast('${dat_parametro}' as date) -1, 'MM'), -1);

-- COMMAND ----------

-- DBTITLE 1,Merge fat_voucher
-- MAGIC %python
-- MAGIC df_merge = fn_merge_direto(""" 
-- MAGIC     MERGE INTO gold_vr_cliente_ec_pii.fat_voucher as destino
-- MAGIC     USING tmp_voucher_criptografado as origem
-- MAGIC        ON destino.num_voucher         = origem.num_voucher
-- MAGIC       and destino.cod_sdprod          = origem.cod_sdprod
-- MAGIC       AND destino.cod_unidade_negocio = origem.cod_unidade_negocio
-- MAGIC       and destino.dat_referencia >= add_months(trunc(cast('""" + dat_parametro + """' as date) -1, 'MM'), -1)
-- MAGIC      WHEN MATCHED AND nvl(origem.cod_hash_md5,'') <> nvl(destino.cod_hash_md5,'') THEN UPDATE SET 
-- MAGIC           destino.dat_referencia                     = origem.dat_referencia                    ,
-- MAGIC           destino.num_guia                           = origem.num_guia                          ,
-- MAGIC           destino.num_alteracoes_guia                = origem.num_alteracoes_guia               ,
-- MAGIC           destino.cod_produto                        = origem.cod_produto                       ,
-- MAGIC           destino.num_sequencial_erp                 = origem.num_sequencial_erp                ,
-- MAGIC           destino.dat_pagamento_contabil             = origem.dat_pagamento_contabil            ,
-- MAGIC           destino.dat_primeiro_pagamento             = origem.dat_primeiro_pagamento            ,
-- MAGIC           destino.cod_tipo_antecipacao               = origem.cod_tipo_antecipacao              ,
-- MAGIC           destino.dsc_tipo_antecipacao               = origem.dsc_tipo_antecipacao              ,
-- MAGIC           destino.cod_unidade_negocio_antecipacao    = origem.cod_unidade_negocio_antecipacao   ,
-- MAGIC           destino.num_voucher_antecipacao            = origem.num_voucher_antecipacao           ,
-- MAGIC           destino.cod_unidade_negocio_compras        = origem.cod_unidade_negocio_compras       ,
-- MAGIC           destino.num_voucher_compras                = origem.num_voucher_compras               ,
-- MAGIC           destino.num_nota_fiscal                    = origem.num_nota_fiscal                   ,
-- MAGIC           destino.val_pagamento                      = origem.val_pagamento                     ,
-- MAGIC           destino.val_tarifa                         = origem.val_tarifa                        ,
-- MAGIC           destino.val_tarifa_adiantamento_automatico = origem.val_tarifa_adiantamento_automatico,
-- MAGIC           destino.val_tarifa_adiantamento_eventual   = origem.val_tarifa_adiantamento_eventual  ,
-- MAGIC           destino.dsc_pagamento_avulso               = origem.dsc_pagamento_avulso              ,
-- MAGIC           destino.cod_hash_md5                       = origem.cod_hash_md5                      ,
-- MAGIC           destino.dat_alteracao_bi                   = current_date()
-- MAGIC      WHEN NOT MATCHED THEN INSERT (
-- MAGIC           destino.dat_referencia                    ,
-- MAGIC           destino.num_voucher                       ,
-- MAGIC           destino.num_guia                          ,
-- MAGIC           destino.num_alteracoes_guia               ,
-- MAGIC           destino.cod_sdprod                        ,
-- MAGIC           destino.cod_produto                       ,
-- MAGIC           destino.cod_unidade_negocio               ,
-- MAGIC           destino.num_sequencial_erp                ,
-- MAGIC           destino.dat_pagamento_contabil            ,
-- MAGIC           destino.dat_primeiro_pagamento            ,
-- MAGIC           destino.cod_tipo_antecipacao              ,
-- MAGIC           destino.dsc_tipo_antecipacao              ,
-- MAGIC           destino.cod_unidade_negocio_antecipacao   ,
-- MAGIC           destino.num_voucher_antecipacao           ,
-- MAGIC           destino.cod_unidade_negocio_compras       ,
-- MAGIC           destino.num_voucher_compras               ,
-- MAGIC           destino.num_nota_fiscal                   ,
-- MAGIC           destino.val_pagamento                     ,
-- MAGIC           destino.val_tarifa                        ,
-- MAGIC           destino.val_tarifa_adiantamento_automatico,
-- MAGIC           destino.val_tarifa_adiantamento_eventual  ,
-- MAGIC           destino.dsc_pagamento_avulso              ,
-- MAGIC           destino.cod_hash_md5                      ,
-- MAGIC           destino.dat_inclusao_bi                   ,
-- MAGIC           destino.dat_alteracao_bi
-- MAGIC         ) VALUES (
-- MAGIC           origem.dat_referencia                     ,
-- MAGIC           origem.num_voucher                        ,
-- MAGIC           origem.num_guia                           ,
-- MAGIC           origem.num_alteracoes_guia                ,
-- MAGIC           origem.cod_sdprod                         ,
-- MAGIC           origem.cod_produto                        ,
-- MAGIC           origem.cod_unidade_negocio                ,
-- MAGIC           origem.num_sequencial_erp                 ,
-- MAGIC           origem.dat_pagamento_contabil             ,
-- MAGIC           origem.dat_primeiro_pagamento             ,
-- MAGIC           origem.cod_tipo_antecipacao               ,
-- MAGIC           origem.dsc_tipo_antecipacao               ,
-- MAGIC           origem.cod_unidade_negocio_antecipacao    ,
-- MAGIC           origem.num_voucher_antecipacao            ,
-- MAGIC           origem.cod_unidade_negocio_compras        ,
-- MAGIC           origem.num_voucher_compras                ,
-- MAGIC           origem.num_nota_fiscal                    ,
-- MAGIC           origem.val_pagamento                      ,
-- MAGIC           origem.val_tarifa                         ,
-- MAGIC           origem.val_tarifa_adiantamento_automatico ,
-- MAGIC           origem.val_tarifa_adiantamento_eventual   ,
-- MAGIC           origem.dsc_pagamento_avulso               ,
-- MAGIC           origem.cod_hash_md5                       ,
-- MAGIC           current_date()                            ,       
-- MAGIC           current_date()
-- MAGIC         )"""
-- MAGIC          )

-- COMMAND ----------

-- DBTITLE 1,Exit notebook
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
