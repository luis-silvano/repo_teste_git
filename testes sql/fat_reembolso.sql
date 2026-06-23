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
-- MAGIC # databricks_catalog (PROD) = prod
-- MAGIC # dsc_tabela_pii = fat_reembolso
-- MAGIC # location_catalogo_temp (DEV) = abfss://gold@adlsvrbeneficiosdev01.dfs.core.windows.net
-- MAGIC # location_catalogo_temp (PROD) = abfss://gold@adlsvrbeneficiosprd.dfs.core.windows.net
-- MAGIC # malha = temp
-- MAGIC # tablename = fat_reembolso

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC - silver_vr_autorizador.sncorecomum_fjjur
-- MAGIC - silver_vr_autorizador.sncoreon_fjcfj
-- MAGIC - silver_vr_kenan.arbor_smt_controle_interf_knn_contab
-- MAGIC - silver_vr_kenan.arbor_smt_extrato_fatura_tbl
-- MAGIC - silver_vr_kenan.arbor_payment_trans
-- MAGIC - silver_vr_kenan.arbor_smt_voucher_reembolso
-- MAGIC - silver_vr_kenan.arbor_cmf
-- MAGIC - silver_vr_kenan.arbor_payment_profile
-- MAGIC - silver_vr_kenan.arbor_bill_period_values
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_rf_inst_prod
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_rf_inst_prod_st
-- MAGIC - silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_dtl_antec_auto
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_rbt_ciclo_auto
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_ro_header
-- MAGIC - gold_vr_cliente_ec.dim_cadastro
-- MAGIC - gold_vr_cliente_ec.fat_voucher
-- MAGIC - gold_vr_cliente_ec.fat_pagamento
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_dtl
-- MAGIC - silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_hdr

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
-- MAGIC Abaixo são retornadas as Guias (movimentação financeira do Kenan) que possuem algum valor de Estorno. Para identificar um estorno de Reembolso são filtrados os seguintes códigos: 
-- MAGIC
-- MAGIC - 10042 - Estorno VR Compras - Subproduto Alimentacao,
-- MAGIC - 10102 - Estorno - IN - VRBen GPA Cartao Mamae,
-- MAGIC - 10124 - Estorno - OUT - VRBen GPA Cartao Mamae,
-- MAGIC - 10022 - Estorno Cultura ON,
-- MAGIC - 10020 - Estorno Refeicao ON,
-- MAGIC - 10018 - Estorno Auto,
-- MAGIC - 10016 - Estorno VRBen Alimentacao.
-- MAGIC
-- MAGIC Esse de-para está presente na tabela silver_vr_kenan.arbor_smt_charges_knn, onde basta filtrar o campo charge_type = 'TYPE_ID_USG' e buscar o id desejado, por exemplo, charge_id = 10042

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_estorno_sum
create or replace temporary view tmp_reembolso_estorno_sum as
select
  arbor_smt_controle_interf_knn_contab.cnpj,
  arbor_smt_controle_interf_knn_contab.id_protocolo_pedido,
  arbor_smt_controle_interf_knn_contab.bill_ref_resets,
  sum(arbor_smt_controle_interf_knn_contab.valor) as valor
from
  prod.silver_vr_kenan.arbor_smt_controle_interf_knn_contab 
where
  substr(id_protocolo_pedido, 1, 1) = 'R'
  and tp_uso_knn IN (10102, 10124, 10022, 10020, 10016, 10018, 10042) --and cnpj = 29796545000138
group by
  arbor_smt_controle_interf_knn_contab.cnpj,
  arbor_smt_controle_interf_knn_contab.id_protocolo_pedido,
  arbor_smt_controle_interf_knn_contab.bill_ref_resets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Como na tabela arbor_smt_controle_interf_knn_contab os valores não possuem ponto de casa decimal, foi preciso acrescentar para não ocorrem divergências posteriores, dessa forma, foi realizada a lógica abaixo com substr.

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_estorno
create or replace temporary view tmp_reembolso_estorno as
select tmp_reembolso_estorno_sum.cnpj as num_cnpj,
       replace(replace(tmp_reembolso_estorno_sum.id_protocolo_pedido,'R;',''),';0','') as num_guia,
       nvl(tmp_reembolso_estorno_sum.bill_ref_resets, 0) as num_alteracoes_guia,
       float(substr(tmp_reembolso_estorno_sum.valor, 1, length(tmp_reembolso_estorno_sum.valor) -2)
             || '.' || substr(tmp_reembolso_estorno_sum.valor, -2, 2)
       ) as val_estorno
  from tmp_reembolso_estorno_sum

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_pagamento
create or replace temporary view tmp_reembolso_pagamento as
select num_guia,
       num_alteracoes_guia,
       num_identificador_transacao,
       cod_sdprod,
       cod_produto,
       max(qtd_tentativa_pagamento) as qtd_tentativa_pagamento
  from gold_vr_cliente_ec.fat_pagamento
 group by num_guia,
          num_alteracoes_guia,
          num_identificador_transacao,
          cod_sdprod,
          cod_produto

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_voucher
 create or replace temporary view tmp_reembolso_voucher as
select num_guia,
       num_alteracoes_guia,
       cod_sdprod,
       cod_produto,
       max(dsc_tipo_antecipacao) as dsc_tipo_antecipacao,
       max(dat_primeiro_pagamento) as dat_primeiro_pagamento,
       --sum(val_pagamento) filter(where cod_unidade_negocio = 'SNBRA') as val_pagamento_reembolso,
       sum(val_tarifa) filter(where cod_unidade_negocio = 'SNBRA') as val_receita_antecipacao_vr,
       sum(val_tarifa) filter(where cod_unidade_negocio = 'JVCEF') as val_receita_antecipacao_cpp
  from gold_vr_cliente_ec.fat_voucher
 group by num_guia,
          num_alteracoes_guia,
          cod_sdprod,
          cod_produto

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_fatura
create or replace temporary view tmp_reembolso_fatura as
select arbor_smt_extrato_fatura_tbl.cnpj,
       arbor_smt_extrato_fatura_tbl.bill_ref_no,
       arbor_smt_extrato_fatura_tbl.bill_ref_resets,
       arbor_smt_extrato_fatura_tbl.convenio,
       arbor_smt_extrato_fatura_tbl.vl_transacoes,
       arbor_smt_extrato_fatura_tbl.vl_liquido_fatura,
       arbor_smt_extrato_fatura_tbl.vl_sva,
       arbor_smt_extrato_fatura_tbl.vl_doc,
       arbor_smt_extrato_fatura_tbl.vl_adesao,
       arbor_smt_extrato_fatura_tbl.vl_anuidade,
       arbor_smt_extrato_fatura_tbl.vl_ajustes,
       arbor_smt_extrato_fatura_tbl.vl_comissao,
       arbor_smt_extrato_fatura_tbl.vl_antecipacao,
       arbor_smt_extrato_fatura_tbl.taxa_antecipacao,
       arbor_smt_extrato_fatura_tbl.qtd_dias_antecipacao,
       arbor_smt_extrato_fatura_tbl.data_reembolso_efetiva,
       arbor_smt_extrato_fatura_tbl.data_geracao_fatura,
       arbor_smt_extrato_fatura_tbl.data_inicio_corte,
       arbor_smt_extrato_fatura_tbl.data_corte,
       arbor_smt_extrato_fatura_tbl.vl_transf_crt_compras,
       arbor_smt_extrato_fatura_tbl.perc_transf_crt_compras,
       arbor_smt_extrato_fatura_tbl.vl_cred_retido,
       arbor_smt_extrato_fatura_tbl.account_category,
       arbor_account_category_values.display_value as dsc_categoria_produto,
       arbor_smt_extrato_fatura_tbl.prep_status,
       arbor_smt_extrato_fatura_tbl.backout_status,
       arbor_smt_extrato_fatura_tbl.ciclo,
       arbor_bill_period_values.display_value,
       arbor_smt_extrato_fatura_tbl.taxa_comissao,
       arbor_smt_extrato_fatura_tbl.account_no,
       arbor_smt_extrato_fatura_tbl.data_reembolso,
       arbor_smt_extrato_fatura_tbl.VL_RET_PMAIS,
       case when arbor_smt_extrato_fatura_tbl.account_category in (127,131) then 1 else 0 end as flg_produto_pat
  from prod.silver_vr_kenan.arbor_smt_extrato_fatura_tbl 
  left join prod.silver_vr_kenan.arbor_bill_period_values arbor_bill_period_values
    on arbor_smt_extrato_fatura_tbl.ciclo = arbor_bill_period_values.bill_period
   and arbor_smt_extrato_fatura_tbl.account_category in (63, 64, 65, 66, 67, 127, 131)
   and arbor_smt_extrato_fatura_tbl.prep_status = 1
   and arbor_smt_extrato_fatura_tbl.backout_status = 0
  left join prod.silver_vr_kenan.arbor_account_category_values
    on arbor_smt_extrato_fatura_tbl.account_category = arbor_account_category_values.account_category
union
--Consulta para trazer reembolso do produto mamãe
select arbor_smt_extrato_fatura_tbl.cnpj,
       arbor_smt_extrato_fatura_tbl.bill_ref_no,
       arbor_smt_extrato_fatura_tbl.bill_ref_resets,
       arbor_smt_extrato_fatura_tbl.convenio,
       arbor_smt_extrato_fatura_tbl.vl_transacoes,
       arbor_smt_extrato_fatura_tbl.vl_liquido_fatura,
       arbor_smt_extrato_fatura_tbl.vl_sva,
       arbor_smt_extrato_fatura_tbl.vl_doc,
       arbor_smt_extrato_fatura_tbl.vl_adesao,
       arbor_smt_extrato_fatura_tbl.vl_anuidade,
       arbor_smt_extrato_fatura_tbl.vl_ajustes,
       arbor_smt_extrato_fatura_tbl.vl_comissao,
       arbor_smt_extrato_fatura_tbl.vl_antecipacao,
       arbor_smt_extrato_fatura_tbl.taxa_antecipacao,
       arbor_smt_extrato_fatura_tbl.qtd_dias_antecipacao,
       arbor_smt_extrato_fatura_tbl.data_reembolso_efetiva,
       arbor_smt_extrato_fatura_tbl.data_geracao_fatura,
       arbor_smt_extrato_fatura_tbl.data_inicio_corte,
       arbor_smt_extrato_fatura_tbl.data_corte,
       arbor_smt_extrato_fatura_tbl.vl_transf_crt_compras,
       arbor_smt_extrato_fatura_tbl.perc_transf_crt_compras,
       arbor_smt_extrato_fatura_tbl.vl_cred_retido,
       arbor_smt_extrato_fatura_tbl.account_category,
       arbor_account_category_values.display_value as dsc_categoria_produto,
       arbor_smt_extrato_fatura_tbl.prep_status,
       arbor_smt_extrato_fatura_tbl.backout_status,
       arbor_smt_extrato_fatura_tbl.ciclo,
       arbor_bill_period_values.display_value,
       arbor_smt_extrato_fatura_tbl.taxa_comissao,
       arbor_smt_extrato_fatura_tbl.account_no,
       arbor_smt_extrato_fatura_tbl.data_reembolso,
       arbor_smt_extrato_fatura_tbl.VL_RET_PMAIS,
       case when arbor_smt_extrato_fatura_tbl.account_category in (127,131) then 1 else 0 end as flg_produto_pat
  from prod.silver_vr_kenan.arbor_smt_extrato_fatura_tbl 
 inner join prod.silver_vr_kenan.arbor_cmf
    on arbor_smt_extrato_fatura_tbl.account_no = arbor_cmf.account_no
   and arbor_smt_extrato_fatura_tbl.account_category in (63, 64, 65, 66, 67, 127, 131)
   and arbor_smt_extrato_fatura_tbl.prep_status = 1
   and arbor_smt_extrato_fatura_tbl.backout_status = 0
   and arbor_cmf.account_category = 101 --Produto Mamãe
 inner join prod.silver_vr_kenan.arbor_payment_profile
    on arbor_cmf.payment_profile_id = arbor_payment_profile.profile_id
   and arbor_payment_profile.clearing_house_id = 'VRP'
  left join prod.silver_vr_kenan.arbor_bill_period_values
    on arbor_smt_extrato_fatura_tbl.ciclo = arbor_bill_period_values.bill_period
  left join prod.silver_vr_kenan.arbor_account_category_values
    on arbor_smt_extrato_fatura_tbl.account_category = arbor_account_category_values.account_category

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_vr_jv
create or replace temporary view tmp_reembolso_vr_jv as

with tmp_reembolso_bpo as (
-- Percentual BPO e Receita VR BPO
select arbor_smt_voucher_reembolso.bill_ref_no as num_guia,
       arbor_smt_voucher_reembolso.bill_ref_resets as num_alteracoes_guia,
       nvl(cast(arbor_smt_voucher_reembolso.tx_mdr_emissor as decimal(12,2)),0) as val_taxa_bpo,
       nvl(arbor_smt_voucher_reembolso.vl_mdr_emissor,0) as val_repasse_mdr_bpo
  from prod.silver_vr_kenan.arbor_smt_voucher_reembolso 
 where business_unit = 'SNBRA'
) ,

tmp_reembolso_receita_jv as (
-- Receita JV
select arbor_smt_voucher_reembolso.bill_ref_no as num_guia,
       arbor_smt_voucher_reembolso.bill_ref_resets as num_alteracoes_guia,
       arbor_smt_voucher_reembolso.vl_mdr_emissor +
       (select sum(nvl(sysadm_ps_vr_voucher_load.gvr_valor_tarifa,0))
          from prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load 
         where sysadm_ps_vr_voucher_load.business_unit = 'JVCEF'
           and sysadm_ps_vr_voucher_load.vr_ke_trans_id = 
               lpad(string(int(arbor_smt_voucher_reembolso.tracking_id_serv)),2,'0') ||
               lpad(string(int(arbor_smt_voucher_reembolso.tracking_id)), 10, '0') ||
               lpad(string(int(arbor_smt_voucher_reembolso.counter)), 3, '0')) as val_receita_jv
 from prod.silver_vr_kenan.arbor_smt_voucher_reembolso 
where business_unit = 'JVCEF'
),

tmp_reembolso_mdr (
-- Valores MDR VR e JV
select arbor_smt_extrato_fatura_tbl.bill_ref_no as num_guia,
       arbor_smt_extrato_fatura_tbl.bill_ref_resets as num_alteracoes_guia,
       arbor_smt_voucher_reembolso.vl_transacoes,
       arbor_smt_extrato_fatura_tbl.taxa_comissao,
       arbor_smt_extrato_fatura_tbl.vl_comissao,
      --  arbor_smt_extrato_fatura_tbl.vl_comissao - round((arbor_smt_voucher_reembolso.vl_transacoes * arbor_smt_extrato_fatura_tbl.taxa_comissao /100),2) as val_mdr_vr,
      --  round((arbor_smt_voucher_reembolso.vl_transacoes * arbor_smt_extrato_fatura_tbl.taxa_comissao /100),2) as val_mdr_cpp
      case when arbor_smt_voucher_reembolso.business_unit = 'SNBRA' then round((nvl(arbor_smt_voucher_reembolso.vl_transacoes,0) * nvl(arbor_smt_extrato_fatura_tbl.taxa_comissao,0) /100),2) else null end as val_mdr_vr,
      case when arbor_smt_voucher_reembolso.business_unit = 'JVCEF' then round((nvl(arbor_smt_voucher_reembolso.vl_transacoes,0) * nvl(arbor_smt_extrato_fatura_tbl.taxa_comissao,0) /100),2) else null end as val_mdr_cpp

  from prod.silver_vr_kenan.arbor_smt_extrato_fatura_tbl arbor_smt_extrato_fatura_tbl
 inner join prod.silver_vr_kenan.arbor_smt_voucher_reembolso 
    on arbor_smt_extrato_fatura_tbl.bill_ref_no = arbor_smt_voucher_reembolso.bill_ref_no
   and arbor_smt_extrato_fatura_tbl.bill_ref_resets = arbor_smt_voucher_reembolso.bill_ref_resets
   --and arbor_smt_voucher_reembolso.business_unit = 'JVCEF'
),

tmp_reembolso_receita_vr (
-- Valor Receita VR Total
select arbor_smt_voucher_reembolso.bill_ref_no as num_guia,
       arbor_smt_voucher_reembolso.bill_ref_resets as num_alteracoes_guia,
       lpad(string(int(arbor_smt_voucher_reembolso.tracking_id_serv)),2,'0') ||
       lpad(string(int(arbor_smt_voucher_reembolso.tracking_id)), 10, '0') ||
       lpad(string(int(arbor_smt_voucher_reembolso.counter)), 3, '0') as vr_ke_trans_id,
       arbor_smt_extrato_fatura_tbl.taxa_comissao as val_taxa_mdr,
       
       cast(
         
         --nvl(arbor_smt_extrato_fatura_tbl.vl_comissao - round((arbor_smt_voucher_reembolso_jv.vl_transacoes * arbor_smt_extrato_fatura_tbl.taxa_comissao /100),2),0) +
         nvl(arbor_smt_extrato_fatura_tbl.vl_doc,0) +
         nvl(arbor_smt_extrato_fatura_tbl.vl_anuidade,0) +
         nvl(arbor_smt_extrato_fatura_tbl.vl_adesao,0) +
         nvl(arbor_smt_extrato_fatura_tbl.vl_sva,0) + 
         nvl(arbor_smt_extrato_fatura_tbl.VL_RET_PMAIS,0)+
         
         nvl((select sum(val_receita_antecipacao_vr) from tmp_reembolso_voucher
          where tmp_reembolso_voucher.num_guia = arbor_smt_voucher_reembolso.bill_ref_no
          and tmp_reembolso_voucher.num_alteracoes_guia = arbor_smt_voucher_reembolso.bill_ref_resets),0) +
          
          nvl((select sum(val_mdr_vr) as val_receita_mdr_vr
          from tmp_reembolso_mdr
          left join tmp_reembolso_bpo
          on tmp_reembolso_mdr.num_guia = tmp_reembolso_bpo.num_guia  
          and tmp_reembolso_mdr.num_alteracoes_guia = tmp_reembolso_bpo.num_alteracoes_guia
          where tmp_reembolso_mdr.num_guia = arbor_smt_voucher_reembolso.bill_ref_no
          and   tmp_reembolso_mdr.num_alteracoes_guia = arbor_smt_voucher_reembolso.bill_ref_resets),0) 


        --  nvl((select sum(sysadm_ps_vr_voucher_load.gvr_valor_tarifa)
        --     from prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load
        --    where sysadm_ps_vr_voucher_load.business_unit = 'SNBRA'
        --      and sysadm_ps_vr_voucher_load.vr_ke_trans_id =
        --          lpad(string(int(arbor_smt_voucher_reembolso.tracking_id_serv)), 2, '0') || 
        --          lpad(string(int(arbor_smt_voucher_reembolso.tracking_id)), 10, '0') ||
        --          lpad(string(int(arbor_smt_voucher_reembolso.counter)), 3, '0')),0) +
        --  nvl((select sum(arbor_smt_voucher_reembolso2.vl_mdr_emissor)
        --     from prod.silver_vr_kenan.arbor_smt_voucher_reembolso arbor_smt_voucher_reembolso2
        --    where arbor_smt_voucher_reembolso2.business_unit = 'SNBRA'
        --      and arbor_smt_voucher_reembolso2.bill_ref_no = arbor_smt_voucher_reembolso.bill_ref_no
        --      and arbor_smt_voucher_reembolso2.bill_ref_resets = arbor_smt_voucher_reembolso.bill_ref_resets),0) 
        as decimal(12,2) ) as val_receita_vr

  --,cast((arbor_smt_voucher_reembolso.vl_transacoes - arbor_smt_extrato_fatura_tbl.vl_liquido_fatura) as decimal(12,2)) as val_receita_vr_novo
  from prod.silver_vr_kenan.arbor_smt_voucher_reembolso arbor_smt_voucher_reembolso
 inner join prod.silver_vr_kenan.arbor_smt_extrato_fatura_tbl
    on arbor_smt_extrato_fatura_tbl.bill_ref_no = arbor_smt_voucher_reembolso.bill_ref_no
   and arbor_smt_extrato_fatura_tbl.bill_ref_resets = arbor_smt_voucher_reembolso.bill_ref_resets
   left join prod.silver_vr_kenan.arbor_smt_voucher_reembolso arbor_smt_voucher_reembolso_jv
   on arbor_smt_extrato_fatura_tbl.bill_ref_no = arbor_smt_voucher_reembolso_jv.bill_ref_no
   and arbor_smt_extrato_fatura_tbl.bill_ref_resets = arbor_smt_voucher_reembolso_jv.bill_ref_resets
   and arbor_smt_voucher_reembolso_jv.business_unit = 'JVCEF'
   where arbor_smt_voucher_reembolso.business_unit = 'SNBRA'
--where business_unit = 'SNBRA'
),

tmp_reembolso_repasse_jv (
-- Valor Repasse JV p/ VR
select arbor_smt_voucher_reembolso.bill_ref_no as num_guia,
       arbor_smt_voucher_reembolso.bill_ref_resets as num_alteracoes_guia,
       nvl(arbor_smt_voucher_reembolso.vl_voucher,0) - 
       nvl((select sum(sysadm_ps_vr_voucher_load.gvr_valor_tarifa)
              from prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load sysadm_ps_vr_voucher_load
             where sysadm_ps_vr_voucher_load.business_unit = 'JVCEF'
               and sysadm_ps_vr_voucher_load.gvr_status_antvchr = 1
               and sysadm_ps_vr_voucher_load.vr_ke_trans_id = 
                   lpad(string(int(arbor_smt_voucher_reembolso.tracking_id_serv)), 2, '0') ||
                   lpad(string(int(arbor_smt_voucher_reembolso.tracking_id)), 10, '0') ||
                   lpad(string(int(arbor_smt_voucher_reembolso.counter)), 3, '0')),0) as val_repasse_jv_vr
  from prod.silver_vr_kenan.arbor_smt_voucher_reembolso 
 where business_unit = 'JVCEF'
)

select tmp_reembolso_bpo.num_guia, 
       tmp_reembolso_bpo.num_alteracoes_guia, 
       max(val_taxa_bpo) as val_taxa_bpo,
       max(val_repasse_mdr_bpo) as val_repasse_mdr_bpo,
       max(nvl(cast(tmp_reembolso_receita_jv.val_receita_jv as decimal(12,2)),0)) as val_receita_jv,
       max(nvl(cast(tmp_reembolso_mdr.val_mdr_vr as decimal(12,2)),0)) as val_mdr_vr,
       max(nvl(cast(tmp_reembolso_mdr.val_mdr_cpp as decimal(12,2)),0)) as val_mdr_cpp,
       max(nvl(tmp_reembolso_receita_vr.val_taxa_mdr,0)) as val_taxa_mdr,
       --nvl( tmp_reembolso_receita_vr.val_receita_vr_novo,0) as val_receita_vr_novo,
       max(nvl(tmp_reembolso_receita_vr.val_receita_vr,0)) as val_receita_vr,
       max(nvl(cast(tmp_reembolso_repasse_jv.val_repasse_jv_vr as decimal(12,2)),0)) val_repasse_jv_vr
  from tmp_reembolso_bpo
  left join tmp_reembolso_receita_jv 
    on tmp_reembolso_bpo.num_guia = tmp_reembolso_receita_jv.num_guia
   and tmp_reembolso_bpo.num_alteracoes_guia = tmp_reembolso_receita_jv.num_alteracoes_guia
  left join tmp_reembolso_mdr
    on tmp_reembolso_bpo.num_guia = tmp_reembolso_mdr.num_guia
   and tmp_reembolso_bpo.num_alteracoes_guia = tmp_reembolso_mdr.num_alteracoes_guia
  left join tmp_reembolso_receita_vr
    on tmp_reembolso_bpo.num_guia = tmp_reembolso_receita_vr.num_guia
   and tmp_reembolso_bpo.num_alteracoes_guia = tmp_reembolso_receita_vr.num_alteracoes_guia
  left join tmp_reembolso_repasse_jv
    on tmp_reembolso_bpo.num_guia = tmp_reembolso_repasse_jv.num_guia
   and tmp_reembolso_bpo.num_alteracoes_guia = tmp_reembolso_repasse_jv.num_alteracoes_guia
--ajuste Alex 28.10.2025 --comentar filtro abaixo
 --where tmp_reembolso_bpo.val_repasse_mdr_bpo is not null
 group by tmp_reembolso_bpo.num_guia, tmp_reembolso_bpo.num_alteracoes_guia
 

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_bruto_vr

create or replace temporary view tmp_reembolso_bruto_vr as
with tmp_consumo_thomr as (
select fat_transacao_consumo.num_guia,
       fat_transacao_consumo.num_resets as num_guia_reinicializacao,
       sum(nvl(fat_transacao_consumo.val_transacao_consumo,0)) as val_transacao_consumo,
       'Thomr' as origem
  from gold_thomr_cliente_trabalhador.fat_transacao_consumo
   group by all)
   

  ,tmp_consumo_vr as (
    select 
    fat_transacao_consumo.num_guia,
    fat_transacao_consumo.num_guia_reinicializacao as num_guia_reinicializacao,
  sum(nvl(fat_transacao_consumo.val_transacao_consumo,0)) as val_transacao_consumo,
  'VR' as origem
 from gold_vr_cliente_trabalhador.fat_transacao_consumo
 group by all
 )

,tmp_union_vr_thomr as (
select * from tmp_consumo_thomr
union all
select * from tmp_consumo_vr)

select 
 num_guia,
 num_guia_reinicializacao,
 sum(val_transacao_consumo) as val_reembolso_bruto_vr
from tmp_union_vr_thomr
group by num_guia,
 num_guia_reinicializacao

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_estorno_vr

create or replace temporary view tmp_reembolso_estorno_vr as
with tmp_estorno_thomr as (
select fat_transacao_estorno.num_guia,
       fat_transacao_estorno.num_resets as num_guia_reinicializacao,
       sum(nvl(fat_transacao_estorno.val_transacao_estorno,0)) as val_transacao_estorno,
       'Thomr' as origem
  from gold_thomr_cliente_trabalhador.fat_transacao_estorno
   group by all)
   

  ,tmp_estorno_vr as (
    select 
    fat_transacao_estorno.num_guia,
    fat_transacao_estorno.num_guia_reinicializacao as num_guia_reinicializacao,
  sum(nvl(fat_transacao_estorno.val_transacao_estorno,0)) as val_transacao_estorno,
  'VR' as origem
 from gold_vr_cliente_trabalhador.fat_transacao_estorno
 group by all
 )

,tmp_union_vr_thomr as (
select * from tmp_estorno_thomr
union all
select * from tmp_estorno_vr)

select 
 num_guia,
 num_guia_reinicializacao,
 sum(val_transacao_estorno) as val_estorno_vr
from tmp_union_vr_thomr
group by num_guia,
 num_guia_reinicializacao

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_bruto_jv
 create or replace temporary view tmp_reembolso_bruto_jv as
 select 
    fat_transacao_consumo.num_guia,
    fat_transacao_consumo.num_resets as num_guia_reinicializacao,
    sum(nvl(fat_transacao_consumo.val_transacao_consumo,0)) as val_reembolso_bruto_cpp
 from gold_jvcef_cliente_trabalhador.fat_transacao_consumo
  group by 
    fat_transacao_consumo.num_guia,
    fat_transacao_consumo.num_resets

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_estorno_jv
create or replace temporary view tmp_reembolso_estorno_jv as
select 
    fat_transacao_estorno.num_guia,
    fat_transacao_estorno.num_resets,
    sum(nvl(fat_transacao_estorno.val_transacao_estorno,0)) as val_estorno_cpp
 from gold_jvcef_cliente_trabalhador.fat_transacao_estorno
 group by 
    fat_transacao_estorno.num_guia,
    fat_transacao_estorno.num_resets


-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Na query abaixo foram incluídos alguns filtros para que sejam obtidas as informações de maneira precisa. São eles: Account Category: 63 = Alimentação; 65 = Refeição; 64 = Auto; 66 = Cultura. Prep_Status: 1 significa que a Guia é efetiva, não proforma (rascunho). Backout_Status: 0 significa que a guia não sofreu um rollback, ou seja, retorna sempre a mais atual. Inst_Prod_Status: INS indica que o "contrato" do EC está ativo.

-- COMMAND ----------

-- DBTITLE 1,tmp_base_reembolso
create or replace temporary view tmp_base_reembolso as
select trunc(tmp_reembolso_voucher.dat_primeiro_pagamento,'MM') as dat_referencia,
       dim_cadastro.srk_ec_cadastro as srk_ec_cadastro,
       tmp_reembolso_fatura.bill_ref_no as num_guia,
       tmp_reembolso_fatura.bill_ref_resets as num_alteracoes_guia,
       sysadm_ps_rf_inst_prod.setid as cod_sdprod,
       sysadm_ps_rf_inst_prod.product_id as cod_produto,
       tmp_reembolso_fatura.convenio as num_ec_conta,
       tmp_reembolso_pagamento.num_identificador_transacao as num_identificador_transacao,
       tmp_reembolso_fatura.vl_transacoes as val_reembolso_bruto,
       (nvl(tmp_reembolso_bruto_vr.val_reembolso_bruto_vr,0) - nvl(tmp_reembolso_estorno_vr.val_estorno_vr,0)) as val_reembolso_bruto_vr,
       (nvl(tmp_reembolso_bruto_jv.val_reembolso_bruto_cpp,0) - nvl(tmp_reembolso_estorno_jv.val_estorno_cpp,0)) as val_reembolso_bruto_cpp,
       nvl(tmp_reembolso_fatura.vl_liquido_fatura,0) as val_reembolso_liquido,
       nvl((nvl(tmp_reembolso_bruto_vr.val_reembolso_bruto_vr,0) - nvl(tmp_reembolso_estorno_vr.val_estorno_vr,0)),0) - nvl(tmp_reembolso_vr_jv.val_receita_vr,0) as val_reembolso_liquido_vr,
       nvl((nvl(tmp_reembolso_bruto_jv.val_reembolso_bruto_cpp,0) - nvl(tmp_reembolso_estorno_jv.val_estorno_cpp,0)),0) - nvl(tmp_reembolso_vr_jv.val_receita_jv,0) as val_reembolso_liquido_cpp,
       nvl(tmp_reembolso_estorno.val_estorno,0) as val_estorno,
       --tmp_reembolso_voucher.val_pagamento_reembolso as val_pagamento_reembolso,
       nvl(tmp_reembolso_pagamento.qtd_tentativa_pagamento,0) as qtd_tentativa_pagamento,
       nvl(tmp_reembolso_fatura.vl_sva,0) as val_receita_produto_sva,
       nvl(tmp_reembolso_fatura.VL_RET_PMAIS,0) as val_receita_pacote_vrg,
       nvl(tmp_reembolso_fatura.vl_doc,0) as val_receita_tarifa_doc,
       nvl(tmp_reembolso_fatura.vl_adesao,0) as val_receita_tarifa_adesao,
       nvl(tmp_reembolso_fatura.vl_anuidade,0) as val_receita_tarifa_anuidade,
       nvl(tmp_reembolso_fatura.vl_ajustes,0) as val_taxa_ajuste,
       nvl(tmp_reembolso_fatura.vl_comissao,0) as val_receita_mdr,
       --nvl(tmp_reembolso_fatura.vl_antecipacao,0) as val_receita_antecipacao,
       
       nvl(tmp_reembolso_fatura.taxa_antecipacao,0) as val_taxa_antecipacao,
       tmp_reembolso_fatura.qtd_dias_antecipacao as qtd_dia_antecipacao_antigo,
       case when lower(dsc_tipo_antecipacao) = 'eventual' then datediff(tmp_reembolso_fatura.data_reembolso_efetiva,tmp_reembolso_voucher.dat_primeiro_pagamento) else tmp_reembolso_fatura.qtd_dias_antecipacao end as qtd_dia_antecipacao,
       tmp_reembolso_fatura.data_reembolso_efetiva as dat_reembolso_efetivo,
       tmp_reembolso_fatura.data_geracao_fatura as dat_criacao_reembolso,
       tmp_reembolso_fatura.data_inicio_corte as dat_inicio_corte,
       tmp_reembolso_fatura.data_corte as dat_fim_corte,
       tmp_reembolso_voucher.dat_primeiro_pagamento as dat_primeiro_pagamento,
       nvl(tmp_reembolso_fatura.vl_transf_crt_compras,0) as val_transferido_cartao,
       tmp_reembolso_fatura.perc_transf_crt_compras as num_percentual_transferido_cartao,
       tmp_reembolso_fatura.vl_cred_retido as val_credito_retido,
       tmp_reembolso_voucher.dsc_tipo_antecipacao as dsc_tipo_antecipacao,
       nvl(tmp_reembolso_vr_jv.val_taxa_bpo,0) as val_taxa_bpo,
       nvl(tmp_reembolso_vr_jv.val_mdr_vr,0) as val_mdr_vr,
       nvl(tmp_reembolso_vr_jv.val_mdr_cpp,0) as val_mdr_cpp,
       nvl(tmp_reembolso_vr_jv.val_repasse_mdr_bpo,0) as val_repasse_mdr_bpo,
       nvl(tmp_reembolso_vr_jv.val_receita_jv,0) as val_receita_cpp,
       nvl(tmp_reembolso_vr_jv.val_mdr_vr,0) + nvl(tmp_reembolso_vr_jv.val_repasse_mdr_bpo,0) as val_receita_mdr_vr,
       nvl(tmp_reembolso_vr_jv.val_mdr_cpp,0) - nvl(tmp_reembolso_vr_jv.val_repasse_mdr_bpo,0) as val_receita_mdr_cpp,
       nvl(tmp_reembolso_fatura.taxa_comissao,0) as val_taxa_mdr,
       nvl(tmp_reembolso_vr_jv.val_receita_vr,0) as val_receita_vr,
       nvl(tmp_reembolso_vr_jv.val_repasse_jv_vr,0) as val_repasse,
       tmp_reembolso_fatura.ciclo as cod_ciclo_reembolso,
       tmp_reembolso_fatura.display_value as dsc_ciclo_reembolso,
       tmp_reembolso_fatura.account_no as num_conta_sistema,
       tmp_reembolso_fatura.data_reembolso as dat_vencimento,
       tmp_reembolso_voucher.val_receita_antecipacao_vr,
       tmp_reembolso_voucher.val_receita_antecipacao_cpp,
       case when (dsc_tipo_antecipacao = 'Antecipação') then nvl(tmp_reembolso_fatura.vl_antecipacao,0)
        else (nvl(tmp_reembolso_voucher.val_receita_antecipacao_vr,0) + nvl(tmp_reembolso_voucher.val_receita_antecipacao_cpp,0)) end as val_receita_antecipacao,
       (nvl(tmp_reembolso_vr_jv.val_receita_jv,0) + nvl(tmp_reembolso_vr_jv.val_receita_vr,0)) as val_receita,
       tmp_reembolso_fatura.dsc_categoria_produto,
       tmp_reembolso_fatura.flg_produto_pat
  from tmp_reembolso_fatura
 inner join prod.silver_vr_kenan.arbor_payment_trans
    on tmp_reembolso_fatura.bill_ref_no = arbor_payment_trans.bill_ref_no
   and tmp_reembolso_fatura.bill_ref_resets = arbor_payment_trans.bill_ref_resets 
   and tmp_reembolso_fatura.account_category in (63, 64, 65, 66, 67, 127, 131)
   and tmp_reembolso_fatura.prep_status = 1
   and tmp_reembolso_fatura.backout_status = 0
 inner join prod.silver_vr_crm_peoplesoft.sysadm_ps_rf_inst_prod
    on tmp_reembolso_fatura.convenio = sysadm_ps_rf_inst_prod.rbtacctid
 inner join prod.silver_vr_crm_peoplesoft.sysadm_ps_rf_inst_prod_st
    on sysadm_ps_rf_inst_prod.inst_prod_id = sysadm_ps_rf_inst_prod_st.inst_prod_id
   and sysadm_ps_rf_inst_prod_st.inst_prod_status = 'INS'
 inner join gold_vr_cliente_ec.dim_cadastro
    on tmp_reembolso_fatura.cnpj = dim_cadastro.num_cnpj
   and dim_cadastro.flg_ativo = 1
  left join tmp_reembolso_estorno
    on tmp_reembolso_fatura.bill_ref_no = tmp_reembolso_estorno.num_guia
   and tmp_reembolso_fatura.bill_ref_resets = tmp_reembolso_estorno.num_alteracoes_guia
   and tmp_reembolso_fatura.cnpj = tmp_reembolso_estorno.num_cnpj
  left join tmp_reembolso_pagamento
    on tmp_reembolso_pagamento.num_guia = tmp_reembolso_fatura.bill_ref_no
   and tmp_reembolso_pagamento.num_alteracoes_guia = tmp_reembolso_fatura.bill_ref_resets
   and tmp_reembolso_pagamento.cod_sdprod = sysadm_ps_rf_inst_prod.setid
   and tmp_reembolso_pagamento.cod_produto = sysadm_ps_rf_inst_prod.product_id
  left join tmp_reembolso_voucher
    on tmp_reembolso_fatura.bill_ref_no = tmp_reembolso_voucher.num_guia
   and tmp_reembolso_fatura.bill_ref_resets = tmp_reembolso_voucher.num_alteracoes_guia
   and sysadm_ps_rf_inst_prod.setid = tmp_reembolso_voucher.cod_sdprod
   and sysadm_ps_rf_inst_prod.product_id = tmp_reembolso_voucher.cod_produto
  left join tmp_reembolso_vr_jv
    on tmp_reembolso_fatura.bill_ref_no = tmp_reembolso_vr_jv.num_guia
   and tmp_reembolso_fatura.bill_ref_resets = tmp_reembolso_vr_jv.num_alteracoes_guia
  left join tmp_reembolso_bruto_vr
    on tmp_reembolso_fatura.bill_ref_no = tmp_reembolso_bruto_vr.num_guia
   and tmp_reembolso_fatura.bill_ref_resets = tmp_reembolso_bruto_vr.num_guia_reinicializacao
    -- and sysadm_ps_rf_inst_prod.product_id = tmp_reembolso_bruto_vr.cod_produto
  left join tmp_reembolso_bruto_jv
    on tmp_reembolso_fatura.bill_ref_no = tmp_reembolso_bruto_jv.num_guia
   and tmp_reembolso_fatura.bill_ref_resets = tmp_reembolso_bruto_jv.num_guia_reinicializacao
    -- and sysadm_ps_rf_inst_prod.product_id = tmp_reembolso_bruto_jv.cod_produto
  left join tmp_reembolso_estorno_vr
    on tmp_reembolso_fatura.bill_ref_no = tmp_reembolso_estorno_vr.num_guia
   and tmp_reembolso_fatura.bill_ref_resets = tmp_reembolso_estorno_vr.num_guia_reinicializacao
  left join tmp_reembolso_estorno_jv
    on tmp_reembolso_fatura.bill_ref_no = tmp_reembolso_estorno_jv.num_guia
   and tmp_reembolso_fatura.bill_ref_resets = tmp_reembolso_estorno_jv.num_resets

-- COMMAND ----------

-- DBTITLE 1,tmp_operador
create or replace temporary view tmp_operador as
with tmp_header_max as (
  select  max(sysadm_ps_rbt_ciclo_auto.effdt) as effdt 
         ,max(sysadm_ps_rbt_ciclo_auto.effseq) as effseq
         ,sysadm_ps_rbt_ciclo_auto.rbtacctid
    from prod.silver_vr_crm_peoplesoft.sysadm_ps_rbt_ciclo_auto
    inner join tmp_base_reembolso
      on tmp_base_reembolso.num_ec_conta = sysadm_ps_rbt_ciclo_auto.rbtacctid
    where effdt <= tmp_base_reembolso.dat_reembolso_efetivo
    group by sysadm_ps_rbt_ciclo_auto.rbtacctid
)
  select 
      sysadm_ps_rbt_ciclo_auto.rbtacctid
      ,nvl(sysadm_ps_ro_header.row_added_oprid, sysadm_ps_rbt_ciclo_auto.row_added_oprid) as dsc_operador_usuario 
    from prod.silver_vr_crm_peoplesoft.sysadm_ps_rbt_ciclo_auto
    inner join tmp_header_max
       on sysadm_ps_rbt_ciclo_auto.rbtacctid = tmp_header_max.rbtacctid
      and sysadm_ps_rbt_ciclo_auto.effdt = tmp_header_max.effdt
      and sysadm_ps_rbt_ciclo_auto.effseq = tmp_header_max.effseq
    left join prod.silver_vr_crm_peoplesoft.sysadm_ps_ro_header
       on sysadm_ps_ro_header.capture_id = sysadm_ps_rbt_ciclo_auto.capture_id


-- COMMAND ----------

-- DBTITLE 1,tmp_dtl_antec_auto
create or replace temporary view tmp_dtl_antec_auto as
Select distinct 
      antec_auto.RBTACCTID
     ,antec_auto.row_added_oprid as dsc_responsavel_antecipacao 
     ,case when antec_auto.ORIGEM_ANTECIPACAO = 'P' then 'Portal'
           when antec_auto.ORIGEM_ANTECIPACAO = 'M' then 'APP'
           when antec_auto.ORIGEM_ANTECIPACAO is null or antec_auto.ORIGEM_ANTECIPACAO = 'A' or antec_auto.ORIGEM_ANTECIPACAO = '' then 'CRM'
           else 'DESCONHECIDO' end as dsc_origem_antecipacao
from prod.silver_vr_crm_peoplesoft.sysadm_ps_dtl_antec_auto as antec_auto
inner join (select 
               RBTACCTID
              ,Max(ROW_LASTMANT_DTTM) as ROW_LASTMANT_DTTM
              from prod.silver_vr_crm_peoplesoft.sysadm_ps_dtl_antec_auto as antec_auto
              inner join tmp_base_reembolso
              on antec_auto.RBTACCTID = tmp_base_reembolso.num_ec_conta
              where ROW_LASTMANT_DTTM <= tmp_base_reembolso.dat_reembolso_efetivo
              group by RBTACCTID
              ) mais_recente
         on antec_auto.RBTACCTID = mais_recente.RBTACCTID
        and antec_auto.ROW_LASTMANT_DTTM = mais_recente.ROW_LASTMANT_DTTM

-- COMMAND ----------

-- DBTITLE 1,tmp_operador_eventual
create or replace temporary view tmp_operador_eventual as

with tmp_reembolso as (
  select distinct sysadm_ps_solic_antec_dtl.sn_bill_ref_no as num_guia_reembolso
         , sysadm_ps_solic_antec_dtl.row_added_oprid as dsc_operador_usuario
         , sysadm_PSOPRDEFN.oprdefndesc as dsc_responsavel_antecipacao
         , sysadm_ps_solic_antec_hdr.origem_antecipacao
         , case sysadm_ps_solic_antec_hdr.origem_antecipacao
              when 'A' then 'Aplicação'
              when 'C' then 'Clube VR EC'
              when 'M' then 'Mobile'
              when 'O' then 'Oracle Field Service Cloud'
              when 'P' then 'Portal'
              when 'S' then 'Salesforce'
              when 'U' then 'UraEC'
              when 'W' then 'WebService Externo'
              when 'X' then 'X-Special'
              when 'Z' then 'WhatsApp'
              else 'Outro'
              end as dsc_origem_antecipacao
        --  , sysadm_ps_solic_antec_dtl.ID_SOLI_ANTECIP_SN as num_solicitacao_antecipacao
         , 1 as rownum
    from prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_dtl
   inner join tmp_base_reembolso
      on tmp_base_reembolso.num_guia = sysadm_ps_solic_antec_dtl.sn_bill_ref_no
   inner join prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_hdr
      on sysadm_ps_solic_antec_hdr.ID_SOLI_ANTECIP_SN = sysadm_ps_solic_antec_dtl.ID_SOLI_ANTECIP_SN
     and sysadm_ps_solic_antec_hdr.status_solicita_sn =  'A'
     and sysadm_ps_solic_antec_dtl.select_chkbox = 'Y' 
   inner join prod.silver_vr_crm_peoplesoft.sysadm_PSOPRDEFN
      on sysadm_ps_solic_antec_hdr.row_added_oprid = sysadm_PSOPRDEFN.oprid
)

, tmp_voucher as (
  select distinct
         fat_voucher.num_guia as num_guia_reembolso
         , sysadm_ps_solic_antec_dtl.row_added_oprid as dsc_operador_usuario
         , sysadm_PSOPRDEFN.oprdefndesc as dsc_responsavel_antecipacao
         , sysadm_ps_solic_antec_hdr.origem_antecipacao
         , case sysadm_ps_solic_antec_hdr.origem_antecipacao
              when 'A' then 'Aplicação'
              when 'C' then 'Clube VR EC'
              when 'M' then 'Mobile'
              when 'O' then 'Oracle Field Service Cloud'
              when 'P' then 'Portal'
              when 'S' then 'Salesforce'
              when 'U' then 'UraEC'
              when 'W' then 'WebService Externo'
              when 'X' then 'X-Special'
              when 'Z' then 'WhatsApp'
              else 'Outro'
              end as dsc_origem_antecipacao
        --  , sysadm_ps_solic_antec_dtl.ID_SOLI_ANTECIP_SN as num_solicitacao_antecipacao
         , row_number () over (partition by fat_voucher.num_guia
                                   order by sysadm_ps_solic_antec_dtl.row_added_dttm asc) + 1 as rownum
    from prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_dtl
   inner join gold_vr_cliente_ec.fat_voucher
      on sysadm_ps_solic_antec_dtl.voucher_id = fat_voucher.num_voucher
   inner join tmp_base_reembolso
      on tmp_base_reembolso.num_guia = fat_voucher.num_guia
   inner join prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_hdr
      on sysadm_ps_solic_antec_hdr.ID_SOLI_ANTECIP_SN = sysadm_ps_solic_antec_dtl.ID_SOLI_ANTECIP_SN
     and sysadm_ps_solic_antec_hdr.status_solicita_sn =  'A'
     and sysadm_ps_solic_antec_dtl.select_chkbox = 'Y' 
   inner join prod.silver_vr_crm_peoplesoft.sysadm_PSOPRDEFN
      on sysadm_ps_solic_antec_hdr.row_added_oprid = sysadm_PSOPRDEFN.oprid
   where nvl(sysadm_ps_solic_antec_dtl.sn_bill_ref_no,0) = 0
)

, tmp_union as (
  Select *
    from tmp_reembolso
  union
  Select *
    from tmp_voucher
)

, tmp_min_rownum as (
  Select num_guia_reembolso,
         min(rownum) as min_rownum
    from tmp_union
   group by all
)

Select tmp_union.*
  from tmp_union
 inner join tmp_min_rownum
    on tmp_union.num_guia_reembolso = tmp_min_rownum.num_guia_reembolso
   and tmp_union.rownum = tmp_min_rownum.min_rownum
 order by tmp_union.num_guia_reembolso, tmp_union.rownum

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso
create or replace temporary view tmp_reembolso as
select distinct
     tmp_base_reembolso.dat_referencia
    ,tmp_base_reembolso.srk_ec_cadastro
    ,tmp_base_reembolso.num_guia
    ,tmp_base_reembolso.num_alteracoes_guia
    ,tmp_base_reembolso.cod_sdprod
    ,tmp_base_reembolso.cod_produto
    ,tmp_base_reembolso.num_ec_conta
    ,tmp_base_reembolso.num_identificador_transacao
    ,tmp_base_reembolso.val_reembolso_bruto
    ,tmp_base_reembolso.val_reembolso_bruto_vr
    ,tmp_base_reembolso.val_reembolso_bruto_cpp
    ,tmp_base_reembolso.val_reembolso_liquido
    --,tmp_base_reembolso.val_reembolso_liquido_vr
    ,case when nvl(tmp_base_reembolso.val_receita,0) = 0 then nvl((tmp_base_reembolso.val_reembolso_liquido_vr - (tmp_base_reembolso.val_receita_tarifa_doc + tmp_base_reembolso.val_receita_mdr)),0)
     else nvl(tmp_base_reembolso.val_reembolso_liquido_vr,0) end as val_reembolso_liquido_vr
    ,nvl(tmp_base_reembolso.val_reembolso_liquido_cpp,0) as val_reembolso_liquido_cpp
    ,nvl(tmp_base_reembolso.val_estorno,0) as val_estorno
    --,tmp_base_reembolso.val_pagamento_reembolso
    ,tmp_base_reembolso.qtd_tentativa_pagamento
    ,tmp_base_reembolso.val_receita_produto_sva
    ,tmp_base_reembolso.val_receita_pacote_vrg
    ,tmp_base_reembolso.val_receita_tarifa_doc
    ,nvl(tmp_tarifa_operacional.val_receita_tarifa_operacional,0) as val_receita_tarifa_operacional
    ,tmp_base_reembolso.val_receita_tarifa_adesao
    ,tmp_base_reembolso.val_receita_tarifa_anuidade
    ,tmp_base_reembolso.val_taxa_ajuste
    ,nvl(tmp_base_reembolso.val_receita_mdr,0) as val_receita_mdr
    ,tmp_base_reembolso.val_receita_antecipacao
   -- ,tmp_base_reembolso.val_taxa_antecipacao 
    ,tmp_base_reembolso.qtd_dia_antecipacao
    ,tmp_base_reembolso.dat_reembolso_efetivo
    ,tmp_base_reembolso.dat_criacao_reembolso
    ,tmp_base_reembolso.dat_inicio_corte
    ,tmp_base_reembolso.dat_fim_corte
    ,tmp_base_reembolso.dat_primeiro_pagamento
    ,tmp_base_reembolso.val_transferido_cartao
    ,tmp_base_reembolso.num_percentual_transferido_cartao
    ,nvl(tmp_base_reembolso.val_credito_retido,0) as val_credito_retido
    ,tmp_base_reembolso.dsc_tipo_antecipacao
    ,case when lower(dsc_tipo_antecipacao) = 'eventual' then (ps_dtl_hdr_eventual.val_taxa_antecipacao)
     when lower(dsc_tipo_antecipacao) like 'autom%' then (tmp_base_reembolso.val_taxa_antecipacao/nvl(tmp_base_reembolso.qtd_dia_antecipacao,1))
     else null end as val_taxa_antecipacao
    ,tmp_base_reembolso.val_taxa_bpo
    ,nvl(tmp_base_reembolso.val_mdr_vr,0) as val_mdr_vr
    ,nvl(tmp_base_reembolso.val_mdr_cpp,0) as val_mdr_cpp
    ,tmp_base_reembolso.val_repasse_mdr_bpo
    ,tmp_base_reembolso.val_receita_cpp
    ,tmp_base_reembolso.val_receita_mdr_vr
    ,tmp_base_reembolso.val_receita_mdr_cpp
    ,tmp_base_reembolso.val_taxa_mdr
    ,tmp_base_reembolso.val_receita_vr
    ,tmp_base_reembolso.val_repasse
    ,tmp_base_reembolso.cod_ciclo_reembolso
    ,tmp_base_reembolso.dsc_ciclo_reembolso
    ,case when lower(dsc_tipo_antecipacao) = 'eventual' then tmp_operador_eventual.dsc_operador_usuario else tmp_operador.dsc_operador_usuario end as dsc_operador_usuario
    ,case when lower(dsc_tipo_antecipacao) = 'eventual' then tmp_operador_eventual.dsc_responsavel_antecipacao else tmp_dtl_antec_auto.dsc_responsavel_antecipacao end as dsc_responsavel_antecipacao
    ,case when lower(dsc_tipo_antecipacao) = 'eventual' then tmp_operador_eventual.dsc_origem_antecipacao else tmp_dtl_antec_auto.dsc_origem_antecipacao end as dsc_origem_antecipacao
    -- ,tmp_dtl_antec_auto.dsc_origem_antecipacao    
    -- ,tmp_dtl_antec_auto.dsc_responsavel_antecipacao
    ,tmp_base_reembolso.dat_vencimento
    ,tmp_base_reembolso.num_conta_sistema
    ,tmp_base_reembolso.val_receita_antecipacao_vr
    ,tmp_base_reembolso.val_receita_antecipacao_cpp
    ,case when nvl(tmp_base_reembolso.val_receita,0) = 0 then (tmp_base_reembolso.val_receita_tarifa_doc + tmp_base_reembolso.val_receita_mdr) else nvl(tmp_base_reembolso.val_receita,0) end as val_receita,
    tmp_base_reembolso.dsc_categoria_produto,
    tmp_base_reembolso.flg_produto_pat
  from tmp_base_reembolso
  left join tmp_operador
  on tmp_base_reembolso.num_ec_conta = tmp_operador.rbtacctid
  left join tmp_dtl_antec_auto
  on  tmp_base_reembolso.num_ec_conta = tmp_dtl_antec_auto.rbtacctid
  left join (select sysadm_ps_solic_antec_dtl.sn_bill_ref_no as num_guia,
                    max(sysadm_ps_solic_antec_hdr.per_taxas_ad_sn) as val_taxa_antecipacao
               from prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_dtl      
               left join prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_hdr
                         on sysadm_ps_solic_antec_dtl.id_soli_antecip_sn = sysadm_ps_solic_antec_hdr.id_soli_antecip_sn
              group by sysadm_ps_solic_antec_dtl.sn_bill_ref_no) ps_dtl_hdr
on tmp_base_reembolso.num_guia = ps_dtl_hdr.num_guia

left join (
select distinct fat_voucher.num_guia as num_guia,
 min(case when sysadm_ps_solic_antec_hdr.tipo_taxa_sn = 'D' then
 (case when nvl(sysadm_ps_solic_antec_hdr.per_txdif_ad_sn,0) = 0 then
           nvl(sysadm_ps_solic_antec_hdr.per_taxas_ad_sn,0)
      else nvl(sysadm_ps_solic_antec_hdr.per_txdif_ad_sn,0) end)
      else nvl(sysadm_ps_solic_antec_hdr.per_taxas_ad_sn,0) end) as val_taxa_antecipacao
 from gold_vr_cliente_ec.fat_voucher
 inner join prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_dtl    
  on sysadm_ps_solic_antec_dtl.voucher_id = fat_voucher.num_voucher  
 left join prod.silver_vr_crm_peoplesoft.sysadm_ps_solic_antec_hdr
  on sysadm_ps_solic_antec_dtl.id_soli_antecip_sn = sysadm_ps_solic_antec_hdr.id_soli_antecip_sn
 inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_gvr_ant_vchr_ta
  on sysadm_ps_gvr_ant_vchr_ta.VOUCHER_ID = fat_voucher.num_voucher
 where fat_voucher.num_voucher_antecipacao is not null  
  and lower(fat_voucher.dsc_tipo_antecipacao) = 'eventual'  
  and fat_voucher.cod_unidade_negocio_antecipacao = 'BVRSA'        
  and sysadm_ps_solic_antec_dtl.SELECT_CHKBOX = 'Y'
  and sysadm_ps_solic_antec_hdr.STATUS_SOLICITA_SN = 'A'
  and sysadm_ps_gvr_ant_vchr_ta.STATUS_AP = 'C'
  and sysadm_ps_solic_antec_hdr.tipo_recusa_sn = ' '
  and (nvl(sysadm_ps_solic_antec_hdr.per_taxas_ad_sn,0) > 0 or
       nvl(sysadm_ps_solic_antec_hdr.per_txdif_ad_sn,0) > 0)
       group by fat_voucher.num_guia) ps_dtl_hdr_eventual
  on tmp_base_reembolso.num_guia = ps_dtl_hdr_eventual.num_guia
 left join tmp_operador_eventual
  on tmp_operador_eventual.num_guia_reembolso = tmp_base_reembolso.num_guia  
 left join  (
    select    loa.ke_bill_ref_no_sn as num_guia_reembolso
		        , loa.ke_bill_rf_rst_sn as num_alteracoes_guia
		        , abs(sum(vch.pymnt_amt)) val_receita_tarifa_operacional
    from
        prod.silver_vr_erp_peoplesoft.sysadm_ps_gvr_vchr_relac vch
    inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_voucher vou on vch.business_unit = vou.business_unit
                                                                  and vch.voucher_id = vou.voucher_id
    inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load loa on 	vou.business_unit = loa.business_unit
                                                                            and vou.voucher_id = loa.voucher_id
    where
        vch.gvr_tipo_vchr = 'OPE'
    group by all
) tmp_tarifa_operacional on tmp_tarifa_operacional.num_guia_reembolso = tmp_base_reembolso.num_guia 
               and tmp_tarifa_operacional.num_alteracoes_guia = tmp_base_reembolso.num_alteracoes_guia  


-- COMMAND ----------

-- DBTITLE 1,tmp_md5_reembolso
 CREATE OR REPLACE TEMPORARY VIEW tmp_md5_reembolso AS
 SELECT *,
   MD5(UPPER(
    nvl(dat_referencia                           ,'') ||
    nvl(srk_ec_cadastro                          ,'') ||
    nvl(num_guia                                 ,'') ||
    nvl(num_alteracoes_guia                      ,'') ||
    nvl(cod_sdprod                               ,'') ||
    nvl(cod_produto                              ,'') ||
    nvl(num_ec_conta                             ,'') ||
    nvl(num_identificador_transacao              ,'') ||
    nvl(val_reembolso_bruto                      ,'') ||
    nvl(val_reembolso_bruto_vr                   ,'') ||
    nvl(val_reembolso_bruto_cpp                  ,'') ||
    nvl(val_reembolso_liquido                    ,'') ||
    nvl(val_reembolso_liquido_vr                 ,'') ||
    nvl(val_reembolso_liquido_cpp                ,'') ||
    nvl(val_estorno                              ,'') ||
    --nvl(val_pagamento_reembolso                ,'') ||
    nvl(val_receita_produto_sva                  ,'') ||
    nvl(val_receita_pacote_vrg                   ,'') ||
    nvl(val_receita_tarifa_doc                   ,'') ||
    nvl(val_receita_tarifa_operacional           ,'') ||
    nvl(val_receita_tarifa_adesao                ,'') ||
    nvl(val_receita_tarifa_anuidade              ,'') ||
    nvl(val_taxa_ajuste                          ,'') ||
    nvl(val_receita_mdr                          ,'') ||
    nvl(val_receita_antecipacao                  ,'') ||
    nvl(val_taxa_antecipacao                     ,'') ||
    nvl(qtd_dia_antecipacao                      ,'') ||
    nvl(qtd_tentativa_pagamento                  ,'') ||
    nvl(dat_reembolso_efetivo                    ,'') ||
    nvl(dat_criacao_reembolso                    ,'') ||
    nvl(dat_inicio_corte                         ,'') ||
    nvl(dat_fim_corte                            ,'') ||
    nvl(dat_primeiro_pagamento                   ,'') ||
    nvl(val_transferido_cartao                   ,'') ||
    nvl(num_percentual_transferido_cartao        ,'') ||
    nvl(val_credito_retido                       ,'') ||
    nvl(dsc_tipo_antecipacao                     ,'') ||
    --nvl(val_tarifa_antecipacao                   ,'') ||
    nvl(val_taxa_bpo                             ,'') ||
    nvl(val_mdr_vr                               ,'') ||
    nvl(val_mdr_cpp                              ,'') ||
    nvl(val_repasse_mdr_bpo                      ,'') ||
    nvl(val_receita_cpp                          ,'') ||
    nvl(val_receita_mdr_vr                       ,'') ||
    nvl(val_receita_mdr_cpp                      ,'') ||
    nvl(val_taxa_mdr                             ,'') ||
    nvl(val_receita_vr                           ,'') ||
    nvl(cod_ciclo_reembolso                      ,'') ||
    nvl(dsc_ciclo_reembolso                      ,'') ||
    nvl(dsc_operador_usuario                     ,'') ||
    nvl(dsc_origem_antecipacao                   ,'') ||
    nvl(dsc_responsavel_antecipacao              ,'') ||
    nvl(dat_vencimento                           ,'') ||
    nvl(val_receita_antecipacao_vr               ,'') ||
    nvl(val_receita_antecipacao_cpp              ,'') ||
    nvl(val_receita                              ,'') ||
    nvl(num_conta_sistema                        ,'') ||
    nvl(dsc_categoria_produto                    ,'') ||
    nvl(flg_produto_pat                          ,'')
    )) as cod_hash_md5
  FROM tmp_reembolso

-- COMMAND ----------

-- DBTITLE 1,Padroniza campos
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select dat_referencia,
-- MAGIC        srk_ec_cadastro,
-- MAGIC        num_guia,
-- MAGIC        num_alteracoes_guia,
-- MAGIC        cod_sdprod,
-- MAGIC        cod_produto,
-- MAGIC        num_ec_conta,
-- MAGIC        num_identificador_transacao,
-- MAGIC        val_reembolso_bruto,
-- MAGIC        val_reembolso_bruto_vr,
-- MAGIC        val_reembolso_bruto_cpp,
-- MAGIC        val_reembolso_liquido,
-- MAGIC        val_reembolso_liquido_vr,
-- MAGIC        val_reembolso_liquido_cpp,
-- MAGIC        val_estorno,
-- MAGIC        qtd_tentativa_pagamento,
-- MAGIC        val_receita_produto_sva,
-- MAGIC        val_receita_pacote_vrg,
-- MAGIC        val_receita_tarifa_doc,
-- MAGIC        val_receita_tarifa_operacional,
-- MAGIC        val_receita_tarifa_adesao,
-- MAGIC        val_receita_tarifa_anuidade,
-- MAGIC        val_taxa_ajuste,
-- MAGIC        val_receita_mdr,
-- MAGIC        val_receita_antecipacao,
-- MAGIC        qtd_dia_antecipacao,
-- MAGIC        dat_reembolso_efetivo,
-- MAGIC        dat_criacao_reembolso,
-- MAGIC        dat_inicio_corte,
-- MAGIC        dat_fim_corte,
-- MAGIC        dat_primeiro_pagamento,
-- MAGIC        val_transferido_cartao,
-- MAGIC        num_percentual_transferido_cartao,
-- MAGIC        val_credito_retido,
-- MAGIC        dsc_tipo_antecipacao,
-- MAGIC        val_taxa_antecipacao,
-- MAGIC        val_taxa_bpo,
-- MAGIC        val_mdr_vr,
-- MAGIC        val_mdr_cpp,
-- MAGIC        val_repasse_mdr_bpo,
-- MAGIC        val_receita_cpp,
-- MAGIC        val_receita_mdr_vr,
-- MAGIC        val_receita_mdr_cpp,
-- MAGIC        val_taxa_mdr,
-- MAGIC        val_receita_vr,
-- MAGIC        val_repasse,
-- MAGIC        cod_ciclo_reembolso,
-- MAGIC        dsc_ciclo_reembolso,
-- MAGIC        dsc_operador_usuario,
-- MAGIC        dsc_responsavel_antecipacao,
-- MAGIC        dsc_origem_antecipacao,
-- MAGIC        dat_vencimento,
-- MAGIC        num_conta_sistema,
-- MAGIC        val_receita_antecipacao_vr,
-- MAGIC        val_receita_antecipacao_cpp,
-- MAGIC        val_receita,
-- MAGIC        dsc_categoria_produto,
-- MAGIC        flg_produto_pat,
-- MAGIC        cod_hash_md5
-- MAGIC   from tmp_md5_reembolso
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_reembolso_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Merge fat_reembolso
-- MAGIC %python
-- MAGIC df_merge = fn_merge_direto(""" 
-- MAGIC     MERGE INTO gold_vr_cliente_ec_pii.fat_reembolso as destino
-- MAGIC     USING tmp_reembolso_criptografado as origem
-- MAGIC        ON origem.num_guia = destino.num_guia_reembolso 
-- MAGIC       AND origem.num_alteracoes_guia = destino.qtd_alteracao_guia
-- MAGIC        WHEN MATCHED AND nvl(origem.cod_hash_md5,'') <> nvl(destino.cod_hash_md5,'') THEN UPDATE SET   
-- MAGIC           destino.dat_referencia                     = origem.dat_referencia                    ,
-- MAGIC           destino.num_ec_conta                       = origem.num_ec_conta                      ,
-- MAGIC           destino.num_identificador_transacao        = origem.num_identificador_transacao       ,
-- MAGIC           destino.val_reembolso_bruto                = origem.val_reembolso_bruto               ,
-- MAGIC           destino.val_reembolso_bruto_vr             = origem.val_reembolso_bruto_vr            ,
-- MAGIC           destino.val_reembolso_bruto_cpp            = origem.val_reembolso_bruto_cpp           ,
-- MAGIC           destino.val_reembolso_liquido              = origem.val_reembolso_liquido             ,
-- MAGIC           destino.val_reembolso_liquido_vr           = origem.val_reembolso_liquido_vr          ,
-- MAGIC           destino.val_reembolso_liquido_cpp          = origem.val_reembolso_liquido_cpp         ,
-- MAGIC           destino.val_estorno                        = origem.val_estorno                       ,
-- MAGIC           destino.val_receita_produto_sva            = origem.val_receita_produto_sva           ,
-- MAGIC           destino.val_receita_pacote_vrg             = origem.val_receita_pacote_vrg            ,
-- MAGIC           destino.val_receita_tarifa_doc             = origem.val_receita_tarifa_doc            ,
-- MAGIC           destino.val_receita_tarifa_operacional     = origem.val_receita_tarifa_operacional    ,
-- MAGIC           destino.val_receita_tarifa_adesao          = origem.val_receita_tarifa_adesao         ,
-- MAGIC           destino.val_receita_tarifa_anuidade        = origem.val_receita_tarifa_anuidade       ,
-- MAGIC           destino.val_taxa_ajuste                    = origem.val_taxa_ajuste                   ,
-- MAGIC           destino.val_receita_mdr                    = origem.val_receita_mdr                   ,
-- MAGIC           destino.val_receita_antecipacao            = origem.val_receita_antecipacao           ,
-- MAGIC           destino.val_taxa_antecipacao               = origem.val_taxa_antecipacao              ,
-- MAGIC           destino.qtd_dia_antecipacao                = origem.qtd_dia_antecipacao               ,
-- MAGIC           destino.qtd_tentativa_pagamento            = origem.qtd_tentativa_pagamento           ,
-- MAGIC           destino.dat_reembolso_efetivo              = origem.dat_reembolso_efetivo             ,
-- MAGIC           destino.dat_criacao_reembolso              = origem.dat_criacao_reembolso             ,
-- MAGIC           destino.dat_inicio_corte                   = origem.dat_inicio_corte                  ,
-- MAGIC           destino.dat_fim_corte                      = origem.dat_fim_corte                     ,
-- MAGIC           destino.dat_primeiro_pagamento             = origem.dat_primeiro_pagamento            ,
-- MAGIC           destino.val_transferido_cartao             = origem.val_transferido_cartao            ,
-- MAGIC           destino.num_percentual_transferido_cartao  = origem.num_percentual_transferido_cartao ,
-- MAGIC           destino.val_credito_retido                 = origem.val_credito_retido                ,
-- MAGIC           destino.val_receita_antecipacao_vr         = origem.val_receita_antecipacao_vr        ,
-- MAGIC           destino.val_receita_antecipacao_cpp        = origem.val_receita_antecipacao_cpp       ,
-- MAGIC           destino.val_receita                        = origem.val_receita                       ,
-- MAGIC           destino.dsc_tipo_antecipacao               = origem.dsc_tipo_antecipacao              ,
-- MAGIC           --destino.val_tarifa_antecipacao             = origem.val_tarifa_antecipacao            ,
-- MAGIC           destino.val_taxa_bpo                       = origem.val_taxa_bpo                      ,
-- MAGIC           destino.val_mdr_vr                         = origem.val_mdr_vr                        ,
-- MAGIC           destino.val_mdr_cpp                        = origem.val_mdr_cpp                       ,
-- MAGIC           destino.val_repasse_mdr_bpo                = origem.val_repasse_mdr_bpo               ,
-- MAGIC           destino.val_receita_cpp                    = origem.val_receita_cpp                   ,
-- MAGIC           destino.val_receita_mdr_vr                 = origem.val_receita_mdr_vr                ,
-- MAGIC           destino.val_receita_mdr_cpp                = origem.val_receita_mdr_cpp               ,
-- MAGIC           destino.val_taxa_mdr                       = origem.val_taxa_mdr                      ,
-- MAGIC           destino.val_receita_vr                     = origem.val_receita_vr                    ,
-- MAGIC           destino.cod_ciclo_reembolso                = origem.cod_ciclo_reembolso               ,
-- MAGIC           destino.dsc_ciclo_reembolso                = origem.dsc_ciclo_reembolso               ,
-- MAGIC           destino.dsc_operador_usuario               = origem.dsc_operador_usuario              ,
-- MAGIC           destino.dsc_origem_antecipacao             = origem.dsc_origem_antecipacao            ,
-- MAGIC           destino.dsc_responsavel_antecipacao        = origem.dsc_responsavel_antecipacao       ,
-- MAGIC           destino.dat_vencimento                     = origem.dat_vencimento                    , 
-- MAGIC           destino.num_conta_sistema                  = origem.num_conta_sistema                 ,   
-- MAGIC           destino.dsc_categoria_produto              = origem.dsc_categoria_produto             ,
-- MAGIC           destino.flg_produto_pat                    = origem.flg_produto_pat                   ,
-- MAGIC           destino.cod_hash_md5                       = origem.cod_hash_md5                      ,
-- MAGIC           destino.dat_alteracao_bi                   = current_date()     
-- MAGIC      WHEN NOT MATCHED THEN INSERT (  
-- MAGIC           destino.dat_referencia                    ,
-- MAGIC           destino.srk_ec_cadastro                   ,
-- MAGIC           destino.num_guia_reembolso                ,
-- MAGIC           destino.qtd_alteracao_guia                ,
-- MAGIC           destino.cod_sdprod                        ,
-- MAGIC           destino.cod_produto                       ,
-- MAGIC           destino.num_ec_conta                      ,
-- MAGIC           destino.num_identificador_transacao       ,
-- MAGIC           destino.val_reembolso_bruto               ,
-- MAGIC           destino.val_reembolso_bruto_vr            ,
-- MAGIC           destino.val_reembolso_bruto_cpp           ,
-- MAGIC           destino.val_reembolso_liquido             ,
-- MAGIC           destino.val_reembolso_liquido_vr          ,
-- MAGIC           destino.val_reembolso_liquido_cpp         ,
-- MAGIC           destino.val_estorno                       ,
-- MAGIC           destino.val_receita_produto_sva           ,
-- MAGIC           destino.val_receita_pacote_vrg            ,
-- MAGIC           destino.val_receita_tarifa_doc            ,
-- MAGIC           destino.val_receita_tarifa_operacional    ,
-- MAGIC           destino.val_receita_tarifa_adesao         ,
-- MAGIC           destino.val_receita_tarifa_anuidade       ,
-- MAGIC           destino.val_taxa_ajuste                   ,
-- MAGIC           destino.val_receita_mdr                   ,
-- MAGIC           destino.val_receita_antecipacao           ,
-- MAGIC           destino.val_taxa_antecipacao              ,
-- MAGIC           destino.qtd_dia_antecipacao               ,
-- MAGIC           destino.qtd_tentativa_pagamento           ,
-- MAGIC           destino.dat_reembolso_efetivo             ,
-- MAGIC           destino.dat_criacao_reembolso             ,
-- MAGIC           destino.dat_inicio_corte                  ,
-- MAGIC           destino.dat_fim_corte                     ,
-- MAGIC           destino.dat_primeiro_pagamento            ,
-- MAGIC           destino.val_transferido_cartao            ,
-- MAGIC           destino.num_percentual_transferido_cartao ,
-- MAGIC           destino.val_credito_retido                ,
-- MAGIC           destino.val_receita_antecipacao_vr        ,
-- MAGIC           destino.val_receita_antecipacao_cpp       ,
-- MAGIC           destino.val_receita                       ,
-- MAGIC           destino.dsc_tipo_antecipacao              ,
-- MAGIC           --destino.val_tarifa_antecipacao            ,
-- MAGIC           destino.val_taxa_bpo                      ,
-- MAGIC           destino.val_mdr_vr                        ,
-- MAGIC           destino.val_mdr_cpp                       ,
-- MAGIC           destino.val_repasse_mdr_bpo               ,
-- MAGIC           destino.val_receita_cpp                   ,
-- MAGIC           destino.val_receita_mdr_vr                ,
-- MAGIC           destino.val_receita_mdr_cpp               ,
-- MAGIC           destino.val_taxa_mdr                      ,
-- MAGIC           destino.val_receita_vr                    ,
-- MAGIC           destino.cod_ciclo_reembolso               ,
-- MAGIC           destino.dsc_ciclo_reembolso               ,
-- MAGIC           destino.dsc_operador_usuario              ,
-- MAGIC           destino.dsc_origem_antecipacao            ,
-- MAGIC           destino.dsc_responsavel_antecipacao       ,
-- MAGIC           destino.dat_vencimento                    ,
-- MAGIC           destino.num_conta_sistema                 ,
-- MAGIC           destino.dsc_categoria_produto             ,
-- MAGIC           destino.flg_produto_pat                   ,
-- MAGIC           destino.cod_hash_md5                      ,
-- MAGIC           destino.dat_inclusao_bi                   ,
-- MAGIC           destino.dat_alteracao_bi
-- MAGIC           )
-- MAGIC           VALUES (
-- MAGIC           origem.dat_referencia                    ,
-- MAGIC           origem.srk_ec_cadastro                   ,
-- MAGIC           origem.num_guia                          ,
-- MAGIC           origem.num_alteracoes_guia               ,
-- MAGIC           origem.cod_sdprod                        ,
-- MAGIC           origem.cod_produto                       ,
-- MAGIC           origem.num_ec_conta                      ,
-- MAGIC           origem.num_identificador_transacao       ,
-- MAGIC           origem.val_reembolso_bruto               ,
-- MAGIC           origem.val_reembolso_bruto_vr            ,
-- MAGIC           origem.val_reembolso_bruto_cpp           ,
-- MAGIC           origem.val_reembolso_liquido             ,
-- MAGIC           origem.val_reembolso_liquido_vr          ,
-- MAGIC           origem.val_reembolso_liquido_cpp         ,
-- MAGIC           origem.val_estorno                       ,
-- MAGIC           origem.val_receita_produto_sva           ,
-- MAGIC           origem.val_receita_pacote_vrg            ,
-- MAGIC           origem.val_receita_tarifa_doc            ,
-- MAGIC           origem.val_receita_tarifa_operacional    ,
-- MAGIC           origem.val_receita_tarifa_adesao         ,
-- MAGIC           origem.val_receita_tarifa_anuidade       ,
-- MAGIC           origem.val_taxa_ajuste                   ,
-- MAGIC           origem.val_receita_mdr                   ,
-- MAGIC           origem.val_receita_antecipacao           ,
-- MAGIC           origem.val_taxa_antecipacao              ,
-- MAGIC           origem.qtd_dia_antecipacao               ,
-- MAGIC           origem.qtd_tentativa_pagamento           ,
-- MAGIC           origem.dat_reembolso_efetivo             ,
-- MAGIC           origem.dat_criacao_reembolso             ,
-- MAGIC           origem.dat_inicio_corte                  ,
-- MAGIC           origem.dat_fim_corte                     ,
-- MAGIC           origem.dat_primeiro_pagamento            ,
-- MAGIC           origem.val_transferido_cartao            ,
-- MAGIC           origem.num_percentual_transferido_cartao ,
-- MAGIC           origem.val_credito_retido                ,
-- MAGIC           origem.val_receita_antecipacao_vr        ,
-- MAGIC           origem.val_receita_antecipacao_cpp       ,
-- MAGIC           origem.val_receita                       ,
-- MAGIC           origem.dsc_tipo_antecipacao              ,
-- MAGIC           --origem.val_tarifa_antecipacao            ,
-- MAGIC           origem.val_taxa_bpo                      ,
-- MAGIC           origem.val_mdr_vr                        ,
-- MAGIC           origem.val_mdr_cpp                       ,
-- MAGIC           origem.val_repasse_mdr_bpo               ,
-- MAGIC           origem.val_receita_cpp                   ,
-- MAGIC           origem.val_receita_mdr_vr                ,
-- MAGIC           origem.val_receita_mdr_cpp               ,
-- MAGIC           origem.val_taxa_mdr                      ,
-- MAGIC           origem.val_receita_vr                    ,
-- MAGIC           origem.cod_ciclo_reembolso               ,
-- MAGIC           origem.dsc_ciclo_reembolso               ,
-- MAGIC           origem.dsc_operador_usuario              ,
-- MAGIC           origem.dsc_origem_antecipacao            ,
-- MAGIC           origem.dsc_responsavel_antecipacao       ,
-- MAGIC           origem.dat_vencimento                    ,
-- MAGIC           origem.num_conta_sistema                 ,
-- MAGIC           origem.dsc_categoria_produto             ,
-- MAGIC           origem.flg_produto_pat                   ,
-- MAGIC           origem.cod_hash_md5                      ,
-- MAGIC           current_date()                           ,       
-- MAGIC           current_date()      
-- MAGIC           )"""
-- MAGIC       )

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
