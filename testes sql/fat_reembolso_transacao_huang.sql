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
-- MAGIC # dat_parametro = 2026-03-02
-- MAGIC # database = gold_vr_cliente_ec
-- MAGIC # databricks_catalog (DEV) = dev
-- MAGIC # databricks_catalog (PROD) = prod
-- MAGIC # dsc_tabela_pii = fat_reembolso_transacao
-- MAGIC # malha = temp
-- MAGIC # tablename = fat_reembolso_transacao

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC - gold_vr_cliente_ec.fat_reembolso
-- MAGIC - gold_vr_cliente_ec.dim_cadastro
-- MAGIC - gold_vr_cliente_ec.dim_endereco
-- MAGIC - gold_vr_cliente_trabalhador.fat_transacao_consumo
-- MAGIC - gold_vr_cliente_trabalhador.fat_transacao_estorno

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

-- DBTITLE 1,tmp_dim_endereco
create or replace temporary view tmp_dim_endereco as 
Select 
  dim_endereco.srk_ec_cadastro, 
  dim_endereco.num_cep,
  dim_endereco.dsc_cidade,
  dim_endereco.sig_estado,
  dim_endereco.dsc_regiao,
  dim_endereco.flg_endereco_primario, 
  dim_endereco.num_ordenacao
from (
      Select 
      dim_endereco.srk_ec_cadastro, 
      dim_endereco.num_cep,
      dim_endereco.dsc_cidade,
      dim_endereco.sig_estado,
      dim_endereco.dsc_regiao,
      dim_endereco.flg_endereco_primario, 
      dim_endereco.num_ordenacao,
      row_number() over(partition by srk_ec_cadastro order by flg_endereco_primario desc, num_ordenacao asc) as rank 
      from gold_vr_cliente_ec.dim_endereco
    ) dim_endereco
where rank = 1

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW tmp_fat_reembolso AS
-- MAGIC SELECT * 
-- MAGIC   FROM gold_vr_cliente_ec.fat_reembolso
-- MAGIC  WHERE nvl(fat_reembolso.dat_referencia,cast('""" + dat_parametro + """' as date)) in (
-- MAGIC                                                                                       add_months(trunc(cast('""" + dat_parametro + """' as date) -1, 'MM'), -2)
-- MAGIC                                                                                       , add_months(trunc(cast('""" + dat_parametro + """' as date) -1, 'MM'), -1)
-- MAGIC                                                                                       , trunc(cast('""" + dat_parametro + """' as date) -1, 'MM')
-- MAGIC                                                                                       , add_months(trunc(cast('""" + dat_parametro + """' as date) -1, 'MM'), 1)
-- MAGIC                                                                                       )
-- MAGIC --TESTE
-- MAGIC --and num_guia_reembolso = 696235357
-- MAGIC """)
-- MAGIC spark.sql("CACHE TABLE tmp_fat_reembolso")
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_transacao
create or replace temporary view tmp_reembolso_transacao as

with tmp_transacao as (
  select trunc(fat_reembolso.dat_primeiro_pagamento,'MM') as dat_referencia,
         fat_reembolso.num_guia_reembolso,
         fat_reembolso.qtd_alteracao_guia,
         fat_transacao_consumo.num_transacao,
         fat_transacao_consumo.cod_sdprod as cod_sdprod_transacao,
         fat_transacao_consumo.cod_produto as cod_produto_transacao,
         'Consumo' as dsc_tipo_transacao,
         fat_reembolso.srk_ec_cadastro,
         fat_transacao_consumo.srk_trabalhador_cadastro,
         fat_transacao_consumo.srk_rh_cadastro,
         fat_reembolso.cod_produto,
         fat_reembolso.cod_sdprod,
         fat_transacao_consumo.cod_emissor,
         fat_reembolso.dat_primeiro_pagamento,
         round((fat_transacao_consumo.val_transacao_consumo / fat_reembolso.val_reembolso_bruto),12) as val_percentual,
         round((fat_transacao_consumo.val_transacao_consumo / fat_reembolso.val_reembolso_bruto_vr), 12) as val_percentual_vr,
         fat_transacao_consumo.dat_transacao,
         fat_transacao_consumo.val_transacao_consumo as val_transacao_credito,
         fat_reembolso.val_estorno * -1 as val_estorno,
         fat_reembolso.val_reembolso_bruto as val_bruto,
         fat_reembolso.val_reembolso_bruto_vr as  val_bruto_vr,
         fat_reembolso.val_reembolso_liquido as val_reembolso_liquido,
         fat_reembolso.val_reembolso_liquido as val_pagamento_reembolso,
         fat_reembolso.val_receita_produto_sva as val_produto_sva,
         fat_reembolso.val_receita_tarifa_doc as val_tarifa_doc,
         fat_reembolso.val_receita_tarifa_operacional as val_receita_tarifa_operacional,
         fat_reembolso.val_receita_tarifa_adesao as val_tarifa_adesao,
         fat_reembolso.val_receita_tarifa_anuidade as val_tarifa_anuidade,
         fat_reembolso.val_receita_mdr as val_comissao,
         fat_reembolso.val_taxa_antecipacao,
         fat_reembolso.val_transferido_cartao as val_transferido_compras,
         fat_reembolso.val_transferido_cartao as val_transferido_compras_percentual,
         fat_reembolso.val_credito_retido,
         case when trim(lower(dsc_tipo_antecipacao)) = 'automática' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_automatico,
         case when trim(lower(dsc_tipo_antecipacao)) = 'eventual' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_eventual,
         fat_reembolso.cod_ciclo_reembolso,
         fat_reembolso.dsc_ciclo_reembolso,
         fat_reembolso.qtd_dia_antecipacao as num_dia_antecipacao,
         fat_transacao_consumo.num_filiacao,
         fat_transacao_consumo.dsc_rede_captura,
         fat_transacao_consumo.cod_sequencial_sncore,
         fat_reembolso.dsc_tipo_antecipacao,
         fat_transacao_consumo.dsc_tecnologia,
         fat_transacao_consumo.val_transacao_consumo,
         fat_reembolso.val_reembolso_bruto,      
         fat_transacao_consumo.srk_trabalhador_cartao,
         fat_transacao_consumo.srk_trabalhador_conta
    from tmp_fat_reembolso fat_reembolso
   inner join gold_vr_cliente_trabalhador.fat_transacao_consumo 
      on fat_reembolso.num_guia_reembolso = fat_transacao_consumo.num_guia
     and fat_reembolso.qtd_alteracao_guia = fat_transacao_consumo.num_guia_reinicializacao
   where fat_transacao_consumo.cod_produto not in ('BN-REFEICAO-SRV', '034-MAMAE-I-SRV')
UNION ALL
  select trunc(fat_reembolso.dat_primeiro_pagamento,'MM') as dat_referencia,
         fat_reembolso.num_guia_reembolso,
         fat_reembolso.qtd_alteracao_guia,
         fat_transacao_estorno.num_transacao,
         fat_transacao_estorno.cod_sdprod as cod_sdprod_transacao,
         fat_transacao_estorno.cod_produto as cod_produto_transacao,
         'Estorno' as dsc_tipo_transacao,
         fat_reembolso.srk_ec_cadastro,
         fat_transacao_estorno.srk_trabalhador_cadastro,
         fat_transacao_estorno.srk_rh_cadastro,
         fat_reembolso.cod_produto,
         fat_reembolso.cod_sdprod,
         fat_transacao_estorno.cod_emissor,
         fat_reembolso.dat_primeiro_pagamento,
         round((fat_transacao_estorno.val_transacao_estorno / fat_reembolso.val_reembolso_bruto) * -1,12) as val_percentual,
         round((fat_transacao_estorno.val_transacao_estorno / fat_reembolso.val_reembolso_bruto_vr)* -1, 12) as val_percentual_vr,
         fat_transacao_estorno.dat_transacao,
         fat_transacao_estorno.val_transacao_estorno * -1 as val_transacao,
         fat_reembolso.val_estorno * -1 as val_estorno,
         fat_reembolso.val_reembolso_bruto as val_bruto,
         fat_reembolso.val_reembolso_bruto_vr as  val_bruto_vr,
         fat_reembolso.val_reembolso_liquido as val_reembolso_liquido,
         fat_reembolso.val_reembolso_liquido as val_pagamento_reembolso,
         fat_reembolso.val_receita_produto_sva as val_produto_sva,
         fat_reembolso.val_receita_tarifa_doc as val_tarifa_doc,
         fat_reembolso.val_receita_tarifa_operacional as val_receita_tarifa_operacional,
         fat_reembolso.val_receita_tarifa_adesao as val_tarifa_adesao,
         fat_reembolso.val_receita_tarifa_anuidade as val_tarifa_anuidade,
         fat_reembolso.val_receita_mdr as val_comissao,
         fat_reembolso.val_taxa_antecipacao,
         fat_reembolso.val_transferido_cartao as val_transferido_compras,
         fat_reembolso.val_transferido_cartao as val_transferido_compras_percentual,
         fat_reembolso.val_credito_retido,
         case when trim(lower(dsc_tipo_antecipacao)) = 'automática' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_automatico,
         case when trim(lower(dsc_tipo_antecipacao)) = 'eventual' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_eventual,
         fat_reembolso.cod_ciclo_reembolso,
         fat_reembolso.dsc_ciclo_reembolso,
         fat_reembolso.qtd_dia_antecipacao as num_dia_antecipacao,
         fat_transacao_estorno.num_filiacao,
         fat_transacao_estorno.dsc_rede_captura,
         fat_transacao_estorno.cod_sequencial_sncore,
         fat_reembolso.dsc_tipo_antecipacao,
         fat_transacao_estorno.dsc_tecnologia,
         0 as val_transacao_consumo,
         fat_reembolso.val_reembolso_bruto,
         fat_transacao_estorno.srk_trabalhador_cartao,
         fat_transacao_estorno.srk_trabalhador_conta
    from tmp_fat_reembolso fat_reembolso
   inner join gold_vr_cliente_trabalhador.fat_transacao_estorno
      on fat_reembolso.num_guia_reembolso = fat_transacao_estorno.num_guia
     and fat_reembolso.qtd_alteracao_guia = fat_transacao_estorno.num_guia_reinicializacao
   where fat_transacao_estorno.cod_produto not in ('BN-REFEICAO-SRV', '034-MAMAE-I-SRV')
)
, tmp_transacao_caixa as (
  select trunc(fat_reembolso.dat_primeiro_pagamento,'MM') as dat_referencia,
         fat_reembolso.num_guia_reembolso,
         fat_reembolso.qtd_alteracao_guia,
         fat_transacao_consumo.num_transacao,
         fat_transacao_consumo.cod_sdprod as cod_sdprod_transacao,
         fat_transacao_consumo.cod_produto as cod_produto_transacao,
         'Consumo' as dsc_tipo_transacao,
         fat_reembolso.srk_ec_cadastro,
         -99 AS srk_trabalhador_cadastro,
         -99 AS srk_rh_cadastro,
         fat_reembolso.cod_produto,
         fat_reembolso.cod_sdprod,
         fat_transacao_consumo.cod_emissor,
         fat_reembolso.dat_primeiro_pagamento,
         round((fat_transacao_consumo.val_transacao_consumo / fat_reembolso.val_reembolso_bruto),12) as val_percentual,
         round((fat_transacao_consumo.val_transacao_consumo / fat_reembolso.val_reembolso_bruto_vr), 12) as val_percentual_vr,
         fat_transacao_consumo.dat_transacao,
         fat_transacao_consumo.val_transacao_consumo as val_transacao_credito,
         fat_reembolso.val_estorno * -1 as val_estorno,
         fat_reembolso.val_reembolso_bruto as val_bruto,
         fat_reembolso.val_reembolso_bruto_vr as  val_bruto_vr,
         fat_reembolso.val_reembolso_liquido as val_reembolso_liquido,
         fat_reembolso.val_reembolso_liquido as val_pagamento_reembolso,
         fat_reembolso.val_receita_produto_sva as val_produto_sva,
         fat_reembolso.val_receita_tarifa_doc as val_tarifa_doc,
         fat_reembolso.val_receita_tarifa_operacional as val_receita_tarifa_operacional,
         fat_reembolso.val_receita_tarifa_adesao as val_tarifa_adesao,
         fat_reembolso.val_receita_tarifa_anuidade as val_tarifa_anuidade,
         fat_reembolso.val_receita_mdr as val_comissao,
         fat_reembolso.val_taxa_antecipacao,
         fat_reembolso.val_transferido_cartao as val_transferido_compras,
         fat_reembolso.val_transferido_cartao as val_transferido_compras_percentual,
         fat_reembolso.val_credito_retido,
         case when trim(lower(dsc_tipo_antecipacao)) = 'automática' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_automatico,
         case when trim(lower(dsc_tipo_antecipacao)) = 'eventual' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_eventual,
         fat_reembolso.cod_ciclo_reembolso,
         fat_reembolso.dsc_ciclo_reembolso,
         fat_reembolso.qtd_dia_antecipacao as num_dia_antecipacao,
         fat_transacao_consumo.num_filiacao,
         fat_transacao_consumo.dsc_rede_captura,
         fat_transacao_consumo.cod_sequencial_sncore,
         fat_reembolso.dsc_tipo_antecipacao,
         fat_transacao_consumo.dsc_tecnologia,
         fat_transacao_consumo.val_transacao_consumo,
         fat_reembolso.val_reembolso_bruto,
         fat_transacao_consumo.srk_trabalhador_cartao,
         fat_transacao_consumo.srk_trabalhador_conta
    from tmp_fat_reembolso fat_reembolso
   inner join gold_jvcef_cliente_trabalhador.fat_transacao_consumo 
      on fat_reembolso.num_guia_reembolso = fat_transacao_consumo.num_guia
     and fat_reembolso.qtd_alteracao_guia = fat_transacao_consumo.num_resets
   where fat_transacao_consumo.cod_produto not in ('BN-REFEICAO-SRV', '034-MAMAE-I-SRV')
UNION ALL
  select trunc(fat_reembolso.dat_primeiro_pagamento,'MM') as dat_referencia,
         fat_reembolso.num_guia_reembolso,
         fat_reembolso.qtd_alteracao_guia,
         fat_transacao_estorno.num_transacao,
         fat_transacao_estorno.cod_sdprod as cod_sdprod_transacao,
         fat_transacao_estorno.cod_produto as cod_produto_transacao,
         'Estorno' as dsc_tipo_transacao,
         fat_reembolso.srk_ec_cadastro,
         -99 AS srk_trabalhador_cadastro,
         -99 AS srk_rh_cadastro,
         fat_reembolso.cod_produto,
         fat_reembolso.cod_sdprod,
         fat_transacao_estorno.cod_emissor,
         fat_reembolso.dat_primeiro_pagamento,
         round((fat_transacao_estorno.val_transacao_estorno / fat_reembolso.val_reembolso_bruto) * -1,12) as val_percentual,
         round((fat_transacao_estorno.val_transacao_estorno / fat_reembolso.val_reembolso_bruto_vr)* -1, 12) as val_percentual_vr,
         fat_transacao_estorno.dat_transacao,
         fat_transacao_estorno.val_transacao_estorno * -1 as val_transacao,
         fat_reembolso.val_estorno * -1 as val_estorno,
         fat_reembolso.val_reembolso_bruto as val_bruto,
         fat_reembolso.val_reembolso_bruto_vr as  val_bruto_vr,
         fat_reembolso.val_reembolso_liquido as val_reembolso_liquido,
         fat_reembolso.val_reembolso_liquido as val_pagamento_reembolso,
         fat_reembolso.val_receita_produto_sva as val_produto_sva,
         fat_reembolso.val_receita_tarifa_doc as val_tarifa_doc,
         fat_reembolso.val_receita_tarifa_operacional as val_receita_tarifa_operacional,
         fat_reembolso.val_receita_tarifa_adesao as val_tarifa_adesao,
         fat_reembolso.val_receita_tarifa_anuidade as val_tarifa_anuidade,
         fat_reembolso.val_receita_mdr as val_comissao,
         fat_reembolso.val_taxa_antecipacao,
         fat_reembolso.val_transferido_cartao as val_transferido_compras,
         fat_reembolso.val_transferido_cartao as val_transferido_compras_percentual,
         fat_reembolso.val_credito_retido,
         case when trim(lower(dsc_tipo_antecipacao)) = 'automática' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_automatico,
         case when trim(lower(dsc_tipo_antecipacao)) = 'eventual' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_eventual,
         fat_reembolso.cod_ciclo_reembolso,
         fat_reembolso.dsc_ciclo_reembolso,
         fat_reembolso.qtd_dia_antecipacao as num_dia_antecipacao,
         fat_transacao_estorno.num_filiacao,
         fat_transacao_estorno.dsc_rede_captura,
         fat_transacao_estorno.cod_sequencial_sncore,
         fat_reembolso.dsc_tipo_antecipacao,
         fat_transacao_estorno.dsc_tecnologia,
         0 as val_transacao_consumo,
         fat_reembolso.val_reembolso_bruto,
         fat_transacao_estorno.srk_trabalhador_cartao,
         fat_transacao_estorno.srk_trabalhador_conta
    from tmp_fat_reembolso fat_reembolso
   inner join gold_jvcef_cliente_trabalhador.fat_transacao_estorno
      on fat_reembolso.num_guia_reembolso = fat_transacao_estorno.num_guia
     and fat_reembolso.qtd_alteracao_guia = fat_transacao_estorno.num_resets
   where fat_transacao_estorno.cod_produto not in ('BN-REFEICAO-SRV', '034-MAMAE-I-SRV')
), tmp_transacao_thomson as (
  select trunc(fat_reembolso.dat_primeiro_pagamento,'MM') as dat_referencia,
         fat_reembolso.num_guia_reembolso,
         fat_reembolso.qtd_alteracao_guia,
         fat_transacao_consumo.num_transacao,
         fat_transacao_consumo.cod_sdprod as cod_sdprod_transacao,
         fat_transacao_consumo.cod_produto as cod_produto_transacao,
         'Consumo' as dsc_tipo_transacao,
         fat_reembolso.srk_ec_cadastro,
         -98 AS srk_trabalhador_cadastro,
         -98 AS srk_rh_cadastro,
         fat_reembolso.cod_produto,
         fat_reembolso.cod_sdprod,
         fat_transacao_consumo.cod_emissor,
         fat_reembolso.dat_primeiro_pagamento,
         round((fat_transacao_consumo.val_transacao_consumo / fat_reembolso.val_reembolso_bruto),12) as val_percentual,
         round((fat_transacao_consumo.val_transacao_consumo / fat_reembolso.val_reembolso_bruto_vr), 12) as val_percentual_vr,
         fat_transacao_consumo.dat_transacao,
         fat_transacao_consumo.val_transacao_consumo as val_transacao_credito,
         fat_reembolso.val_estorno * -1 as val_estorno,
         fat_reembolso.val_reembolso_bruto as val_bruto,
         fat_reembolso.val_reembolso_bruto_vr as  val_bruto_vr,
         fat_reembolso.val_reembolso_liquido as val_reembolso_liquido,
         fat_reembolso.val_reembolso_liquido as val_pagamento_reembolso,
         fat_reembolso.val_receita_produto_sva as val_produto_sva,
         fat_reembolso.val_receita_tarifa_doc as val_tarifa_doc,
         fat_reembolso.val_receita_tarifa_operacional as val_receita_tarifa_operacional,
         fat_reembolso.val_receita_tarifa_adesao as val_tarifa_adesao,
         fat_reembolso.val_receita_tarifa_anuidade as val_tarifa_anuidade,
         fat_reembolso.val_receita_mdr as val_comissao,
         fat_reembolso.val_taxa_antecipacao,
         fat_reembolso.val_transferido_cartao as val_transferido_compras,
         fat_reembolso.val_transferido_cartao as val_transferido_compras_percentual,
         fat_reembolso.val_credito_retido,
         case when trim(lower(dsc_tipo_antecipacao)) = 'automática' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_automatico,
         case when trim(lower(dsc_tipo_antecipacao)) = 'eventual' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_eventual,
         fat_reembolso.cod_ciclo_reembolso,
         fat_reembolso.dsc_ciclo_reembolso,
         fat_reembolso.qtd_dia_antecipacao as num_dia_antecipacao,
         fat_transacao_consumo.num_filiacao,
         fat_transacao_consumo.dsc_rede_captura,
         fat_transacao_consumo.cod_sequencial_sncore,
         fat_reembolso.dsc_tipo_antecipacao,
         fat_transacao_consumo.dsc_tecnologia,
         fat_transacao_consumo.val_transacao_consumo,
         fat_reembolso.val_reembolso_bruto,
         fat_transacao_consumo.srk_trabalhador_cartao,
         fat_transacao_consumo.srk_trabalhador_conta
    from tmp_fat_reembolso fat_reembolso
   inner join gold_thomr_cliente_trabalhador.fat_transacao_consumo 
      on fat_reembolso.num_guia_reembolso = fat_transacao_consumo.num_guia
     and fat_reembolso.qtd_alteracao_guia = fat_transacao_consumo.num_resets
   where fat_transacao_consumo.cod_produto not in ('BN-REFEICAO-SRV', '034-MAMAE-I-SRV')
UNION ALL
  select trunc(fat_reembolso.dat_primeiro_pagamento,'MM') as dat_referencia,
         fat_reembolso.num_guia_reembolso,
         fat_reembolso.qtd_alteracao_guia,
         fat_transacao_estorno.num_transacao,
         fat_transacao_estorno.cod_sdprod as cod_sdprod_transacao,
         fat_transacao_estorno.cod_produto as cod_produto_transacao,
         'Estorno' as dsc_tipo_transacao,
         fat_reembolso.srk_ec_cadastro,
         -98 AS srk_trabalhador_cadastro,
         -98 AS srk_rh_cadastro,
         fat_reembolso.cod_produto,
         fat_reembolso.cod_sdprod,
         fat_transacao_estorno.cod_emissor,
         fat_reembolso.dat_primeiro_pagamento,
         round((fat_transacao_estorno.val_transacao_estorno / fat_reembolso.val_reembolso_bruto) * -1,12) as val_percentual,
         round((fat_transacao_estorno.val_transacao_estorno / fat_reembolso.val_reembolso_bruto_vr)* -1, 12) as val_percentual_vr,
         fat_transacao_estorno.dat_transacao,
         fat_transacao_estorno.val_transacao_estorno * -1 as val_transacao,
         fat_reembolso.val_estorno * -1 as val_estorno,
         fat_reembolso.val_reembolso_bruto as val_bruto,
         fat_reembolso.val_reembolso_bruto_vr as  val_bruto_vr,
         fat_reembolso.val_reembolso_liquido as val_reembolso_liquido,
         fat_reembolso.val_reembolso_liquido as val_pagamento_reembolso,
         fat_reembolso.val_receita_produto_sva as val_produto_sva,
         fat_reembolso.val_receita_tarifa_doc as val_tarifa_doc,
         fat_reembolso.val_receita_tarifa_operacional as val_receita_tarifa_operacional,
         fat_reembolso.val_receita_tarifa_adesao as val_tarifa_adesao,
         fat_reembolso.val_receita_tarifa_anuidade as val_tarifa_anuidade,
         fat_reembolso.val_receita_mdr as val_comissao,
         fat_reembolso.val_taxa_antecipacao,
         fat_reembolso.val_transferido_cartao as val_transferido_compras,
         fat_reembolso.val_transferido_cartao as val_transferido_compras_percentual,
         fat_reembolso.val_credito_retido,
         case when trim(lower(dsc_tipo_antecipacao)) = 'automática' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_automatico,
         case when trim(lower(dsc_tipo_antecipacao)) = 'eventual' then fat_reembolso.val_receita_antecipacao else null end as val_tarifa_antecipacao_eventual,
         fat_reembolso.cod_ciclo_reembolso,
         fat_reembolso.dsc_ciclo_reembolso,
         fat_reembolso.qtd_dia_antecipacao as num_dia_antecipacao,
         fat_transacao_estorno.num_filiacao,
         fat_transacao_estorno.dsc_rede_captura,
         fat_transacao_estorno.cod_sequencial_sncore,
         fat_reembolso.dsc_tipo_antecipacao,
         fat_transacao_estorno.dsc_tecnologia,
         0 as val_transacao_consumo,
         fat_reembolso.val_reembolso_bruto,
         fat_transacao_estorno.srk_trabalhador_cartao,
         fat_transacao_estorno.srk_trabalhador_conta
    from tmp_fat_reembolso fat_reembolso
   inner join gold_thomr_cliente_trabalhador.fat_transacao_estorno
      on fat_reembolso.num_guia_reembolso = fat_transacao_estorno.num_guia
     and fat_reembolso.qtd_alteracao_guia = fat_transacao_estorno.num_resets
   where fat_transacao_estorno.cod_produto not in ('BN-REFEICAO-SRV', '034-MAMAE-I-SRV')
)
select tmp_transacao.dat_referencia,
       tmp_transacao.num_guia_reembolso,
       tmp_transacao.qtd_alteracao_guia,
       tmp_transacao.num_transacao,
       tmp_transacao.cod_sdprod_transacao,
       tmp_transacao.cod_produto_transacao,
       tmp_transacao.dsc_tipo_transacao,
       tmp_transacao.srk_ec_cadastro,
       tmp_transacao.srk_trabalhador_cadastro,
       tmp_transacao.srk_rh_cadastro,
       tmp_transacao.cod_produto,
       tmp_transacao.cod_sdprod,
       tmp_transacao.cod_emissor,
       dim_cadastro.cod_tipo_ec,
       dim_cadastro.dsc_tipo_ec,
       tmp_transacao.dat_primeiro_pagamento,
       (case when tmp_transacao.val_estorno = tmp_transacao.val_transacao_credito then 0 else val_percentual end)  as val_percentual,
       (case when tmp_transacao.val_estorno = tmp_transacao.val_transacao_credito then 0 else val_percentual_vr end) as val_percentual_vr,
       tmp_transacao.dat_transacao,
       tmp_transacao.val_transacao_credito,
       tmp_transacao.val_bruto,
       tmp_transacao.val_bruto_vr,
       tmp_transacao.val_reembolso_liquido,
       tmp_transacao.val_pagamento_reembolso,
       tmp_transacao.val_produto_sva,
       tmp_transacao.val_tarifa_doc,
       tmp_transacao.val_receita_tarifa_operacional,
       tmp_transacao.val_tarifa_adesao,
       tmp_transacao.val_tarifa_anuidade,
       tmp_transacao.val_comissao,
       tmp_transacao.val_taxa_antecipacao,
       tmp_transacao.val_transferido_compras,
       tmp_transacao.val_transferido_compras_percentual,
       tmp_transacao.val_credito_retido,
       tmp_transacao.val_tarifa_antecipacao_automatico,
       tmp_transacao.val_tarifa_antecipacao_eventual,
       dim_endereco.num_cep,
       dim_endereco.dsc_cidade,
       dim_endereco.sig_estado,
       dim_endereco.dsc_regiao,
       tmp_transacao.cod_ciclo_reembolso,
       tmp_transacao.dsc_ciclo_reembolso,
       tmp_transacao.num_dia_antecipacao,
       tmp_transacao.dsc_rede_captura,
       tmp_transacao.num_filiacao,
       tmp_transacao.cod_sequencial_sncore,
       tmp_transacao.dsc_tipo_antecipacao,
       tmp_transacao.dsc_tecnologia,
       tmp_transacao.val_transacao_consumo,
       tmp_transacao.val_reembolso_bruto,
       tmp_transacao.srk_trabalhador_cartao,
       tmp_transacao.srk_trabalhador_conta
  from (select * from tmp_transacao union all select * from tmp_transacao_caixa union all select * from tmp_transacao_thomson) tmp_transacao
 inner join gold_vr_cliente_ec.dim_cadastro
    on tmp_transacao.srk_ec_cadastro = dim_cadastro.srk_ec_cadastro
   and dim_cadastro.flg_ativo = 1
  left join tmp_dim_endereco dim_endereco
    on dim_cadastro.srk_ec_cadastro = dim_endereco.srk_ec_cadastro

-- COMMAND ----------

-- DBTITLE 1,tmp_reembolso_transacao_calculo
create or replace temporary view tmp_reembolso_transacao_calculo as
select tmp_reembolso_transacao.dat_referencia,
       tmp_reembolso_transacao.num_guia_reembolso,
       tmp_reembolso_transacao.qtd_alteracao_guia,
       tmp_reembolso_transacao.num_transacao,
       tmp_reembolso_transacao.cod_sdprod_transacao,
       tmp_reembolso_transacao.cod_produto_transacao,
       tmp_reembolso_transacao.dsc_tipo_transacao,
       tmp_reembolso_transacao.srk_ec_cadastro,
       tmp_reembolso_transacao.srk_trabalhador_cadastro,
       tmp_reembolso_transacao.srk_rh_cadastro,
       tmp_reembolso_transacao.cod_produto,
       tmp_reembolso_transacao.cod_sdprod,
       tmp_reembolso_transacao.cod_emissor,
       tmp_reembolso_transacao.cod_tipo_ec,
       tmp_reembolso_transacao.dsc_tipo_ec,
       tmp_reembolso_transacao.dat_primeiro_pagamento,
       tmp_reembolso_transacao.val_transacao_credito,
       tmp_reembolso_transacao.val_transacao_credito as val_reembolso_bruto,
       round(tmp_reembolso_transacao.val_reembolso_liquido * tmp_reembolso_transacao.val_percentual,5) as val_reembolso_liquido,
       round(tmp_reembolso_transacao.val_pagamento_reembolso * tmp_reembolso_transacao.val_percentual,5) as val_pagamento,
       round(tmp_reembolso_transacao.val_produto_sva * tmp_reembolso_transacao.val_percentual_vr,5) as val_sva,
       round(tmp_reembolso_transacao.val_tarifa_doc * tmp_reembolso_transacao.val_percentual_vr,5) as val_doc,
       round(nvl(tmp_reembolso_transacao.val_receita_tarifa_operacional,0)  * tmp_reembolso_transacao.val_percentual,5) as val_tarifa_operacional,
       round(tmp_reembolso_transacao.val_tarifa_adesao * tmp_reembolso_transacao.val_percentual_vr,5) as val_adesao,
       round(tmp_reembolso_transacao.val_tarifa_anuidade * tmp_reembolso_transacao.val_percentual_vr,5) as val_anuidade,
       round(tmp_reembolso_transacao.val_comissao * tmp_reembolso_transacao.val_percentual,5) as val_comissao,
       round(tmp_reembolso_transacao.val_taxa_antecipacao * tmp_reembolso_transacao.val_percentual_vr,5) as val_taxa_antecipacao,
       round(tmp_reembolso_transacao.val_transferido_compras * tmp_reembolso_transacao.val_percentual_vr,5) as val_transferido_compras,
       round(tmp_reembolso_transacao.val_transferido_compras_percentual * tmp_reembolso_transacao.val_percentual_vr,5) as val_transferido_compras_percentual,
       round(tmp_reembolso_transacao.val_credito_retido * tmp_reembolso_transacao.val_percentual_vr,5) as val_credito_retido,
       round(tmp_reembolso_transacao.val_tarifa_antecipacao_automatico * tmp_reembolso_transacao.val_percentual,5) as val_antecipacao_automatico,
       (round(tmp_reembolso_transacao.val_tarifa_antecipacao_automatico * tmp_reembolso_transacao.val_percentual,5)/ tmp_reembolso_transacao.val_bruto) * 100 as val_taxa_antecipacao_automatico,
       round(tmp_reembolso_transacao.val_tarifa_antecipacao_eventual * tmp_reembolso_transacao.val_percentual,5) as val_antecipacao_eventual,
       (round(tmp_reembolso_transacao.val_tarifa_antecipacao_eventual * tmp_reembolso_transacao.val_percentual,5)/ tmp_reembolso_transacao.val_bruto) * 100 as val_taxa_antecipacao_eventual,
       round(tmp_reembolso_transacao.val_percentual * 100,10) as val_percentual,
       round(tmp_reembolso_transacao.val_percentual_vr * 100,10) as val_percentual_vr,
       tmp_reembolso_transacao.num_cep,
       tmp_reembolso_transacao.dsc_cidade,
       tmp_reembolso_transacao.sig_estado,
       tmp_reembolso_transacao.dsc_regiao,
       tmp_reembolso_transacao.cod_ciclo_reembolso,
       tmp_reembolso_transacao.dsc_ciclo_reembolso,
       tmp_reembolso_transacao.num_dia_antecipacao,
       tmp_reembolso_transacao.dsc_rede_captura,
       tmp_reembolso_transacao.num_filiacao,
       tmp_reembolso_transacao.cod_sequencial_sncore,
       tmp_reembolso_transacao.dsc_tipo_antecipacao,
       tmp_reembolso_transacao.dsc_tecnologia,
       nvl(cast(round(tmp_reembolso_transacao.val_tarifa_antecipacao_automatico * (tmp_reembolso_transacao.val_percentual) ,5) as decimal(18,2)),0) as val_receita_antecipacao_automatico,
       nvl(cast(round(tmp_reembolso_transacao.val_tarifa_antecipacao_eventual * (tmp_reembolso_transacao.val_percentual) ,5) as decimal(18,2)),0) as val_receita_antecipacao_eventual,
       tmp_reembolso_transacao.val_tarifa_antecipacao_automatico,
       tmp_reembolso_transacao.val_tarifa_antecipacao_eventual,
       tmp_reembolso_transacao.srk_trabalhador_cartao,
       tmp_reembolso_transacao.srk_trabalhador_conta
  from tmp_reembolso_transacao



-- COMMAND ----------

-- DBTITLE 1,tmp_receita
create or replace temporary view tmp_receita as

select dat_referencia,
       num_guia_reembolso,
       qtd_alteracao_guia,
       num_transacao,
       cod_sdprod_transacao,
       cod_produto_transacao,
       dsc_tipo_transacao,
       srk_ec_cadastro,
       srk_trabalhador_cadastro,
       srk_rh_cadastro,
       cod_produto,
       cod_sdprod,
       cod_emissor,
       cod_tipo_ec,
       dsc_tipo_ec,
       dat_primeiro_pagamento,
       val_transacao_credito,
       val_reembolso_bruto,
       val_reembolso_liquido,
       val_pagamento,
       val_sva,
       val_doc,
       val_tarifa_operacional,
       val_adesao,
       val_anuidade,
       val_comissao,
       val_taxa_antecipacao,
       val_transferido_compras,
       val_transferido_compras_percentual,
       val_credito_retido,
       val_antecipacao_automatico,
       val_taxa_antecipacao_automatico,
       val_antecipacao_eventual,
       val_taxa_antecipacao_eventual,
       val_percentual,
       val_percentual_vr,
       (nvl(val_comissao,0) + nvl(val_doc,0) + nvl(val_adesao,0) + nvl(val_anuidade,0) + nvl(val_antecipacao_automatico,0) + nvl(val_antecipacao_eventual,0)) as val_receita,
       num_cep,
       dsc_cidade,
       sig_estado,
       dsc_regiao,
       cod_ciclo_reembolso,
       dsc_ciclo_reembolso,
       num_dia_antecipacao,
       dsc_rede_captura,
       num_filiacao,
       cod_sequencial_sncore,
       dsc_tipo_antecipacao,
       dsc_tecnologia,
       val_receita_antecipacao_automatico,
       val_receita_antecipacao_eventual,
       val_tarifa_antecipacao_automatico,
       val_tarifa_antecipacao_eventual,
       srk_trabalhador_cartao,
       srk_trabalhador_conta

  from tmp_reembolso_transacao_calculo


-- COMMAND ----------

-- DBTITLE 1,tmp_md5_reembolso_transacao
create or replace temporary view tmp_md5_reembolso_transacao as

Select tmp_receita.*,
       md5(nvl(tmp_receita.dat_referencia,'') ||
           nvl(tmp_receita.num_guia_reembolso,'') ||
           nvl(tmp_receita.qtd_alteracao_guia,'') ||
           nvl(tmp_receita.num_transacao,'') ||
           nvl(tmp_receita.cod_sdprod_transacao,'') ||
           nvl(tmp_receita.cod_produto_transacao,'') ||
           nvl(tmp_receita.dsc_tipo_transacao,'') ||
           nvl(tmp_receita.srk_ec_cadastro,'') ||
           nvl(tmp_receita.srk_trabalhador_cadastro,'') ||
           nvl(tmp_receita.srk_rh_cadastro,'') ||
           nvl(tmp_receita.cod_produto,'') ||
           nvl(tmp_receita.cod_sdprod,'') ||
           nvl(tmp_receita.cod_emissor,'') ||
           nvl(tmp_receita.cod_tipo_ec,'') ||
           nvl(tmp_receita.dsc_tipo_ec,'') ||
           nvl(tmp_receita.dat_primeiro_pagamento,'') ||
           nvl(tmp_receita.val_transacao_credito,'') ||
           nvl(tmp_receita.val_reembolso_bruto,'') ||
           nvl(tmp_receita.val_reembolso_liquido,'') ||
           nvl(tmp_receita.val_pagamento,'') ||
           nvl(tmp_receita.val_sva,'') ||
           nvl(tmp_receita.val_doc,'') ||
           nvl(tmp_receita.val_tarifa_operacional,'') ||
           nvl(tmp_receita.val_adesao,'') ||
           nvl(tmp_receita.val_anuidade,'') ||
           nvl(tmp_receita.val_comissao,'') ||
           nvl(tmp_receita.val_taxa_antecipacao,'') ||
           nvl(tmp_receita.val_transferido_compras,'') ||
           nvl(tmp_receita.val_transferido_compras_percentual,'') ||
           nvl(tmp_receita.val_credito_retido,'') ||
           nvl(tmp_receita.val_antecipacao_automatico,'') ||
           nvl(tmp_receita.val_taxa_antecipacao_automatico,'') ||
           nvl(tmp_receita.val_antecipacao_eventual,'') ||
           nvl(tmp_receita.val_taxa_antecipacao_eventual,'') ||
           nvl(tmp_receita.val_percentual,'') ||
           nvl(tmp_receita.val_percentual_vr,'') ||
           nvl(tmp_receita.val_receita,'') ||
           nvl(tmp_receita.num_cep,'') ||
           nvl(tmp_receita.dsc_cidade,'') ||
           nvl(tmp_receita.sig_estado,'') ||
           nvl(tmp_receita.dsc_regiao,'') ||
           nvl(tmp_receita.cod_ciclo_reembolso,'') ||
           nvl(tmp_receita.dsc_ciclo_reembolso,'') ||
           nvl(tmp_receita.num_dia_antecipacao,'') ||
           nvl(tmp_receita.dsc_rede_captura,'') ||
           nvl(tmp_receita.num_filiacao,'') ||
           nvl(tmp_receita.cod_sequencial_sncore,'') ||
           nvl(tmp_receita.dsc_tipo_antecipacao,'') ||
           nvl(tmp_receita.dsc_tecnologia,'') ||
           nvl(tmp_receita.val_receita_antecipacao_automatico,'') ||
           nvl(tmp_receita.val_receita_antecipacao_eventual,'') ||
           nvl(tmp_receita.val_tarifa_antecipacao_automatico,'') ||
           nvl(tmp_receita.val_tarifa_antecipacao_eventual,'') ||
           nvl(tmp_receita.srk_trabalhador_cartao,'') ||
           nvl(tmp_receita.srk_trabalhador_conta,'')
       ) as cod_hash_md5
  from tmp_receita
;

-- COMMAND ----------

-- DBTITLE 1,Padroniza campos
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select dat_referencia,
-- MAGIC        num_guia_reembolso,
-- MAGIC        qtd_alteracao_guia,
-- MAGIC        num_transacao,
-- MAGIC        cod_sdprod_transacao,
-- MAGIC        cod_produto_transacao,
-- MAGIC        dsc_tipo_transacao,
-- MAGIC        srk_ec_cadastro,
-- MAGIC        srk_trabalhador_cadastro,
-- MAGIC        srk_rh_cadastro,
-- MAGIC        cod_produto,
-- MAGIC        cod_sdprod,
-- MAGIC        cod_emissor,
-- MAGIC        cod_tipo_ec,
-- MAGIC        dsc_tipo_ec,
-- MAGIC        dat_primeiro_pagamento,
-- MAGIC        val_transacao_credito,
-- MAGIC        val_reembolso_bruto,
-- MAGIC        val_reembolso_liquido,
-- MAGIC        val_pagamento,
-- MAGIC        val_sva,
-- MAGIC        val_doc,
-- MAGIC        val_tarifa_operacional,
-- MAGIC        val_adesao,
-- MAGIC        val_anuidade,
-- MAGIC        val_comissao,
-- MAGIC        val_taxa_antecipacao,
-- MAGIC        val_transferido_compras,
-- MAGIC        val_transferido_compras_percentual,
-- MAGIC        val_credito_retido,
-- MAGIC        val_antecipacao_automatico,
-- MAGIC        val_taxa_antecipacao_automatico,
-- MAGIC        val_antecipacao_eventual,
-- MAGIC        val_taxa_antecipacao_eventual,
-- MAGIC        val_percentual,
-- MAGIC        val_percentual_vr,
-- MAGIC        val_receita,
-- MAGIC        num_cep,
-- MAGIC        dsc_cidade,
-- MAGIC        sig_estado,
-- MAGIC        dsc_regiao,
-- MAGIC        cod_ciclo_reembolso,
-- MAGIC        dsc_ciclo_reembolso,
-- MAGIC        num_dia_antecipacao,
-- MAGIC        dsc_rede_captura,
-- MAGIC        num_filiacao,
-- MAGIC        cod_sequencial_sncore,
-- MAGIC        dsc_tipo_antecipacao,
-- MAGIC        dsc_tecnologia,
-- MAGIC        val_receita_antecipacao_automatico,
-- MAGIC        val_receita_antecipacao_eventual,
-- MAGIC        val_tarifa_antecipacao_automatico,
-- MAGIC        val_tarifa_antecipacao_eventual,
-- MAGIC        srk_trabalhador_cartao,
-- MAGIC        srk_trabalhador_conta,
-- MAGIC        cod_hash_md5
-- MAGIC   from tmp_md5_reembolso_transacao
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_reembolso_transacao_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Merge fat_reembolso_transacao
-- MAGIC %python
-- MAGIC df_merge = fn_merge_direto("""
-- MAGIC     MERGE INTO gold_vr_cliente_ec_pii.fat_reembolso_transacao as destino
-- MAGIC       USING tmp_reembolso_transacao_criptografado as origem
-- MAGIC          ON origem.num_guia_reembolso = destino.num_guia_reembolso
-- MAGIC         AND origem.qtd_alteracao_guia = destino.qtd_alteracao_guia
-- MAGIC         AND origem.num_transacao = destino.num_transacao
-- MAGIC       WHEN MATCHED AND nvl(origem.cod_hash_md5,'') <> nvl(destino.cod_hash_md5,'') THEN UPDATE SET
-- MAGIC            destino.dat_referencia                     = origem.dat_referencia,
-- MAGIC            destino.srk_ec_cadastro                    = origem.srk_ec_cadastro,
-- MAGIC            destino.srk_trabalhador_cadastro           = origem.srk_trabalhador_cadastro,
-- MAGIC            destino.srk_trabalhador_cartao             = origem.srk_trabalhador_cartao,
-- MAGIC            destino.srk_trabalhador_conta              = origem.srk_trabalhador_conta,
-- MAGIC            destino.srk_rh_cadastro                    = origem.srk_rh_cadastro,
-- MAGIC            destino.cod_sdprod_transacao               = origem.cod_sdprod_transacao,
-- MAGIC            destino.cod_produto_transacao              = origem.cod_produto_transacao,
-- MAGIC            destino.cod_emissor                        = origem.cod_emissor,
-- MAGIC            destino.cod_tipo_ec                        = origem.cod_tipo_ec,
-- MAGIC            destino.dsc_tipo_ec                        = origem.dsc_tipo_ec,
-- MAGIC            destino.dat_primeiro_pagamento             = origem.dat_primeiro_pagamento,
-- MAGIC            destino.val_transacao                      = origem.val_transacao_credito,
-- MAGIC            destino.val_reembolso_bruto                = origem.val_reembolso_bruto,
-- MAGIC            destino.val_reembolso_liquido              = origem.val_reembolso_liquido,
-- MAGIC            destino.val_pagamento                      = origem.val_pagamento,
-- MAGIC            destino.val_sva                            = origem.val_sva,
-- MAGIC            destino.val_doc                            = origem.val_doc,
-- MAGIC            destino.val_tarifa_operacional             = origem.val_tarifa_operacional, 
-- MAGIC            destino.val_adesao                         = origem.val_adesao,
-- MAGIC            destino.val_anuidade                       = origem.val_anuidade,
-- MAGIC            destino.val_comissao                       = origem.val_comissao,
-- MAGIC            destino.val_taxa_antecipacao               = origem.val_taxa_antecipacao,
-- MAGIC            destino.val_transferido_compras            = origem.val_transferido_compras,
-- MAGIC            destino.val_transferido_compras_percentual = origem.val_transferido_compras_percentual,
-- MAGIC            destino.val_credito_retido                 = origem.val_credito_retido,
-- MAGIC            destino.val_antecipacao_automatico         = origem.val_antecipacao_automatico,
-- MAGIC            destino.val_taxa_antecipacao_automatico    = origem.val_taxa_antecipacao_automatico,
-- MAGIC            destino.val_antecipacao_eventual           = origem.val_antecipacao_eventual,
-- MAGIC            destino.val_taxa_antecipacao_eventual      = origem.val_taxa_antecipacao_eventual,
-- MAGIC            destino.num_percentual_reembolso           = origem.val_percentual,
-- MAGIC            destino.val_percentual_reembolso_vr        = origem.val_percentual_vr,
-- MAGIC            destino.val_receita                        = origem.val_receita,
-- MAGIC            destino.num_cep                            = origem.num_cep,
-- MAGIC            destino.dsc_cidade                         = origem.dsc_cidade,
-- MAGIC            destino.sig_estado                         = origem.sig_estado,
-- MAGIC            destino.dsc_regiao                         = origem.dsc_regiao,
-- MAGIC            destino.cod_ciclo_reembolso                = origem.cod_ciclo_reembolso,
-- MAGIC            destino.dsc_ciclo_reembolso                = origem.dsc_ciclo_reembolso,
-- MAGIC            destino.num_dia_antecipacao                = origem.num_dia_antecipacao,
-- MAGIC            destino.dsc_rede_captura                   = origem.dsc_rede_captura,
-- MAGIC            destino.num_filiacao                       = origem.num_filiacao,
-- MAGIC            destino.cod_sequencial_sncore              = origem.cod_sequencial_sncore,
-- MAGIC            destino.dsc_tipo_antecipacao               = origem.dsc_tipo_antecipacao,
-- MAGIC            destino.dsc_tecnologia_transacao           = origem.dsc_tecnologia,
-- MAGIC            destino.val_receita_antecipacao_automatico = origem.val_receita_antecipacao_automatico,
-- MAGIC            destino.val_receita_antecipacao_eventual   = origem.val_receita_antecipacao_eventual,
-- MAGIC            destino.val_tarifa_antecipacao_automatico   = origem.val_tarifa_antecipacao_automatico,
-- MAGIC           destino.val_tarifa_antecipacao_eventual      = origem.val_tarifa_antecipacao_eventual,
-- MAGIC            destino.cod_hash_md5                       = origem.cod_hash_md5,
-- MAGIC            destino.dat_alteracao_bi                   = current_timestamp()
-- MAGIC         WHEN NOT MATCHED THEN INSERT (
-- MAGIC            destino.dat_referencia,
-- MAGIC            destino.num_guia_reembolso,
-- MAGIC            destino.qtd_alteracao_guia,
-- MAGIC            destino.num_transacao,
-- MAGIC            destino.cod_sdprod_transacao,
-- MAGIC            destino.cod_produto_transacao,
-- MAGIC            destino.dsc_tipo_transacao,
-- MAGIC            destino.srk_ec_cadastro,
-- MAGIC            destino.srk_trabalhador_cadastro,
-- MAGIC            destino.srk_trabalhador_cartao,
-- MAGIC            destino.srk_trabalhador_conta,
-- MAGIC            destino.srk_rh_cadastro,
-- MAGIC            destino.cod_produto,
-- MAGIC            destino.cod_sdprod,
-- MAGIC            destino.cod_emissor,
-- MAGIC            destino.cod_tipo_ec,
-- MAGIC            destino.dsc_tipo_ec,
-- MAGIC            destino.dat_primeiro_pagamento,
-- MAGIC            destino.val_transacao,
-- MAGIC            destino.val_reembolso_bruto,
-- MAGIC            destino.val_reembolso_liquido,
-- MAGIC            destino.val_pagamento,
-- MAGIC            destino.val_sva,
-- MAGIC            destino.val_doc,
-- MAGIC            destino.val_tarifa_operacional,
-- MAGIC            destino.val_adesao,
-- MAGIC            destino.val_anuidade,
-- MAGIC            destino.val_comissao,
-- MAGIC            destino.val_taxa_antecipacao,
-- MAGIC            destino.val_transferido_compras,
-- MAGIC            destino.val_transferido_compras_percentual,
-- MAGIC            destino.val_credito_retido,
-- MAGIC            destino.val_antecipacao_automatico,
-- MAGIC            destino.val_taxa_antecipacao_automatico,
-- MAGIC            destino.val_tarifa_antecipacao_automatico,
-- MAGIC            destino.val_receita_antecipacao_automatico,
-- MAGIC            destino.val_antecipacao_eventual,
-- MAGIC            destino.val_taxa_antecipacao_eventual,
-- MAGIC            destino.val_tarifa_antecipacao_eventual,
-- MAGIC            destino.val_receita_antecipacao_eventual,
-- MAGIC            destino.num_percentual_reembolso,
-- MAGIC            destino.val_percentual_reembolso_vr,
-- MAGIC            destino.val_receita,
-- MAGIC            destino.num_cep,
-- MAGIC            destino.dsc_cidade,
-- MAGIC            destino.sig_estado,
-- MAGIC            destino.dsc_regiao,
-- MAGIC            destino.cod_ciclo_reembolso,
-- MAGIC            destino.dsc_ciclo_reembolso,
-- MAGIC            destino.num_dia_antecipacao,
-- MAGIC            destino.dsc_rede_captura,
-- MAGIC            destino.num_filiacao,
-- MAGIC            destino.cod_sequencial_sncore,
-- MAGIC            destino.dsc_tipo_antecipacao,
-- MAGIC            destino.dsc_tecnologia_transacao,       
-- MAGIC            destino.cod_hash_md5,
-- MAGIC            destino.dat_inclusao_bi,
-- MAGIC            destino.dat_alteracao_bi
-- MAGIC          ) VALUES (
-- MAGIC            origem.dat_referencia,
-- MAGIC            origem.num_guia_reembolso,
-- MAGIC            origem.qtd_alteracao_guia,
-- MAGIC            origem.num_transacao,
-- MAGIC            origem.cod_sdprod_transacao,
-- MAGIC            origem.cod_produto_transacao,
-- MAGIC            origem.dsc_tipo_transacao,
-- MAGIC            origem.srk_ec_cadastro,
-- MAGIC            origem.srk_trabalhador_cadastro,
-- MAGIC            origem.srk_trabalhador_cartao,
-- MAGIC            origem.srk_trabalhador_conta,
-- MAGIC            origem.srk_rh_cadastro,
-- MAGIC            origem.cod_produto,
-- MAGIC            origem.cod_sdprod,
-- MAGIC            origem.cod_emissor,
-- MAGIC            origem.cod_tipo_ec,
-- MAGIC            origem.dsc_tipo_ec,
-- MAGIC            origem.dat_primeiro_pagamento,
-- MAGIC            origem.val_transacao_credito,
-- MAGIC            origem.val_reembolso_bruto,
-- MAGIC            origem.val_reembolso_liquido,
-- MAGIC            origem.val_pagamento,
-- MAGIC            origem.val_sva,
-- MAGIC            origem.val_doc,
-- MAGIC            origem.val_tarifa_operacional,
-- MAGIC            origem.val_adesao,
-- MAGIC            origem.val_anuidade,
-- MAGIC            origem.val_comissao,
-- MAGIC            origem.val_taxa_antecipacao,
-- MAGIC            origem.val_transferido_compras,
-- MAGIC            origem.val_transferido_compras_percentual,
-- MAGIC            origem.val_credito_retido,
-- MAGIC            origem.val_antecipacao_automatico,
-- MAGIC            origem.val_taxa_antecipacao_automatico,
-- MAGIC            origem.val_tarifa_antecipacao_automatico,
-- MAGIC            origem.val_receita_antecipacao_automatico,
-- MAGIC            origem.val_antecipacao_eventual,
-- MAGIC            origem.val_taxa_antecipacao_eventual,
-- MAGIC            origem.val_tarifa_antecipacao_eventual,
-- MAGIC            origem.val_receita_antecipacao_eventual, 
-- MAGIC            origem.val_percentual,
-- MAGIC            origem.val_percentual_vr,
-- MAGIC            origem.val_receita,
-- MAGIC            origem.num_cep,
-- MAGIC            origem.dsc_cidade,
-- MAGIC            origem.sig_estado,
-- MAGIC            origem.dsc_regiao,
-- MAGIC            origem.cod_ciclo_reembolso,
-- MAGIC            origem.dsc_ciclo_reembolso,
-- MAGIC            origem.num_dia_antecipacao,
-- MAGIC            origem.dsc_rede_captura,
-- MAGIC            origem.num_filiacao,
-- MAGIC            origem.cod_sequencial_sncore,
-- MAGIC            origem.dsc_tipo_antecipacao,
-- MAGIC            origem.dsc_tecnologia,        
-- MAGIC            origem.cod_hash_md5,
-- MAGIC            current_timestamp(),
-- MAGIC            current_timestamp()
-- MAGIC          )"""
-- MAGIC       )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC try:
-- MAGIC     spark.sql("UNCACHE TABLE tmp_fat_reembolso")
-- MAGIC except Exception:
-- MAGIC     pass
-- MAGIC spark.sql("DROP VIEW IF EXISTS tmp_fat_reembolso")

-- COMMAND ----------

-- DBTITLE 1,Segura
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
-- MAGIC fn_gera_arquivo_bastao(malha,database,tablename)
-- MAGIC fn_exit_notebook(df_merge)
