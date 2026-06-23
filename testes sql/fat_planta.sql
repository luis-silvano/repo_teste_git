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
-- MAGIC # dat_parametro = 2026-02-26
-- MAGIC # database = gold_vr_cliente_rh
-- MAGIC # databricks_catalog (DEV) = dev
-- MAGIC # databricks_catalog (PRD) = prod
-- MAGIC # dsc_tabela_pii = fat_planta
-- MAGIC # location_catalogo_temp (DEV) = abfss://temp@adlsvrbeneficiosdev01.dfs.core.windows.net
-- MAGIC # location_catalogo_temp (PRD) = abfss://temp@adlsvrbeneficiosprd.dfs.core.windows.net
-- MAGIC # malha = temp
-- MAGIC # tablename = fat_planta

-- COMMAND ----------

-- DBTITLE 1,Arquivos de passagem de bastão
-- MAGIC %md
-- MAGIC
-- MAGIC - gold_vr_cliente_rh.fat_planta_contrato
-- MAGIC - gold_vr_cliente_rh.dim_contrato
-- MAGIC - gold_vr_cliente_rh.dim_cadastro
-- MAGIC - gold_vr_cliente_rh.dim_produto
-- MAGIC - gold_vr_cliente_rh.fat_transacao_credito
-- MAGIC - gold_vr_cliente_trabalhador.dim_conta
-- MAGIC - vr_parametro.dim_mes

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
-- MAGIC - Nesta fato, temos uma foto mensal dos contratos/valores/quantidade de funcionarios por rh

-- COMMAND ----------

-- DBTITLE 1,tmp_planta_cnpj
create or replace temporary view tmp_planta_cnpj as

with 
tmp_segmentacao_mes as 
(Select dim_mes.dat_mes as dat_referencia,
         dim_segmentacao.srk_rh_cadastro,
         dim_segmentacao.cod_canal_venda,
         dim_segmentacao.dsc_canal_venda,
         dim_segmentacao.cod_subcanal,
         dim_segmentacao.dsc_subcanal,
         dim_segmentacao.dat_inicio_vigencia
    from gold_vr_cliente_rh.dim_segmentacao
    join prod.vr_parametro.dim_mes
      on dim_mes.dat_mes >= trunc(dim_segmentacao.dat_inicio_vigencia,'MM')
     and dim_mes.dat_mes <= trunc(cast('${dat_parametro}' as date), 'MM') 
   where dim_mes.dat_mes >= trunc(add_months(cast('${dat_parametro}' as date), -8), 'MM')
     and dim_segmentacao.dsc_tipo_segmentacao = 'Entrada'
)


,tmp_segmento_mes as 
(Select dim_mes.dat_mes as dat_referencia,
         dim_canal_venda_segmento.cod_canal_venda,
         dim_canal_venda_segmento.dsc_segmento_comercial,
         dim_canal_venda_segmento.dsc_subsegmento_comercial,
         dim_canal_venda_segmento.dsc_torre_comercial,
         dim_canal_venda_segmento.dat_inicio_vigencia
    from gold_vr_cliente_rh.dim_canal_venda_segmento
    join prod.vr_parametro.dim_mes
      on dim_mes.dat_mes >= trunc(dim_canal_venda_segmento.dat_inicio_vigencia,'MM')
     and dim_mes.dat_mes <= trunc(cast('${dat_parametro}' as date), 'MM') 
   where dim_mes.dat_mes >= trunc(add_months(cast('${dat_parametro}' as date), -8), 'MM')
     and dim_canal_venda_segmento.dsc_ilha_usuario is null
     and dim_canal_venda_segmento.dsc_perfil_criador_proposta is null
)

--Obtém o segmento com maior data de início de vigência dentro do mês corrente
,tmp_segmento_max_vigencia as 
(Select tmp_segmento_mes.dat_referencia,
         tmp_segmento_mes.cod_canal_venda,
         max(tmp_segmento_mes.dat_inicio_vigencia) as dat_inicio_vigencia
    from tmp_segmento_mes
   group by tmp_segmento_mes.dat_referencia,
         tmp_segmento_mes.cod_canal_venda
)

,tmp_segmento as 
(select tmp_segmento_mes.dat_referencia,
         tmp_segmento_mes.cod_canal_venda,
         tmp_segmento_mes.dsc_segmento_comercial,
         tmp_segmento_mes.dsc_subsegmento_comercial,
         tmp_segmento_mes.dsc_torre_comercial
    from tmp_segmento_mes
   inner join tmp_segmento_max_vigencia
      on tmp_segmento_max_vigencia.cod_canal_venda = tmp_segmento_mes.cod_canal_venda
     and tmp_segmento_max_vigencia.dat_referencia = tmp_segmento_mes.dat_referencia
     and tmp_segmento_max_vigencia.dat_inicio_vigencia = tmp_segmento_mes.dat_inicio_vigencia
)

,tmp_segmento_entrada as
(Select tmp_segmentacao_mes.dat_referencia,
       tmp_segmentacao_mes.srk_rh_cadastro,
       tmp_segmentacao_mes.cod_canal_venda as cod_canal_venda_entrada,
       tmp_segmentacao_mes.dsc_canal_venda as dsc_canal_venda_entrada,
       tmp_segmentacao_mes.cod_subcanal as cod_subcanal_entrada,
       tmp_segmentacao_mes.dsc_subcanal as dsc_subcanal_entrada,
       tmp_segmento.dsc_segmento_comercial as dsc_segmento_comercial_entrada,
       tmp_segmento.dsc_subsegmento_comercial as dsc_subsegmento_comercial_entrada,
       tmp_segmento.dsc_torre_comercial as dsc_torre_comercial_entrada
  from tmp_segmentacao_mes
  left join tmp_segmento
    on tmp_segmento.cod_canal_venda = tmp_segmentacao_mes.cod_canal_venda
   and tmp_segmento.dat_referencia = tmp_segmentacao_mes.dat_referencia
)

,tmp_contrato as 
(select dat_referencia,
       srk_rh_cadastro,
       num_cnpj,
       max(dsc_razao_social) as dsc_razao_social,
       max(cod_canal_venda_alocacao) as cod_canal_venda_alocacao,
       max(dsc_canal_venda_alocacao) as dsc_canal_venda_alocacao,
       max(cod_subcanal_alocacao) as cod_subcanal_alocacao,
       max(dsc_subcanal_alocacao) as dsc_subcanal_alocacao,
       max(dsc_segmento_comercial_alocacao) as dsc_segmento_comercial_alocacao,
       max(dsc_subsegmento_comercial_alocacao) as dsc_subsegmento_comercial_alocacao,
       max(dsc_torre_comercial_alocacao) as dsc_torre_comercial_alocacao,
       cod_emissor as cod_emissor,
       null as dsc_status_contrato,
       max(dsc_cidade_entrega) as dsc_cidade_entrega,
       max(sig_estado_entrega) as sig_estado_entrega,
       max(dsc_regiao_entrega) as dsc_regiao_entrega,
       max(upper(dsc_cidade_cobranca)) as dsc_cidade_cobranca,
       max(sig_estado_cobranca) as sig_estado_cobranca,
       max(dsc_regiao_cobranca) as dsc_regiao_cobranca,
       max(cod_cnae_principal) as cod_cnae_principal,
       max(dsc_ramo_atividade) as dsc_ramo_atividade,
       max(dsc_setor) as dsc_setor,
       max(qtd_funcionario) as qtd_funcionario,
       min(dat_primeiro_credito) as dat_primeiro_credito,
       min(dat_primeiro_credito_produto) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as dat_primeiro_credito_filtro,
       max(dat_ultimo_credito) as dat_ultimo_credito,
       max(dat_ultimo_credito) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as dat_ultimo_credito_filtro,
       max(flg_tipo_pagamento_pos_pago) as flg_tipo_pagamento_pos_pago,
       max(flg_tipo_pagamento_pre_pago) as flg_tipo_pagamento_pre_pago,
       count (distinct srk_rh_contrato) filter (where dsc_status_contrato = 'Ativo') as qtd_contrato_ativo,
       count (flg_tipo_pagamento_pos_pago) filter (where flg_tipo_pagamento_pos_pago = 1) as qtd_contrato_pos_pago,
       count (flg_tipo_pagamento_pre_pago) filter (where flg_tipo_pagamento_pre_pago = 1) as qtd_contrato_pre_pago,
       max(flg_cliente_premium) as flg_cliente_premium,
       max(flg_cliente_farmer) as flg_cliente_farmer,
       max(flg_possui_fidelidade) as flg_possui_fidelidade,
       max(cod_grupo_economico) as cod_grupo_economico,
       max(upper(dsc_grupo_economico)) as dsc_grupo_economico,

       sum(nvl(val_creditado,'0')) as val_creditado,
       sum(nvl(val_creditado_1m,'0')) as val_creditado_m1,
       sum(nvl(val_maximo_creditado_3m,'0')) as val_creditado_3m,
       sum(nvl(qtd_conta_creditada_3m,'0')) as qtd_conta_creditada_3m,
       sum(nvl(qtd_conta_creditada_3m,'0')) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_creditada_filtro_3m,
       sum(nvl(qtd_conta,'0')) as qtd_conta,
       sum(nvl(qtd_conta_status_ativo,'0')) as qtd_conta_status_ativo,
       sum(nvl(qtd_conta_ativada,'0')) as qtd_conta_ativada,
       sum(nvl(qtd_conta_ativada_nova,'0')) as qtd_conta_ativada_nova,
       sum(nvl(qtd_conta_ativada_base,'0')) as qtd_conta_ativada_base,
       sum(nvl(qtd_conta_creditada,'0')) as qtd_conta_creditada,
       sum(nvl(qtd_estoque_conta_ativada,'0')) as qtd_estoque_conta_ativada,
       sum(nvl(val_faturado_bruto,'0')) as val_faturado_bruto,
       sum(nvl(val_faturado_liquido,'0')) as val_faturado_liquido,
       sum(nvl(val_facial_real,'0')) as val_facial_real,
       sum(nvl(num_porte_3m,'0')) as num_porte_3m,
       sum(nvl(num_porte_6m,'0')) as num_porte_6m,
       sum(nvl(val_creditado,'0')) filter (where cod_produto not in ('VRS-VRSAUDE-SRV','NAT-NATAL-SRV')) as val_creditado_extrai_natal_saude,
       sum(nvl(qtd_conta,'0')) filter (where cod_produto not in ('VRS-VRSAUDE-SRV','NAT-NATAL-SRV')) as qtd_conta_extrai_natal_saude,

       -- conceitos novos Planejamento
       sum(nvl(qtd_conta_nova_faseado,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_nova_faseado_filtro,
       sum(nvl(val_creditado_faseado,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_faseado_filtro,
       sum(nvl(val_creditado_sistemica_faseado,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_sistemica_faseado_filtro,
       sum(nvl(val_creditado_alta_comercial,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_sistemica_filtro,
       sum(nvl(val_creditado,'0')) filter (where cod_produto NOT IN('VRS-VRSAUDE-SRV','COM-COMPRAS-SRV')) as val_creditado_extrai_saude_compras,
       sum(nvl(qtd_bop_conta_creditada,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_bop_conta_creditada_filtro,
       sum(nvl(qtd_eop_conta_creditada,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_eop_conta_creditada_filtro,
       sum(nvl(qtd_conta_nova_sistemica,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_nova_sistemica_filtro,
       sum(nvl(qtd_conta_nova_sistemica_faseado,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_nova_sistemica_faseado_filtro,
       sum(nvl(qtd_bop_conta_ativo_40d,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_bop_conta_ativo_filtro_40d,
       sum(nvl(qtd_eop_conta_ativo_40d,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_eop_conta_ativo_filtro_40d,
       sum(nvl(val_creditado_bop_ativo_40d,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_bop_ativo_filtro_40d,
       sum(nvl(val_creditado_eop_ativo_40d,0)) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_eop_ativo_filtro_40d,
       max(dat_ultimo_credito_40d) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as dat_ultimo_credito_filtro_40d,

       --Quantidade de cartões
       round(sum(qtd_cartao_emitido) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')),0) as qtd_cartao_emitido_filtro,
       round(sum(qtd_cartao_emitido_1via) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')),0) as qtd_cartao_emitido_1via_filtro,
       round(sum(qtd_cartao_emitido_novo_1via) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')),0) as qtd_cartao_emitido_novo_1via_filtro,
       round(sum(qtd_cartao_reemitido) filter (where cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')),0) as qtd_cartao_reemitido_filtro,

      -- campos do sergmento de entrada
      max(upper(dsc_segmento_comercial_entrada)) filter (where upper(dsc_segmento_comercial_entrada) in ('RETENÇÃO', 'CS','RETENCAO')) as dsc_segmento_alta_atendiento,
      sum(qtd_conta_nova_sistemica_faseado) filter (where upper(dsc_segmento_comercial_entrada) in ('RETENÇÃO', 'CS','RETENCAO') and cod_produto not in ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_alta_atendimento

  from gold_vr_cliente_rh.fat_planta_contrato
 where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
   and dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
 group by dat_referencia,
       srk_rh_cadastro,
       num_cnpj,
       cod_emissor
)

select tmp_contrato.*,
       tmp_segmento_entrada.cod_canal_venda_entrada,
       tmp_segmento_entrada.dsc_canal_venda_entrada,
       tmp_segmento_entrada.cod_subcanal_entrada,
       tmp_segmento_entrada.dsc_subcanal_entrada,
       tmp_segmento_entrada.dsc_segmento_comercial_entrada,
       tmp_segmento_entrada.dsc_subsegmento_comercial_entrada,
       tmp_segmento_entrada.dsc_torre_comercial_entrada
  from tmp_contrato
  left join tmp_segmento_entrada
            on tmp_segmento_entrada.dat_referencia = tmp_contrato.dat_referencia
           and tmp_segmento_entrada.srk_rh_cadastro = tmp_contrato.srk_rh_cadastro
;

-- COMMAND ----------

-- DBTITLE 1,tmp_fat_credito_3m

--Visão do rh com informação da soma dos 3 meses
create or replace temporary view tmp_fat_credito_3m as

--M0
Select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro) 
             FILTER(WHERE fat_transacao_credito.dat_referencia = trunc(cast('${dat_parametro}' as date), 'MM')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE fat_transacao_credito.dat_referencia = trunc(cast('${dat_parametro}' as date), 'MM')
                      AND fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_3m,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro_3m,
       count(distinct fat_transacao_credito.num_transacao) as qtd_transacao_3m,
       sum(fat_transacao_credito.val_transacao_credito)
           FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro_3m
  from gold_vr_cliente_rh.fat_transacao_credito
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
   and dim_cadastro.flg_ativo = 1
 where fat_transacao_credito.dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
   and fat_transacao_credito.dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor
union
--M-1
Select trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro) 
             FILTER(WHERE fat_transacao_credito.dat_referencia =  trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE fat_transacao_credito.dat_referencia =  trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
                      AND fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_3m,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro_3m,
       count(distinct fat_transacao_credito.num_transacao) as qtd_transacao_3m,
       sum(fat_transacao_credito.val_transacao_credito)
           FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro_3m
  from gold_vr_cliente_rh.fat_transacao_credito
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
   and dim_cadastro.flg_ativo = 1
 where fat_transacao_credito.dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -3), 'MM')
   and fat_transacao_credito.dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor
union
--M-2
Select trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro) 
             FILTER(WHERE fat_transacao_credito.dat_referencia = trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE fat_transacao_credito.dat_referencia = trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
                      AND fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_3m,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro_3m,
       count(distinct fat_transacao_credito.num_transacao) as qtd_transacao_3m,
       sum(fat_transacao_credito.val_transacao_credito)
           FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro_3m
  from gold_vr_cliente_rh.fat_transacao_credito
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
   and dim_cadastro.flg_ativo = 1
 where fat_transacao_credito.dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -4), 'MM')
   and fat_transacao_credito.dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor


-- COMMAND ----------

-- DBTITLE 1,tmp_fat_credito_maximo_3m
--Visão do rh com informação do máximo dos 3 meses
create or replace temporary view tmp_fat_credito_maximo_3m as

with tmp_credito_mes as (
  Select fat_transacao_credito.dat_referencia,
         fat_transacao_credito.srk_rh_cadastro,
         fat_transacao_credito.cod_emissor,
         count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
               FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                        AND dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro,
         count(distinct fat_transacao_credito.srk_trabalhador_conta)
               FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_ativo_filtro_3m,
         sum(fat_transacao_credito.val_transacao_credito)
             FILTER(WHERE fat_transacao_credito.cod_produto NOT IN('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro
    from gold_vr_cliente_rh.fat_transacao_credito
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
     and dim_cadastro.flg_ativo = 1
   where fat_transacao_credito.dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -4), 'MM')
     and fat_transacao_credito.dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by fat_transacao_credito.dat_referencia,
         fat_transacao_credito.srk_rh_cadastro,
         fat_transacao_credito.cod_emissor
)
--M0
Select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
       srk_rh_cadastro,
       cod_emissor,
       max(qtd_trabalhador_ativo_filtro) as qtd_maximo_trabalhador_ativo_filtro_3m,
       max(qtd_conta_ativo_filtro_3m) as qtd_maximo_conta_ativo_filtro_3m,
       max(val_creditado_ativo_filtro) as val_maximo_creditado_ativo_filtro_3m
  from tmp_credito_mes
 where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
   and dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
 group by srk_rh_cadastro,
       cod_emissor
union
--M-1
Select trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM') as dat_referencia,
       srk_rh_cadastro,
       cod_emissor,
       max(qtd_trabalhador_ativo_filtro) as qtd_maximo_trabalhador_ativo_filtro_3m,
       max(qtd_conta_ativo_filtro_3m) as qtd_maximo_conta_ativo_filtro_3m,
       max(val_creditado_ativo_filtro) as val_maximo_creditado_ativo_filtro_3m
  from tmp_credito_mes
 where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -3), 'MM')
   and dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
 group by srk_rh_cadastro,
       cod_emissor
union
--M-2
Select trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM') as dat_referencia,
       srk_rh_cadastro,
       cod_emissor,
       max(qtd_trabalhador_ativo_filtro) as qtd_maximo_trabalhador_ativo_filtro_3m,
       max(qtd_conta_ativo_filtro_3m) as qtd_maximo_conta_ativo_filtro_3m,
       max(val_creditado_ativo_filtro) as val_maximo_creditado_ativo_filtro_3m
  from tmp_credito_mes
 where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -4), 'MM')
   and dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
 group by srk_rh_cadastro,
       cod_emissor


-- COMMAND ----------

-- DBTITLE 1,tmp_fat_credito_maximo_12m
--Visão do rh com informação do máximo dos 12 meses
create or replace temporary view tmp_fat_credito_maximo_12m as

with tmp_transacao_12m as (
  Select fat_transacao_credito.dat_referencia,
         fat_transacao_credito.srk_rh_cadastro,
         fat_transacao_credito.cod_emissor,
         count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
               filter(where cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                        and dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro,
         count(distinct fat_transacao_credito.srk_trabalhador_conta)
               filter(where cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_ativo_filtro,
         sum(fat_transacao_credito.val_transacao_credito) as val_creditado_ativo,
         sum(fat_transacao_credito.val_transacao_credito)
             filter(where cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro
    from gold_vr_cliente_rh.fat_transacao_credito
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
     and dim_cadastro.flg_ativo = 1
   where fat_transacao_credito.dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -13), 'MM')
     and fat_transacao_credito.dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by fat_transacao_credito.dat_referencia,
         fat_transacao_credito.srk_rh_cadastro,
         fat_transacao_credito.cod_emissor
)

, tmp_transacao_maximo_12m as (
  --M0
  Select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
         srk_rh_cadastro,
         cod_emissor,
         max(qtd_trabalhador_ativo_filtro) as qtd_maximo_trabalhador_ativo_filtro_12m,
         max(qtd_conta_ativo_filtro) as qtd_maximo_conta_ativo_filtro_12m,
         max(val_creditado_ativo) as val_maximo_creditado_ativo_12m,
         max(val_creditado_ativo_filtro) as val_maximo_creditado_ativo_filtro_12m
    from tmp_transacao_12m
   where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -11), 'MM')
     and dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by srk_rh_cadastro,
         cod_emissor
  union
  --M-1
  Select trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM') as dat_referencia,
         srk_rh_cadastro,
         cod_emissor,
         max(qtd_trabalhador_ativo_filtro) as qtd_maximo_trabalhador_ativo_filtro_12m,
         max(qtd_conta_ativo_filtro) as qtd_maximo_conta_ativo_filtro_12m,
         max(val_creditado_ativo) as val_maximo_creditado_ativo_12m,
         max(val_creditado_ativo_filtro) as val_maximo_creditado_ativo_filtro_12m
    from tmp_transacao_12m
   where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -12), 'MM')
     and dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
   group by srk_rh_cadastro,
         cod_emissor
  union
  --M-2
  Select trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM') as dat_referencia,
         srk_rh_cadastro,
         cod_emissor,
         max(qtd_trabalhador_ativo_filtro) as qtd_maximo_trabalhador_ativo_filtro_12m,
         max(qtd_conta_ativo_filtro) as qtd_maximo_conta_ativo_filtro_12m,
         max(val_creditado_ativo) as val_maximo_creditado_ativo_12m,
         max(val_creditado_ativo_filtro) as val_maximo_creditado_ativo_filtro_12m
    from tmp_transacao_12m
   where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -13), 'MM')
     and dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
   group by srk_rh_cadastro,
         cod_emissor
)

Select tmp_transacao_maximo_12m.dat_referencia,
       tmp_transacao_maximo_12m.srk_rh_cadastro,
       tmp_transacao_maximo_12m.cod_emissor,
       max(tmp_transacao_maximo_12m.qtd_maximo_trabalhador_ativo_filtro_12m) as qtd_maximo_trabalhador_ativo_filtro_12m,
       max(tmp_transacao_12m.dat_referencia) filter(where tmp_transacao_maximo_12m.qtd_maximo_trabalhador_ativo_filtro_12m = tmp_transacao_12m.qtd_trabalhador_ativo_filtro) as dat_referencia_maximo_trabalhador_ativo_filtro_12m,
       max(tmp_transacao_maximo_12m.qtd_maximo_conta_ativo_filtro_12m) as qtd_maximo_conta_ativo_filtro_12m,
       max(tmp_transacao_12m.dat_referencia) filter(where tmp_transacao_maximo_12m.qtd_maximo_conta_ativo_filtro_12m = tmp_transacao_12m.qtd_conta_ativo_filtro) as dat_referencia_maximo_conta_ativo_filtro_12m,
       max(tmp_transacao_maximo_12m.val_maximo_creditado_ativo_12m) as val_maximo_creditado_ativo_12m,
       max(tmp_transacao_12m.dat_referencia) filter(where tmp_transacao_maximo_12m.val_maximo_creditado_ativo_12m = tmp_transacao_12m.val_creditado_ativo) as dat_referencia_maximo_creditado_ativo_12m,
       max(tmp_transacao_maximo_12m.val_maximo_creditado_ativo_filtro_12m) as val_maximo_creditado_ativo_filtro_12m,
       max(tmp_transacao_12m.dat_referencia) filter(where tmp_transacao_maximo_12m.val_maximo_creditado_ativo_filtro_12m = tmp_transacao_12m.val_creditado_ativo_filtro) as dat_referencia_maximo_creditado_ativo_filtro_12m
  from tmp_transacao_maximo_12m
  left join tmp_transacao_12m
    on tmp_transacao_12m.srk_rh_cadastro = tmp_transacao_maximo_12m.srk_rh_cadastro
   and tmp_transacao_12m.cod_emissor = tmp_transacao_maximo_12m.cod_emissor
   and tmp_transacao_12m.dat_referencia <= tmp_transacao_maximo_12m.dat_referencia 
   and tmp_transacao_12m.dat_referencia >= trunc(add_months(tmp_transacao_maximo_12m.dat_referencia, -11), 'MM')
 group by tmp_transacao_maximo_12m.dat_referencia,
       tmp_transacao_maximo_12m.srk_rh_cadastro,
       tmp_transacao_maximo_12m.cod_emissor


-- COMMAND ----------

-- DBTITLE 1,tmp_fat_credito_40d
create or replace temporary view tmp_fat_credito_40d as

--M0
Select trunc(cast('${dat_parametro}' as date),'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      and  dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro_40d,
       count(distinct fat_transacao_credito.srk_trabalhador_conta)
             filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_ativo_filtro_40d,
       sum(fat_transacao_credito.val_transacao_credito)
           filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro_40d,
       max(fat_transacao_credito.dat_transacao) as dat_ultimo_credito_40d
  from gold_vr_cliente_rh.fat_transacao_credito
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
     and dim_cadastro.flg_ativo = 1
 where fat_transacao_credito.dat_transacao >= date_add(date_add(trunc(add_months(cast('${dat_parametro}' as date), 1),'MM'), 4), -39)
   and fat_transacao_credito.dat_transacao <= date_add(trunc(add_months(cast('${dat_parametro}' as date), 1),'MM'), 4)
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor
union
--M-1
Select trunc(add_months(cast('${dat_parametro}' as date),-1),'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      and  dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro_40d,
       count(distinct fat_transacao_credito.srk_trabalhador_conta)
             filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_ativo_filtro_40d,
       sum(fat_transacao_credito.val_transacao_credito)
           filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro_40d,
       max(fat_transacao_credito.dat_transacao) as dat_ultimo_credito_40d
  from gold_vr_cliente_rh.fat_transacao_credito
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
     and dim_cadastro.flg_ativo = 1
 where fat_transacao_credito.dat_transacao >= date_add(date_add(trunc(cast('${dat_parametro}' as date),'MM'), 4), -39)
   and fat_transacao_credito.dat_transacao <= date_add(trunc(cast('${dat_parametro}' as date),'MM'), 4)
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor
union
--M-2
Select trunc(add_months(cast('${dat_parametro}' as date),-2),'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro)
             filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
                      and  dim_cadastro.num_cpf <> '00000000000') as qtd_trabalhador_ativo_filtro_40d,
       count(distinct fat_transacao_credito.srk_trabalhador_conta)
             filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as qtd_conta_ativo_filtro_40d,
       sum(fat_transacao_credito.val_transacao_credito)
           filter(where fat_transacao_credito.cod_produto not in('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')) as val_creditado_ativo_filtro_40d,
       max(fat_transacao_credito.dat_transacao) as dat_ultimo_credito_40d
  from gold_vr_cliente_rh.fat_transacao_credito
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
     and dim_cadastro.flg_ativo = 1
 where fat_transacao_credito.dat_transacao >= date_add(date_add(trunc(add_months(cast('${dat_parametro}' as date), -1),'MM'), 4), -39)
   and fat_transacao_credito.dat_transacao <= date_add(trunc(add_months(cast('${dat_parametro}' as date), -1),'MM'), 4)
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor


-- COMMAND ----------

-- DBTITLE 1,tmp_quantidade_trabalhador_sem_credito_3m
create or replace temporary view tmp_quantidade_trabalhador_sem_credito_3m as

select dat_referencia,
       srk_rh_cadastro,
       count(distinct srk_trabalhador_cadastro) as qtd_trabalhador_sem_credito_3m
  from (select dat_referencia,
               srk_rh_cadastro,
               srk_trabalhador_cadastro,
               max(dat_ultimo_credito)
          from gold_vr_cliente_trabalhador.fat_planta_conta
         where dat_referencia = trunc(cast('${dat_parametro}' as date), 'MM')
           and num_cpf <> '00000000000'
         group by dat_referencia,
                  srk_rh_cadastro,
                  srk_trabalhador_cadastro
        having trunc(max(dat_ultimo_credito), 'MM') = trunc(add_months(cast('${dat_parametro}' as date), -3), 'MM') 
       )
group by dat_referencia,
         srk_rh_cadastro
union
select dat_referencia,
       srk_rh_cadastro,
       count(distinct srk_trabalhador_cadastro) as qtd_trabalhador_sem_credito_3m
  from (select dat_referencia,
               srk_rh_cadastro,
               srk_trabalhador_cadastro,
               max(dat_ultimo_credito)
          from gold_vr_cliente_trabalhador.fat_planta_conta
         where dat_referencia = trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
           and num_cpf <> '00000000000'
         group by dat_referencia,
                  srk_rh_cadastro,
                  srk_trabalhador_cadastro
        having trunc(max(dat_ultimo_credito), 'MM') = trunc(add_months(cast('${dat_parametro}' as date), -4), 'MM') 
       )
group by dat_referencia,
         srk_rh_cadastro
union
select dat_referencia,
       srk_rh_cadastro,
       count(distinct srk_trabalhador_cadastro) as qtd_trabalhador_sem_credito_3m
  from (select dat_referencia,
               srk_rh_cadastro,
               srk_trabalhador_cadastro,
               max(dat_ultimo_credito)
          from gold_vr_cliente_trabalhador.fat_planta_conta
         where dat_referencia = trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
           and num_cpf <> '00000000000'
         group by dat_referencia,
                  srk_rh_cadastro,
                  srk_trabalhador_cadastro
        having trunc(max(dat_ultimo_credito), 'MM') = trunc(add_months(cast('${dat_parametro}' as date), -5), 'MM') 
       )
group by dat_referencia,
         srk_rh_cadastro

-- COMMAND ----------

-- DBTITLE 1,tmp_quantidade_trabalhador_primeiro_credito
create or replace temporary view tmp_quantidade_trabalhador_primeiro_credito as

with 
tmp_primeiro_credito as (
  Select dim_conta.srk_rh_cadastro,
         dim_conta.srk_trabalhador_cadastro,
         dim_conta.cod_emissor,
         min(dim_conta.dat_primeiro_credito) as dat_primeiro_credito
    from gold_vr_cliente_trabalhador.dim_conta
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_conta.srk_trabalhador_cadastro = dim_cadastro.srk_trabalhador_cadastro
     and dim_conta.flg_ativo = 1
     and dim_cadastro.flg_ativo = 1
   where dim_conta.cod_produto NOT IN ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV')
     and dim_cadastro.num_cpf <> '00000000000'
     and dim_conta.dat_primeiro_credito >= '2013-01-01'
   group by dim_conta.srk_rh_cadastro,
         dim_conta.srk_trabalhador_cadastro,
         dim_conta.cod_emissor
)

--M0
Select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
       srk_rh_cadastro,
       cod_emissor,
       count(distinct srk_trabalhador_cadastro) as qtd_trabalhador_primeiro_credito
  from tmp_primeiro_credito
 where trunc(dat_primeiro_credito, 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
 group by srk_rh_cadastro,
       cod_emissor
union
--M-1
Select trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM') as dat_referencia,
       srk_rh_cadastro,
       cod_emissor,
       count(distinct srk_trabalhador_cadastro) as qtd_trabalhador_primeiro_credito
  from tmp_primeiro_credito
 where trunc(dat_primeiro_credito, 'MM') = trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
 group by srk_rh_cadastro,
       cod_emissor
union
--M-2
Select trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM') as dat_referencia,
       srk_rh_cadastro,
       cod_emissor,
       count(distinct srk_trabalhador_cadastro) as qtd_trabalhador_primeiro_credito
  from tmp_primeiro_credito
 where trunc(dat_primeiro_credito, 'MM') = trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
 group by srk_rh_cadastro,
       cod_emissor
;

-- COMMAND ----------

-- DBTITLE 1,tmp_quantidade_trabalhador_primeiro_credito_regra_antiga
create or replace temporary view tmp_quantidade_trabalhador_primeiro_credito_regra_antiga as

with tmp_primeiro_credito as (
  Select dim_conta.srk_rh_cadastro,
         dim_conta.srk_trabalhador_cadastro,
         dim_conta.cod_produto,
         dim_conta.cod_emissor,
         min(dim_conta.dat_primeiro_credito) as dat_primeiro_credito
    from gold_vr_cliente_trabalhador.dim_conta
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_conta.srk_trabalhador_cadastro = dim_cadastro.srk_trabalhador_cadastro
     and dim_conta.flg_ativo = 1
     and dim_cadastro.flg_ativo = 1
   where dim_conta.cod_produto NOT IN ('COM-COMPRAS-SRV','VRS-VRSAUDE-SRV','NAT-NATAL-SRV','VBT-TRANSPORTE-SRV','AVR-TRANSPORTE-SRV','PRM-MULTPREMIA-SRV')
     and dim_cadastro.num_cpf <> '00000000000'
     and dim_conta.dat_primeiro_credito >= '2013-01-01'
   group by dim_conta.srk_rh_cadastro,
         dim_conta.srk_trabalhador_cadastro,
         dim_conta.cod_produto,
         dim_conta.cod_emissor
)

--M0
Select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
       tmp_primeiro_credito.srk_rh_cadastro,
       tmp_primeiro_credito.cod_emissor,
       count(distinct tmp_primeiro_credito.srk_trabalhador_cadastro) as qtd_trabalhador_primeiro_credito_anterior
  from tmp_primeiro_credito
 inner join gold_vr_cliente_rh.fat_planta_contrato
    on tmp_primeiro_credito.srk_rh_cadastro = fat_planta_contrato.srk_rh_cadastro
   and tmp_primeiro_credito.cod_produto = fat_planta_contrato.cod_produto
   and tmp_primeiro_credito.cod_emissor = fat_planta_contrato.cod_emissor
   and trunc(tmp_primeiro_credito.dat_primeiro_credito,'MM') = fat_planta_contrato.dat_referencia
   and fat_planta_contrato.num_safra in (1,2,3)
 where trunc(tmp_primeiro_credito.dat_primeiro_credito, 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
 group by tmp_primeiro_credito.srk_rh_cadastro,
       tmp_primeiro_credito.cod_emissor
union
--M-1
Select trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM') as dat_referencia,
       tmp_primeiro_credito.srk_rh_cadastro,
       tmp_primeiro_credito.cod_emissor,
       count(distinct tmp_primeiro_credito.srk_trabalhador_cadastro) as qtd_trabalhador_primeiro_credito_anterior
  from tmp_primeiro_credito
 inner join gold_vr_cliente_rh.fat_planta_contrato
    on tmp_primeiro_credito.srk_rh_cadastro = fat_planta_contrato.srk_rh_cadastro
   and tmp_primeiro_credito.cod_produto = fat_planta_contrato.cod_produto
   and tmp_primeiro_credito.cod_emissor = fat_planta_contrato.cod_emissor
   and trunc(tmp_primeiro_credito.dat_primeiro_credito,'MM') = fat_planta_contrato.dat_referencia
   and fat_planta_contrato.num_safra in (1,2,3)
 where trunc(tmp_primeiro_credito.dat_primeiro_credito, 'MM') = trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
 group by tmp_primeiro_credito.srk_rh_cadastro,
       tmp_primeiro_credito.cod_emissor
union
--M-2
Select trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM') as dat_referencia,
       tmp_primeiro_credito.srk_rh_cadastro,
       tmp_primeiro_credito.cod_emissor,
       count(distinct tmp_primeiro_credito.srk_trabalhador_cadastro) as qtd_trabalhador_primeiro_credito_anterior
  from tmp_primeiro_credito
 inner join gold_vr_cliente_rh.fat_planta_contrato
    on tmp_primeiro_credito.srk_rh_cadastro = fat_planta_contrato.srk_rh_cadastro
   and tmp_primeiro_credito.cod_produto = fat_planta_contrato.cod_produto
   and tmp_primeiro_credito.cod_emissor = fat_planta_contrato.cod_emissor
   and trunc(tmp_primeiro_credito.dat_primeiro_credito,'MM') = fat_planta_contrato.dat_referencia
   and fat_planta_contrato.num_safra in (1,2,3)
 where trunc(tmp_primeiro_credito.dat_primeiro_credito, 'MM') = trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
 group by tmp_primeiro_credito.srk_rh_cadastro,
       tmp_primeiro_credito.cod_emissor


-- COMMAND ----------

-- DBTITLE 1,tmp_quantidade_trabalhador
create or replace temporary view tmp_quantidade_trabalhador as

--M0
Select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro) as qtd_trabalhador
  from gold_vr_cliente_rh.fat_transacao_credito
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
   and dim_cadastro.flg_ativo = 1
   and dim_cadastro.num_cpf <> '00000000000'
 where fat_transacao_credito.dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor
union
--M-1
Select trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro) as qtd_trabalhador
  from gold_vr_cliente_rh.fat_transacao_credito
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
   and dim_cadastro.flg_ativo = 1
   and dim_cadastro.num_cpf <> '00000000000'
 where dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor
union
--M-2
Select trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM') as dat_referencia,
       fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor,
       count(distinct fat_transacao_credito.srk_trabalhador_cadastro) as qtd_trabalhador
  from gold_vr_cliente_rh.fat_transacao_credito
 inner join gold_vr_cliente_trabalhador.dim_cadastro
    on dim_cadastro.srk_trabalhador_cadastro = fat_transacao_credito.srk_trabalhador_cadastro
   and dim_cadastro.flg_ativo = 1
   and dim_cadastro.num_cpf <> '00000000000'
 where fat_transacao_credito.dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
 group by fat_transacao_credito.srk_rh_cadastro,
       fat_transacao_credito.cod_emissor


-- COMMAND ----------

-- DBTITLE 1,tmp_trabalhador_alta_atendimento
create or replace temporary view tmp_trabalhador_alta_atendimento as

with tmp_primeiro_credito as (
  Select dim_conta.srk_trabalhador_cadastro,
         dim_conta.srk_rh_cadastro,
         min(dim_conta.dat_primeiro_credito) as dat_primeiro_credito
    from gold_vr_cliente_trabalhador.dim_conta
   inner join gold_vr_cliente_trabalhador.dim_cadastro
      on dim_cadastro.srk_trabalhador_cadastro = dim_conta.srk_trabalhador_cadastro
     and dim_cadastro.flg_ativo = 1
     and dim_conta.flg_ativo = 1
   where dim_cadastro.num_cpf <> '00000000000'
   group by dim_conta.srk_trabalhador_cadastro,
         dim_conta.srk_rh_cadastro
)

Select fat_planta_contrato.dat_referencia,
       fat_planta_contrato.srk_rh_cadastro,
       fat_planta_contrato.num_cnpj,
       count(distinct tmp_primeiro_credito.srk_trabalhador_cadastro) as qtd_trabalhador_alta_atendimento
  from gold_vr_cliente_rh.fat_planta_contrato
 inner join gold_vr_cliente_trabalhador.dim_conta
    on dim_conta.srk_rh_cadastro = fat_planta_contrato.srk_rh_cadastro
   and dim_conta.cod_sdprod = fat_planta_contrato.cod_sdprod
   and dim_conta.cod_produto = fat_planta_contrato.cod_produto
   and dim_conta.flg_ativo = 1
 inner join tmp_primeiro_credito
    on tmp_primeiro_credito.srk_trabalhador_cadastro = dim_conta.srk_trabalhador_cadastro
   and tmp_primeiro_credito.srk_rh_cadastro = dim_conta.srk_rh_cadastro
   and trunc(tmp_primeiro_credito.dat_primeiro_credito,'MM') = fat_planta_contrato.dat_referencia
 where upper(fat_planta_contrato.dsc_segmento_comercial_entrada) in ('RETENÇÃO', 'CS','RETENCAO')
   and fat_planta_contrato.dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
   and fat_planta_contrato.dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   and fat_planta_contrato.qtd_conta_nova_sistemica_faseado > 0
 group by fat_planta_contrato.dat_referencia,
       fat_planta_contrato.srk_rh_cadastro,
       fat_planta_contrato.num_cnpj


-- COMMAND ----------

-- DBTITLE 1,tmp_mes_creditando
create or replace temporary view tmp_mes_creditando as

with tmp_qtd_mes_creditando as (
  Select dat_referencia,
         srk_rh_cadastro,
         cod_emissor,
         qtd_mes_creditando
    from gold_vr_cliente_rh.fat_planta
   where dat_referencia = trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
)

select tmp_planta_cnpj.dat_referencia, 
       tmp_planta_cnpj.srk_rh_cadastro, 
       tmp_planta_cnpj.cod_emissor,
       case 
         when tmp_planta_cnpj.dat_primeiro_credito_filtro is null
           then 0
           when cast(months_between(tmp_planta_cnpj.dat_referencia, nvl(tmp_planta_cnpj.dat_ultimo_credito_filtro,'2000-01-01')) as int) >= 12
            then 0
         when trunc(tmp_planta_cnpj.dat_primeiro_credito_filtro,'MM') > trunc(tmp_planta_cnpj.dat_referencia,'MM')
           then 0
         when tmp_planta_cnpj.dat_primeiro_credito_filtro < tmp_planta_cnpj.dat_referencia
              and tmp_planta_cnpj.dat_referencia < '2020-01-01'
           then 0
         when cast(months_between(tmp_planta_cnpj.dat_referencia, nvl(tmp_planta_cnpj.dat_ultimo_credito_filtro,'2000-01-01')) as int) >= 12
           then 0
         when tmp_planta_cnpj.dat_referencia = trunc(tmp_planta_cnpj.dat_primeiro_credito,'MM')
           then 1 
         when trunc(tmp_planta_cnpj.dat_ultimo_credito_filtro,'MM') = trunc(tmp_planta_cnpj.dat_referencia,'MM')
              and tmp_qtd_mes_creditando.qtd_mes_creditando = 0
              and trunc(lag(tmp_planta_cnpj.dat_ultimo_credito_filtro,1,null) 
                            OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                     ORDER BY tmp_planta_cnpj.dat_referencia),'MM') <>
                  lag(tmp_planta_cnpj.dat_referencia,1,null)
                      OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                               ORDER BY tmp_planta_cnpj.dat_referencia)
           then 1
         when tmp_qtd_mes_creditando.qtd_mes_creditando is not null
              and tmp_planta_cnpj.dat_referencia = tmp_qtd_mes_creditando.dat_referencia
              then tmp_qtd_mes_creditando.qtd_mes_creditando
         when tmp_qtd_mes_creditando.qtd_mes_creditando is null
              and trunc(tmp_planta_cnpj.dat_ultimo_credito_filtro,'MM') = trunc(tmp_planta_cnpj.dat_referencia,'MM')
              and trunc(lag(tmp_planta_cnpj.dat_ultimo_credito_filtro,1,null) 
                            OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                     ORDER BY tmp_planta_cnpj.dat_referencia),'MM') is null
           then 1
         when tmp_qtd_mes_creditando.qtd_mes_creditando is null
              and trunc(tmp_planta_cnpj.dat_ultimo_credito_filtro,'MM') = trunc(tmp_planta_cnpj.dat_referencia,'MM')
              and trunc(lag(tmp_planta_cnpj.dat_ultimo_credito_filtro,1,null) 
                            OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                     ORDER BY tmp_planta_cnpj.dat_referencia),'MM') is not null
           then 2
         else nvl(lag(tmp_qtd_mes_creditando.qtd_mes_creditando,1,0) 
                      OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                               ORDER BY tmp_planta_cnpj.dat_referencia),0)
              + cast(months_between(tmp_planta_cnpj.dat_referencia, tmp_qtd_mes_creditando.dat_referencia) as int)
       end as qtd_mes_creditando
  from tmp_planta_cnpj
  left join tmp_qtd_mes_creditando
    on tmp_planta_cnpj.srk_rh_cadastro = tmp_qtd_mes_creditando.srk_rh_cadastro
   and tmp_planta_cnpj.cod_emissor = tmp_qtd_mes_creditando.cod_emissor


-- COMMAND ----------

-- DBTITLE 1,tmp_base_inadimplente
create or replace temporary view tmp_base_inadimplente as

with tmp_inadimplente_todo as (
  Select cast(dt_referencia as date) as dat_referencia,
         regexp_replace(num_id,'[^0-9]','') as num_cnpj,
         sum(saldo_titulo) as val_inadimplencia
    from prod.silver_vr_sharepoint.arquivo_inadimplente_financeiro
   where dt_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
     and dt_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by cast(dt_referencia as date),
         regexp_replace(num_id,'[^0-9]','')
)

, tmp_menor_inadimplente (
  Select min(dat_referencia) as dat_min_referencia
    from tmp_inadimplente_todo
   group by trunc(dat_referencia,'MM')
)

, tmp_inadimplente as (
  Select trunc(add_months(tmp_inadimplente_todo.dat_referencia,-1),'MM') as dat_referencia,
         tmp_inadimplente_todo.num_cnpj,
         tmp_inadimplente_todo.val_inadimplencia
    from tmp_inadimplente_todo
   inner join tmp_menor_inadimplente
      on tmp_inadimplente_todo.dat_referencia = tmp_menor_inadimplente.dat_min_referencia
)

, tmp_blindagem_todo as (
  Select cast(dt_referencia as date) as dat_referencia,
         regexp_replace(num_id,'[^0-9]','') as num_cnpj,
         sum(saldo_titulo) as val_inadimplencia
    from prod.silver_vr_sharepoint.arquivo_inadimplente_blindagem_financeiro
   where dt_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
     and dt_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by cast(dt_referencia as date),
         regexp_replace(num_id,'[^0-9]','')
)

, tmp_menor_blindagem (
  Select min(dat_referencia) as dat_min_referencia
    from tmp_blindagem_todo
   group by trunc(dat_referencia,'MM')
)

, tmp_blindagem as (
  Select trunc(add_months(tmp_blindagem_todo.dat_referencia,-1),'MM') as dat_referencia,
         tmp_blindagem_todo.num_cnpj,
         tmp_blindagem_todo.val_inadimplencia
    from tmp_blindagem_todo
   inner join tmp_menor_blindagem
      on tmp_blindagem_todo.dat_referencia = tmp_menor_blindagem.dat_min_referencia
)

, tmp_cnpjs (
  Select dat_referencia,
         num_cnpj
    from tmp_inadimplente
  union
  Select dat_referencia,
         num_cnpj
    from tmp_blindagem
)

Select tmp_cnpjs.dat_referencia,
       tmp_cnpjs.num_cnpj,
       nvl(tmp_inadimplente.val_inadimplencia,0) + nvl(tmp_blindagem.val_inadimplencia,0) as val_inadimplencia
  from tmp_cnpjs
  left join tmp_inadimplente
    on tmp_cnpjs.dat_referencia = tmp_inadimplente.dat_referencia
   and tmp_cnpjs.num_cnpj = tmp_inadimplente.num_cnpj   
  left join tmp_blindagem
    on tmp_cnpjs.dat_referencia = tmp_blindagem.dat_referencia
   and tmp_cnpjs.num_cnpj = tmp_blindagem.num_cnpj


-- COMMAND ----------

-- DBTITLE 1,tmp_planta_join
create or replace temporary view tmp_planta_join as

with tmp_segmento_mes as (
  Select dim_mes.dat_mes as dat_referencia,
         dim_canal_venda_segmento.cod_canal_venda,
         dim_canal_venda_segmento.dsc_segmento_comercial,
         dim_canal_venda_segmento.dsc_subsegmento_comercial,
         dim_canal_venda_segmento.dsc_torre_comercial,
         dim_canal_venda_segmento.dat_inicio_vigencia
    from gold_vr_cliente_rh.dim_canal_venda_segmento
    join prod.vr_parametro.dim_mes
      on dim_mes.dat_mes >= trunc(dim_canal_venda_segmento.dat_inicio_vigencia,'MM')
     and dim_mes.dat_mes <= trunc(cast('${dat_parametro}' as date), 'MM') 
   where dim_mes.dat_mes >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
     and dim_canal_venda_segmento.dsc_ilha_usuario is null
     and dim_canal_venda_segmento.dsc_perfil_criador_proposta is null
)

--Obtém o segmento com maior data de início de vigência dentro do mês corrente
, tmp_segmento_max_vigencia as ( 
  Select tmp_segmento_mes.dat_referencia,
         tmp_segmento_mes.cod_canal_venda,
         max(tmp_segmento_mes.dat_inicio_vigencia) as dat_inicio_vigencia
    from tmp_segmento_mes
   group by tmp_segmento_mes.dat_referencia,
         tmp_segmento_mes.cod_canal_venda
)

, tmp_segmento as (
  select tmp_segmento_mes.dat_referencia,
         tmp_segmento_mes.cod_canal_venda,
         tmp_segmento_mes.dsc_segmento_comercial,
         tmp_segmento_mes.dsc_subsegmento_comercial,
         tmp_segmento_mes.dsc_torre_comercial
    from tmp_segmento_mes
   inner join tmp_segmento_max_vigencia
      on tmp_segmento_max_vigencia.cod_canal_venda = tmp_segmento_mes.cod_canal_venda
     and tmp_segmento_max_vigencia.dat_referencia = tmp_segmento_mes.dat_referencia
     and tmp_segmento_max_vigencia.dat_inicio_vigencia = tmp_segmento_mes.dat_inicio_vigencia
)

, tmp_reembolso as (
  Select dat_referencia, 
         srk_rh_cadastro,
         cod_emissor,
         round(sum(nvl(val_receita,0)),2) as val_receita
    from gold_vr_cliente_ec.fat_reembolso_transacao
   where dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
     and dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by dat_referencia,
         srk_rh_cadastro,
         cod_emissor
)

, tmp_transacao_outros as (
  Select dat_referencia,
         srk_rh_cadastro,
         cod_emissor,
         sum(nvl(val_transacao,0)) as val_tarifa_operacional
    from gold_vr_cliente_trabalhador.fat_transacao_outros
   where cod_operacao in ('628000','660000')
     and dat_referencia >= trunc(add_months(cast('${dat_parametro}' as date), -2), 'MM')
     and dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by dat_referencia,
            srk_rh_cadastro,
            cod_emissor
)

, tmp_planta_historico as (
  --M0
  Select trunc(cast('${dat_parametro}' as date),'MM') as dat_referencia,
         num_cnpj,
         cod_emissor,
         max(nvl(qtd_maximo_trabalhador_ativo_filtro_3m,0)) as qtd_maximo_trabalhador_ativo_filtro_3m_max,
         max(nvl(val_maximo_creditado_ativo_filtro_3m,0)) as val_maximo_creditado_ativo_filtro_3m_max
    from gold_vr_cliente_rh.fat_planta
   where dat_referencia <= trunc(cast('${dat_parametro}' as date), 'MM')
   group by num_cnpj,
         cod_emissor
  union
  --M-1
  Select trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM') as dat_referencia,
         num_cnpj,
         cod_emissor,
         max(nvl(qtd_maximo_trabalhador_ativo_filtro_3m,0)) as qtd_maximo_trabalhador_ativo_filtro_3m_max,
         max(nvl(val_maximo_creditado_ativo_filtro_3m,0)) as val_maximo_creditado_ativo_filtro_3m_max
    from gold_vr_cliente_rh.fat_planta
   where dat_referencia <= trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM')
   group by num_cnpj,
         cod_emissor
)

Select tmp_planta_cnpj.*,
       nvl(case 
           when tmp_planta_cnpj.dat_referencia < trunc(tmp_planta_cnpj.dat_primeiro_credito,'MM')
             then 0
           when tmp_planta_cnpj.dat_referencia >= trunc(tmp_planta_cnpj.dat_primeiro_credito,'MM')
             then cast(months_between(tmp_planta_cnpj.dat_referencia, trunc(tmp_planta_cnpj.dat_primeiro_credito,'MM')) as int) + 1
       end,0) as num_safra,
       tmp_quantidade_trabalhador.qtd_trabalhador,
       tmp_quantidade_trabalhador_primeiro_credito.qtd_trabalhador_primeiro_credito,
       tmp_quantidade_trabalhador_primeiro_credito_regra_antiga.qtd_trabalhador_primeiro_credito_anterior,
       tmp_quantidade_trabalhador_sem_credito_3m.qtd_trabalhador_sem_credito_3m,
       tmp_fat_credito_3m.qtd_trabalhador_ativo,
       tmp_fat_credito_3m.qtd_trabalhador_ativo_filtro,
       tmp_fat_credito_3m.qtd_trabalhador_ativo_3m,
       tmp_fat_credito_3m.qtd_trabalhador_ativo_filtro_3m,
       tmp_fat_credito_3m.val_creditado_ativo_filtro_3m,
       tmp_fat_credito_3m.qtd_transacao_3m,
       tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,
        nvl(lag(tmp_fat_credito_3m.qtd_trabalhador_ativo_filtro,1,0) 
               OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                        ORDER BY tmp_planta_cnpj.dat_referencia),0) as qtd_bop_trabalhador_creditado_filtro,
       tmp_fat_credito_3m.qtd_trabalhador_ativo_filtro as qtd_eop_trabalhador_creditado_filtro,
       nvl(lag(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,1,0) 
               OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                        ORDER BY tmp_planta_cnpj.dat_referencia),0) as qtd_bop_maximo_trabalhador_ativo_filtro_3m,
       tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m as qtd_eop_maximo_trabalhador_ativo_filtro_3m,
       nvl(lag(tmp_fat_credito_maximo_3m.qtd_maximo_conta_ativo_filtro_3m,1,0)
               OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                        ORDER BY tmp_planta_cnpj.dat_referencia),0) as qtd_bop_maximo_conta_ativo_filtro_3m,
       tmp_fat_credito_maximo_3m.qtd_maximo_conta_ativo_filtro_3m as qtd_eop_maximo_conta_ativo_filtro_3m,
       tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m,
       nvl(lag(tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m,1,0)
               OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                        ORDER BY tmp_planta_cnpj.dat_referencia),0) as val_bop_maximo_creditado_ativo_filtro_3m,
       tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m as val_eop_maximo_creditado_ativo_filtro_3m,
       tmp_fat_credito_maximo_12m.qtd_maximo_trabalhador_ativo_filtro_12m as num_porte_trabalhador_filtro_12m,
       tmp_fat_credito_maximo_12m.dat_referencia_maximo_trabalhador_ativo_filtro_12m as dat_referencia_porte_trabalhador_12m,
       tmp_fat_credito_maximo_12m.qtd_maximo_conta_ativo_filtro_12m as num_porte_conta_filtro_12m,
       tmp_fat_credito_maximo_12m.dat_referencia_maximo_conta_ativo_filtro_12m as dat_referencia_porte_conta_12m,
       tmp_fat_credito_maximo_12m.val_maximo_creditado_ativo_12m,
       tmp_fat_credito_maximo_12m.dat_referencia_maximo_creditado_ativo_12m,
       tmp_fat_credito_maximo_12m.val_maximo_creditado_ativo_filtro_12m,
       tmp_fat_credito_maximo_12m.dat_referencia_maximo_creditado_ativo_filtro_12m,
       tmp_mes_creditando.qtd_mes_creditando,
       nvl(case when tmp_mes_creditando.qtd_mes_creditando <= 3
                 and nvl(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,0) -
                     nvl(lag(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,1,0)
                         OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                  ORDER BY tmp_planta_cnpj.dat_referencia),0) >= 0
           then nvl(nvl(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,0) -
                    nvl(lag(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,1,0)
                        OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                 ORDER BY tmp_planta_cnpj.dat_referencia),0),0)
           end,0) as qtd_alta_comercial,
        nvl(case when tmp_mes_creditando.qtd_mes_creditando <= 3
                 and nvl(tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m,0) -
                     nvl(lag(tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m,1,0)
                         OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                  ORDER BY tmp_planta_cnpj.dat_referencia),0) >= 0
           then nvl(nvl(tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m,0) -
                    nvl(lag(tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m,1,0)
                        OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                 ORDER BY tmp_planta_cnpj.dat_referencia),0),0)
           end,0) as val_creditado_alta_comercial,
       nvl(case when tmp_mes_creditando.qtd_mes_creditando > 3
                 and nvl(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,0) -
                     nvl(lag(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,1,0)
                             OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                      ORDER BY tmp_planta_cnpj.dat_referencia),0) >= 0
           then nvl(nvl(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,0) -
                    nvl(lag(tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m,1,0)
                            OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                                     ORDER BY tmp_planta_cnpj.dat_referencia),0),0)
           end,0) as qtd_alta_involuntaria,
       nvl(lag(tmp_fat_credito_40d.qtd_trabalhador_ativo_filtro_40d,1,0)
               OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                        ORDER BY tmp_planta_cnpj.dat_referencia),0) as qtd_bop_trabalhador_ativo_filtro_40d,
       tmp_fat_credito_40d.qtd_trabalhador_ativo_filtro_40d as qtd_eop_trabalhador_ativo_filtro_40d,
       nvl(lag(tmp_fat_credito_40d.qtd_conta_ativo_filtro_40d,1,0)
               OVER(PARTITION BY tmp_planta_cnpj.srk_rh_cadastro, tmp_planta_cnpj.cod_emissor
                        ORDER BY tmp_planta_cnpj.dat_referencia),0) as qtd_bop_conta_ativo_filtro_40d_alta_churn,
       tmp_fat_credito_40d.qtd_conta_ativo_filtro_40d,
       nvl(nvl(tmp_planta_cnpj.qtd_eop_conta_creditada_filtro,0) / nvl(tmp_fat_credito_3m.qtd_trabalhador_ativo_filtro,0),0) as var_calculo_fisicos,
       tmp_trabalhador_alta_atendimento.qtd_trabalhador_alta_atendimento,
       nvl(tmp_reembolso.val_receita,0) as val_receita,
       nvl(tmp_transacao_outros.val_tarifa_operacional,0) as val_tarifa_operacional,
       nvl(tmp_reembolso.val_receita,0) + nvl(tmp_transacao_outros.val_tarifa_operacional,0) as val_receita_total,
       nvl(case
             when tmp_planta_historico.qtd_maximo_trabalhador_ativo_filtro_3m_max > tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m
             then tmp_planta_historico.qtd_maximo_trabalhador_ativo_filtro_3m_max
             else tmp_fat_credito_maximo_3m.qtd_maximo_trabalhador_ativo_filtro_3m
           end,0) as qtd_maximo_trabalhador_3m_historico,
       nvl(case
             when tmp_planta_historico.val_maximo_creditado_ativo_filtro_3m_max > tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m
             then tmp_planta_historico.val_maximo_creditado_ativo_filtro_3m_max
             else tmp_fat_credito_maximo_3m.val_maximo_creditado_ativo_filtro_3m
           end,0) as val_maximo_creditado_3m_historico,
       case when nvl(tmp_base_inadimplente.val_inadimplencia,0) > 0 then 1 else 0 end as flg_inadimplencia,
       tmp_base_inadimplente.val_inadimplencia,
       case when excecao_tombamento.dsc_tipo is not null then 1 else 0 end as flg_excecao_tombamento,
       case when excecao_outliers.dsc_tipo is not null then 1 else 0 end as flg_excecao_outlier,
       case when excecao_projetos.dsc_tipo is not null then 1 else 0 end as flg_excecao_projeto

  from tmp_planta_cnpj
  left join tmp_quantidade_trabalhador
    on tmp_quantidade_trabalhador.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_quantidade_trabalhador.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_quantidade_trabalhador.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_fat_credito_3m
    on tmp_fat_credito_3m.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_fat_credito_3m.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_fat_credito_3m.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_quantidade_trabalhador_primeiro_credito
    on tmp_quantidade_trabalhador_primeiro_credito.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_quantidade_trabalhador_primeiro_credito.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_quantidade_trabalhador_primeiro_credito.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_quantidade_trabalhador_primeiro_credito_regra_antiga
    on tmp_quantidade_trabalhador_primeiro_credito_regra_antiga.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_quantidade_trabalhador_primeiro_credito_regra_antiga.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_quantidade_trabalhador_primeiro_credito_regra_antiga.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_quantidade_trabalhador_sem_credito_3m
    on tmp_quantidade_trabalhador_sem_credito_3m.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_quantidade_trabalhador_sem_credito_3m.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
  left join tmp_fat_credito_maximo_3m
    on tmp_fat_credito_maximo_3m.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_fat_credito_maximo_3m.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_fat_credito_maximo_3m.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_mes_creditando
    on tmp_mes_creditando.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_mes_creditando.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_mes_creditando.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_fat_credito_maximo_12m
    on tmp_fat_credito_maximo_12m.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_fat_credito_maximo_12m.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_fat_credito_maximo_12m.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_fat_credito_40d
    on tmp_fat_credito_40d.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_fat_credito_40d.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_fat_credito_40d.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join gold_vr_cliente_rh.dim_segmentacao
    on dim_segmentacao.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and dim_segmentacao.dsc_tipo_segmentacao = 'Entrada'
  left join tmp_segmento
    on tmp_segmento.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_segmento.cod_canal_venda = dim_segmentacao.cod_canal_venda
  left join tmp_trabalhador_alta_atendimento
    on tmp_trabalhador_alta_atendimento.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_trabalhador_alta_atendimento.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
  left join tmp_reembolso
    on tmp_reembolso.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_reembolso.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_reembolso.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_transacao_outros
    on tmp_transacao_outros.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_transacao_outros.srk_rh_cadastro = tmp_planta_cnpj.srk_rh_cadastro
   and tmp_transacao_outros.cod_emissor = tmp_planta_cnpj.cod_emissor
  left join tmp_planta_historico
    on tmp_planta_historico.dat_referencia = tmp_planta_cnpj.dat_referencia
   and tmp_planta_historico.num_cnpj = tmp_planta_cnpj.num_cnpj
   and tmp_planta_historico.cod_emissor = tmp_planta_cnpj.cod_emissor
 left join tmp_base_inadimplente
   on tmp_base_inadimplente.dat_referencia = tmp_planta_cnpj.dat_referencia
  and tmp_base_inadimplente.num_cnpj = tmp_planta_cnpj.num_cnpj
  and tmp_planta_cnpj.cod_emissor = 'VRPAT'
 left join vr_parametro.dim_excecao as excecao_tombamento
   on tmp_planta_cnpj.num_cnpj = excecao_tombamento.num_cnpj
  and tmp_planta_cnpj.dat_referencia >= trunc(excecao_tombamento.dat_inicio_periodo,'MM')
  and tmp_planta_cnpj.dat_referencia < trunc(excecao_tombamento.dat_fim_periodo,'MM')
  and excecao_tombamento.dsc_tipo = 'Tombamento CEF'
  and tmp_planta_cnpj.cod_emissor = 'VRPAT'
 left join vr_parametro.dim_excecao as excecao_outliers
   on tmp_planta_cnpj.num_cnpj = excecao_outliers.num_cnpj
  and tmp_planta_cnpj.dat_referencia >= trunc(excecao_outliers.dat_inicio_periodo,'MM')
  and tmp_planta_cnpj.dat_referencia < trunc(excecao_outliers.dat_fim_periodo,'MM')
  and excecao_outliers.dsc_tipo = 'Outliers'
  and tmp_planta_cnpj.cod_emissor = 'VRPAT'
 left join vr_parametro.dim_excecao as excecao_projetos
   on dim_segmentacao.cod_canal_venda = excecao_projetos.cod_canal_venda
  and tmp_planta_cnpj.dat_referencia >= trunc(excecao_projetos.dat_inicio_periodo,'MM')
  and tmp_planta_cnpj.dat_referencia < trunc(excecao_projetos.dat_fim_periodo,'MM')
  and excecao_tombamento.dsc_tipo = 'Projetos'
  and tmp_planta_cnpj.cod_emissor = 'VRPAT'
;

-- COMMAND ----------

-- DBTITLE 1,tmp_planta_calculo
create or replace temporary view tmp_planta_calculo as

with tmp_calculo_fisicos as (
  Select *,
         nvl(case when num_safra in (1,2,3)
                then qtd_trabalhador_primeiro_credito
                else 0
           end,0) as qtd_trabalhador_novo_sistemico_filtro,
         qtd_trabalhador_primeiro_credito_anterior as qtd_trabalhador_novo_sistema_anterior,
         nvl(case when num_safra in (1,2,3)
                then qtd_trabalhador_primeiro_credito
                else 0
           end,0) + nvl(round((nvl(qtd_conta_nova_faseado_filtro,0) / var_calculo_fisicos),0),0) as qtd_trabalhador_novo_sistemico_faseado_filtro,
         nvl(qtd_trabalhador_primeiro_credito_anterior,0) + nvl(round((nvl(qtd_conta_nova_faseado_filtro,0) / var_calculo_fisicos),0),0) as qtd_trabalhador_novo_sistemico_anterior,
         case
           when nvl(qtd_eop_maximo_conta_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_conta_ativo_filtro_3m,0) >= 0
           then nvl(qtd_eop_maximo_conta_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_conta_ativo_filtro_3m,0)
           else 0
         end as qtd_conta_alta_total_filtro,
         case
           when nvl(qtd_eop_maximo_conta_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_conta_ativo_filtro_3m,0) <= 0
           then nvl(qtd_eop_maximo_conta_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_conta_ativo_filtro_3m,0)
           else 0
         end as qtd_conta_churn_bruto_filtro,
         case
           when nvl(qtd_eop_maximo_trabalhador_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_trabalhador_ativo_filtro_3m,0) >= 0
           then nvl(qtd_eop_maximo_trabalhador_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_trabalhador_ativo_filtro_3m,0)
           else 0
         end as qtd_trabalhador_alta_total_filtro,
         case
           when nvl(qtd_eop_maximo_trabalhador_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_trabalhador_ativo_filtro_3m,0) <= 0
           then nvl(qtd_eop_maximo_trabalhador_ativo_filtro_3m,0) - nvl(qtd_bop_maximo_trabalhador_ativo_filtro_3m,0)
           else 0
         end as qtd_trabalhador_churn_bruto_filtro,
         round((nvl(qtd_conta_nova_faseado_filtro,0) / var_calculo_fisicos),0) as qtd_trabalhador_novo_faseado_filtro,
         case
           when nvl(qtd_conta_ativo_filtro_40d,0) - nvl(qtd_bop_conta_ativo_filtro_40d_alta_churn,0) >= 0
           then nvl(qtd_conta_ativo_filtro_40d,0) - nvl(qtd_bop_conta_ativo_filtro_40d_alta_churn,0)
           else 0
         end as qtd_conta_alta_total_filtro_40d,
         case
           when nvl(qtd_conta_ativo_filtro_40d,0) - nvl(qtd_bop_conta_ativo_filtro_40d_alta_churn,0) <= 0
           then nvl(qtd_conta_ativo_filtro_40d,0) - nvl(qtd_bop_conta_ativo_filtro_40d_alta_churn,0)
           else 0
         end as qtd_conta_churn_bruto_filtro_40d,
         case
           when nvl(qtd_eop_trabalhador_ativo_filtro_40d,0) - nvl(qtd_bop_trabalhador_ativo_filtro_40d,0) >= 0
           then nvl(qtd_eop_trabalhador_ativo_filtro_40d,0) - nvl(qtd_bop_trabalhador_ativo_filtro_40d,0)
           else 0
         end as qtd_trabalhador_alta_total_filtro_40d,
         case
           when nvl(qtd_eop_trabalhador_ativo_filtro_40d,0) - nvl(qtd_bop_trabalhador_ativo_filtro_40d,0) <= 0
           then nvl(qtd_eop_trabalhador_ativo_filtro_40d,0) - nvl(qtd_bop_trabalhador_ativo_filtro_40d,0)
           else 0
         end as qtd_trabalhador_churn_bruto_filtro_40d
    from tmp_planta_join
)

Select *,
       nvl(qtd_conta_alta_total_filtro,0) - nvl(qtd_conta_nova_sistemica_faseado_filtro,0) as qtd_conta_nova_involuntaria_filtro,
       (nvl(qtd_conta_alta_total_filtro,0) - nvl(qtd_conta_nova_sistemica_faseado_filtro,0)) + qtd_conta_churn_bruto_filtro as qtd_conta_churn_liquido_filtro,
       nvl(qtd_trabalhador_alta_total_filtro,0) - nvl(qtd_trabalhador_novo_sistemico_faseado_filtro,0) as qtd_trabalhador_novo_involuntario_filtro,
       nvl(qtd_trabalhador_alta_total_filtro,0) - nvl(qtd_trabalhador_novo_sistemico_anterior,0) as qtd_trabalhador_involuntario_anterior,
       (nvl(qtd_trabalhador_alta_total_filtro,0) - nvl(qtd_trabalhador_novo_sistemico_faseado_filtro,0)) + nvl(qtd_trabalhador_churn_bruto_filtro,0) as qtd_trabalhador_churn_liquido_filtro,
       (nvl(qtd_trabalhador_alta_total_filtro,0) - nvl(qtd_trabalhador_novo_sistemico_anterior,0)) + nvl(qtd_trabalhador_churn_bruto_filtro,0) as qtd_trabalhador_churn_anterior
  from tmp_calculo_fisicos
;

-- COMMAND ----------

-- DBTITLE 1,tmp_planta_tratamento
create or replace temporary view tmp_planta_tratamento as

select dat_referencia,
       srk_rh_cadastro,
       num_cnpj,
       dsc_razao_social,
       cod_canal_venda_entrada,
       dsc_canal_venda_entrada,
       cod_subcanal_entrada,
       dsc_subcanal_entrada,
       dsc_segmento_comercial_entrada,
       dsc_subsegmento_comercial_entrada,
       dsc_torre_comercial_entrada,
       cod_canal_venda_alocacao,
       dsc_canal_venda_alocacao,
       cod_subcanal_alocacao,
       dsc_subcanal_alocacao,
       dsc_segmento_comercial_alocacao,
       dsc_subsegmento_comercial_alocacao,
       dsc_torre_comercial_alocacao,
       cod_emissor,
       cast(dsc_status_contrato as string) as dsc_status_contrato,
       dsc_cidade_entrega,
       sig_estado_entrega,
       dsc_regiao_entrega,
       dsc_cidade_cobranca,
       sig_estado_cobranca,
       dsc_regiao_cobranca,
       cod_cnae_principal,
       dsc_ramo_atividade,
       dsc_setor,
       qtd_funcionario,
       dat_primeiro_credito,
       dat_primeiro_credito_filtro,
       dat_ultimo_credito,
       dat_ultimo_credito_filtro,
       flg_tipo_pagamento_pos_pago,
       flg_tipo_pagamento_pre_pago,
       qtd_contrato_ativo,
       qtd_contrato_pos_pago,
       qtd_contrato_pre_pago,
       flg_cliente_premium,
       flg_cliente_farmer,
       flg_possui_fidelidade,
       cod_grupo_economico,
       dsc_grupo_economico,
       val_creditado,
       val_creditado_m1,
       val_creditado_3m,
       qtd_conta_creditada_3m,
       qtd_conta_creditada_filtro_3m,
       qtd_conta,
       qtd_conta_status_ativo,
       qtd_conta_ativada,
       qtd_conta_ativada_nova,
       qtd_conta_ativada_base,
       qtd_conta_creditada,
       qtd_estoque_conta_ativada,
       val_faturado_bruto,
       val_faturado_liquido,
       val_facial_real,
       num_porte_3m,
       num_porte_6m,
       val_creditado_extrai_natal_saude,
       qtd_conta_extrai_natal_saude,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0   
         else qtd_conta_nova_faseado_filtro
       end as qtd_conta_nova_faseado_filtro,
       val_creditado_faseado_filtro,
       val_creditado_sistemica_faseado_filtro,
       val_creditado_sistemica_filtro,
       val_creditado_extrai_saude_compras,
       qtd_cartao_emitido_filtro,
       qtd_cartao_emitido_1via_filtro,
       qtd_cartao_emitido_novo_1via_filtro,
       qtd_cartao_reemitido_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_bop_conta_creditada_filtro
       end as qtd_bop_conta_creditada_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_eop_conta_creditada_filtro
       end as qtd_eop_conta_creditada_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_conta_nova_sistemica_filtro
       end as qtd_conta_nova_sistemica_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_conta_nova_sistemica_faseado_filtro
       end as qtd_conta_nova_sistemica_faseado_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_bop_conta_ativo_filtro_40d
       end as qtd_bop_conta_ativo_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_eop_conta_ativo_filtro_40d
       end as qtd_eop_conta_ativo_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else val_creditado_bop_ativo_filtro_40d
       end as val_creditado_bop_ativo_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else val_creditado_eop_ativo_filtro_40d
       end as val_creditado_eop_ativo_filtro_40d,
       dat_ultimo_credito_filtro_40d,
       dsc_segmento_alta_atendiento,
       qtd_conta_alta_atendimento,
       qtd_trabalhador_alta_atendimento,
       flg_inadimplencia,
       val_inadimplencia,
       flg_excecao_tombamento,
       flg_excecao_outlier,
       flg_excecao_projeto,
       num_safra,
       qtd_trabalhador,
       qtd_trabalhador_primeiro_credito,
       qtd_trabalhador_primeiro_credito_anterior,
       qtd_trabalhador_sem_credito_3m,
       qtd_trabalhador_ativo,
       qtd_trabalhador_ativo_filtro,
       qtd_trabalhador_ativo_3m,
       qtd_trabalhador_ativo_filtro_3m,
       val_creditado_ativo_filtro_3m,
       qtd_transacao_3m,
       qtd_maximo_trabalhador_ativo_filtro_3m,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_bop_trabalhador_creditado_filtro
       end as qtd_bop_trabalhador_creditado_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_eop_trabalhador_creditado_filtro
       end as qtd_eop_trabalhador_creditado_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_bop_maximo_trabalhador_ativo_filtro_3m
       end as qtd_bop_maximo_trabalhador_ativo_filtro_3m,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_eop_maximo_trabalhador_ativo_filtro_3m
       end as qtd_eop_maximo_trabalhador_ativo_filtro_3m,
              case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_bop_maximo_conta_ativo_filtro_3m
       end as qtd_bop_maximo_conta_ativo_filtro_3m,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_eop_maximo_conta_ativo_filtro_3m
       end as qtd_eop_maximo_conta_ativo_filtro_3m,
       val_maximo_creditado_ativo_filtro_3m,
       val_bop_maximo_creditado_ativo_filtro_3m,
       val_eop_maximo_creditado_ativo_filtro_3m,
       num_porte_trabalhador_filtro_12m,
       dat_referencia_porte_trabalhador_12m,
       num_porte_conta_filtro_12m,
       dat_referencia_porte_conta_12m,
       val_maximo_creditado_ativo_12m,
       dat_referencia_maximo_creditado_ativo_12m,
       val_maximo_creditado_ativo_filtro_12m,
       dat_referencia_maximo_creditado_ativo_filtro_12m,
       qtd_mes_creditando,
       qtd_alta_comercial,
       val_creditado_alta_comercial,
       qtd_alta_involuntaria,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_bop_trabalhador_ativo_filtro_40d
       end as qtd_bop_trabalhador_ativo_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_eop_trabalhador_ativo_filtro_40d
       end as qtd_eop_trabalhador_ativo_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_novo_sistemico_filtro
       end as qtd_trabalhador_novo_sistemico_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_novo_sistema_anterior
       end as qtd_trabalhador_novo_sistema_anterior,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_novo_sistemico_faseado_filtro
       end as qtd_trabalhador_novo_sistemico_faseado_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_novo_sistemico_anterior
       end as qtd_trabalhador_novo_sistemico_anterior,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0 
         else qtd_conta_alta_total_filtro
       end as qtd_conta_alta_total_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_conta_churn_bruto_filtro
       end as qtd_conta_churn_bruto_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_alta_total_filtro
       end as qtd_trabalhador_alta_total_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_churn_bruto_filtro
       end as qtd_trabalhador_churn_bruto_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_novo_faseado_filtro
       end as qtd_trabalhador_novo_faseado_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_conta_alta_total_filtro_40d
       end as qtd_conta_alta_total_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_conta_churn_bruto_filtro_40d
       end as qtd_conta_churn_bruto_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_alta_total_filtro_40d
       end as qtd_trabalhador_alta_total_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_churn_bruto_filtro_40d
       end as qtd_trabalhador_churn_bruto_filtro_40d,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_conta_nova_involuntaria_filtro
       end as qtd_conta_nova_involuntaria_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_conta_churn_liquido_filtro
       end as qtd_conta_churn_liquido_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_novo_involuntario_filtro
       end as qtd_trabalhador_novo_involuntario_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_involuntario_anterior
       end as qtd_trabalhador_involuntario_anterior,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_churn_liquido_filtro
       end as qtd_trabalhador_churn_liquido_filtro,
       case
         when flg_excecao_tombamento + flg_excecao_outlier + flg_excecao_projeto > 0
         then 0
         else qtd_trabalhador_churn_anterior
       end as qtd_trabalhador_churn_anterior,
       val_receita,
       val_tarifa_operacional,
       val_receita_total,
       qtd_maximo_trabalhador_3m_historico,
       val_maximo_creditado_3m_historico

  from tmp_planta_calculo
 where dat_referencia in (trunc(add_months(cast('${dat_parametro}' as date), -1), 'MM'), trunc(cast('${dat_parametro}' as date), 'MM'))
;

-- COMMAND ----------

-- DBTITLE 1,tmp_fat_planta_vrb_cnpj_final
create or replace table vr_temporaria.tmp_fat_planta_vrb_cnpj_final as

select *,
       md5(
         nvl(dat_referencia,'') ||
         nvl(srk_rh_cadastro,'') ||
         nvl(num_cnpj,'') ||
         nvl(dsc_razao_social,'') ||
         nvl(cod_canal_venda_entrada,'') ||
         nvl(dsc_canal_venda_entrada,'') ||
         nvl(cod_subcanal_entrada,'') ||
         nvl(dsc_subcanal_entrada,'') ||
         nvl(dsc_segmento_comercial_entrada,'') ||
         nvl(dsc_subsegmento_comercial_entrada,'') ||
         nvl(dsc_torre_comercial_entrada,'') ||
         nvl(cod_canal_venda_alocacao,'') ||
         nvl(dsc_canal_venda_alocacao,'') ||
         nvl(cod_subcanal_alocacao,'') ||
         nvl(dsc_subcanal_alocacao,'') ||
         nvl(dsc_segmento_comercial_alocacao,'') ||
         nvl(dsc_subsegmento_comercial_alocacao,'') ||
         nvl(dsc_torre_comercial_alocacao,'') ||
         nvl(cod_emissor,'') ||
         nvl(dsc_status_contrato,'') ||
         nvl(dsc_cidade_entrega,'') ||
         nvl(sig_estado_entrega,'') ||
         nvl(dsc_regiao_entrega,'') ||
         nvl(dsc_cidade_cobranca,'') ||
         nvl(sig_estado_cobranca,'') ||
         nvl(dsc_regiao_cobranca,'') ||
         nvl(cod_cnae_principal,'') ||
         nvl(dsc_ramo_atividade,'') ||
         nvl(dsc_setor,'') ||
         nvl(qtd_funcionario,'') ||
         nvl(dat_primeiro_credito,'') ||
         nvl(dat_primeiro_credito_filtro,'') ||
         nvl(dat_ultimo_credito,'') ||
         nvl(dat_ultimo_credito_filtro,'') ||
         nvl(flg_tipo_pagamento_pos_pago,'') ||
         nvl(flg_tipo_pagamento_pre_pago,'') ||
         nvl(qtd_contrato_ativo,'') ||
         nvl(qtd_contrato_pos_pago,'') ||
         nvl(qtd_contrato_pre_pago,'') ||
         nvl(flg_cliente_premium,'') ||
         nvl(flg_cliente_farmer,'') ||
         nvl(flg_possui_fidelidade,'') ||
         nvl(cod_grupo_economico,'') ||
         nvl(dsc_grupo_economico,'') ||
         nvl(val_creditado,'') ||
         nvl(val_creditado_m1,'') ||
         nvl(val_creditado_3m,'') ||
         nvl(qtd_conta_creditada_3m,'') ||
         nvl(qtd_conta_creditada_filtro_3m,'') ||
         nvl(qtd_conta,'') ||
         nvl(qtd_conta_status_ativo,'') ||
         nvl(qtd_conta_ativada,'') ||
         nvl(qtd_conta_ativada_nova,'') ||
         nvl(qtd_conta_ativada_base,'') ||
         nvl(qtd_conta_creditada,'') ||
         nvl(qtd_estoque_conta_ativada,'') ||
         nvl(val_faturado_bruto,'') ||
         nvl(val_faturado_liquido,'') ||
         nvl(val_facial_real,'') ||
         nvl(num_porte_3m,'') ||
         nvl(num_porte_6m,'') ||
         nvl(val_creditado_extrai_natal_saude,'') ||
         nvl(qtd_conta_extrai_natal_saude,'') ||
         nvl(qtd_conta_nova_faseado_filtro,'') ||
         nvl(val_creditado_faseado_filtro,'') ||
         nvl(val_creditado_sistemica_faseado_filtro,'') ||
         nvl(val_creditado_sistemica_filtro,'') ||
         nvl(val_creditado_extrai_saude_compras,'') ||
         nvl(qtd_cartao_emitido_filtro,'') ||
         nvl(qtd_cartao_emitido_1via_filtro,'') ||
         nvl(qtd_cartao_emitido_novo_1via_filtro,'') ||
         nvl(qtd_cartao_reemitido_filtro,'') ||
         nvl(qtd_bop_conta_creditada_filtro,'') ||
         nvl(qtd_eop_conta_creditada_filtro,'') ||
         nvl(qtd_conta_nova_sistemica_filtro,'') ||
         nvl(qtd_conta_nova_sistemica_faseado_filtro,'') ||
         nvl(qtd_bop_conta_ativo_filtro_40d,'') ||
         nvl(qtd_eop_conta_ativo_filtro_40d,'') ||
         nvl(val_creditado_bop_ativo_filtro_40d,'') ||
         nvl(val_creditado_eop_ativo_filtro_40d,'') ||
         nvl(dat_ultimo_credito_filtro_40d,'') ||
         nvl(dsc_segmento_alta_atendiento,'') ||
         nvl(qtd_conta_alta_atendimento,'') ||
         nvl(qtd_trabalhador_alta_atendimento,'') ||
         nvl(flg_inadimplencia,'') ||
         nvl(val_inadimplencia,'') ||
         nvl(flg_excecao_tombamento,'') ||
         nvl(flg_excecao_outlier,'') ||
         nvl(flg_excecao_projeto,'') ||
         nvl(num_safra,'') ||
         nvl(qtd_trabalhador,'') ||
         nvl(qtd_trabalhador_primeiro_credito,'') ||
         nvl(qtd_trabalhador_primeiro_credito_anterior,'') ||
         nvl(qtd_trabalhador_sem_credito_3m,'') ||
         nvl(qtd_trabalhador_ativo,'') ||
         nvl(qtd_trabalhador_ativo_filtro,'') ||
         nvl(qtd_trabalhador_ativo_3m,'') ||
         nvl(qtd_trabalhador_ativo_filtro_3m,'') ||
         nvl(val_creditado_ativo_filtro_3m,'') ||
         nvl(qtd_transacao_3m,'') ||
         nvl(qtd_maximo_trabalhador_ativo_filtro_3m,'') ||
         nvl(qtd_bop_trabalhador_creditado_filtro,'') ||
         nvl(qtd_eop_trabalhador_creditado_filtro,'') ||
         nvl(qtd_bop_maximo_trabalhador_ativo_filtro_3m,'') ||
         nvl(qtd_eop_maximo_trabalhador_ativo_filtro_3m,'') ||
         nvl(qtd_bop_maximo_conta_ativo_filtro_3m,'') ||
         nvl(qtd_eop_maximo_conta_ativo_filtro_3m,'') ||
         nvl(val_maximo_creditado_ativo_filtro_3m,'') ||
         nvl(val_bop_maximo_creditado_ativo_filtro_3m,'') ||
         nvl(val_eop_maximo_creditado_ativo_filtro_3m,'') ||
         nvl(num_porte_trabalhador_filtro_12m,'') ||
         nvl(dat_referencia_porte_trabalhador_12m,'') ||
         nvl(num_porte_conta_filtro_12m,'') ||
         nvl(dat_referencia_porte_conta_12m,'') ||
         nvl(val_maximo_creditado_ativo_12m,'') ||
         nvl(dat_referencia_maximo_creditado_ativo_12m,'') ||
         nvl(val_maximo_creditado_ativo_filtro_12m,'') ||
         nvl(dat_referencia_maximo_creditado_ativo_filtro_12m,'') ||
         nvl(qtd_mes_creditando,'') ||
         nvl(qtd_alta_comercial,'') ||
         nvl(val_creditado_alta_comercial,'') ||
         nvl(qtd_alta_involuntaria,'') ||
         nvl(qtd_bop_trabalhador_ativo_filtro_40d,'') ||
         nvl(qtd_eop_trabalhador_ativo_filtro_40d,'') ||
         nvl(qtd_trabalhador_novo_sistemico_filtro,'') ||
         nvl(qtd_trabalhador_novo_sistema_anterior,'') ||
         nvl(qtd_trabalhador_novo_sistemico_faseado_filtro,'') ||
         nvl(qtd_trabalhador_novo_sistemico_anterior,'') ||
         nvl(qtd_conta_alta_total_filtro,'') ||
         nvl(qtd_conta_churn_bruto_filtro,'') ||
         nvl(qtd_trabalhador_alta_total_filtro,'') ||
         nvl(qtd_trabalhador_churn_bruto_filtro,'') ||
         nvl(qtd_trabalhador_novo_faseado_filtro,'') ||
         nvl(qtd_conta_alta_total_filtro_40d,'') ||
         nvl(qtd_conta_churn_bruto_filtro_40d,'') ||
         nvl(qtd_trabalhador_alta_total_filtro_40d,'') ||
         nvl(qtd_trabalhador_churn_bruto_filtro_40d,'') ||
         nvl(qtd_conta_nova_involuntaria_filtro,'') ||
         nvl(qtd_conta_churn_liquido_filtro,'') ||
         nvl(qtd_trabalhador_novo_involuntario_filtro,'') ||
         nvl(qtd_trabalhador_involuntario_anterior,'') ||
         nvl(qtd_trabalhador_churn_liquido_filtro,'') ||
         nvl(qtd_trabalhador_churn_anterior,'') ||
         nvl(val_receita,'') ||
         nvl(val_tarifa_operacional,'') ||
         nvl(val_receita_total,'') ||
         nvl(qtd_maximo_trabalhador_3m_historico,'') ||
         nvl(val_maximo_creditado_3m_historico,'')
       ) as cod_hash_md5
  from tmp_planta_tratamento
;

-- COMMAND ----------

-- DBTITLE 1,Select caso de teste
-- MAGIC %python
-- MAGIC df_base_final = spark.sql("""
-- MAGIC Select dat_referencia,
-- MAGIC        srk_rh_cadastro,
-- MAGIC        num_cnpj,
-- MAGIC        dsc_razao_social,
-- MAGIC        cod_canal_venda_entrada,
-- MAGIC        dsc_canal_venda_entrada,
-- MAGIC        cod_subcanal_entrada,
-- MAGIC        dsc_subcanal_entrada,
-- MAGIC        dsc_segmento_comercial_entrada,
-- MAGIC        dsc_subsegmento_comercial_entrada,
-- MAGIC        dsc_torre_comercial_entrada,
-- MAGIC        cod_canal_venda_alocacao,
-- MAGIC        dsc_canal_venda_alocacao,
-- MAGIC        cod_subcanal_alocacao,
-- MAGIC        dsc_subcanal_alocacao,
-- MAGIC        dsc_segmento_comercial_alocacao,
-- MAGIC        dsc_subsegmento_comercial_alocacao,
-- MAGIC        dsc_torre_comercial_alocacao,
-- MAGIC        cod_emissor,
-- MAGIC        dsc_status_contrato,
-- MAGIC        dsc_cidade_entrega,
-- MAGIC        sig_estado_entrega,
-- MAGIC        dsc_regiao_entrega,
-- MAGIC        dsc_cidade_cobranca,
-- MAGIC        sig_estado_cobranca,
-- MAGIC        dsc_regiao_cobranca,
-- MAGIC        cod_cnae_principal,
-- MAGIC        dsc_ramo_atividade,
-- MAGIC        dsc_setor,
-- MAGIC        qtd_funcionario,
-- MAGIC        dat_primeiro_credito,
-- MAGIC        dat_primeiro_credito_filtro,
-- MAGIC        dat_ultimo_credito,
-- MAGIC        dat_ultimo_credito_filtro,
-- MAGIC        flg_tipo_pagamento_pos_pago,
-- MAGIC        flg_tipo_pagamento_pre_pago,
-- MAGIC        qtd_contrato_ativo,
-- MAGIC        qtd_contrato_pos_pago,
-- MAGIC        qtd_contrato_pre_pago,
-- MAGIC        flg_cliente_premium,
-- MAGIC        flg_cliente_farmer,
-- MAGIC        flg_possui_fidelidade,
-- MAGIC        cod_grupo_economico,
-- MAGIC        dsc_grupo_economico,
-- MAGIC        val_creditado,
-- MAGIC        val_creditado_m1,
-- MAGIC        val_creditado_3m,
-- MAGIC        qtd_conta_creditada_3m,
-- MAGIC        qtd_conta_creditada_filtro_3m,
-- MAGIC        qtd_conta,
-- MAGIC        qtd_conta_status_ativo,
-- MAGIC        qtd_conta_ativada,
-- MAGIC        qtd_conta_ativada_nova,
-- MAGIC        qtd_conta_ativada_base,
-- MAGIC        qtd_conta_creditada,
-- MAGIC        qtd_estoque_conta_ativada,
-- MAGIC        val_faturado_bruto,
-- MAGIC        val_faturado_liquido,
-- MAGIC        val_facial_real,
-- MAGIC        num_porte_3m,
-- MAGIC        num_porte_6m,
-- MAGIC        val_creditado_extrai_natal_saude,
-- MAGIC        qtd_conta_extrai_natal_saude,
-- MAGIC        qtd_conta_nova_faseado_filtro,
-- MAGIC        val_creditado_faseado_filtro,
-- MAGIC        val_creditado_sistemica_faseado_filtro,
-- MAGIC        val_creditado_sistemica_filtro,
-- MAGIC        val_creditado_extrai_saude_compras,
-- MAGIC        qtd_cartao_emitido_filtro,
-- MAGIC        qtd_cartao_emitido_1via_filtro,
-- MAGIC        qtd_cartao_emitido_novo_1via_filtro,
-- MAGIC        qtd_cartao_reemitido_filtro,
-- MAGIC        qtd_bop_conta_creditada_filtro,
-- MAGIC        qtd_eop_conta_creditada_filtro,
-- MAGIC        qtd_conta_nova_sistemica_filtro,
-- MAGIC        qtd_conta_nova_sistemica_faseado_filtro,
-- MAGIC        qtd_bop_conta_ativo_filtro_40d,
-- MAGIC        qtd_eop_conta_ativo_filtro_40d,
-- MAGIC        val_creditado_bop_ativo_filtro_40d,
-- MAGIC        val_creditado_eop_ativo_filtro_40d,
-- MAGIC        dat_ultimo_credito_filtro_40d,
-- MAGIC        dsc_segmento_alta_atendiento,
-- MAGIC        qtd_conta_alta_atendimento,
-- MAGIC        qtd_trabalhador_alta_atendimento,
-- MAGIC        flg_inadimplencia,
-- MAGIC        val_inadimplencia,
-- MAGIC        flg_excecao_tombamento,
-- MAGIC        flg_excecao_outlier,
-- MAGIC        flg_excecao_projeto,
-- MAGIC        num_safra,
-- MAGIC        qtd_trabalhador,
-- MAGIC        qtd_trabalhador_primeiro_credito,
-- MAGIC        qtd_trabalhador_primeiro_credito_anterior,
-- MAGIC        qtd_trabalhador_sem_credito_3m,
-- MAGIC        qtd_trabalhador_ativo,
-- MAGIC        qtd_trabalhador_ativo_filtro,
-- MAGIC        qtd_trabalhador_ativo_3m,
-- MAGIC        qtd_trabalhador_ativo_filtro_3m,
-- MAGIC        val_creditado_ativo_filtro_3m,
-- MAGIC        qtd_transacao_3m,
-- MAGIC        qtd_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC        qtd_bop_trabalhador_creditado_filtro,
-- MAGIC        qtd_eop_trabalhador_creditado_filtro,
-- MAGIC        qtd_bop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC        qtd_eop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC        qtd_bop_maximo_conta_ativo_filtro_3m,
-- MAGIC        qtd_eop_maximo_conta_ativo_filtro_3m,
-- MAGIC        val_maximo_creditado_ativo_filtro_3m,
-- MAGIC        val_bop_maximo_creditado_ativo_filtro_3m,
-- MAGIC        val_eop_maximo_creditado_ativo_filtro_3m,
-- MAGIC        num_porte_trabalhador_filtro_12m,
-- MAGIC        dat_referencia_porte_trabalhador_12m,
-- MAGIC        num_porte_conta_filtro_12m,
-- MAGIC        dat_referencia_porte_conta_12m,
-- MAGIC        val_maximo_creditado_ativo_12m,
-- MAGIC        dat_referencia_maximo_creditado_ativo_12m,
-- MAGIC        val_maximo_creditado_ativo_filtro_12m,
-- MAGIC        dat_referencia_maximo_creditado_ativo_filtro_12m,
-- MAGIC        qtd_mes_creditando,
-- MAGIC        qtd_alta_comercial,
-- MAGIC        val_creditado_alta_comercial,
-- MAGIC        qtd_alta_involuntaria,
-- MAGIC        qtd_bop_trabalhador_ativo_filtro_40d,
-- MAGIC        qtd_eop_trabalhador_ativo_filtro_40d,
-- MAGIC        qtd_trabalhador_novo_sistemico_filtro,
-- MAGIC        qtd_trabalhador_novo_sistema_anterior,
-- MAGIC        qtd_trabalhador_novo_sistemico_faseado_filtro,
-- MAGIC        qtd_trabalhador_novo_sistemico_anterior,
-- MAGIC        qtd_conta_alta_total_filtro,
-- MAGIC        qtd_conta_churn_bruto_filtro,
-- MAGIC        qtd_trabalhador_alta_total_filtro,
-- MAGIC        qtd_trabalhador_churn_bruto_filtro,
-- MAGIC        qtd_trabalhador_novo_faseado_filtro,
-- MAGIC        qtd_conta_alta_total_filtro_40d,
-- MAGIC        qtd_conta_churn_bruto_filtro_40d,
-- MAGIC        qtd_trabalhador_alta_total_filtro_40d,
-- MAGIC        qtd_trabalhador_churn_bruto_filtro_40d,
-- MAGIC        qtd_conta_nova_involuntaria_filtro,
-- MAGIC        qtd_conta_churn_liquido_filtro,
-- MAGIC        qtd_trabalhador_novo_involuntario_filtro,
-- MAGIC        qtd_trabalhador_involuntario_anterior,
-- MAGIC        qtd_trabalhador_churn_liquido_filtro,
-- MAGIC        qtd_trabalhador_churn_anterior,
-- MAGIC        val_receita,
-- MAGIC        val_tarifa_operacional,
-- MAGIC        val_receita_total,
-- MAGIC        qtd_maximo_trabalhador_3m_historico,
-- MAGIC        val_maximo_creditado_3m_historico,
-- MAGIC        cod_hash_md5
-- MAGIC   from vr_temporaria.tmp_fat_planta_vrb_cnpj_final
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Criptografa campos
-- MAGIC %python
-- MAGIC
-- MAGIC df_criptografado = fn_normaliza_e_criptografa(df_base_final, databricks_catalog, database, dsc_tabela_pii, spark)
-- MAGIC
-- MAGIC df_criptografado.createOrReplaceTempView("tmp_fat_planta_vrb_cnpj_criptografado")

-- COMMAND ----------

-- DBTITLE 1,Merge fat_planta
-- MAGIC %python
-- MAGIC
-- MAGIC df_merge = fn_merge_direto("""
-- MAGIC MERGE INTO gold_vr_cliente_rh_pii.fat_planta as destino 
-- MAGIC USING tmp_fat_planta_vrb_cnpj_criptografado as origem 
-- MAGIC    on origem.dat_referencia = destino.dat_referencia
-- MAGIC   and origem.srk_rh_cadastro = destino.srk_rh_cadastro
-- MAGIC   and origem.num_cnpj = destino.num_cnpj
-- MAGIC   and origem.cod_emissor = destino.cod_emissor
-- MAGIC   and destino.dat_referencia in (trunc(add_months(cast('""" + dat_parametro + """' as date), -1), 'MM'), trunc(cast('""" + dat_parametro + """' as date), 'MM'))
-- MAGIC  WHEN MATCHED AND nvl(origem.cod_hash_md5,'') <> nvl(destino.cod_hash_md5,'') THEN UPDATE SET
-- MAGIC       destino.dsc_razao_social = origem.dsc_razao_social,
-- MAGIC       destino.cod_canal_venda_entrada = origem.cod_canal_venda_entrada,
-- MAGIC       destino.dsc_canal_venda_entrada = origem.dsc_canal_venda_entrada,
-- MAGIC       destino.cod_subcanal_entrada = origem.cod_subcanal_entrada,
-- MAGIC       destino.dsc_subcanal_entrada = origem.dsc_subcanal_entrada,
-- MAGIC       destino.dsc_segmento_comercial_entrada = origem.dsc_segmento_comercial_entrada,
-- MAGIC       destino.dsc_subsegmento_comercial_entrada = origem.dsc_subsegmento_comercial_entrada,
-- MAGIC       destino.dsc_torre_comercial_entrada = origem.dsc_torre_comercial_entrada,
-- MAGIC       destino.cod_canal_venda_alocacao = origem.cod_canal_venda_alocacao,
-- MAGIC       destino.dsc_canal_venda_alocacao = origem.dsc_canal_venda_alocacao,
-- MAGIC       destino.cod_subcanal_alocacao = origem.cod_subcanal_alocacao,
-- MAGIC       destino.dsc_subcanal_alocacao = origem.dsc_subcanal_alocacao,
-- MAGIC       destino.dsc_segmento_comercial_alocacao = origem.dsc_segmento_comercial_alocacao,
-- MAGIC       destino.dsc_subsegmento_comercial_alocacao = origem.dsc_subsegmento_comercial_alocacao,
-- MAGIC       destino.dsc_torre_comercial_alocacao = origem.dsc_torre_comercial_alocacao,
-- MAGIC       destino.dsc_status_contrato = origem.dsc_status_contrato,
-- MAGIC       destino.dsc_cidade_entrega = origem.dsc_cidade_entrega,
-- MAGIC       destino.sig_estado_entrega = origem.sig_estado_entrega,
-- MAGIC       destino.dsc_regiao_entrega = origem.dsc_regiao_entrega,
-- MAGIC       destino.dsc_cidade_cobranca = origem.dsc_cidade_cobranca,
-- MAGIC       destino.sig_estado_cobranca = origem.sig_estado_cobranca,
-- MAGIC       destino.dsc_regiao_cobranca = origem.dsc_regiao_cobranca,
-- MAGIC       destino.cod_cnae_principal = origem.cod_cnae_principal,
-- MAGIC       destino.dsc_ramo_atividade = origem.dsc_ramo_atividade,
-- MAGIC       destino.dsc_setor = origem.dsc_setor,
-- MAGIC       destino.qtd_funcionario = origem.qtd_funcionario,
-- MAGIC       destino.dat_primeiro_credito = origem.dat_primeiro_credito,
-- MAGIC       destino.dat_primeiro_credito_filtro = origem.dat_primeiro_credito_filtro,
-- MAGIC       destino.dat_ultimo_credito = origem.dat_ultimo_credito,
-- MAGIC       destino.dat_ultimo_credito_filtro = origem.dat_ultimo_credito_filtro,
-- MAGIC       destino.flg_tipo_pagamento_pos_pago = origem.flg_tipo_pagamento_pos_pago,
-- MAGIC       destino.flg_tipo_pagamento_pre_pago = origem.flg_tipo_pagamento_pre_pago,
-- MAGIC       destino.qtd_contrato_ativo = origem.qtd_contrato_ativo,
-- MAGIC       destino.qtd_contrato_pos_pago = origem.qtd_contrato_pos_pago,
-- MAGIC       destino.qtd_contrato_pre_pago = origem.qtd_contrato_pre_pago,
-- MAGIC       destino.flg_cliente_premium = origem.flg_cliente_premium,
-- MAGIC       destino.flg_cliente_farmer = origem.flg_cliente_farmer,
-- MAGIC       destino.flg_possui_fidelidade = origem.flg_possui_fidelidade,
-- MAGIC       destino.cod_grupo_economico = origem.cod_grupo_economico,
-- MAGIC       destino.dsc_grupo_economico = origem.dsc_grupo_economico,
-- MAGIC       destino.val_creditado = origem.val_creditado,
-- MAGIC       destino.val_creditado_m1 = origem.val_creditado_m1,
-- MAGIC       destino.val_creditado_3m = origem.val_creditado_3m,
-- MAGIC       destino.qtd_conta_creditada_3m = origem.qtd_conta_creditada_3m,
-- MAGIC       destino.qtd_conta_creditada_filtro_3m = origem.qtd_conta_creditada_filtro_3m,
-- MAGIC       destino.qtd_conta = origem.qtd_conta,
-- MAGIC       destino.qtd_conta_status_ativo = origem.qtd_conta_status_ativo,
-- MAGIC       destino.qtd_conta_ativada = origem.qtd_conta_ativada,
-- MAGIC       destino.qtd_conta_ativada_nova = origem.qtd_conta_ativada_nova,
-- MAGIC       destino.qtd_conta_ativada_base = origem.qtd_conta_ativada_base,
-- MAGIC       destino.qtd_conta_creditada = origem.qtd_conta_creditada,
-- MAGIC       destino.qtd_estoque_conta_ativada = origem.qtd_estoque_conta_ativada,
-- MAGIC       destino.val_faturado_bruto = origem.val_faturado_bruto,
-- MAGIC       destino.val_faturado_liquido = origem.val_faturado_liquido,
-- MAGIC       destino.val_facial_real = origem.val_facial_real,
-- MAGIC       destino.num_porte_3m = origem.num_porte_3m,
-- MAGIC       destino.num_porte_6m = origem.num_porte_6m,
-- MAGIC       destino.val_creditado_extrai_natal_saude = origem.val_creditado_extrai_natal_saude,
-- MAGIC       destino.qtd_conta_extrai_natal_saude = origem.qtd_conta_extrai_natal_saude,
-- MAGIC       destino.qtd_conta_nova_faseado_filtro = origem.qtd_conta_nova_faseado_filtro,
-- MAGIC       destino.val_creditado_faseado_filtro = origem.val_creditado_faseado_filtro,
-- MAGIC       destino.val_creditado_sistemica_faseado_filtro = origem.val_creditado_sistemica_faseado_filtro,
-- MAGIC       destino.val_creditado_sistemica_filtro = origem.val_creditado_sistemica_filtro,
-- MAGIC       destino.val_creditado_extrai_saude_compras = origem.val_creditado_extrai_saude_compras,
-- MAGIC       destino.qtd_cartao_emitido_filtro = origem.qtd_cartao_emitido_filtro,
-- MAGIC       destino.qtd_cartao_emitido_1via_filtro = origem.qtd_cartao_emitido_1via_filtro,
-- MAGIC       destino.qtd_cartao_emitido_novo_1via_filtro = origem.qtd_cartao_emitido_novo_1via_filtro,
-- MAGIC       destino.qtd_cartao_reemitido_filtro = origem.qtd_cartao_reemitido_filtro,
-- MAGIC       destino.qtd_bop_conta_creditada_filtro = origem.qtd_bop_conta_creditada_filtro,
-- MAGIC       destino.qtd_eop_conta_creditada_filtro = origem.qtd_eop_conta_creditada_filtro,
-- MAGIC       destino.qtd_conta_nova_sistemica_filtro = origem.qtd_conta_nova_sistemica_filtro,
-- MAGIC       destino.qtd_conta_nova_sistemica_faseado_filtro = origem.qtd_conta_nova_sistemica_faseado_filtro,
-- MAGIC       destino.qtd_bop_conta_ativo_filtro_40d = origem.qtd_bop_conta_ativo_filtro_40d,
-- MAGIC       destino.qtd_eop_conta_ativo_filtro_40d = origem.qtd_eop_conta_ativo_filtro_40d,
-- MAGIC       destino.val_creditado_bop_ativo_filtro_40d = origem.val_creditado_bop_ativo_filtro_40d,
-- MAGIC       destino.val_creditado_eop_ativo_filtro_40d = origem.val_creditado_eop_ativo_filtro_40d,
-- MAGIC       destino.dat_ultimo_credito_filtro_40d = origem.dat_ultimo_credito_filtro_40d,
-- MAGIC       destino.dsc_segmento_alta_atendiento = origem.dsc_segmento_alta_atendiento,
-- MAGIC       destino.qtd_conta_alta_atendimento = origem.qtd_conta_alta_atendimento,
-- MAGIC       destino.qtd_trabalhador_alta_atendimento = origem.qtd_trabalhador_alta_atendimento,
-- MAGIC       destino.flg_inadimplencia = origem.flg_inadimplencia,
-- MAGIC       destino.val_inadimplencia = origem.val_inadimplencia,
-- MAGIC       destino.flg_excecao_tombamento = origem.flg_excecao_tombamento,
-- MAGIC       destino.flg_excecao_outlier = origem.flg_excecao_outlier,
-- MAGIC       destino.flg_excecao_projeto = origem.flg_excecao_projeto,
-- MAGIC       destino.num_safra = origem.num_safra,
-- MAGIC       destino.qtd_trabalhador = origem.qtd_trabalhador,
-- MAGIC       destino.qtd_trabalhador_primeiro_credito = origem.qtd_trabalhador_primeiro_credito,
-- MAGIC       destino.qtd_trabalhador_primeiro_credito_anterior = origem.qtd_trabalhador_primeiro_credito_anterior,
-- MAGIC       destino.qtd_trabalhador_sem_credito_3m = origem.qtd_trabalhador_sem_credito_3m,
-- MAGIC       destino.qtd_trabalhador_ativo = origem.qtd_trabalhador_ativo,
-- MAGIC       destino.qtd_trabalhador_ativo_filtro = origem.qtd_trabalhador_ativo_filtro,
-- MAGIC       destino.qtd_trabalhador_ativo_3m = origem.qtd_trabalhador_ativo_3m,
-- MAGIC       destino.qtd_trabalhador_ativo_filtro_3m = origem.qtd_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.val_creditado_ativo_filtro_3m = origem.val_creditado_ativo_filtro_3m,
-- MAGIC       destino.qtd_transacao_3m = origem.qtd_transacao_3m,
-- MAGIC       destino.qtd_maximo_trabalhador_ativo_filtro_3m = origem.qtd_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.qtd_bop_trabalhador_creditado_filtro = origem.qtd_bop_trabalhador_creditado_filtro,
-- MAGIC       destino.qtd_eop_trabalhador_creditado_filtro = origem.qtd_eop_trabalhador_creditado_filtro,
-- MAGIC       destino.qtd_bop_maximo_trabalhador_ativo_filtro_3m = origem.qtd_bop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.qtd_eop_maximo_trabalhador_ativo_filtro_3m = origem.qtd_eop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.qtd_bop_maximo_conta_ativo_filtro_3m = origem.qtd_bop_maximo_conta_ativo_filtro_3m,
-- MAGIC       destino.qtd_eop_maximo_conta_ativo_filtro_3m = origem.qtd_eop_maximo_conta_ativo_filtro_3m,
-- MAGIC       destino.val_maximo_creditado_ativo_filtro_3m = origem.val_maximo_creditado_ativo_filtro_3m,
-- MAGIC       destino.val_bop_maximo_creditado_ativo_filtro_3m = origem.val_bop_maximo_creditado_ativo_filtro_3m,
-- MAGIC       destino.val_eop_maximo_creditado_ativo_filtro_3m = origem.val_eop_maximo_creditado_ativo_filtro_3m,
-- MAGIC       destino.num_porte_trabalhador_filtro_12m = origem.num_porte_trabalhador_filtro_12m,
-- MAGIC       destino.dat_referencia_porte_trabalhador_12m = origem.dat_referencia_porte_trabalhador_12m,
-- MAGIC       destino.num_porte_conta_filtro_12m = origem.num_porte_conta_filtro_12m,
-- MAGIC       destino.dat_referencia_porte_conta_12m = origem.dat_referencia_porte_conta_12m,
-- MAGIC       destino.val_maximo_creditado_ativo_12m = origem.val_maximo_creditado_ativo_12m,
-- MAGIC       destino.dat_referencia_maximo_creditado_ativo_12m = origem.dat_referencia_maximo_creditado_ativo_12m,
-- MAGIC       destino.val_maximo_creditado_ativo_filtro_12m = origem.val_maximo_creditado_ativo_filtro_12m,
-- MAGIC       destino.dat_referencia_maximo_creditado_ativo_filtro_12m = origem.dat_referencia_maximo_creditado_ativo_filtro_12m,
-- MAGIC       destino.qtd_mes_creditando = origem.qtd_mes_creditando,
-- MAGIC       destino.qtd_alta_comercial = origem.qtd_alta_comercial,
-- MAGIC       destino.val_creditado_alta_comercial = origem.val_creditado_alta_comercial,
-- MAGIC       destino.qtd_alta_involuntaria = origem.qtd_alta_involuntaria,
-- MAGIC       destino.qtd_bop_trabalhador_ativo_filtro_40d = origem.qtd_bop_trabalhador_ativo_filtro_40d,
-- MAGIC       destino.qtd_eop_trabalhador_ativo_filtro_40d = origem.qtd_eop_trabalhador_ativo_filtro_40d,
-- MAGIC       destino.qtd_trabalhador_novo_sistemico_filtro = origem.qtd_trabalhador_novo_sistemico_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_sistema_anterior = origem.qtd_trabalhador_novo_sistema_anterior,
-- MAGIC       destino.qtd_trabalhador_novo_sistemico_faseado_filtro = origem.qtd_trabalhador_novo_sistemico_faseado_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_sistemico_anterior = origem.qtd_trabalhador_novo_sistemico_anterior,
-- MAGIC       destino.qtd_conta_alta_total_filtro = origem.qtd_conta_alta_total_filtro,
-- MAGIC       destino.qtd_conta_churn_bruto_filtro = origem.qtd_conta_churn_bruto_filtro,
-- MAGIC       destino.qtd_trabalhador_alta_total_filtro = origem.qtd_trabalhador_alta_total_filtro,
-- MAGIC       destino.qtd_trabalhador_churn_bruto_filtro = origem.qtd_trabalhador_churn_bruto_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_faseado_filtro = origem.qtd_trabalhador_novo_faseado_filtro,
-- MAGIC       destino.qtd_conta_alta_total_filtro_40d = origem.qtd_conta_alta_total_filtro_40d,
-- MAGIC       destino.qtd_conta_churn_bruto_filtro_40d = origem.qtd_conta_churn_bruto_filtro_40d,
-- MAGIC       destino.qtd_trabalhador_alta_total_filtro_40d = origem.qtd_trabalhador_alta_total_filtro_40d,
-- MAGIC       destino.qtd_trabalhador_churn_bruto_filtro_40d = origem.qtd_trabalhador_churn_bruto_filtro_40d,
-- MAGIC       destino.qtd_conta_nova_involuntaria_filtro = origem.qtd_conta_nova_involuntaria_filtro,
-- MAGIC       destino.qtd_conta_churn_liquido_filtro = origem.qtd_conta_churn_liquido_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_involuntario_filtro = origem.qtd_trabalhador_novo_involuntario_filtro,
-- MAGIC       destino.qtd_trabalhador_involuntario_anterior = origem.qtd_trabalhador_involuntario_anterior,
-- MAGIC       destino.qtd_trabalhador_churn_liquido_filtro = origem.qtd_trabalhador_churn_liquido_filtro,
-- MAGIC       destino.qtd_trabalhador_churn_anterior = origem.qtd_trabalhador_churn_anterior,
-- MAGIC       destino.val_receita = origem.val_receita,
-- MAGIC       destino.val_tarifa_operacional = origem.val_tarifa_operacional,
-- MAGIC       destino.val_receita_total = origem.val_receita_total,
-- MAGIC       destino.qtd_maximo_trabalhador_3m_historico = origem.qtd_maximo_trabalhador_3m_historico,
-- MAGIC       destino.val_maximo_creditado_3m_historico = origem.val_maximo_creditado_3m_historico,
-- MAGIC       destino.cod_hash_md5 = origem.cod_hash_md5,
-- MAGIC       destino.dat_atualizacao_bi = current_timestamp()
-- MAGIC  WHEN NOT MATCHED THEN INSERT (
-- MAGIC       destino.dat_referencia,
-- MAGIC       destino.srk_rh_cadastro,
-- MAGIC       destino.num_cnpj,
-- MAGIC       destino.dsc_razao_social,
-- MAGIC       destino.cod_canal_venda_entrada,
-- MAGIC       destino.dsc_canal_venda_entrada,
-- MAGIC       destino.cod_subcanal_entrada,
-- MAGIC       destino.dsc_subcanal_entrada,
-- MAGIC       destino.dsc_segmento_comercial_entrada,
-- MAGIC       destino.dsc_subsegmento_comercial_entrada,
-- MAGIC       destino.dsc_torre_comercial_entrada,
-- MAGIC       destino.cod_canal_venda_alocacao,
-- MAGIC       destino.dsc_canal_venda_alocacao,
-- MAGIC       destino.cod_subcanal_alocacao,
-- MAGIC       destino.dsc_subcanal_alocacao,
-- MAGIC       destino.dsc_segmento_comercial_alocacao,
-- MAGIC       destino.dsc_subsegmento_comercial_alocacao,
-- MAGIC       destino.dsc_torre_comercial_alocacao,
-- MAGIC       destino.cod_emissor,
-- MAGIC       destino.dsc_status_contrato,
-- MAGIC       destino.dsc_cidade_entrega,
-- MAGIC       destino.sig_estado_entrega,
-- MAGIC       destino.dsc_regiao_entrega,
-- MAGIC       destino.dsc_cidade_cobranca,
-- MAGIC       destino.sig_estado_cobranca,
-- MAGIC       destino.dsc_regiao_cobranca,
-- MAGIC       destino.cod_cnae_principal,
-- MAGIC       destino.dsc_ramo_atividade,
-- MAGIC       destino.dsc_setor,
-- MAGIC       destino.qtd_funcionario,
-- MAGIC       destino.dat_primeiro_credito,
-- MAGIC       destino.dat_primeiro_credito_filtro,
-- MAGIC       destino.dat_ultimo_credito,
-- MAGIC       destino.dat_ultimo_credito_filtro,
-- MAGIC       destino.flg_tipo_pagamento_pos_pago,
-- MAGIC       destino.flg_tipo_pagamento_pre_pago,
-- MAGIC       destino.qtd_contrato_ativo,
-- MAGIC       destino.qtd_contrato_pos_pago,
-- MAGIC       destino.qtd_contrato_pre_pago,
-- MAGIC       destino.flg_cliente_premium,
-- MAGIC       destino.flg_cliente_farmer,
-- MAGIC       destino.flg_possui_fidelidade,
-- MAGIC       destino.cod_grupo_economico,
-- MAGIC       destino.dsc_grupo_economico,
-- MAGIC       destino.val_creditado,
-- MAGIC       destino.val_creditado_m1,
-- MAGIC       destino.val_creditado_3m,
-- MAGIC       destino.qtd_conta_creditada_3m,
-- MAGIC       destino.qtd_conta_creditada_filtro_3m,
-- MAGIC       destino.qtd_conta,
-- MAGIC       destino.qtd_conta_status_ativo,
-- MAGIC       destino.qtd_conta_ativada,
-- MAGIC       destino.qtd_conta_ativada_nova,
-- MAGIC       destino.qtd_conta_ativada_base,
-- MAGIC       destino.qtd_conta_creditada,
-- MAGIC       destino.qtd_estoque_conta_ativada,
-- MAGIC       destino.val_faturado_bruto,
-- MAGIC       destino.val_faturado_liquido,
-- MAGIC       destino.val_facial_real,
-- MAGIC       destino.num_porte_3m,
-- MAGIC       destino.num_porte_6m,
-- MAGIC       destino.val_creditado_extrai_natal_saude,
-- MAGIC       destino.qtd_conta_extrai_natal_saude,
-- MAGIC       destino.qtd_conta_nova_faseado_filtro,
-- MAGIC       destino.val_creditado_faseado_filtro,
-- MAGIC       destino.val_creditado_sistemica_faseado_filtro,
-- MAGIC       destino.val_creditado_sistemica_filtro,
-- MAGIC       destino.val_creditado_extrai_saude_compras,
-- MAGIC       destino.qtd_cartao_emitido_filtro,
-- MAGIC       destino.qtd_cartao_emitido_1via_filtro,
-- MAGIC       destino.qtd_cartao_emitido_novo_1via_filtro,
-- MAGIC       destino.qtd_cartao_reemitido_filtro,
-- MAGIC       destino.qtd_bop_conta_creditada_filtro,
-- MAGIC       destino.qtd_eop_conta_creditada_filtro,
-- MAGIC       destino.qtd_conta_nova_sistemica_filtro,
-- MAGIC       destino.qtd_conta_nova_sistemica_faseado_filtro,
-- MAGIC       destino.qtd_bop_conta_ativo_filtro_40d,
-- MAGIC       destino.qtd_eop_conta_ativo_filtro_40d,
-- MAGIC       destino.val_creditado_bop_ativo_filtro_40d,
-- MAGIC       destino.val_creditado_eop_ativo_filtro_40d,
-- MAGIC       destino.dat_ultimo_credito_filtro_40d,
-- MAGIC       destino.dsc_segmento_alta_atendiento,
-- MAGIC       destino.qtd_conta_alta_atendimento,
-- MAGIC       destino.qtd_trabalhador_alta_atendimento,
-- MAGIC       destino.flg_inadimplencia,
-- MAGIC       destino.val_inadimplencia,
-- MAGIC       destino.flg_excecao_tombamento,
-- MAGIC       destino.flg_excecao_outlier,
-- MAGIC       destino.flg_excecao_projeto,
-- MAGIC       destino.num_safra,
-- MAGIC       destino.qtd_trabalhador,
-- MAGIC       destino.qtd_trabalhador_primeiro_credito,
-- MAGIC       destino.qtd_trabalhador_primeiro_credito_anterior,
-- MAGIC       destino.qtd_trabalhador_sem_credito_3m,
-- MAGIC       destino.qtd_trabalhador_ativo,
-- MAGIC       destino.qtd_trabalhador_ativo_filtro,
-- MAGIC       destino.qtd_trabalhador_ativo_3m,
-- MAGIC       destino.qtd_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.val_creditado_ativo_filtro_3m,
-- MAGIC       destino.qtd_transacao_3m,
-- MAGIC       destino.qtd_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.qtd_bop_trabalhador_creditado_filtro,
-- MAGIC       destino.qtd_eop_trabalhador_creditado_filtro,
-- MAGIC       destino.qtd_bop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.qtd_eop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       destino.qtd_bop_maximo_conta_ativo_filtro_3m,
-- MAGIC       destino.qtd_eop_maximo_conta_ativo_filtro_3m,
-- MAGIC       destino.val_maximo_creditado_ativo_filtro_3m,
-- MAGIC       destino.val_bop_maximo_creditado_ativo_filtro_3m,
-- MAGIC       destino.val_eop_maximo_creditado_ativo_filtro_3m,
-- MAGIC       destino.num_porte_trabalhador_filtro_12m,
-- MAGIC       destino.dat_referencia_porte_trabalhador_12m,
-- MAGIC       destino.num_porte_conta_filtro_12m,
-- MAGIC       destino.dat_referencia_porte_conta_12m,
-- MAGIC       destino.val_maximo_creditado_ativo_12m,
-- MAGIC       destino.dat_referencia_maximo_creditado_ativo_12m,
-- MAGIC       destino.val_maximo_creditado_ativo_filtro_12m,
-- MAGIC       destino.dat_referencia_maximo_creditado_ativo_filtro_12m,
-- MAGIC       destino.qtd_mes_creditando,
-- MAGIC       destino.qtd_alta_comercial,
-- MAGIC       destino.val_creditado_alta_comercial,
-- MAGIC       destino.qtd_alta_involuntaria,
-- MAGIC       destino.qtd_bop_trabalhador_ativo_filtro_40d,
-- MAGIC       destino.qtd_eop_trabalhador_ativo_filtro_40d,
-- MAGIC       destino.qtd_trabalhador_novo_sistemico_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_sistema_anterior,
-- MAGIC       destino.qtd_trabalhador_novo_sistemico_faseado_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_sistemico_anterior,
-- MAGIC       destino.qtd_conta_alta_total_filtro,
-- MAGIC       destino.qtd_conta_churn_bruto_filtro,
-- MAGIC       destino.qtd_trabalhador_alta_total_filtro,
-- MAGIC       destino.qtd_trabalhador_churn_bruto_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_faseado_filtro,
-- MAGIC       destino.qtd_conta_alta_total_filtro_40d,
-- MAGIC       destino.qtd_conta_churn_bruto_filtro_40d,
-- MAGIC       destino.qtd_trabalhador_alta_total_filtro_40d,
-- MAGIC       destino.qtd_trabalhador_churn_bruto_filtro_40d,
-- MAGIC       destino.qtd_conta_nova_involuntaria_filtro,
-- MAGIC       destino.qtd_conta_churn_liquido_filtro,
-- MAGIC       destino.qtd_trabalhador_novo_involuntario_filtro,
-- MAGIC       destino.qtd_trabalhador_involuntario_anterior,
-- MAGIC       destino.qtd_trabalhador_churn_liquido_filtro,
-- MAGIC       destino.qtd_trabalhador_churn_anterior,
-- MAGIC       destino.val_receita,
-- MAGIC       destino.val_tarifa_operacional,
-- MAGIC       destino.val_receita_total,
-- MAGIC       destino.qtd_maximo_trabalhador_3m_historico,
-- MAGIC       destino.val_maximo_creditado_3m_historico,
-- MAGIC       destino.cod_hash_md5,
-- MAGIC       destino.dat_inclusao_bi,
-- MAGIC       destino.dat_atualizacao_bi
-- MAGIC     ) VALUES (
-- MAGIC       origem.dat_referencia,
-- MAGIC       origem.srk_rh_cadastro,
-- MAGIC       origem.num_cnpj,
-- MAGIC       origem.dsc_razao_social,
-- MAGIC       origem.cod_canal_venda_entrada,
-- MAGIC       origem.dsc_canal_venda_entrada,
-- MAGIC       origem.cod_subcanal_entrada,
-- MAGIC       origem.dsc_subcanal_entrada,
-- MAGIC       origem.dsc_segmento_comercial_entrada,
-- MAGIC       origem.dsc_subsegmento_comercial_entrada,
-- MAGIC       origem.dsc_torre_comercial_entrada,
-- MAGIC       origem.cod_canal_venda_alocacao,
-- MAGIC       origem.dsc_canal_venda_alocacao,
-- MAGIC       origem.cod_subcanal_alocacao,
-- MAGIC       origem.dsc_subcanal_alocacao,
-- MAGIC       origem.dsc_segmento_comercial_alocacao,
-- MAGIC       origem.dsc_subsegmento_comercial_alocacao,
-- MAGIC       origem.dsc_torre_comercial_alocacao,
-- MAGIC       origem.cod_emissor,
-- MAGIC       origem.dsc_status_contrato,
-- MAGIC       origem.dsc_cidade_entrega,
-- MAGIC       origem.sig_estado_entrega,
-- MAGIC       origem.dsc_regiao_entrega,
-- MAGIC       origem.dsc_cidade_cobranca,
-- MAGIC       origem.sig_estado_cobranca,
-- MAGIC       origem.dsc_regiao_cobranca,
-- MAGIC       origem.cod_cnae_principal,
-- MAGIC       origem.dsc_ramo_atividade,
-- MAGIC       origem.dsc_setor,
-- MAGIC       origem.qtd_funcionario,
-- MAGIC       origem.dat_primeiro_credito,
-- MAGIC       origem.dat_primeiro_credito_filtro,
-- MAGIC       origem.dat_ultimo_credito,
-- MAGIC       origem.dat_ultimo_credito_filtro,
-- MAGIC       origem.flg_tipo_pagamento_pos_pago,
-- MAGIC       origem.flg_tipo_pagamento_pre_pago,
-- MAGIC       origem.qtd_contrato_ativo,
-- MAGIC       origem.qtd_contrato_pos_pago,
-- MAGIC       origem.qtd_contrato_pre_pago,
-- MAGIC       origem.flg_cliente_premium,
-- MAGIC       origem.flg_cliente_farmer,
-- MAGIC       origem.flg_possui_fidelidade,
-- MAGIC       origem.cod_grupo_economico,
-- MAGIC       origem.dsc_grupo_economico,
-- MAGIC       origem.val_creditado,
-- MAGIC       origem.val_creditado_m1,
-- MAGIC       origem.val_creditado_3m,
-- MAGIC       origem.qtd_conta_creditada_3m,
-- MAGIC       origem.qtd_conta_creditada_filtro_3m,
-- MAGIC       origem.qtd_conta,
-- MAGIC       origem.qtd_conta_status_ativo,
-- MAGIC       origem.qtd_conta_ativada,
-- MAGIC       origem.qtd_conta_ativada_nova,
-- MAGIC       origem.qtd_conta_ativada_base,
-- MAGIC       origem.qtd_conta_creditada,
-- MAGIC       origem.qtd_estoque_conta_ativada,
-- MAGIC       origem.val_faturado_bruto,
-- MAGIC       origem.val_faturado_liquido,
-- MAGIC       origem.val_facial_real,
-- MAGIC       origem.num_porte_3m,
-- MAGIC       origem.num_porte_6m,
-- MAGIC       origem.val_creditado_extrai_natal_saude,
-- MAGIC       origem.qtd_conta_extrai_natal_saude,
-- MAGIC       origem.qtd_conta_nova_faseado_filtro,
-- MAGIC       origem.val_creditado_faseado_filtro,
-- MAGIC       origem.val_creditado_sistemica_faseado_filtro,
-- MAGIC       origem.val_creditado_sistemica_filtro,
-- MAGIC       origem.val_creditado_extrai_saude_compras,
-- MAGIC       origem.qtd_cartao_emitido_filtro,
-- MAGIC       origem.qtd_cartao_emitido_1via_filtro,
-- MAGIC       origem.qtd_cartao_emitido_novo_1via_filtro,
-- MAGIC       origem.qtd_cartao_reemitido_filtro,
-- MAGIC       origem.qtd_bop_conta_creditada_filtro,
-- MAGIC       origem.qtd_eop_conta_creditada_filtro,
-- MAGIC       origem.qtd_conta_nova_sistemica_filtro,
-- MAGIC       origem.qtd_conta_nova_sistemica_faseado_filtro,
-- MAGIC       origem.qtd_bop_conta_ativo_filtro_40d,
-- MAGIC       origem.qtd_eop_conta_ativo_filtro_40d,
-- MAGIC       origem.val_creditado_bop_ativo_filtro_40d,
-- MAGIC       origem.val_creditado_eop_ativo_filtro_40d,
-- MAGIC       origem.dat_ultimo_credito_filtro_40d,
-- MAGIC       origem.dsc_segmento_alta_atendiento,
-- MAGIC       origem.qtd_conta_alta_atendimento,
-- MAGIC       origem.qtd_trabalhador_alta_atendimento,
-- MAGIC       origem.flg_inadimplencia,
-- MAGIC       origem.val_inadimplencia,
-- MAGIC       origem.flg_excecao_tombamento,
-- MAGIC       origem.flg_excecao_outlier,
-- MAGIC       origem.flg_excecao_projeto,
-- MAGIC       origem.num_safra,
-- MAGIC       origem.qtd_trabalhador,
-- MAGIC       origem.qtd_trabalhador_primeiro_credito,
-- MAGIC       origem.qtd_trabalhador_primeiro_credito_anterior,
-- MAGIC       origem.qtd_trabalhador_sem_credito_3m,
-- MAGIC       origem.qtd_trabalhador_ativo,
-- MAGIC       origem.qtd_trabalhador_ativo_filtro,
-- MAGIC       origem.qtd_trabalhador_ativo_3m,
-- MAGIC       origem.qtd_trabalhador_ativo_filtro_3m,
-- MAGIC       origem.val_creditado_ativo_filtro_3m,
-- MAGIC       origem.qtd_transacao_3m,
-- MAGIC       origem.qtd_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       origem.qtd_bop_trabalhador_creditado_filtro,
-- MAGIC       origem.qtd_eop_trabalhador_creditado_filtro,
-- MAGIC       origem.qtd_bop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       origem.qtd_eop_maximo_trabalhador_ativo_filtro_3m,
-- MAGIC       origem.qtd_bop_maximo_conta_ativo_filtro_3m,
-- MAGIC       origem.qtd_eop_maximo_conta_ativo_filtro_3m,
-- MAGIC       origem.val_maximo_creditado_ativo_filtro_3m,
-- MAGIC       origem.val_bop_maximo_creditado_ativo_filtro_3m,
-- MAGIC       origem.val_eop_maximo_creditado_ativo_filtro_3m,
-- MAGIC       origem.num_porte_trabalhador_filtro_12m,
-- MAGIC       origem.dat_referencia_porte_trabalhador_12m,
-- MAGIC       origem.num_porte_conta_filtro_12m,
-- MAGIC       origem.dat_referencia_porte_conta_12m,
-- MAGIC       origem.val_maximo_creditado_ativo_12m,
-- MAGIC       origem.dat_referencia_maximo_creditado_ativo_12m,
-- MAGIC       origem.val_maximo_creditado_ativo_filtro_12m,
-- MAGIC       origem.dat_referencia_maximo_creditado_ativo_filtro_12m,
-- MAGIC       origem.qtd_mes_creditando,
-- MAGIC       origem.qtd_alta_comercial,
-- MAGIC       origem.val_creditado_alta_comercial,
-- MAGIC       origem.qtd_alta_involuntaria,
-- MAGIC       origem.qtd_bop_trabalhador_ativo_filtro_40d,
-- MAGIC       origem.qtd_eop_trabalhador_ativo_filtro_40d,
-- MAGIC       origem.qtd_trabalhador_novo_sistemico_filtro,
-- MAGIC       origem.qtd_trabalhador_novo_sistema_anterior,
-- MAGIC       origem.qtd_trabalhador_novo_sistemico_faseado_filtro,
-- MAGIC       origem.qtd_trabalhador_novo_sistemico_anterior,
-- MAGIC       origem.qtd_conta_alta_total_filtro,
-- MAGIC       origem.qtd_conta_churn_bruto_filtro,
-- MAGIC       origem.qtd_trabalhador_alta_total_filtro,
-- MAGIC       origem.qtd_trabalhador_churn_bruto_filtro,
-- MAGIC       origem.qtd_trabalhador_novo_faseado_filtro,
-- MAGIC       origem.qtd_conta_alta_total_filtro_40d,
-- MAGIC       origem.qtd_conta_churn_bruto_filtro_40d,
-- MAGIC       origem.qtd_trabalhador_alta_total_filtro_40d,
-- MAGIC       origem.qtd_trabalhador_churn_bruto_filtro_40d,
-- MAGIC       origem.qtd_conta_nova_involuntaria_filtro,
-- MAGIC       origem.qtd_conta_churn_liquido_filtro,
-- MAGIC       origem.qtd_trabalhador_novo_involuntario_filtro,
-- MAGIC       origem.qtd_trabalhador_involuntario_anterior,
-- MAGIC       origem.qtd_trabalhador_churn_liquido_filtro,
-- MAGIC       origem.qtd_trabalhador_churn_anterior,
-- MAGIC       origem.val_receita,
-- MAGIC       origem.val_tarifa_operacional,
-- MAGIC       origem.val_receita_total,
-- MAGIC       origem.qtd_maximo_trabalhador_3m_historico,
-- MAGIC       origem.val_maximo_creditado_3m_historico,
-- MAGIC       origem.cod_hash_md5,
-- MAGIC       current_timestamp(),
-- MAGIC       current_timestamp()
-- MAGIC )"""
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
drop table if exists vr_temporaria.tmp_fat_planta_vrb_cnpj_final;

-- COMMAND ----------

-- DBTITLE 1,Exit notebook
-- MAGIC %python
-- MAGIC
-- MAGIC fn_gera_arquivo_bastao(malha,database,tablename)
-- MAGIC fn_exit_notebook(df_merge)
