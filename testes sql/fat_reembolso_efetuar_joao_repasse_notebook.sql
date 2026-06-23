-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("dat_parametro","")
-- MAGIC dbutils.widgets.text("tablename","")
-- MAGIC dbutils.widgets.text("database","")
-- MAGIC dbutils.widgets.text("malha","")
-- MAGIC dbutils.widgets.text("databricks_catalog","")
-- MAGIC dbutils.widgets.text("location_catalogo_temp","")
-- MAGIC
-- MAGIC dat_parametro = getArgument("dat_parametro")
-- MAGIC tablename = getArgument("tablename")
-- MAGIC database = getArgument("database")
-- MAGIC malha = getArgument("malha")
-- MAGIC databricks_catalog = getArgument("databricks_catalog")
-- MAGIC location_catalogo_temp = getArgument("location_catalogo_temp")
-- MAGIC
-- MAGIC # Exemplo de preenchimento
-- MAGIC # dat_parametro = 2026-04-19
-- MAGIC # tablename = fat_beneficio_circulacao_rh_mensal
-- MAGIC # database = gold_vr_corporativo_contabilidade
-- MAGIC # malha = temp
-- MAGIC # databricks_catalog = dev
-- MAGIC # location_catalogo_temp = abfss://temp@adlsvrbeneficiosdev01.dfs.core.windows.net

-- COMMAND ----------

-- MAGIC %run ../../../../Utils/Functions/core

-- COMMAND ----------

use catalog '${databricks_catalog}'

-- COMMAND ----------

-- reembolso
create or replace temporary view tmp_reeembolso as
select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
--a.num_guia_reembolso,
case when  ( a.cod_produto = 'VRO-REFEICAO-SRV' or 
             a.cod_produto = 'VBV-AUTO-SRV' or
             a.cod_produto = 'VBC-CULTURA-SRV' or
             a.cod_produto = 'VBA-ALIMENT-SRV' or
             a.cod_produto = '034-MAMAE-I-SRV' or
             a.cod_produto = 'VRO-ADICIONAL-SRV' or
             a.cod_produto = 'COM-COMPRAS-SRV' or
             a.cod_produto = 'NAT-NATAL-SRV' or
             a.cod_produto = 'VCA-CESTA-SRV' or
             a.cod_produto = 'AXR-REFEICAO-SRV' or
             a.cod_produto = 'AXA-ALIMENT-SRV'  or
             a.cod_produto = 'VRS-VRSAUDE-SRV' or
             a.cod_produto = 'HOM-HOMMULT-SRV' or
             a.cod_produto = 'AXM-MSALDO-SRV' or
             a.cod_produto = 'VAM-MSALPAT-SRV' or
             a.cod_produto = 'FXM-MSALFLEX-SRV' or
             a.cod_produto = 'VRM-MSALPAT-SRV' or
             a.cod_produto = 'RXM-MSALDO-SRV' or
             a.cod_produto = 'VBM-MSALTO-SRV' or
             a.cod_produto = 'FLX-FLEX-SRV' or
             a.cod_produto = 'MBF-MULTI-SRV' or
             a.cod_produto = 'AUB-AUX-MOBI-SRV' or
             a.cod_produto = 'PRM-MULTPREMIA-SRV') then 'SNFS1'
             else a.cod_sdprod end as cod_sdprod ,
a.cod_produto,
e.num_cnpj,
dsc_razao_social,
dsc_nome_fantasia,
sum(c.valor) as val_reembolso_bruto
from prod.gold_vr_cliente_ec.fat_reembolso a
join prod.gold_vr_cliente_ec.dim_cadastro e
on a.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
inner join (SELECT 
  id_protocolo_pedido, 
  SUBSTR(LTRIM(RTRIM(ID_PROTOCOLO_PEDIDO)), 1,1) ID_REEMB,
  CASE 
    WHEN instr(id_protocolo_pedido, ';') > 0 THEN 
        substr(id_protocolo_pedido, 3, 9)
    ELSE 
      substr(id_protocolo_pedido, 3, 8) 
  END AS nr_guiareem,
  cast(valor/100 as decimal(15,2)) valor
FROM 
  prod.silver_vr_kenan.arbor_smt_controle_interf_knn_contab
  where TP_USO_KNN IN ( 10021, 10015, 10017, 10019, 10123, 10041, 10043, 10045, 10047, 10049, 10070, 10072, 10074, 10076, 10078, 10080, 10278, 
    10082, 10084, 10098, 10015, 10094, 10096, 10222, 10220,10090,10092, 10257, 10263, 10267, 10271, 10275)) c 
on a.num_guia_reembolso = c.nr_guiareem
where trunc(cast(dat_criacao_reembolso as date), 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and c.ID_REEMB = 'R'
group by trunc(cast('${dat_parametro}' as date), 'MM') ,
case when  ( a.cod_produto = 'VRO-REFEICAO-SRV' or 
             a.cod_produto = 'VBV-AUTO-SRV' or
             a.cod_produto = 'VBC-CULTURA-SRV' or
             a.cod_produto = 'VBA-ALIMENT-SRV' or
             a.cod_produto = '034-MAMAE-I-SRV' or
             a.cod_produto = 'VRO-ADICIONAL-SRV' or
             a.cod_produto = 'COM-COMPRAS-SRV' or
             a.cod_produto = 'NAT-NATAL-SRV' or
             a.cod_produto = 'VCA-CESTA-SRV' or
             a.cod_produto = 'AXR-REFEICAO-SRV' or
             a.cod_produto = 'AXA-ALIMENT-SRV'  or
             a.cod_produto = 'VRS-VRSAUDE-SRV' or
             a.cod_produto = 'HOM-HOMMULT-SRV' or
             a.cod_produto = 'AXM-MSALDO-SRV' or
             a.cod_produto = 'VAM-MSALPAT-SRV' or
             a.cod_produto = 'FXM-MSALFLEX-SRV' or
             a.cod_produto = 'VRM-MSALPAT-SRV' or
             a.cod_produto = 'RXM-MSALDO-SRV' or
             a.cod_produto = 'VBM-MSALTO-SRV' or
             a.cod_produto = 'FLX-FLEX-SRV' or
             a.cod_produto = 'MBF-MULTI-SRV' or
             a.cod_produto = 'AUB-AUX-MOBI-SRV' or
             a.cod_produto = 'PRM-MULTPREMIA-SRV') then 'SNFS1'
             else a.cod_sdprod end ,
a.cod_produto,
e.num_cnpj,
--a.num_guia_reembolso,
dsc_razao_social,
dsc_nome_fantasia

-- COMMAND ----------

--estorno reembolso bruto
create or replace temporary view tmp_estorno_reembolso_bruto as 
select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
case when  ( a.cod_produto = 'VRO-REFEICAO-SRV' or 
             a.cod_produto = 'VBV-AUTO-SRV' or
             a.cod_produto = 'VBC-CULTURA-SRV' or
             a.cod_produto = 'VBA-ALIMENT-SRV' or
             a.cod_produto = '034-MAMAE-I-SRV' or
             a.cod_produto = 'VRO-ADICIONAL-SRV' or
             a.cod_produto = 'COM-COMPRAS-SRV' or
             a.cod_produto = 'NAT-NATAL-SRV' or
             a.cod_produto = 'VCA-CESTA-SRV' or
             a.cod_produto = 'AXR-REFEICAO-SRV' or
             a.cod_produto = 'AXA-ALIMENT-SRV'  or
             a.cod_produto = 'VRS-VRSAUDE-SRV' or
             a.cod_produto = 'HOM-HOMMULT-SRV' or
             a.cod_produto = 'AXM-MSALDO-SRV' or
             a.cod_produto = 'VAM-MSALPAT-SRV' or
             a.cod_produto = 'FXM-MSALFLEX-SRV' or
             a.cod_produto = 'VRM-MSALPAT-SRV' or
             a.cod_produto = 'RXM-MSALDO-SRV' or
             a.cod_produto = 'VBM-MSALTO-SRV' or
             a.cod_produto = 'FLX-FLEX-SRV' or
             a.cod_produto = 'MBF-MULTI-SRV' or
             a.cod_produto = 'AUB-AUX-MOBI-SRV' or
             a.cod_produto = 'PRM-MULTPREMIA-SRV') then 'SNFS1'
             else a.cod_sdprod end as cod_sdprod ,
             e.num_cnpj,
             dsc_razao_social,
dsc_nome_fantasia,
a.cod_produto,
sum(c.valor) as val_reembolso_estorno_bruto
from prod.gold_vr_cliente_ec.fat_reembolso a
join prod.gold_vr_cliente_ec.dim_cadastro e
on a.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
inner join (SELECT 
  id_protocolo_pedido, 
  SUBSTR(LTRIM(RTRIM(ID_PROTOCOLO_PEDIDO)), 1,1) ID_REEMB,
  CASE 
    WHEN instr(id_protocolo_pedido, ';') > 0 THEN 
        substr(id_protocolo_pedido, 3, 9)
    ELSE 
      substr(id_protocolo_pedido, 3, 8) 
  END AS nr_guiareem,
  cast(valor/100 as decimal(15,2)) valor
FROM 
  prod.silver_vr_kenan.arbor_smt_controle_interf_knn_contab
  where TP_USO_KNN IN (10016,10018,10020,10022,10044,10048,10050,10073,10075,10079,10099,10223,10221,10042,10081,10077,10071,10091,10093, 10260, 10265,  10279, 10269, 10273, 10277)) c
on a.num_guia_reembolso = c.nr_guiareem
where trunc(cast(dat_criacao_reembolso as date), 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and c.ID_REEMB = 'R'
group by trunc(cast('${dat_parametro}' as date), 'MM') ,
case when  ( a.cod_produto = 'VRO-REFEICAO-SRV' or 
             a.cod_produto = 'VBV-AUTO-SRV' or
             a.cod_produto = 'VBC-CULTURA-SRV' or
             a.cod_produto = 'VBA-ALIMENT-SRV' or
             a.cod_produto = '034-MAMAE-I-SRV' or
             a.cod_produto = 'VRO-ADICIONAL-SRV' or
             a.cod_produto = 'COM-COMPRAS-SRV' or
             a.cod_produto = 'NAT-NATAL-SRV' or
             a.cod_produto = 'VCA-CESTA-SRV' or
             a.cod_produto = 'AXR-REFEICAO-SRV' or
             a.cod_produto = 'AXA-ALIMENT-SRV'  or
             a.cod_produto = 'VRS-VRSAUDE-SRV' or
             a.cod_produto = 'HOM-HOMMULT-SRV' or
             a.cod_produto = 'AXM-MSALDO-SRV' or
             a.cod_produto = 'VAM-MSALPAT-SRV' or
             a.cod_produto = 'FXM-MSALFLEX-SRV' or
             a.cod_produto = 'VRM-MSALPAT-SRV' or
             a.cod_produto = 'RXM-MSALDO-SRV' or
             a.cod_produto = 'VBM-MSALTO-SRV' or
             a.cod_produto = 'FLX-FLEX-SRV' or
             a.cod_produto = 'MBF-MULTI-SRV' or
             a.cod_produto = 'AUB-AUX-MOBI-SRV' or
             a.cod_produto = 'PRM-MULTPREMIA-SRV') then 'SNFS1'
             else a.cod_sdprod end ,
a.cod_produto,
e.num_cnpj,
dsc_razao_social,
dsc_nome_fantasia


-- COMMAND ----------

create or replace temporary view tmp_reembolso_confirmado as
select 
trunc(dat_pagamento_contabil, 'MM') dat_referencia,
sum(val_pagamento) val_pagamento_reembolso,
cod_sdprod,
cod_produto,
--a.num_guia_reembolso,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia
from 
(select distinct dat_pagamento_contabil, num_guia, val_pagamento from prod.gold_vr_cliente_ec.fat_voucher 
where cod_unidade_negocio = 'SNBRA'
and cod_produto is not null
and cod_produto != 'PTM'
and trunc(dat_pagamento_contabil, 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')) a
inner join  prod.gold_vr_cliente_ec.fat_reembolso b 
on a.num_guia = b.num_guia_reembolso
 join prod.gold_vr_cliente_ec.dim_cadastro e
on b.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
where b.cod_produto in ('VRO-REFEICAO-SRV', '034-MAMAEVR-I-SRV', '034-MAMAE-I-SRV', 'VBC-CULTURA-SRV', 'VBA-ALIMENT-SRV', 'VBV-AUTO-SRV','RED-COMPRAS-SRV', '00269-ALIMPAT-SRV', '00268-REFPAT-SRV')
--and a.num_guia_reembolso = ,
group by 
trunc(dat_pagamento_contabil, 'MM'),
cod_sdprod,
cod_produto,
--a.num_guia_reembolso,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia

-- COMMAND ----------

create or replace temporary view tmp_ajuste_credito as 
select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
cod_sdprod,
cod_produto,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia,
sum(val_taxa_ajuste) as val_taxa_credito
from prod.gold_vr_cliente_ec.fat_reembolso a
join prod.gold_vr_cliente_ec.dim_cadastro e
on a.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
where trunc(cast(dat_primeiro_pagamento as date), 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and  cod_produto IN ('VBC-CULTURA-SRV', 'VRO-REFEICAO-SRV', 'VBA-ALIMENT-SRV', 'VBV-AUTO-SRV', '034-MAMAEVR-I-SRV', '034-MAMAE-I-SRV')
and val_taxa_ajuste < 0
group by trunc(cast('${dat_parametro}' as date), 'MM'),
cod_sdprod,
e.num_cnpj,
cod_produto,
e.dsc_razao_social,
e.dsc_nome_fantasia

-- COMMAND ----------

create or replace temporary view tmp_repasse as
with tmp_produto as (
select
    p.cod_sdprod as cod_sdprod,
    p.cod_produto as cod_produto,
    p.dsc_produto as dsc_produto,
    p.cod_aplicacao as cod_aplicacao,
    k.account_category as account_caregory
    from prod.gold_vr_cliente_rh.dim_produto p
    inner join prod.silver_vr_kenan.arbor_smt_charges_knn k
    on k.charge_id = p.cod_aplicacao
    and k.charge_type = 'COD_APLICACAO'
    where flg_ativo = 1
)
select 
trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
p.cod_sdprod  as cod_sdprod,
p.cod_produto,
k.cnpj as num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia,
sum(k.valor/100) as val_repasse_caixa
from prod.silver_vr_kenan.arbor_smt_controle_interf_knn_contab k
left join prod.gold_vr_cliente_ec.dim_cadastro e
on k.cnpj = e.num_cnpj
and e.flg_ativo = 1
left join (select max(cod_sdprod) cod_sdprod, cod_produto, account_caregory from tmp_produto group by cod_produto, account_caregory) p on p.account_caregory = k.account_category
where trunc(k.dt_corte, 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and substr(trim(k.id_protocolo_pedido), 1, 1) in ('V')
group by trunc(cast('${dat_parametro}' as date), 'MM'),
p.cod_sdprod,
p.cod_produto,
k.cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia

-- COMMAND ----------

create or replace temporary view tmpreembolso_compras_sva as
select trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
a.cod_sdprod,
a.cod_produto,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia,
sum(a.val_receita_produto_sva) val_produto_sva,
sum(a.val_transferido_cartao) val_transferido_compras
from prod.gold_vr_cliente_ec.fat_reembolso a
join prod.gold_vr_cliente_ec.dim_cadastro e
on a.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
inner join prod.gold_vr_cliente_ec.fat_voucher c
on a.num_guia_reembolso = c.num_guia
inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_gvr_vchr_relac b
on c.num_voucher = b.voucher_id
where trunc(b.scheduled_pay_dt, 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and b.business_unit = 'SNBRA'
and b.gvr_tipo_vchr = 'SVA'
and  (b.nf_brl_id is not null
or trim(b.nf_brl_id) != '')
group by 
trunc(cast('${dat_parametro}' as date), 'MM') ,
a.cod_sdprod,
e.num_cnpj,
a.cod_produto,
e.dsc_razao_social,
e.dsc_nome_fantasia

-- COMMAND ----------

create or replace temporary view tmp_tarifa_operacional as
select trunc(cast('${dat_parametro}' as date), 'MM') dat_referencia,
a.cod_sdprod,
a.cod_produto,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia,
SUM(b.PYMNT_AMT) AS val_tarifa_operacional
 from prod.gold_vr_cliente_ec.fat_reembolso a
 join prod.gold_vr_cliente_ec.dim_cadastro e
on a.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
join prod.silver_vr_erp_peoplesoft.sysadm_ps_gvr_vchr_tar_op b
on a.num_guia_reembolso = b.ke_bill_ref_no_sn 
where trunc(b.scheduled_pay_dt, 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and b.nf_brl_id is not null
and trim(b.nf_brl_id) != ''
group by trunc(cast('${dat_parametro}' as date), 'MM'),
a.cod_sdprod,
a.cod_produto,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia

-- COMMAND ----------

create or replace temporary view tmp_faturamento_item as
select a.*, d.inv_item_id as cd_itemfatr
from
(SELECT B.BUSINESS_UNIT,
       B.NF_BRL_ID,
       DECODE(B.ITM_SETID, ' ',B.BUSINESS_UNIT,B.ITM_SETID) item_setid,
       B.INV_ITEM_ID,
       B.PURCH_PROP_BRL,
       B.QTY_NF_BRL,
       B.NET_AMT_BSE_BBL as f_vl_item,
       B.PIS_AMT_BBL,
       B.COFINS_PCT_BBL,
       B.COFINS_AMT_BBL,
       B.ISSTAX_BRL_PCT,
       B.ISSTAX_BRL_AMT,
       B.ISS_BRL_RETENTION,
       A.NF_STATUS_BBL,
       B.NF_BRL_LINE_NUM,
       A.NF_BRL_DATE,
       B.TOF_PBL,
       B.PIS_PCT_BBL,
       B.DSCNT_AMT_BSE,
       B.DSCNT_PCT,
       B.UNIT_AMT_BSE_BBL   
  FROM prod.silver_vr_erp_peoplesoft.sysadm_ps_nf_hdr_bbl_fs A
  inner join  prod.silver_vr_erp_peoplesoft.sysadm_ps_nf_ln_bbl_fs  B
  on A.BUSINESS_UNIT = B.BUSINESS_UNIT
   AND A.NF_BRL_ID = B.NF_BRL_ID
   where A.BUSINESS_UNIT IN ('SNBRA', 'VRDEN','VRGPA')
   AND A.LT_GRP_ID_BBL IN ('LOCBEMOVEL',
                           'VNDSERVICO',
                           'FATVRBEN',
                           'FATVRCULT',
                           'VENDAPRE',
                           'VENDAPOS',
                           'REEMBPAT',
			                    	'GPAPOS',
				                    'GPACON',
                          'VENDAVT', 'GPAPRE'
)
   AND trunc(a.NF_BRL_DATE, 'MM') >= '2024-05-01'
   and trunc(a.LAST_UPDATE_DT, 'MM') >= '2024-05-01') a
   left join prod.silver_vr_erp_peoplesoft.sysadm_ps_master_item_tbl d
   on a.item_setid = d.setid
   and a.INV_ITEM_ID = d.inv_item_id

-- COMMAND ----------

create or replace temporary view tmp_faturamento as 
SELECT A.BUSINESS_UNIT,
       A.NF_BRL_ID,
       A.NF_BRL,
       A.NF_BRL_SERIES,
       A.BILL_TO_CUST_ID,
       A.NF_BRL_DATE,
       A.LAST_UPDATE_DT,
       A.NF_STATUS_BBL,
       A.NF_ISSUE_DT_BBL,
       A.LT_GRP_ID_BBL,
       A.GROSS_AMT_BSE,
       C.DUPLICATA_NBR_BBL ,
       A.DSCNT_AMT_BSE,
       d.ke_bill_ref_no_sn
  FROM prod.silver_vr_erp_peoplesoft.sysadm_ps_nf_hdr_bbl_fs A
 left join prod.silver_vr_erp_peoplesoft.sysadm_ps_nf_dates_bbl C 
 on A.BUSINESS_UNIT = C.BUSINESS_UNIT 
AND A.NF_BRL_ID = C.NF_BRL_ID 
left join prod.silver_vr_erp_peoplesoft.sysadm_ps_ke_xref_sn d
on  a.business_unit = d.business_unit
and a.nf_brl_id = d.nf_brl_id
       where A.BUSINESS_UNIT IN ('SNBRA', 'VRDEN','VRGPA')
       AND A.LT_GRP_ID_BBL IN ('LOCBEMOVEL',
                         'VNDSERVICO',
                         'FATVRBEN',
                         'FATVRCULT',
                         'VENDAPRE',
                         'VENDAPOS',
                         'REEMBPAT',
	                   'GPAPOS',
                          'GPACON',
                          'VENDAVT', 'GPAPRE')
AND ( 
  trunc(A.LAST_UPDATE_DT, 'MM') >= '2024-05-01'   OR
  trunc(A.ENTERED_DT, 'MM') >= '2024-05-01'       OR 
trunc(A.NF_CONF_DT_BBL, 'MM') >= '2024-05-01'           OR 
trunc(A.NF_CREATE_DT_BBL, 'MM') >= '2024-05-01'   OR 
trunc(A.NF_ISSUE_DT_BBL, 'MM') >= '2024-05-01'
)


-- COMMAND ----------

create or replace temporary view tmp_faturamento_reembolso as 
select  sum(val_faturamento_reembolso) val_faturamento_reembolso,
   dat_referencia,
   num_cnpj,
   dsc_razao_social,
   dsc_nome_fantasia,
   cod_produto,
   cod_sdprod from (
select 
  max(val_faturamento_reembolso) val_faturamento_reembolso,
  num_cnpj,
  dat_referencia,
  cod_produto,
  concat_ws(',', collect_set(num_guia_reembolso)) as num_guia_reembolso,
  nf_brl,
  nf_brl_id,
  NF_STATUS_BBL,
  dsc_razao_social,
  dsc_nome_fantasia,
  cod_sdprod,
  dat_emissao
from (
  -- BLOCO 1
  select 
    cast(sum(c.QTY_NF_BRL * f_vl_item) as decimal(15,2)) val_faturamento_reembolso, 
    trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
    dim.num_cnpj,
    a.num_guia_reembolso,
    b.nf_brl,
    b.nf_brl_id,
    b.NF_STATUS_BBL,
    dim.dsc_razao_social,
    dim.dsc_nome_fantasia, 
    a.cod_produto, 
    a.cod_sdprod,
    b.nf_brl_date as dat_emissao
  from prod.gold_vr_cliente_ec.fat_reembolso a
  inner join prod.gold_vr_cliente_ec.dim_cadastro dim
    on a.srk_ec_cadastro = dim.srk_ec_cadastro
    and flg_ativo = 1
  inner join tmp_faturamento b
    on b.ke_bill_ref_no_sn = a.num_guia_reembolso
  inner join tmp_faturamento_item c
    on b.nf_brl_id = c.nf_brl_id
   and b.business_unit = c.business_unit
  where 
     trunc(c.NF_BRL_DATE , 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
    and b.NF_STATUS_BBL IN ('CNFM')
  group by 
    a.cod_produto, a.cod_sdprod, dim.num_cnpj, a.num_guia_reembolso,
    b.nf_brl, b.nf_brl_id, b.NF_STATUS_BBL,
    dim.dsc_razao_social, dim.dsc_nome_fantasia, b.nf_brl_date,
    trunc(cast('${dat_parametro}' as date), 'MM') 

  union all

  -- BLOCO 2
  select 
    sum(valor) val_faturamento_reembolso, 
    trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
    a.num_cnpj, 
    a.num_guia_reembolso,
    a.nf_bbl,
    a.num_nota_fiscal,
    a.NF_STATUS_BBL,
    e.dsc_razao_social,
    e.dsc_nome_fantasia,
    a.cod_produto,
    a.cod_sdprod,
    a.dat_emissao
  from dev.gold_vr_corporativo_contabilidade.tmp_notas_manuais a
  inner join prod.gold_vr_cliente_ec.dim_cadastro e
    on a.num_cnpj = e.num_cnpj
    and e.flg_ativo = 1
  where trunc(dat_referencia, 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
  group by 
    a.num_cnpj, e.dsc_razao_social, e.dsc_nome_fantasia,
    a.num_guia_reembolso, a.nf_bbl, a.num_nota_fiscal,
    a.NF_STATUS_BBL, cod_produto, cod_sdprod,
    a.dat_emissao, trunc(cast('${dat_parametro}' as date), 'MM')
)
group by 
  cod_produto, nf_brl, nf_brl_id,
  dat_emissao, NF_STATUS_BBL, num_cnpj, dat_referencia,
  dsc_razao_social, dsc_nome_fantasia, cod_sdprod)
  group by dat_referencia,
   num_cnpj,
   dsc_razao_social,
   dsc_nome_fantasia,
   cod_produto,
   cod_sdprod 

-- COMMAND ----------

create or replace temporary view tmp_antecipacao_caixa as 
with tmp_snd_cliente_dim as 
( select voucher_id,
replace(replace(replace(cgc_brl, '.', ''), '/', ''), '-', '') as cgc
from prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load
),

tmp_produto as (
select distinct
p.cod_sdprod  as cd_sdprod,
p.cod_produto as cd_produto,
p.dsc_produto as ds_produto,
p.cod_aplicacao   as cd_aplic,
k.voucher_id 
from prod.silver_jvcef_erp_peoplesoft.sysadm_ps_gvr_ant_vchr_ta k
inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load t
on k.VOUCHER_ID = t.voucher_id
inner join prod.gold_vr_cliente_rh.dim_produto p
on t.gvr_produto = p.cod_aplicacao
where p.cod_emissor = 'VRPAT'
and k.gvr_emissor_benef = 'JVCEF'
and ltrim(rtrim(k.nf_brl_id)) is not null
)

select 
trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
p.cd_sdprod as cod_sdprod,
p.cd_produto as cod_produto,
b.cgc as num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia,
sum(a.gvr_valor_tar_emis) as val_antecipacao
from prod.silver_jvcef_erp_peoplesoft.sysadm_ps_gvr_ant_vchr_ta a
inner join prod.silver_vr_erp_peoplesoft.sysadm_ps_vr_voucher_load t
on a.voucher_id = t.voucher_id
inner join prod.silver_vr_kenan.arbor_payment_trans d
on t.vr_ke_trans_id = LPAD(cast(D.TRACKING_ID_SERV as int), 2, 0) || LPAD(cast(D.TRACKING_ID as int), 10, 0) ||
  LPAD(cast(D.COUNTER as int), 3, 0)
inner join prod.silver_jvcef_erp_peoplesoft.sysadm_ps_nf_hdr_bbl_fs j 
on j.nf_brl_id = a.nf_brl_id
left join tmp_snd_cliente_dim b
on a.voucher_id = b.voucher_id
left join prod.gold_vr_cliente_ec.dim_cadastro e
on b.cgc = e.num_cnpj
and e.flg_ativo = 1
inner join tmp_produto p
on p.voucher_id = a.voucher_id
where 1 = 1 
and gvr_emissor_benef = 'JVCEF'
and ltrim(rtrim(a.nf_brl_id)) is not null
and ltrim(rtrim(a.NF_brl_id)) != '' 
and trunc(cast(j.nf_brl_date as  date), 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
group by trunc(cast('${dat_parametro}' as date), 'MM'),
p.cd_sdprod ,
p.cd_produto ,
b.cgc ,
e.dsc_razao_social,
e.dsc_nome_fantasia

-- COMMAND ----------

-- ajuste a debito
create or replace temporary view tmp_ajuste_a_debito as
select 
trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia,
num_guia_reembolso,
cod_produto,
cod_sdprod,
num_cnpj,
dsc_razao_social,
dsc_nome_fantasia,
sum(case when val_taxa_ajuste = f_vl_item then 0 else nvl(val_taxa_ajuste, 0) end) as val_ajuste_debito 
from (
select distinct 
a.num_guia_reembolso,
a.cod_produto,
a.cod_sdprod,
a.val_taxa_ajuste,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia,
c.f_vl_item
from prod.gold_vr_cliente_ec.fat_reembolso a
join prod.gold_vr_cliente_ec.dim_cadastro e
on a.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
inner join tmp_faturamento b
on b.ke_bill_ref_no_sn = a.num_guia_reembolso
inner join tmp_faturamento_item c
on b.nf_brl_id = c.nf_brl_id
and b.business_unit = c.business_unit
and c.cd_itemfatr = '000000000000107638'
inner join prod.gold_vr_cliente_ec.fat_voucher  e
on e.num_guia = a.num_guia_reembolso
where trunc(cast(a.dat_primeiro_pagamento as date), 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and not exists (select 1 from prod.gold_vr_cliente_ec.fat_reembolso d 
where d.num_guia_reembolso = e.num_guia
and d.num_guia_reembolso = b.ke_bill_ref_no_sn
and trunc(cast(d.dat_primeiro_pagamento as date), 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and (d.val_receita_antecipacao = c.f_vl_item or e.val_tarifa_adiantamento_eventual = c.f_vl_item))
and  a.cod_produto IN ('VBC-CULTURA-SRV', 'VRO-REFEICAO-SRV', 'VBA-ALIMENT-SRV', 'VBV-AUTO-SRV', '034-MAMAEVR-I-SRV', '034-MAMAE-I-SRV', 'VCA-CESTA-SRV', 'NAT-NATAL-SRV')
and val_taxa_ajuste > 0
)
group by num_guia_reembolso,
cod_produto,
cod_sdprod,
num_cnpj,
dsc_razao_social,
dsc_nome_fantasia,
trunc(cast('${dat_parametro}' as date), 'MM') 

-- COMMAND ----------

create or replace temporary view tmp_reembolso_pagamento_confirmado as
select trunc(fat_voucher.dat_pagamento_contabil, 'MM') as dat_referencia,
a.cod_sdprod,
a.cod_produto,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia,
sum(fat_voucher.val_pagamento) as val_pagamento_confirmado_vrgente
from prod.gold_vr_cliente_ec.fat_voucher 
left join prod.gold_vr_cliente_ec.fat_reembolso a
on a.num_guia_reembolso = fat_voucher.num_guia
left join prod.gold_vr_cliente_ec.dim_cadastro e
on a.srk_ec_cadastro = e.srk_ec_cadastro
and flg_ativo = 1
where 
val_receita_pacote_vrg > 0
and trunc(cast(fat_voucher.dat_pagamento_contabil as date), 'MM') = trunc(cast('${dat_parametro}' as date), 'MM')
and (fat_voucher.cod_produto is null or fat_voucher.cod_produto = 'PTM')
group by trunc(fat_voucher.dat_pagamento_contabil, 'MM'),
a.cod_sdprod,
a.cod_produto,
e.num_cnpj,
e.dsc_razao_social,
e.dsc_nome_fantasia

-- COMMAND ----------

create or replace temporary view tmp_produto as 
select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_reeembolso
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_estorno_reembolso_bruto
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_reembolso_confirmado
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_ajuste_a_debito
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_ajuste_credito
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_repasse
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmpreembolso_compras_sva
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_tarifa_operacional
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_faturamento_reembolso
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_antecipacao_caixa
 union 
 select distinct trunc(cast('${dat_parametro}' as date), 'MM') as dat_referencia, num_cnpj, dsc_razao_social, dsc_nome_fantasia, cod_produto, cod_sdprod from tmp_reembolso_pagamento_confirmado

-- COMMAND ----------

create or replace temporary view tmp_unificacao as
select tmp_produto.*,
nvl(dim_produto.dsc_produto, produto_thomr.dsc_produto) as dsc_produto,
nvl(val_reembolso_bruto, 0) val_reembolso_bruto,
nvl(val_reembolso_estorno_bruto, 0) val_estorno_reembolso,
nvl(val_pagamento_reembolso, 0) val_reembolso_confirmado,
nvl(val_ajuste_debito, 0) val_ajuste_debito,
nvl(val_taxa_credito, 0) val_ajuste_credito,
nvl(val_faturamento_reembolso, 0) val_faturamento_reembolso,
nvl(val_tarifa_operacional, 0) val_tarifa_operacional,
nvl(val_produto_sva, 0) val_reembolso_confirmado_sva,
nvl(val_transferido_compras, 0) val_reembolso_confirmado_compra,
nvl(val_repasse_caixa, 0) val_repasse,
nvl(val_antecipacao, 0) val_taxa_antecipacao,
nvl(val_pagamento_confirmado_vrgente, 0) val_pagamento_confirmado_vrgente,
nvl(val_reembolso_bruto, 0) + nvl(val_reembolso_estorno_bruto, 0) - nvl(val_pagamento_reembolso, 0) - nvl(val_ajuste_debito, 0) - nvl(val_taxa_credito, 0) -  nvl(val_faturamento_reembolso, 0) + nvl(val_tarifa_operacional, 0) - nvl(val_produto_sva, 0)  - nvl(val_pagamento_confirmado_vrgente, 0) - nvl(val_transferido_compras, 0) + 
nvl(val_repasse_caixa, 0) - nvl(val_antecipacao, 0) as val_movimento_mes
 from tmp_produto
left join tmp_reeembolso
on  tmp_produto.dat_referencia    = trunc(tmp_reeembolso.dat_referencia, 'MM')
and tmp_produto.cod_produto       = tmp_reeembolso.cod_produto
and tmp_produto.cod_sdprod        = tmp_reeembolso.cod_sdprod
and tmp_produto.num_cnpj          = tmp_reeembolso.num_cnpj
left join tmp_estorno_reembolso_bruto
on tmp_produto.dat_referencia     = trunc(tmp_estorno_reembolso_bruto.dat_referencia, 'MM')
and tmp_produto.cod_produto       = tmp_estorno_reembolso_bruto.cod_produto
and tmp_produto.cod_sdprod        = tmp_estorno_reembolso_bruto.cod_sdprod
and tmp_produto.num_cnpj          = tmp_estorno_reembolso_bruto.num_cnpj
 left join tmp_reembolso_confirmado
 on tmp_produto.dat_referencia     = trunc(tmp_reembolso_confirmado.dat_referencia, 'MM')
 and tmp_produto.cod_produto       = tmp_reembolso_confirmado.cod_produto
 and tmp_produto.cod_sdprod        = tmp_reembolso_confirmado.cod_sdprod
 and tmp_produto.num_cnpj          = tmp_reembolso_confirmado.num_cnpj
left join tmp_ajuste_a_debito
on tmp_produto.dat_referencia     = trunc(tmp_ajuste_a_debito.dat_referencia, 'MM')
and tmp_produto.cod_produto       = tmp_ajuste_a_debito.cod_produto
and tmp_produto.cod_sdprod        = tmp_ajuste_a_debito.cod_sdprod
and tmp_produto.num_cnpj          = tmp_ajuste_a_debito.num_cnpj
left join tmp_ajuste_credito
on tmp_produto.dat_referencia     = trunc(tmp_ajuste_credito.dat_referencia, 'MM')
and tmp_produto.cod_produto       = tmp_ajuste_credito.cod_produto
and tmp_produto.cod_sdprod        = tmp_ajuste_credito.cod_sdprod
and tmp_produto.num_cnpj          = tmp_ajuste_credito.num_cnpj
left join tmp_repasse
on tmp_produto.dat_referencia     = trunc(tmp_repasse.dat_referencia, 'MM')
and tmp_produto.cod_produto       = tmp_repasse.cod_produto
and tmp_produto.cod_sdprod        = tmp_repasse.cod_sdprod
and tmp_produto.num_cnpj          = tmp_repasse.num_cnpj
left join tmpreembolso_compras_sva
on tmp_produto.dat_referencia     = trunc(tmpreembolso_compras_sva.dat_referencia, 'MM')
and tmp_produto.num_cnpj          = tmpreembolso_compras_sva.num_cnpj
and tmp_produto.cod_produto       = tmpreembolso_compras_sva.cod_produto
and tmp_produto.cod_sdprod        = tmpreembolso_compras_sva.cod_sdprod
left join tmp_tarifa_operacional
on tmp_produto.dat_referencia     = trunc(tmp_tarifa_operacional.dat_referencia, 'MM')
and tmp_produto.num_cnpj          = tmp_tarifa_operacional.num_cnpj
and tmp_produto.cod_produto       = tmp_tarifa_operacional.cod_produto
and tmp_produto.cod_sdprod        = tmp_tarifa_operacional.cod_sdprod
left join tmp_faturamento_reembolso
on tmp_produto.dat_referencia     = trunc(tmp_faturamento_reembolso.dat_referencia, 'MM')
and tmp_produto.num_cnpj          = tmp_faturamento_reembolso.num_cnpj
and tmp_produto.cod_produto       = tmp_faturamento_reembolso.cod_produto
and tmp_produto.cod_sdprod        = tmp_faturamento_reembolso.cod_sdprod
left join tmp_antecipacao_caixa
on tmp_produto.dat_referencia     = trunc(tmp_antecipacao_caixa.dat_referencia, 'MM')
and tmp_produto.num_cnpj          = tmp_antecipacao_caixa.num_cnpj
and tmp_produto.cod_produto       = tmp_antecipacao_caixa.cod_produto
and tmp_produto.cod_sdprod        = tmp_antecipacao_caixa.cod_sdprod
left join tmp_reembolso_pagamento_confirmado
on tmp_produto.dat_referencia     = trunc(tmp_reembolso_pagamento_confirmado.dat_referencia, 'MM')
and tmp_produto.num_cnpj          = tmp_reembolso_pagamento_confirmado.num_cnpj
and tmp_produto.cod_produto       = tmp_reembolso_pagamento_confirmado.cod_produto
and tmp_produto.cod_sdprod        = tmp_reembolso_pagamento_confirmado.cod_sdprod
left join (select distinct cod_produto, dsc_produto, cod_sdprod, flg_ativo from prod.gold_vr_cliente_rh.dim_produto) dim_produto
on tmp_produto.cod_produto = dim_produto.cod_produto
and tmp_produto.cod_sdprod = dim_produto.cod_sdprod
and dim_produto.flg_ativo = 1
left join prod.gold_thomr_cliente_rh.dim_produto produto_thomr
on tmp_produto.cod_produto = produto_thomr.cod_produto
and tmp_produto.cod_sdprod = produto_thomr.cod_sdprod
and produto_thomr.flg_ativo = 1

-- COMMAND ----------

create
or replace temporary view tmp_ult_dado as
select
  case
    when tmp_unificacao.dat_referencia is not null then trunc(tmp_unificacao.dat_referencia, 'MM')
    else add_months(
      trunc(fat_reembolso_efetuar_cnpj.dat_referencia, 'MM'),
      + 1
    )
  end as dat_referencia,
  case
    when tmp_unificacao.dsc_produto is not null then tmp_unificacao.dsc_produto
    else fat_reembolso_efetuar_cnpj.dsc_produto
  end as dsc_produto,
  case
    when tmp_unificacao.num_cnpj is not null then tmp_unificacao.num_cnpj
    else fat_reembolso_efetuar_cnpj.num_cnpj
  end as num_cnpj,
  case
    when tmp_unificacao.dsc_razao_social is not null then tmp_unificacao.dsc_razao_social
    else fat_reembolso_efetuar_cnpj.dsc_razao_social
  end as dsc_razao_social,
  case
    when tmp_unificacao.dsc_nome_fantasia is not null then tmp_unificacao.dsc_nome_fantasia
    else fat_reembolso_efetuar_cnpj.dsc_nome_fantasia
  end as dsc_nome_fantasia,
  case
    when tmp_unificacao.cod_produto is not null then tmp_unificacao.cod_produto
    else fat_reembolso_efetuar_cnpj.cod_produto
  end as cod_produto,
  nvl(fat_reembolso_efetuar_cnpj.val_saldo_final, 0) as val_saldo_inicial,
  case
    when tmp_unificacao.val_reembolso_bruto is not null then nvl(tmp_unificacao.val_reembolso_bruto, 0)
    else 0
  end as val_reembolso_bruto,
  case
    when tmp_unificacao.val_estorno_reembolso is not null then nvl(tmp_unificacao.val_estorno_reembolso, 0)
    else 0
  end as val_estorno_reembolso,
  case
    when tmp_unificacao.val_reembolso_confirmado is not null then nvl(tmp_unificacao.val_reembolso_confirmado, 0)
    else 0
  end as val_reembolso_confirmado,
  case
    when tmp_unificacao.val_ajuste_debito is not null then nvl(tmp_unificacao.val_ajuste_debito, 0)
    else 0
  end as val_ajuste_debito,
  case
    when tmp_unificacao.val_ajuste_credito is not null then nvl(tmp_unificacao.val_ajuste_credito, 0)
    else 0
  end as val_ajuste_credito,
  case
    when tmp_unificacao.val_faturamento_reembolso is not null then nvl(tmp_unificacao.val_faturamento_reembolso, 0)
    else 0
  end as val_faturamento_reembolso,
  case
    when tmp_unificacao.val_tarifa_operacional is not null then nvl(tmp_unificacao.val_tarifa_operacional, 0)
    else 0
  end as val_tarifa_operacional,
  case
    when tmp_unificacao.val_reembolso_confirmado_sva is not null then nvl(tmp_unificacao.val_reembolso_confirmado_sva, 0)
    else 0
  end as val_reembolso_confirmado_sva,
  case
    when tmp_unificacao.val_reembolso_confirmado_compra is not null then nvl(
      tmp_unificacao.val_reembolso_confirmado_compra,
      0
    )
    else 0
  end as val_reembolso_confirmado_compra,
  case
    when tmp_unificacao.val_repasse is not null then nvl(tmp_unificacao.val_repasse, 0)
    else 0
  end as val_repasse,
  case
    when tmp_unificacao.val_taxa_antecipacao is not null then nvl(tmp_unificacao.val_taxa_antecipacao, 0)
    else 0
  end as val_taxa_antecipacao,
  case
    when tmp_unificacao.val_pagamento_confirmado_vrgente is not null then nvl(
      tmp_unificacao.val_pagamento_confirmado_vrgente,
      0
    )
    else 0
  end as val_pagamento_confirmado_vrgente,
  case
    when fat_reembolso_efetuar_cnpj.val_saldo_final is not null then (
      nvl(fat_reembolso_efetuar_cnpj.val_saldo_final, 0) + nvl(tmp_unificacao.val_movimento_mes, 0)
    )
    else nvl(tmp_unificacao.val_movimento_mes, 0)
  end as val_saldo_final
from
  tmp_unificacao full
  outer join (
    select
      dat_referencia,
      max(dsc_razao_social) dsc_razao_social,
      max(dsc_nome_fantasia) dsc_nome_fantasia,
      num_cnpj,
      cod_produto,
      dsc_produto,
      max(val_saldo_inicial) val_saldo_inicial,
      max(val_reembolso_bruto) val_reembolso_bruto,
      max(val_estorno_reembolso) val_estorno_reembolso,
      max(val_reembolso_confirmado) val_reembolso_confirmado,
      max(val_ajuste_debito) val_ajuste_debito,
      max(val_ajuste_credito) val_ajuste_credito,
      max(val_faturamento_reembolso) val_faturamento_reembolso,
      max(val_tarifa_operacional) val_tarifa_operacional,
      max(val_reembolso_confirmado_sva) val_reembolso_confirmado_sva,
      max(val_reembolso_confirmado_compra) val_reembolso_confirmado_compra,
      max(val_repasse) val_repasse,
      max(val_taxa_antecipacao) val_taxa_antecipacao,
      max(val_pagamento_confirmado_vrgente) val_pagamento_confirmado_vrgente,
      max(val_saldo_final) val_saldo_final
    from
      prod.gold_vr_corporativo_contabilidade.fat_reembolso_efetuar_cnpj
    where
      trunc(fat_reembolso_efetuar_cnpj.dat_referencia, 'MM') = add_months(
        trunc(cast('${dat_parametro}' as date), 'MM'),
        -1
      )
    group by
      dat_referencia,
      num_cnpj,
      cod_produto,
      dsc_produto
  ) as fat_reembolso_efetuar_cnpj on trunc(fat_reembolso_efetuar_cnpj.dat_referencia, 'MM') = trunc(
    add_months(tmp_unificacao.dat_referencia, -1),
    'MM'
  )
  and fat_reembolso_efetuar_cnpj.cod_produto = tmp_unificacao.cod_produto
  and fat_reembolso_efetuar_cnpj.num_cnpj = tmp_unificacao.num_cnpj

-- COMMAND ----------

create or replace temporary view tmp_md5_final as 
select tmp_ult_dado.*,
MD5(
  NVL(tmp_ult_dado.val_saldo_inicial                ,'')||
  NVL(tmp_ult_dado.val_reembolso_bruto              ,'')||
  NVL(tmp_ult_dado.val_estorno_reembolso            ,'')||
  NVL(tmp_ult_dado.val_reembolso_confirmado         ,'')||
  NVL(tmp_ult_dado.val_ajuste_debito                ,'')||
  NVL(tmp_ult_dado.val_ajuste_credito               ,'')||
  NVL(tmp_ult_dado.val_faturamento_reembolso        ,'')||
  NVL(tmp_ult_dado.val_tarifa_operacional           ,'')||
  NVL(tmp_ult_dado.val_reembolso_confirmado_sva     ,'')||
  NVL(tmp_ult_dado.val_reembolso_confirmado_compra  ,'')||
  NVL(tmp_ult_dado.val_repasse                      ,'')||
  NVL(tmp_ult_dado.val_taxa_antecipacao             ,'')||
  NVL(tmp_ult_dado.val_pagamento_confirmado_vrgente ,'')||
  NVL(tmp_ult_dado.val_saldo_final                  ,'')
) as cod_hash_md5,
fat_reembolso_efetuar_cnpj.cod_hash_md5 as cod_hash_md5_restino
from tmp_ult_dado
left join prod.gold_vr_corporativo_contabilidade.fat_reembolso_efetuar_cnpj
on    fat_reembolso_efetuar_cnpj.dat_referencia = tmp_ult_dado.dat_referencia
and   fat_reembolso_efetuar_cnpj.cod_produto    = tmp_ult_dado.cod_produto
and   fat_reembolso_efetuar_cnpj.num_cnpj       = tmp_ult_dado.num_cnpj
where tmp_ult_dado.dat_referencia >= TRUNC(CAST('${dat_parametro}' AS DATE), 'MM')
and tmp_ult_dado.num_cnpj is not null

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df_merge = fn_merge_direto("""
-- MAGIC MERGE INTO prod.gold_vr_corporativo_contabilidade.fat_reembolso_efetuar_cnpj as destino 
-- MAGIC USING tmp_md5_final as origem
-- MAGIC on   origem.dat_referencia = destino.dat_referencia 
-- MAGIC and  origem.cod_produto = destino.cod_produto
-- MAGIC and  origem.num_cnpj = destino.num_cnpj
-- MAGIC WHEN MATCHED AND nvl(origem.cod_hash_md5,'') <> nvl(destino.cod_hash_md5,'') THEN UPDATE SET
-- MAGIC destino.dsc_razao_social = origem.dsc_razao_social,
-- MAGIC destino.dsc_produto = origem.dsc_produto,
-- MAGIC destino.dsc_nome_fantasia = origem.dsc_nome_fantasia,
-- MAGIC destino.val_saldo_inicial = origem.val_saldo_inicial,
-- MAGIC destino.val_reembolso_bruto = origem.val_reembolso_bruto,
-- MAGIC destino.val_estorno_reembolso = origem.val_estorno_reembolso,
-- MAGIC destino.val_reembolso_confirmado = origem.val_reembolso_confirmado,
-- MAGIC destino.val_ajuste_debito = origem.val_ajuste_debito,
-- MAGIC destino.val_ajuste_credito = origem.val_ajuste_credito,
-- MAGIC destino.val_faturamento_reembolso = origem.val_faturamento_reembolso,
-- MAGIC destino.val_tarifa_operacional = origem.val_tarifa_operacional,
-- MAGIC destino.val_reembolso_confirmado_sva = origem.val_reembolso_confirmado_sva,
-- MAGIC destino.val_reembolso_confirmado_compra = origem.val_reembolso_confirmado_compra,
-- MAGIC destino.val_repasse = origem.val_repasse,
-- MAGIC destino.val_taxa_antecipacao = origem.val_taxa_antecipacao,
-- MAGIC destino.val_pagamento_confirmado_vrgente = origem.val_pagamento_confirmado_vrgente,
-- MAGIC destino.val_saldo_final = origem.val_saldo_final
-- MAGIC WHEN NOT MATCHED THEN INSERT (
-- MAGIC  destino.dat_referencia,
-- MAGIC  destino.num_cnpj,
-- MAGIC  destino.cod_produto,
-- MAGIC  destino.dsc_produto,
-- MAGIC  destino.dsc_razao_social,
-- MAGIC  destino.dsc_nome_fantasia,
-- MAGIC  destino.val_saldo_inicial,
-- MAGIC  destino.val_reembolso_bruto,
-- MAGIC  destino.val_estorno_reembolso,
-- MAGIC  destino.val_reembolso_confirmado,
-- MAGIC  destino.val_ajuste_debito,
-- MAGIC  destino.val_ajuste_credito,
-- MAGIC  destino.val_faturamento_reembolso,
-- MAGIC  destino.val_tarifa_operacional,
-- MAGIC  destino.val_reembolso_confirmado_sva,
-- MAGIC  destino.val_reembolso_confirmado_compra,
-- MAGIC  destino.val_repasse,
-- MAGIC  destino.val_taxa_antecipacao,
-- MAGIC  destino.val_pagamento_confirmado_vrgente,
-- MAGIC  destino.val_saldo_final,
-- MAGIC  destino.cod_hash_md5,
-- MAGIC  destino.dat_inclusao_bi,
-- MAGIC  destino.dat_alteracao_bi
-- MAGIC  ) VALUES (
-- MAGIC origem.dat_referencia,
-- MAGIC  origem.num_cnpj,
-- MAGIC  origem.cod_produto,
-- MAGIC  origem.dsc_produto,
-- MAGIC  origem.dsc_razao_social,
-- MAGIC  origem.dsc_nome_fantasia,
-- MAGIC  origem.val_saldo_inicial,
-- MAGIC  origem.val_reembolso_bruto,
-- MAGIC  origem.val_estorno_reembolso,
-- MAGIC  origem.val_reembolso_confirmado,
-- MAGIC  origem.val_ajuste_debito,
-- MAGIC  origem.val_ajuste_credito,
-- MAGIC  origem.val_faturamento_reembolso,
-- MAGIC  origem.val_tarifa_operacional,
-- MAGIC  origem.val_reembolso_confirmado_sva,
-- MAGIC  origem.val_reembolso_confirmado_compra,
-- MAGIC  origem.val_repasse,
-- MAGIC  origem.val_taxa_antecipacao,
-- MAGIC  origem.val_pagamento_confirmado_vrgente,
-- MAGIC  origem.val_saldo_final,
-- MAGIC  origem.cod_hash_md5,
-- MAGIC  current_timestamp(),
-- MAGIC  current_timestamp()
-- MAGIC  )"""
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC fn_gera_arquivo_bastao(malha,database,tablename)
-- MAGIC fn_exit_notebook(df_merge)
