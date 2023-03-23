# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SAP_ATT_CODE_0

df_0=spark.sql("""
    SELECT
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_CODE_DESC AS SAP_ATT_CODE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATT_CODE""")

df_0.createOrReplaceTempView("SAP_ATT_CODE_0")

# COMMAND ----------

# DBTITLE 1, SAP_ATTRIBUTE_1

df_1=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATTRIBUTE""")

df_1.createOrReplaceTempView("SAP_ATTRIBUTE_1")

# COMMAND ----------

# DBTITLE 1, SAP_ATT_VALUE_2

df_2=spark.sql("""
    SELECT
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATT_VALUE""")

df_2.createOrReplaceTempView("SAP_ATT_VALUE_2")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_3

df_3=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_TYPE AS SKU_TYPE,
        PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
        STATUS_ID AS STATUS_ID,
        SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
        SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
        SKU_DESC AS SKU_DESC,
        ALT_DESC AS ALT_DESC,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        COUNTRY_CD AS COUNTRY_CD,
        IMPORT_FLAG AS IMPORT_FLAG,
        HTS_CODE_ID AS HTS_CODE_ID,
        CONTENTS AS CONTENTS,
        CONTENTS_UNITS AS CONTENTS_UNITS,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
        SIZE_DESC AS SIZE_DESC,
        BUM_QTY AS BUM_QTY,
        UOM_CD AS UOM_CD,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        BUYER_ID AS BUYER_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_COST_AMT AS PURCH_COST_AMT,
        NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        BRAND_CD AS BRAND_CD,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        STATELINE_FLAG AS STATELINE_FLAG,
        SIGN_TYPE_CD AS SIGN_TYPE_CD,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        INIT_MKDN_DT AS INIT_MKDN_DT,
        DISC_START_DT AS DISC_START_DT,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        FIRST_INV_DT AS FIRST_INV_DT,
        LAST_INV_DT AS LAST_INV_DT,
        LOAD_DT AS LOAD_DT,
        BASE_NBR AS BASE_NBR,
        BP_COLOR_ID AS BP_COLOR_ID,
        BP_SIZE_ID AS BP_SIZE_ID,
        BP_BREED_ID AS BP_BREED_ID,
        BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
        BP_AEROSOL_FLAG AS BP_AEROSOL_FLAG,
        BP_HAZMAT_FLAG AS BP_HAZMAT_FLAG,
        CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
        NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
        NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
        RTV_DEPT_CD AS RTV_DEPT_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
        COMPONENT_FLAG AS COMPONENT_FLAG,
        ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
        ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
        ZDISCO_PID_DT AS ZDISCO_PID_DT,
        ZDISCO_START_DT AS ZDISCO_START_DT,
        ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
        ZDISCO_DC_DT AS ZDISCO_DC_DT,
        ZDISCO_STR_DT AS ZDISCO_STR_DT,
        ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
        ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE""")

df_3.createOrReplaceTempView("SKU_PROFILE_3")

# COMMAND ----------

# DBTITLE 1, ASQ_PRODUCT_ATTRIBUTE_MV_4

df_4=spark.sql("""
    SELECT
        sap.product_id,
        sap.sku_nbr,
        SUBSTR(sap.old_article_nbr,
        1,
        6) AS base_nbr,
        NVL(SUBSTR(dclr.dclr_id,
        1,
        3),
        ' ') AS color_code,
        NVL(dclr.dclr_desc,
        ' ') AS color_desc,
        NVL(dsze.dsze_id,
        ' ') AS size_code,
        NVL(dsze.dsze_desc,
        ' ') AS size_desc,
        NVL(dbrd.dbrd_id,
        ' ') AS breed_code,
        NVL(dbrd.dbrd_desc,
        ' ') AS breed_desc,
        NVL(ccst.ccst_id,
        ' ') AS consign_id,
        NVL(ccst.ccst_desc,
        ' ') AS consign_cost,
        NVL(stor.stor_id,
        ' ') AS store_channel_id,
        NVL(stor.stor_desc,
        ' ') AS store_channel_desc,
        NVL(web.web_id,
        ' ') AS web_channel_id,
        NVL(web.web_desc,
        ' ') AS web_channel_desc,
        NVL(catg.catg_id,
        ' ') AS catg_channel_id,
        NVL(catg.catg_desc,
        ' ') AS catg_channel_desc,
        NVL(drpi.drpi_id,
        ' ') AS drop_ship_id,
        NVL(drpi.drpi_desc,
        ' ') AS drop_ship_desc,
        NVL(drpc.drpc_id,
        ' ') AS drop_ship_channel_id,
        NVL(drpc.drpc_desc,
        ' ') AS drop_ship_channel_desc,
        NVL(dlbl.dlbl_id,
        ' ') AS psm_label_flag,
        NVL(dlbl.dlbl_desc,
        ' ') AS psm_label_desc,
        NVL(pgrp.pgrp_id,
        ' ') AS web_product_grp,
        NVL(pgrp.pgrp_desc,
        ' ') AS web_product_grp_desc,
        NVL(dfrt.dfrt_id,
        ' ') AS direct_add_frgt_id,
        NVL(dfrt.dfrt_desc,
        ' ') AS direct_add_frgt_amt,
        NVL(dscd.dscd_id,
        ' ') AS direct_status_cd,
        NVL(dscd.dscd_desc,
        ' ') AS direct_status_desc,
        NVL(dtcd.dtcd_id,
        ' ') AS date_code_flag,
        NVL(dtcd.dtcd_desc,
        ' ') AS date_code_flag_desc,
        NVL(dweb.dweb_id,
        'N') AS web_access_flag,
        NVL(dweb.dweb_desc,
        'NO') AS web_access_flag_desc,
        NVL(khop.khop_id,
        ' ') AS khim_optimize_id,
        NVL(khop.khop_desc,
        ' ') AS khim_optimize_desc,
        NVL(nfta.nfta_id,
        'N') AS nafta_flag,
        NVL(nfta.nfta_desc,
        ' ') AS nafta_desc,
        NVL(gbb.gbb_id,
        ' ') AS gbb_id,
        NVL(gbb.gbb_desc,
        ' ') AS gbb_desc,
        NVL(fmly.species_id,
        ' ') AS species_id,
        NVL(fmly.species_desc,
        ' ') AS species_desc,
        NVL(cbyr.can_buyer_id,
        ' ') AS can_buyer_id,
        NVL(cbyr.can_buyer_desc,
        ' ') AS can_buyer_desc,
        NVL(prol.pricing_role_id,
        ' ') AS pricing_role_id,
        NVL(prol.pricing_role_desc,
        ' ') AS pricing_role_desc,
        NVL(CPST.CHANNEL_POSSITION_ID,
        ' ') AS CHANNEL_POSSITION_ID,
        NVL(CPST.CHANNEL_POSSITION_DESC,
        ' ') AS CHANNEL_POSSITION_DESC,
        NVL(CLTN.COLLECTION_ID,
        ' ') AS COLLECTION_ID,
        NVL(CLTN.COLLECTION_DESC,
        ' ') AS COLLECTION_DESC,
        NVL(CPTN.COLOR_PATTERN_ID,
        ' ') AS COLOR_PATTERN_ID,
        NVL(CPTN.COLOR_PATTERN_DESC,
        ' ') AS COLOR_PATTERN_DESC,
        NVL(CSHD.COLOR_SHADE_ID,
        ' ') AS COLOR_SHADE_ID,
        NVL(CSHD.COLOR_SHADE_DESC,
        ' ') AS COLOR_SHADE_DESC,
        NVL(FRM.FORM_ID,
        ' ') AS FORM_ID,
        NVL(FRM.FORM_DESC,
        ' ') AS FORM_DESC,
        NVL(FMTN.FORMULATION_ID,
        ' ') AS FORMULATION_ID,
        NVL(FMTN.FORMULATION_DESC,
        ' ') AS FORMULATION_DESC,
        NVL(PENV.PET_ENVIRONMENT_ID,
        ' ') AS PET_ENVIRONMENT_ID,
        NVL(PENV.PET_ENVIRONMENT_DESC,
        ' ') AS PET_ENVIRONMENT_DESC,
        NVL(LSTG.LIFE_STAGE_ID,
        ' ') AS LIFE_STAGE_ID,
        NVL(LSTG.LIFE_STAGE_DESC,
        ' ') AS LIFE_STAGE_DESC,
        NVL(MSKU.MANDATORY_SKU_ID,
        ' ') AS MANDATORY_SKU_ID,
        NVL(MSKU.MANDATORY_SKU_DESC,
        ' ') AS MANDATORY_SKU_DESC,
        NVL(SLTN.SOLUTION_ID,
        ' ') AS SOLUTION_ID,
        NVL(SLTN.SOLUTION_DESC,
        ' ') AS SOLUTION_DESC,
        NVL(SEAS.SEASONALITY_ID,
        ' ') AS SEASONALITY_ID,
        NVL(SEAS.SEASONALITY_DESC,
        ' ') AS SEASONALITY_DESC 
    FROM
        (SELECT
            product_id,
            sku_nbr,
            old_article_nbr 
        FROM
            sku_profile) sap 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS dbrd_id,
                val.sap_att_value_desc AS dbrd_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                att.sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'BRED' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dbrd 
            ON sap.product_id = dbrd.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS dclr_id,
                val.sap_att_value_desc AS dclr_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DCLR' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dclr 
            ON sap.product_id = dclr.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS dsze_id,
                val.sap_att_value_desc AS dsze_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'SDSC' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dsze 
            ON sap.product_id = dsze.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS ccst_id,
                val.sap_att_value_desc AS ccst_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                att.sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = '0002' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) ccst 
            ON sap.product_id = ccst.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS web_id,
                val.sap_att_value_desc AS web_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                att.sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'WEB' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) web 
            ON sap.product_id = web.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS stor_id,
                val.sap_att_value_desc AS stor_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                att.sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'STOR' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) stor 
            ON sap.product_id = stor.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS catg_id,
                val.sap_att_value_desc AS catg_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                att.sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'CATL' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) catg 
            ON sap.product_id = catg.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS drpi_id,
                val.sap_att_value_desc AS drpi_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DRPI' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) drpi 
            ON sap.product_id = drpi.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS drpc_id,
                val.sap_att_value_desc AS drpc_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DRPC' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) drpc 
            ON sap.product_id = drpc.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS dlbl_id,
                val.sap_att_value_desc AS dlbl_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DLBL' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dlbl 
            ON sap.product_id = dlbl.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS pgrp_id,
                val.sap_att_value_desc AS pgrp_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'PGRP' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) pgrp 
            ON sap.product_id = pgrp.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS dfrt_id,
                val.sap_att_value_desc AS dfrt_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DFRT' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dfrt 
            ON sap.product_id = dfrt.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS dscd_id,
                val.sap_att_value_desc AS dscd_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DSCD' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dscd 
            ON sap.product_id = dscd.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS dtcd_id,
                val.sap_att_value_desc AS dtcd_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DTCD' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dtcd 
            ON sap.product_id = dtcd.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                NVL(SUBSTR(val.sap_att_value_desc,
                1,
                1),
                'N') AS dweb_id,
                val.sap_att_value_desc AS dweb_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'DWEB' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) dweb 
            ON sap.product_id = dweb.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS khop_id,
                val.sap_att_value_desc AS khop_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'KHOP' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) khop 
            ON sap.product_id = khop.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS nfta_id,
                val.sap_att_value_desc AS nfta_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'NFTA' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_value_id = 'Y' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) nfta 
            ON sap.product_id = nfta.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS gbb_id,
                val.sap_att_value_desc AS gbb_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'GBB' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) gbb 
            ON sap.product_id = gbb.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS species_id,
                val.sap_att_value_desc AS species_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'FMLY' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) fmly 
            ON sap.product_id = fmly.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS can_buyer_id,
                val.sap_att_value_desc AS can_buyer_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'CBYR' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) cbyr 
            ON sap.product_id = cbyr.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                att.product_id,
                att.sap_att_value_id AS pricing_role_id,
                val.sap_att_value_desc AS pricing_role_desc 
            FROM
                sap_attribute att,
                sap_att_code cod,
                sap_att_value val 
            WHERE
                sap_att_type_id = 'ATT' 
                AND att.sap_att_code_id = 'PROL' 
                AND att.delete_flag <> 'X' 
                AND att.sap_att_code_id = cod.sap_att_code_id 
                AND att.sap_att_code_id = val.sap_att_code_id 
                AND att.sap_att_value_id = val.sap_att_value_id
        ) prol 
            ON sap.product_id = prol.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS CHANNEL_POSSITION_ID,
                VAL.SAP_ATT_VALUE_DESC AS CHANNEL_POSSITION_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'CPST' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) CPST 
            ON sap.product_id = CPST.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS COLLECTION_ID,
                VAL.SAP_ATT_VALUE_DESC AS COLLECTION_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'CLTN' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) CLTN 
            ON sap.product_id = CLTN.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS COLOR_PATTERN_ID,
                VAL.SAP_ATT_VALUE_DESC AS COLOR_PATTERN_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'CPTN' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) CPTN 
            ON sap.product_id = CPTN.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS COLOR_SHADE_ID,
                VAL.SAP_ATT_VALUE_DESC AS COLOR_SHADE_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'CSHD' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) CSHD 
            ON sap.product_id = CSHD.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS FORM_ID,
                VAL.SAP_ATT_VALUE_DESC AS FORM_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'FORM' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) FRM 
            ON sap.product_id = FRM.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS FORMULATION_ID,
                VAL.SAP_ATT_VALUE_DESC AS FORMULATION_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'FMTN' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) FMTN 
            ON sap.product_id = FMTN.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS PET_ENVIRONMENT_ID,
                VAL.SAP_ATT_VALUE_DESC AS PET_ENVIRONMENT_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'PENV' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) PENV 
            ON sap.product_id = PENV.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS LIFE_STAGE_ID,
                VAL.SAP_ATT_VALUE_DESC AS LIFE_STAGE_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'LSTG' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) LSTG 
            ON sap.product_id = LSTG.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS MANDATORY_SKU_ID,
                VAL.SAP_ATT_VALUE_DESC AS MANDATORY_SKU_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'MSKU' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) MSKU 
            ON sap.product_id = MSKU.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS SOLUTION_ID,
                VAL.SAP_ATT_VALUE_DESC AS SOLUTION_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'SLTN' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) SLTN 
            ON sap.product_id = SLTN.product_id 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                VAL.SAP_ATT_VALUE_ID AS SEASONALITY_ID,
                VAL.SAP_ATT_VALUE_DESC AS SEASONALITY_DESC 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'SEAS' 
                AND ATT.DELETE_FLAG = '' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) SEAS 
            ON sap.product_id = SEAS.product_id""")

df_4.createOrReplaceTempView("ASQ_PRODUCT_ATTRIBUTE_MV_4")

# COMMAND ----------

# DBTITLE 1, EXP_CONVERSION_5

df_5=spark.sql("""
    SELECT
        TO_DECIMAL(CONSIGN_COST) AS CONSIGN_COST,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_PRODUCT_ATTRIBUTE_MV_4""")

df_5.createOrReplaceTempView("EXP_CONVERSION_5")

# COMMAND ----------

# DBTITLE 1, PRODUCT_ATTRIBUTE_MV

spark.sql("""INSERT INTO PRODUCT_ATTRIBUTE_MV SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR,
BASE_NBR AS BASE_NBR,
COLOR_CODE AS COLOR_CODE,
COLOR_DESC AS COLOR_DESC,
SIZE_CODE AS SIZE_CODE,
SIZE_DESC AS SIZE_DESC,
BREED_CODE AS BREED_CODE,
BREED_DESC AS BREED_DESC,
CONSIGN_ID AS CONSIGN_ID,
CONSIGN_COST AS CONSIGN_COST,
STORE_CHANNEL_ID AS STORE_CHANNEL_ID,
STORE_CHANNEL_DESC AS STORE_CHANNEL_DESC,
WEB_CHANNEL_ID AS WEB_CHANNEL_ID,
WEB_CHANNEL_DESC AS WEB_CHANNEL_DESC,
CATG_CHANNEL_ID AS CATG_CHANNEL_ID,
CATG_CHANNEL_DESC AS CATG_CHANNEL_DESC,
DROP_SHIP_ID AS DROP_SHIP_ID,
DROP_SHIP_DESC AS DROP_SHIP_DESC,
DROP_SHIP_CHANNEL_ID AS DROP_SHIP_CHANNEL_ID,
DROP_SHIP_CHANNEL_DESC AS DROP_SHIP_CHANNEL_DESC,
PSM_LABEL_FLAG AS PSM_LABEL_FLAG,
PSM_LABEL_DESC AS PSM_LABEL_DESC,
WEB_PRODUCT_GRP AS WEB_PRODUCT_GRP,
WEB_PRODUCT_GRP_DESC AS WEB_PRODUCT_GRP_DESC,
DIRECT_ADD_FRGT_ID AS DIRECT_ADD_FRGT_ID,
DIRECT_ADD_FRGT_AMT AS DIRECT_ADD_FRGT_AMT,
DIRECT_STATUS_CD AS DIRECT_STATUS_CD,
DIRECT_STATUS_DESC AS DIRECT_STATUS_DESC,
DATE_CODE_FLAG AS DATE_CODE_FLAG,
DATE_CODE_FLAG_DESC AS DATE_CODE_FLAG_DESC,
WEB_ACCESS_FLAG AS WEB_ACCESS_FLAG,
WEB_ACCESS_FLAG_DESC AS WEB_ACCESS_FLAG_DESC,
KHIM_OPTIMIZE_ID AS KHIM_OPTIMIZE_ID,
KHIM_OPTIMIZE_DESC AS KHIM_OPTIMIZE_DESC,
NAFTA_FLAG AS NAFTA_FLAG,
NAFTA_DESC AS NAFTA_DESC,
GBB_ID AS GBB_ID,
GBB_DESC AS GBB_DESC,
SPECIES_ID AS SPECIES_ID,
SPECIES_DESC AS SPECIES_DESC,
CAN_BUYER_ID AS CAN_BUYER_ID,
CAN_BUYER_DESC AS CAN_BUYER_DESC,
PRICING_ROLE_ID AS PRICING_ROLE_ID,
PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
SPECIES_ID AS SUB_SPECIES_ID,
SPECIES_DESC AS SUB_SPECIES_DESC,
CHANNEL_POSSITION_ID AS CHANNEL_POSITION_ID,
CHANNEL_POSSITION_DESC AS CHANNEL_POSITION_DESC,
COLLECTION_ID AS COLLECTION_ID,
COLLECTION_DESC AS COLLECTION_DESC,
COLOR_PATTERN_ID AS COLOR_PATTERN_ID,
COLOR_PATTERN_DESC AS COLOR_PATTERN_DESC,
COLOR_SHADE_ID AS COLOR_SHADE_ID,
COLOR_SHADE_DESC AS COLOR_SHADE_DESC,
FORM_ID AS FORM_ID,
FORM_DESC AS FORM_DESC,
FORMULATION_ID AS FORMULATION_ID,
FORMULATION_DESC AS FORMULATION_DESC,
PET_ENVIRONMENT_ID AS PET_ENVIRONMENT_ID,
PET_ENVIRONMENT_DESC AS PET_ENVIRONMENT_DESC,
LIFE_STAGE_ID AS LIFE_STAGE_ID,
LIFE_STAGE_DESC AS LIFE_STAGE_DESC,
MANDATORY_SKU_ID AS MANDATORY_SKU_ID,
MANDATORY_SKU_DESC AS MANDATORY_SKU_DESC,
SOLUTION_ID AS SOLUTION_ID,
SOLUTION_DESC AS SOLUTION_DESC,
SEASONALITY_ID AS SEASONALITY_ID,
SEASONALITY_DESC AS SEASONALITY_DESC FROM ASQ_PRODUCT_ATTRIBUTE_MV_4""")

spark.sql("""INSERT INTO PRODUCT_ATTRIBUTE_MV SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR,
BASE_NBR AS BASE_NBR,
COLOR_CODE AS COLOR_CODE,
COLOR_DESC AS COLOR_DESC,
SIZE_CODE AS SIZE_CODE,
SIZE_DESC AS SIZE_DESC,
BREED_CODE AS BREED_CODE,
BREED_DESC AS BREED_DESC,
CONSIGN_ID AS CONSIGN_ID,
CONSIGN_COST AS CONSIGN_COST,
STORE_CHANNEL_ID AS STORE_CHANNEL_ID,
STORE_CHANNEL_DESC AS STORE_CHANNEL_DESC,
WEB_CHANNEL_ID AS WEB_CHANNEL_ID,
WEB_CHANNEL_DESC AS WEB_CHANNEL_DESC,
CATG_CHANNEL_ID AS CATG_CHANNEL_ID,
CATG_CHANNEL_DESC AS CATG_CHANNEL_DESC,
DROP_SHIP_ID AS DROP_SHIP_ID,
DROP_SHIP_DESC AS DROP_SHIP_DESC,
DROP_SHIP_CHANNEL_ID AS DROP_SHIP_CHANNEL_ID,
DROP_SHIP_CHANNEL_DESC AS DROP_SHIP_CHANNEL_DESC,
PSM_LABEL_FLAG AS PSM_LABEL_FLAG,
PSM_LABEL_DESC AS PSM_LABEL_DESC,
WEB_PRODUCT_GRP AS WEB_PRODUCT_GRP,
WEB_PRODUCT_GRP_DESC AS WEB_PRODUCT_GRP_DESC,
DIRECT_ADD_FRGT_ID AS DIRECT_ADD_FRGT_ID,
DIRECT_ADD_FRGT_AMT AS DIRECT_ADD_FRGT_AMT,
DIRECT_STATUS_CD AS DIRECT_STATUS_CD,
DIRECT_STATUS_DESC AS DIRECT_STATUS_DESC,
DATE_CODE_FLAG AS DATE_CODE_FLAG,
DATE_CODE_FLAG_DESC AS DATE_CODE_FLAG_DESC,
WEB_ACCESS_FLAG AS WEB_ACCESS_FLAG,
WEB_ACCESS_FLAG_DESC AS WEB_ACCESS_FLAG_DESC,
KHIM_OPTIMIZE_ID AS KHIM_OPTIMIZE_ID,
KHIM_OPTIMIZE_DESC AS KHIM_OPTIMIZE_DESC,
NAFTA_FLAG AS NAFTA_FLAG,
NAFTA_DESC AS NAFTA_DESC,
GBB_ID AS GBB_ID,
GBB_DESC AS GBB_DESC,
SPECIES_ID AS SPECIES_ID,
SPECIES_DESC AS SPECIES_DESC,
CAN_BUYER_ID AS CAN_BUYER_ID,
CAN_BUYER_DESC AS CAN_BUYER_DESC,
PRICING_ROLE_ID AS PRICING_ROLE_ID,
PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
SPECIES_ID AS SUB_SPECIES_ID,
SPECIES_DESC AS SUB_SPECIES_DESC,
CHANNEL_POSSITION_ID AS CHANNEL_POSITION_ID,
CHANNEL_POSSITION_DESC AS CHANNEL_POSITION_DESC,
COLLECTION_ID AS COLLECTION_ID,
COLLECTION_DESC AS COLLECTION_DESC,
COLOR_PATTERN_ID AS COLOR_PATTERN_ID,
COLOR_PATTERN_DESC AS COLOR_PATTERN_DESC,
COLOR_SHADE_ID AS COLOR_SHADE_ID,
COLOR_SHADE_DESC AS COLOR_SHADE_DESC,
FORM_ID AS FORM_ID,
FORM_DESC AS FORM_DESC,
FORMULATION_ID AS FORMULATION_ID,
FORMULATION_DESC AS FORMULATION_DESC,
PET_ENVIRONMENT_ID AS PET_ENVIRONMENT_ID,
PET_ENVIRONMENT_DESC AS PET_ENVIRONMENT_DESC,
LIFE_STAGE_ID AS LIFE_STAGE_ID,
LIFE_STAGE_DESC AS LIFE_STAGE_DESC,
MANDATORY_SKU_ID AS MANDATORY_SKU_ID,
MANDATORY_SKU_DESC AS MANDATORY_SKU_DESC,
SOLUTION_ID AS SOLUTION_ID,
SOLUTION_DESC AS SOLUTION_DESC,
SEASONALITY_ID AS SEASONALITY_ID,
SEASONALITY_DESC AS SEASONALITY_DESC FROM EXP_CONVERSION_5""")
