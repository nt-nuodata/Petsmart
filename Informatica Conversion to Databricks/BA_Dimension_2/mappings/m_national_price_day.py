# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, NATIONAL_PRICE_PRE_0

df_0=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        SALES_ORG_CD AS SALES_ORG_CD,
        COND_TYPE_CD AS COND_TYPE_CD,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        DELETE_IND AS DELETE_IND,
        PROMOTION_CD AS PROMOTION_CD,
        COND_AMT AS COND_AMT,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        PRICING_REASON_CD AS PRICING_REASON_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        NATIONAL_PRICE_PRE""")

df_0.createOrReplaceTempView("NATIONAL_PRICE_PRE_0")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_to_NATIONAL_PRICE_PRE_1

df_1=spark.sql("""
    SELECT
        np.SKU_NBR,
        np.SALES_ORG_CD,
        np.COND_TYPE_CD,
        np.COND_RECORD_NBR,
        np.COND_END_DT,
        np.COND_EFF_DT,
        np.DELETE_IND,
        np.COND_AMT,
        np.PROMOTION_CD,
        np.COND_RT_UNIT,
        np.COND_PRICE_UNIT,
        np.COND_UNIT,
        np.UNIT_NUMERATOR,
        np.UNIT_DENOMINATOR,
        np.PRICING_REASON_CD,
        CURRENT_DATE AS UPDATE_DT,
        NVL(nd.LOAD_DT,
        CURRENT_DATE) AS LOAD_DT,
        CASE 
            WHEN nd.sku_nbr IS NULL THEN 1 
            ELSE 2 
        END AS ROW_ACTION 
    FROM
        NATIONAL_PRICE_PRE np 
    LEFT OUTER JOIN
        NATIONAL_PRICE_DAY nd 
            ON np.sku_nbr = nd.sku_nbr 
            AND np.sales_org_cd = nd.sales_org_cd 
            AND np.cond_type_cd = nd.cond_type_cd 
            AND np.cond_end_dt = nd.cond_end_dt""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_NATIONAL_PRICE_PRE_1")

# COMMAND ----------

# DBTITLE 1, UPD_ins_upd_2

df_2=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        SALES_ORG_CD AS SALES_ORG_CD,
        COND_TYPE_CD AS COND_TYPE_CD,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        DELETE_IND AS DELETE_IND,
        COND_AMT AS COND_AMT,
        PROMOTION_CD AS PROMOTION_CD,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        PRICING_REASON_CD AS PRICING_REASON_CD,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        ROW_ACTION AS ROWACTION,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_to_NATIONAL_PRICE_PRE_1""")

df_2.createOrReplaceTempView("UPD_ins_upd_2")

# COMMAND ----------

# DBTITLE 1, NATIONAL_PRICE_DAY

spark.sql("""INSERT INTO NATIONAL_PRICE_DAY SELECT SKU_NBR AS SKU_NBR,
SALES_ORG_CD AS SALES_ORG_CD,
COND_TYPE_CD AS COND_TYPE_CD,
COND_END_DT AS COND_END_DT,
COND_EFF_DT AS COND_EFF_DT,
COND_RECORD_NBR AS COND_RECORD_NBR,
DELETE_IND AS DELETE_IND,
PROMOTION_CD AS PROMOTION_CD,
COND_AMT AS COND_AMT,
COND_RT_UNIT AS COND_RT_UNIT,
COND_PRICE_UNIT AS COND_PRICE_UNIT,
COND_UNIT AS COND_UNIT,
UNIT_NUMERATOR AS UNIT_NUMERATOR,
UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
PRICING_REASON_CD AS PRICING_REASON_CD,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPD_ins_upd_2""")
