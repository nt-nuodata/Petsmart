# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_DAY_0


df_0=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        DELETE_IND AS DELETE_IND,
        SAFETY_QTY AS SAFETY_QTY,
        SERVICE_LVL_RT AS SERVICE_LVL_RT,
        REORDER_POINT_QTY AS REORDER_POINT_QTY,
        PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
        TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
        PRESENT_QTY AS PRESENT_QTY,
        PROMO_QTY AS PROMO_QTY,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REPLENISHMENT_DAY""")

df_0.createOrReplaceTempView("REPLENISHMENT_DAY_0")

# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_PRE_1


df_1=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        DELETE_IND AS DELETE_IND,
        SAFETY_QTY AS SAFETY_QTY,
        SERVICE_LVL_RT AS SERVICE_LVL_RT,
        REORDER_POINT_QTY AS REORDER_POINT_QTY,
        PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
        TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
        PRESENT_QTY AS PRESENT_QTY,
        PROMO_QTY AS PROMO_QTY,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REPLENISHMENT_PRE""")

df_1.createOrReplaceTempView("REPLENISHMENT_PRE_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_REPLENISHMENT_DAY_2


df_2=spark.sql("""
    SELECT
        RP.SKU_NBR,
        RP.STORE_NBR,
        RP.DELETE_IND,
        RP.SAFETY_QTY,
        RP.SERVICE_LVL_RT,
        RP.REORDER_POINT_QTY,
        RP.PLAN_DELIV_DAYS,
        RP.TARGET_STOCK_QTY,
        RP.PRESENT_QTY,
        RP.PROMO_QTY,
        CURRENT_TIMESTAMP,
        RD.SKU_NBR AS OLD_SKU_NBR 
    FROM
        REPLENISHMENT_PRE RP 
    LEFT OUTER JOIN
        REPLENISHMENT_DAY RD 
            ON RP.SKU_NBR = RD.SKU_NBR 
            AND RP.STORE_NBR = RD.STORE_NBR""")

df_2.createOrReplaceTempView("ASQ_Shortcut_to_REPLENISHMENT_DAY_2")

# COMMAND ----------
# DBTITLE 1, UPD_REPLENISHMENT_DAY_3


df_3=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        DELETE_IND AS DELETE_IND,
        SAFETY_QTY AS SAFETY_QTY,
        SERVICE_LVL_RT AS SERVICE_LVL_RT,
        REORDER_POINT_QTY AS REORDER_POINT_QTY,
        PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
        TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
        PRESENT_QTY AS PRESENT_QTY,
        PROMO_QTY AS PROMO_QTY,
        LOAD_DT AS LOAD_DT,
        OLD_SKU_NBR AS OLD_SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_to_REPLENISHMENT_DAY_2""")

df_3.createOrReplaceTempView("UPD_REPLENISHMENT_DAY_3")

# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_DAY


spark.sql("""INSERT INTO REPLENISHMENT_DAY SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
DELETE_IND AS DELETE_IND,
SAFETY_QTY AS SAFETY_QTY,
SERVICE_LVL_RT AS SERVICE_LVL_RT,
REORDER_POINT_QTY AS REORDER_POINT_QTY,
PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
PRESENT_QTY AS PRESENT_QTY,
PROMO_QTY AS PROMO_QTY,
LOAD_DT AS LOAD_DT FROM UPD_REPLENISHMENT_DAY_3""")