# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, POG_PROMO_HST_0

df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        POG_NBR AS POG_NBR,
        REPL_START_DT AS REPL_START_DT,
        REPL_END_DT AS REPL_END_DT,
        LIST_START_DT AS LIST_START_DT,
        LIST_END_DT AS LIST_END_DT,
        PROMO_QTY AS PROMO_QTY,
        LAST_CHNG_DT AS LAST_CHNG_DT,
        POG_STATUS_CD AS POG_STATUS_CD,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        POG_PROMO_HST""")

df_0.createOrReplaceTempView("POG_PROMO_HST_0")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_to_POG_PROMO_HST_1

df_1=spark.sql("""
    SELECT
        (CURRENT_DATE - (DATE_PART('DOW',
        CURRENT_DATE) - 1)) AS WEEK_DT,
        P.PRODUCT_ID AS PRODUCT_ID,
        P.LOCATION_ID AS LOCATION_ID,
        P.POG_NBR AS POG_NBR,
        P.PROMO_QTY AS PROMO_QTY,
        P.POG_STATUS_CD AS POG_STATUS_CD,
        CURRENT_DATE AS LOAD_DT 
    FROM
        POG_PROMO_HST P 
    WHERE
        (
            CURRENT_DATE - (
                DATE_PART('DOW', CURRENT_DATE) - 1
            )
        ) BETWEEN P.REPL_START_DT AND P.REPL_END_DT""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_POG_PROMO_HST_1")

# COMMAND ----------

# DBTITLE 1, POG_PROMO_WK

spark.sql("""INSERT INTO POG_PROMO_WK SELECT WEEK_DT AS WEEK_DT,
PRODUCT_ID AS PRODUCT_ID,
LOCATION_ID AS LOCATION_ID,
POG_NBR AS POG_NBR,
PROMO_QTY AS PROMO_QTY,
POG_STATUS_CD AS POG_STATUS_CD,
LOAD_DT AS LOAD_DT FROM ASQ_Shortcut_to_POG_PROMO_HST_1""")
