# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, WEEKS_0

df_0=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        CAL_WK AS CAL_WK,
        CAL_WK_NBR AS CAL_WK_NBR,
        CAL_MO AS CAL_MO,
        CAL_MO_NBR AS CAL_MO_NBR,
        CAL_MO_NAME AS CAL_MO_NAME,
        CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
        CAL_QTR AS CAL_QTR,
        CAL_QTR_NBR AS CAL_QTR_NBR,
        CAL_HALF AS CAL_HALF,
        CAL_YR AS CAL_YR,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_WK_NBR AS FISCAL_WK_NBR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_YR AS FISCAL_YR,
        LYR_WEEK_DT AS LYR_WEEK_DT,
        LWK_WEEK_DT AS LWK_WEEK_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        WEEKS""")

df_0.createOrReplaceTempView("WEEKS_0")

# COMMAND ----------

# DBTITLE 1, ASQ_SHORTCUT_TO_WEEKS_1

df_1=spark.sql("""
    SELECT
        CURRENT_DATE""")

df_1.createOrReplaceTempView("ASQ_SHORTCUT_TO_WEEKS_1")

# COMMAND ----------

# DBTITLE 1, FILTRANS_2

df_2=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        WEEK_NEW AS WEEK_NEW,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_SHORTCUT_TO_WEEKS_1 
    WHERE
        false""")

df_2.createOrReplaceTempView("FILTRANS_2")

# COMMAND ----------

# DBTITLE 1, BIW_SCRIPT_DUMMY

spark.sql("""INSERT INTO BIW_SCRIPT_DUMMY SELECT WEEK_DT AS DATE_TIME FROM ASQ_SHORTCUT_TO_WEEKS_1""")

# COMMAND ----------

# DBTITLE 1, SKU_STORE_PRICE_COSTS_PRE

spark.sql("""INSERT INTO SKU_STORE_PRICE_COSTS_PRE SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
RETAIL_PRICE_AMT AS RETAIL_PRICE_AMT,
WEEK_NEW AS PETPERKS_AMT,
SUM_COST AS SUM_COST,
BUM_COST AS BUM_COST FROM FILTRANS_2""")
