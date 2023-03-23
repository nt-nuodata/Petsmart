# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SUPPLY_CHAIN_0

df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        DIRECT_VENDOR_ID AS DIRECT_VENDOR_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SUPPLY_CHAIN""")

df_0.createOrReplaceTempView("SUPPLY_CHAIN_0")

# COMMAND ----------

# DBTITLE 1, WEEKS_1

df_1=spark.sql("""
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

df_1.createOrReplaceTempView("WEEKS_1")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_To_Dummy_Source_2

df_2=spark.sql("""
    SELECT
        'SUPPLY_CHAIN'""")

df_2.createOrReplaceTempView("ASQ_Shortcut_To_Dummy_Source_2")

# COMMAND ----------

# DBTITLE 1, FLT_TRUNC_PROD_TABLE_3

df_3=spark.sql("""
    SELECT
        TABLE_NAME AS TABLE_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_To_Dummy_Source_2 
    WHERE
        FALSE""")

df_3.createOrReplaceTempView("FLT_TRUNC_PROD_TABLE_3")

# COMMAND ----------

# DBTITLE 1, DUMMY_TARGET

spark.sql("""INSERT INTO DUMMY_TARGET SELECT TABLE_NAME AS COMMENT FROM ASQ_Shortcut_To_Dummy_Source_2""")

# COMMAND ----------

# DBTITLE 1, SUPPLY_CHAIN

spark.sql("""INSERT INTO SUPPLY_CHAIN SELECT TABLE_NAME AS PRODUCT_ID,
LOCATION_ID AS LOCATION_ID,
DIRECT_VENDOR_ID AS DIRECT_VENDOR_ID,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID FROM FLT_TRUNC_PROD_TABLE_3""")
