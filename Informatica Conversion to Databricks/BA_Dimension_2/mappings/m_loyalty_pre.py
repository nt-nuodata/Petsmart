# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, LOYALTY_0

df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LOYALTY""")

df_0.createOrReplaceTempView("LOYALTY_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_To_LOYALTY_1

df_1=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LOYALTY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_LOYALTY_1")

# COMMAND ----------

# DBTITLE 1, EXP_NULL_2

df_2=spark.sql("""
    SELECT
        ISNULL(STORE_NBR) AS IS_NULL_FLAG,
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        SUBSTR(LOYALTY_PGM_START_DT,
        0,
        2) AS V_LOYALTY_PGM_START_MON,
        SUBSTR(LOYALTY_PGM_START_DT,
        4,
        2) AS V_LOYALTY_PGM_START_DAY,
        SUBSTR(LOYALTY_PGM_START_DT,
        7,
        4) AS V_LOYALTY_PGM_START_YR,
        IN_LOYALTY_PGM_CHANGE_DT AS IN_LOYALTY_PGM_CHANGE_DT,
        SUBSTR(LOYALTY_PGM_CHANGE_DT,
        0,
        2) AS V_LOYALTY_PGM_CHANGE_MON,
        SUBSTR(LOYALTY_PGM_CHANGE_DT,
        4,
        2) AS V_LOYALTY_PGM_CHANGE_DAY,
        SUBSTR(LOYALTY_PGM_CHANGE_DT,
        7,
        4) AS V_LOYALTY_PGM_CHANGE_YR,
        date_trunc('DAY',
        current_timestamp) AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_LOYALTY_1""")

df_2.createOrReplaceTempView("EXP_NULL_2")

# COMMAND ----------

# DBTITLE 1, EXP_DATE_FORMATTING_3

df_3=spark.sql("""
    SELECT
        OUT_DATE_VAR AS OUT_DATE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_NULL_2""")

df_3.createOrReplaceTempView("EXP_DATE_FORMATTING_3")

# COMMAND ----------

# DBTITLE 1, FLT_NULLS_4

df_4=spark.sql("""
    SELECT
        IS_NULL_FLAG AS IS_NULL_FLAG,
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_NULL_2 
    WHERE
        IS_NULL_FLAG = 0""")

df_4.createOrReplaceTempView("FLT_NULLS_4")

df_4=spark.sql("""
    SELECT
        IS_NULL_FLAG AS IS_NULL_FLAG,
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DATE_FORMATTING_3 
    WHERE
        IS_NULL_FLAG = 0""")

df_4.createOrReplaceTempView("FLT_NULLS_4")

df_4=spark.sql("""
    SELECT
        IS_NULL_FLAG AS IS_NULL_FLAG,
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DATE_FORMATTING_3 
    WHERE
        IS_NULL_FLAG = 0""")

df_4.createOrReplaceTempView("FLT_NULLS_4")

# COMMAND ----------

# DBTITLE 1, LOYALTY_PRE

spark.sql("""INSERT INTO LOYALTY_PRE SELECT STORE_NBR AS STORE_NBR,
STORE_DESC AS STORE_DESC,
PETSMART_DMA_CD AS PETSMART_DMA_CD,
PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
LOAD_DT AS LOAD_DT FROM FLT_NULLS_4""")
