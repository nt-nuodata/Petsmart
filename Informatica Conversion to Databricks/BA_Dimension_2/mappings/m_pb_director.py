# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, PB_DIRECTOR_0


df_0=spark.sql("""
    SELECT
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PB_DIRECTOR""")

df_0.createOrReplaceTempView("PB_DIRECTOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_DIRECTOR_1


df_1=spark.sql("""
    SELECT
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PB_DIRECTOR_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PB_DIRECTOR_1")

# COMMAND ----------
# DBTITLE 1, PB_DIRECTOR_PRE_2


df_2=spark.sql("""
    SELECT
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PB_DIRECTOR_PRE""")

df_2.createOrReplaceTempView("PB_DIRECTOR_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PB_DIRECTOR_PRE_3


df_3=spark.sql("""
    SELECT
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PB_DIRECTOR_PRE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PB_DIRECTOR_PRE_3")

# COMMAND ----------
# DBTITLE 1, JNR_DIRECTOR_4


df_4=spark.sql("""
    SELECT
        MASTER.PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        MASTER.PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
        DETAIL.PB_DIRECTOR_ID AS PB_DIRECTOR_ID1,
        DETAIL.PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME1,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PB_DIRECTOR_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_PB_DIRECTOR_PRE_3 DETAIL 
            ON MASTER.PB_DIRECTOR_ID = DETAIL.PB_DIRECTOR_ID""")

df_4.createOrReplaceTempView("JNR_DIRECTOR_4")

# COMMAND ----------
# DBTITLE 1, FIL_DIRECTOR_5


df_5=spark.sql("""
    SELECT
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
        PB_DIRECTOR_ID1 AS PB_DIRECTOR_ID1,
        PB_DIRECTOR_NAME1 AS PB_DIRECTOR_NAME1,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_DIRECTOR_4 
    WHERE
        NOT ISNULL(PB_DIRECTOR_ID) 
        AND NOT ISNULL(PB_DIRECTOR_ID1) 
        AND PB_DIRECTOR_NAME != PB_DIRECTOR_NAME1 
        OR NOT ISNULL(PB_DIRECTOR_ID) 
        AND ISNULL(PB_DIRECTOR_ID1) 
        OR ISNULL(PB_DIRECTOR_ID) 
        AND NOT ISNULL(PB_DIRECTOR_ID1)""")

df_5.createOrReplaceTempView("FIL_DIRECTOR_5")

# COMMAND ----------
# DBTITLE 1, EXP_DIRECTOR_6


df_6=spark.sql("""
    SELECT
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND NOT ISNULL(PB_DIRECTOR_ID1),
        PB_DIRECTOR_ID1,
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND ISNULL(PB_DIRECTOR_ID1),
        PB_DIRECTOR_ID,
        PB_DIRECTOR_ID1)) AS PB_DIRECTOR_ID,
        IFF(NOT ISNULL(PB_DIRECTOR_NAME) 
        AND NOT ISNULL(PB_DIRECTOR_NAME1),
        PB_DIRECTOR_NAME1,
        IFF(NOT ISNULL(PB_DIRECTOR_NAME) 
        AND ISNULL(PB_DIRECTOR_NAME1),
        PB_DIRECTOR_NAME,
        PB_DIRECTOR_NAME1)) AS PB_DIRECTOR_NAME,
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND NOT ISNULL(PB_DIRECTOR_ID1),
        0,
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND ISNULL(PB_DIRECTOR_ID1),
        1,
        0)) AS DELETE_FLAG,
        current_timestamp AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND NOT ISNULL(PB_DIRECTOR_ID1),
        'U',
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND ISNULL(PB_DIRECTOR_ID1),
        'D',
        'I')) AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_DIRECTOR_5""")

df_6.createOrReplaceTempView("EXP_DIRECTOR_6")

# COMMAND ----------
# DBTITLE 1, UPS_PB_DIRECTOR_7


df_7=spark.sql("""
    SELECT
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DIRECTOR_6""")

df_7.createOrReplaceTempView("UPS_PB_DIRECTOR_7")

# COMMAND ----------
# DBTITLE 1, PB_DIRECTOR


spark.sql("""INSERT INTO PB_DIRECTOR SELECT PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
PB_DIRECTOR_NAME AS PB_DIRECTOR_NAME,
DELETE_FLAG AS DELETE_FLAG,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPS_PB_DIRECTOR_7""")