# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, PB_MANAGER_0

df_0=spark.sql("""
    SELECT
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_MANAGER_NAME AS PB_MANAGER_NAME,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PB_MANAGER""")

df_0.createOrReplaceTempView("PB_MANAGER_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PB_MANAGER_1

df_1=spark.sql("""
    SELECT
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_MANAGER_NAME AS PB_MANAGER_NAME,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PB_MANAGER_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PB_MANAGER_1")

# COMMAND ----------

# DBTITLE 1, PB_MANAGER_PRE_2

df_2=spark.sql("""
    SELECT
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_MANAGER_NAME AS PB_MANAGER_NAME,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PB_MANAGER_PRE""")

df_2.createOrReplaceTempView("PB_MANAGER_PRE_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PB_MANAGER_PRE_3

df_3=spark.sql("""
    SELECT
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_MANAGER_NAME AS PB_MANAGER_NAME,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PB_MANAGER_PRE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PB_MANAGER_PRE_3")

# COMMAND ----------

# DBTITLE 1, JNR_MANAGER_4

df_4=spark.sql("""
    SELECT
        MASTER.PB_MANAGER_ID AS PB_MANAGER_ID,
        MASTER.PB_MANAGER_NAME AS PB_MANAGER_NAME,
        MASTER.PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        DETAIL.PB_MANAGER_ID AS PB_MANAGER_ID_pre,
        DETAIL.PB_MANAGER_NAME AS PB_MANAGER_NAME_pre,
        DETAIL.PB_DIRECTOR_ID AS PB_DIRECTOR_ID_pre,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PB_MANAGER_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_PB_MANAGER_PRE_3 DETAIL 
            ON MASTER.PB_MANAGER_ID = DETAIL.PB_MANAGER_ID""")

df_4.createOrReplaceTempView("JNR_MANAGER_4")

# COMMAND ----------

# DBTITLE 1, FIL_MANAGER_5

df_5=spark.sql("""
    SELECT
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_MANAGER_NAME AS PB_MANAGER_NAME,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        PB_MANAGER_ID_pre AS PB_MANAGER_ID_pre,
        PB_MANAGER_NAME_pre AS PB_MANAGER_NAME_pre,
        PB_DIRECTOR_ID_pre AS PB_DIRECTOR_ID_pre,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MANAGER_4 
    WHERE
        NOT ISNULL(PB_MANAGER_ID) 
        AND NOT ISNULL(PB_MANAGER_ID_pre) 
        AND PB_MANAGER_NAME != PB_MANAGER_NAME_pre 
        OR NOT ISNULL(PB_MANAGER_ID) 
        AND ISNULL(PB_MANAGER_ID_pre) 
        OR ISNULL(PB_MANAGER_ID) 
        AND NOT ISNULL(PB_MANAGER_ID_pre)""")

df_5.createOrReplaceTempView("FIL_MANAGER_5")

# COMMAND ----------

# DBTITLE 1, EXP_MANAGER_6

df_6=spark.sql("""
    SELECT
        IFF(NOT ISNULL(PB_MANAGER_ID) 
        AND NOT ISNULL(PB_MANAGER_ID_pre),
        PB_MANAGER_ID_pre,
        IFF(NOT ISNULL(PB_MANAGER_ID) 
        AND ISNULL(PB_MANAGER_ID_pre),
        PB_MANAGER_ID,
        PB_MANAGER_ID_pre)) AS PB_MANAGER_ID,
        IFF(NOT ISNULL(PB_MANAGER_NAME) 
        AND NOT ISNULL(PB_MANAGER_NAME_pre),
        PB_MANAGER_NAME_pre,
        IFF(NOT ISNULL(PB_MANAGER_NAME) 
        AND ISNULL(PB_MANAGER_NAME_pre),
        PB_MANAGER_NAME,
        PB_MANAGER_NAME_pre)) AS PB_MANAGER_NAME,
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND NOT ISNULL(PB_DIRECTOR_ID_pre),
        PB_DIRECTOR_ID_pre,
        IFF(NOT ISNULL(PB_DIRECTOR_ID) 
        AND ISNULL(PB_DIRECTOR_ID_pre),
        PB_DIRECTOR_ID,
        PB_DIRECTOR_ID_pre)) AS PB_DIRECTOR_ID,
        IFF(NOT ISNULL(PB_MANAGER_ID) 
        AND NOT ISNULL(PB_MANAGER_ID_pre),
        0,
        IFF(NOT ISNULL(PB_MANAGER_ID) 
        AND ISNULL(PB_MANAGER_ID_pre),
        1,
        0)) AS DELETE_FLAG,
        current_timestamp AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        IFF(NOT ISNULL(PB_MANAGER_ID) 
        AND NOT ISNULL(PB_MANAGER_ID_pre),
        'U',
        IFF(NOT ISNULL(PB_MANAGER_ID) 
        AND ISNULL(PB_MANAGER_ID_pre),
        'D',
        'I')) AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_MANAGER_5""")

df_6.createOrReplaceTempView("EXP_MANAGER_6")

# COMMAND ----------

# DBTITLE 1, UPS_PB_BRAND_7

df_7=spark.sql("""
    SELECT
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_MANAGER_NAME AS PB_MANAGER_NAME,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_MANAGER_6""")

df_7.createOrReplaceTempView("UPS_PB_BRAND_7")

# COMMAND ----------

# DBTITLE 1, PB_MANAGER

spark.sql("""INSERT INTO PB_MANAGER SELECT PB_MANAGER_ID AS PB_MANAGER_ID,
PB_MANAGER_NAME AS PB_MANAGER_NAME,
PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
DELETE_FLAG AS DELETE_FLAG,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPS_PB_BRAND_7""")
