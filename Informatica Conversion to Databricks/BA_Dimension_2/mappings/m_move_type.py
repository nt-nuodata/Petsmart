# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MOVE_INFO_0


df_0=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MOVE_INFO""")

df_0.createOrReplaceTempView("MOVE_INFO_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_MOVE_INFO_1


df_1=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MOVE_INFO_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_MOVE_INFO_1")

# COMMAND ----------
# DBTITLE 1, EXP_RTRIM_2


df_2=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        RTRIM(MOVE_TYPE_DESC) AS OUT_MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_MOVE_INFO_1""")

df_2.createOrReplaceTempView("EXP_RTRIM_2")

# COMMAND ----------
# DBTITLE 1, FIL_NULLS_3


df_3=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_RTRIM_2 
    WHERE
        IFF(ISNULL(MOVEMENT_ID) 
        OR IS_SPACES(TO_CHAR(MOVEMENT_ID)) 
        OR MOVEMENT_ID = 9070080, FALSE, TRUE)""")

df_3.createOrReplaceTempView("FIL_NULLS_3")

# COMMAND ----------
# DBTITLE 1, LKP_MOVE_TYPE_4


df_4=spark.sql("""
    SELECT
        FIL_NULLS_3.MOVE_TYPE_ID AS IN_MOVE_TYPE_ID,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        FIL_NULLS_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MOVE_TYPE 
    RIGHT OUTER JOIN
        FIL_NULLS_3 
            ON MOVE_TYPE_ID = FIL_NULLS_3.MOVE_TYPE_ID""")

df_4.createOrReplaceTempView("LKP_MOVE_TYPE_4")

# COMMAND ----------
# DBTITLE 1, EXP_DetectChanges_5


df_5=spark.sql("""
    SELECT
        LKP_MOVE_TYPE_4.MOVE_TYPE_ID AS L_MOVE_TYPE_ID,
        IFF(ISNULL(FIL_NULLS_3.MOVE_TYPE_ID),
        TRUE,
        FALSE) AS NewFlagType,
        IFF(ISNULL(FIL_NULLS_3.MOVE_TYPE_ID),
        FALSE,
        DECODE(TRUE,
        FIL_NULLS_3.MOVE_TYPE_DESC != trim_MOVE_TYPE_DESC,
        TRUE,
        FALSE)) AS ChangedFlagType,
        FIL_NULLS_3.MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        FIL_NULLS_3.MOVE_TYPE_ID AS MOVE_TYPE_ID,
        FIL_NULLS_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_NULLS_3 
    INNER JOIN
        LKP_MOVE_TYPE_4 
            ON FIL_NULLS_3.Monotonically_Increasing_Id = LKP_MOVE_TYPE_4.Monotonically_Increasing_Id""")

df_5.createOrReplaceTempView("EXP_DetectChanges_5")

# COMMAND ----------
# DBTITLE 1, UPD_Ins_Upd_6


df_6=spark.sql("""
    SELECT
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        NewFlagType AS NewFlagType,
        ChangedFlagType AS ChangedFlagType,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DetectChanges_5""")

df_6.createOrReplaceTempView("UPD_Ins_Upd_6")

# COMMAND ----------
# DBTITLE 1, MOVE_TYPE


spark.sql("""INSERT INTO MOVE_TYPE SELECT MOVE_TYPE_ID AS MOVE_TYPE_ID,
MOVE_TYPE_DESC AS MOVE_TYPE_DESC FROM UPD_Ins_Upd_6""")