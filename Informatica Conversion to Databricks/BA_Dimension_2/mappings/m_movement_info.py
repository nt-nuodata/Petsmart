# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, new_move_info_0

df_0=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        new_move_info""")

df_0.createOrReplaceTempView("new_move_info_0")

# COMMAND ----------

# DBTITLE 1, SQ_SHORTCUT_NEW_MOVE_INFO_1

df_1=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        new_move_info_0""")

df_1.createOrReplaceTempView("SQ_SHORTCUT_NEW_MOVE_INFO_1")

# COMMAND ----------

# DBTITLE 1, EXP_CONVERT_2

df_2=spark.sql("""
    SELECT
        TO_DECIMAL(MOVEMENT_ID) AS OUT_MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        TO_DECIMAL(MOVE_TYPE_ID) AS OUT_MOVE_TYPE_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        TO_decimal(MOVE_REASON_ID) AS OUT_MOVE_REASON_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_SHORTCUT_NEW_MOVE_INFO_1""")

df_2.createOrReplaceTempView("EXP_CONVERT_2")

# COMMAND ----------

# DBTITLE 1, LKP_MOVE_REASON_3

df_3=spark.sql("""
    SELECT
        EXP_CONVERT_2.OUT_MOVE_REASON_ID AS IN_MOVE_REASON_ID,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        EXP_CONVERT_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MOVE_REASON 
    RIGHT OUTER JOIN
        EXP_CONVERT_2 
            ON MOVE_REASON_ID = EXP_CONVERT_2.OUT_MOVE_REASON_ID""")

df_3.createOrReplaceTempView("LKP_MOVE_REASON_3")

# COMMAND ----------

# DBTITLE 1, LKP_MOVE_TYPE_4

df_4=spark.sql("""
    SELECT
        EXP_CONVERT_2.OUT_MOVE_TYPE_ID AS IN_MOVE_TYPE_ID,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        EXP_CONVERT_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MOVE_TYPE 
    RIGHT OUTER JOIN
        EXP_CONVERT_2 
            ON MOVE_TYPE_ID = EXP_CONVERT_2.OUT_MOVE_TYPE_ID""")

df_4.createOrReplaceTempView("LKP_MOVE_TYPE_4")

# COMMAND ----------

# DBTITLE 1, EXP_RTRIM_5

df_5=spark.sql("""
    SELECT
        EXP_CONVERT_2.OUT_MOVEMENT_ID AS MOVEMENT_ID,
        RTRIM(MOVE_TYPE_DESC) AS OUT_MOVE_TYPE_DESC,
        LKP_MOVE_TYPE_4.MOVE_TYPE_ID AS MOVE_TYPE_ID,
        RTRIM(MOVE_REASON_DESC) AS OUT_MOVE_REASON_DESC,
        LKP_MOVE_REASON_3.MOVE_REASON_ID AS MOVE_REASON_ID,
        LKP_MOVE_TYPE_4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_MOVE_TYPE_4 
    INNER JOIN
        LKP_MOVE_REASON_3 
            ON LKP_MOVE_TYPE_4.Monotonically_Increasing_Id = LKP_MOVE_REASON_3.Monotonically_Increasing_Id 
    INNER JOIN
        EXP_CONVERT_2 
            ON LKP_MOVE_TYPE_4.Monotonically_Increasing_Id = EXP_CONVERT_2.Monotonically_Increasing_Id""")

df_5.createOrReplaceTempView("EXP_RTRIM_5")

# COMMAND ----------

# DBTITLE 1, FIL_NULLS_6

df_6=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_RTRIM_5 
    WHERE
        IFF(ISNULL(MOVEMENT_ID) 
        OR IS_SPACES(TO_CHAR(MOVEMENT_ID)), FALSE, TRUE)""")

df_6.createOrReplaceTempView("FIL_NULLS_6")

# COMMAND ----------

# DBTITLE 1, LKP_MOVEMENT_INFO_7

df_7=spark.sql("""
    SELECT
        FIL_NULLS_6.MOVEMENT_ID AS IN_MOVEMENT_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_CLASS_DESC AS MOVE_CLASS_DESC,
        MOVE_CLASS_ID AS MOVE_CLASS_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        FIL_NULLS_6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MOVEMENT_INFO 
    RIGHT OUTER JOIN
        FIL_NULLS_6 
            ON MOVEMENT_ID = FIL_NULLS_6.MOVEMENT_ID""")

df_7.createOrReplaceTempView("LKP_MOVEMENT_INFO_7")

# COMMAND ----------

# DBTITLE 1, EXP_DetectChanges_8

df_8=spark.sql("""
    SELECT
        IFF(ISNULL(FIL_NULLS_6.MOVEMENT_ID),
        TRUE,
        FALSE) AS NewFlag,
        IFF(ISNULL(FIL_NULLS_6.MOVEMENT_ID),
        FALSE,
        DECODE(TRUE,
        FIL_NULLS_6.MOVE_TYPE_DESC != trim_MOVE_TYPE_DESC,
        TRUE,
        FIL_NULLS_6.MOVE_TYPE_ID != trim_MOVE_TYPE_ID,
        TRUE,
        FIL_NULLS_6.MOVE_REASON_DESC != trim_MOVE_REASON_DESC,
        TRUE,
        FIL_NULLS_6.MOVE_REASON_ID != trim_MOVE_REASON_ID,
        TRUE,
        isnull(FIL_NULLS_6.MOVE_TYPE_ID),
        true,
        FALSE)) AS ChangedFlag,
        FIL_NULLS_6.MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        FIL_NULLS_6.MOVE_TYPE_ID AS MOVE_TYPE_ID,
        FIL_NULLS_6.MOVE_REASON_DESC AS MOVE_REASON_DESC,
        FIL_NULLS_6.MOVE_REASON_ID AS MOVE_REASON_ID,
        IFF(ISNULL(MOVE_CLASS_ID),
        99,
        MOVE_CLASS_ID) AS MOVE_CLASS_ID,
        IFF(ISNULL(MOVE_CLASS_DESC),
        'default',
        MOVE_CLASS_DESC) AS MOVE_CLASS_DESC,
        FIL_NULLS_6.MOVEMENT_ID AS MOVEMENT_ID,
        FIL_NULLS_6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_NULLS_6 
    INNER JOIN
        LKP_MOVEMENT_INFO_7 
            ON FIL_NULLS_6.Monotonically_Increasing_Id = LKP_MOVEMENT_INFO_7.Monotonically_Increasing_Id""")

df_8.createOrReplaceTempView("EXP_DetectChanges_8")

# COMMAND ----------

# DBTITLE 1, UPD_ins_upd_9

df_9=spark.sql("""
    SELECT
        MOVEMENT_ID AS MOVEMENT_ID,
        MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
        MOVE_TYPE_ID AS MOVE_TYPE_ID,
        MOVE_REASON_DESC AS MOVE_REASON_DESC,
        MOVE_REASON_ID AS MOVE_REASON_ID,
        MOVE_CLASS_ID AS MOVE_CLASS_ID,
        MOVE_CLASS_DESC AS MOVE_CLASS_DESC,
        NewFlag AS NewFlag,
        ChangedFlag AS ChangedFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DetectChanges_8""")

df_9.createOrReplaceTempView("UPD_ins_upd_9")

# COMMAND ----------

# DBTITLE 1, MOVEMENT_INFO

spark.sql("""INSERT INTO MOVEMENT_INFO SELECT MOVEMENT_ID AS MOVEMENT_ID,
MOVE_CLASS_DESC AS MOVE_CLASS_DESC,
MOVE_CLASS_ID AS MOVE_CLASS_ID,
MOVE_REASON_DESC AS MOVE_REASON_DESC,
MOVE_REASON_ID AS MOVE_REASON_ID,
MOVE_TYPE_DESC AS MOVE_TYPE_DESC,
MOVE_TYPE_ID AS MOVE_TYPE_ID FROM UPD_ins_upd_9""")
