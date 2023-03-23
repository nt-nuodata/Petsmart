# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, PET_CONDITION_PRE_0

df_0=spark.sql("""
    SELECT
        CONDITION_ID AS CONDITION_ID,
        DESCRIPTION AS DESCRIPTION,
        IS_ACTIVE AS IS_ACTIVE,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_CONDITION_PRE""")

df_0.createOrReplaceTempView("PET_CONDITION_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PET_CONDITION_PRE_1

df_1=spark.sql("""
    SELECT
        CONDITION_ID AS CONDITION_ID,
        DESCRIPTION AS DESCRIPTION,
        IS_ACTIVE AS IS_ACTIVE,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_CONDITION_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PET_CONDITION_PRE_1")

# COMMAND ----------

# DBTITLE 1, PET_CONDITION_2

df_2=spark.sql("""
    SELECT
        PET_CONDITION_ID AS PET_CONDITION_ID,
        PET_CONDITION_DESC AS PET_CONDITION_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_CONDITION""")

df_2.createOrReplaceTempView("PET_CONDITION_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PET_CONDITION_3

df_3=spark.sql("""
    SELECT
        PET_CONDITION_ID AS PET_CONDITION_ID,
        PET_CONDITION_DESC AS PET_CONDITION_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_CONDITION_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PET_CONDITION_3")

# COMMAND ----------

# DBTITLE 1, jnr_Pre_DWTable_4

df_4=spark.sql("""
    SELECT
        MASTER.CONDITION_ID AS CONDITION_ID,
        MASTER.DESCRIPTION AS DESCRIPTION,
        MASTER.IS_ACTIVE AS IS_ACTIVE,
        DETAIL.PET_CONDITION_ID AS PET_CONDITION_ID,
        DETAIL.PET_CONDITION_DESC AS PET_CONDITION_DESC,
        DETAIL.IS_ACTIVE AS IS_ACTIVE1,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PET_CONDITION_PRE_1 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_PET_CONDITION_3 DETAIL 
            ON MASTER.CONDITION_ID = DETAIL.PET_CONDITION_ID""")

df_4.createOrReplaceTempView("jnr_Pre_DWTable_4")

# COMMAND ----------

# DBTITLE 1, exp_FLAGS_5

df_5=spark.sql("""
    SELECT
        PET_CONDITION_ID AS PET_CONDITION_ID,
        PET_CONDITION_DESC AS PET_CONDITION_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        current_timestamp AS UPDATE_TSTMP,
        current_timestamp AS LOAD_TSTMP,
        v_LOAD_FLAG AS LOAD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        jnr_Pre_DWTable_4""")

df_5.createOrReplaceTempView("exp_FLAGS_5")

# COMMAND ----------

# DBTITLE 1, fil_FLAGS_6

df_6=spark.sql("""
    SELECT
        LOAD_FLAG AS LOAD_FLAG,
        PET_CONDITION_ID AS PET_CONDITION_ID,
        PET_CONDITION_DESC AS PET_CONDITION_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        exp_FLAGS_5 
    WHERE
        LOAD_FLAG IN (
            'INSERT', 'UPDATE'
        )""")

df_6.createOrReplaceTempView("fil_FLAGS_6")

# COMMAND ----------

# DBTITLE 1, upd_FLAG_7

df_7=spark.sql("""
    SELECT
        LOAD_FLAG AS LOAD_FLAG,
        PET_CONDITION_ID AS PET_CONDITION_ID,
        PET_CONDITION_DESC AS PET_CONDITION_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        fil_FLAGS_6""")

df_7.createOrReplaceTempView("upd_FLAG_7")

# COMMAND ----------

# DBTITLE 1, PET_CONDITION

spark.sql("""INSERT INTO PET_CONDITION SELECT PET_CONDITION_ID AS PET_CONDITION_ID,
PET_CONDITION_DESC AS PET_CONDITION_DESC,
IS_ACTIVE AS IS_ACTIVE,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM upd_FLAG_7""")
