# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, PET_GENDER_PRE_0


df_0=spark.sql("""
    SELECT
        GENDER_ID AS GENDER_ID,
        NAME AS NAME,
        IS_ACTIVE AS IS_ACTIVE,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_GENDER_PRE""")

df_0.createOrReplaceTempView("PET_GENDER_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_GENDER_PRE_1


df_1=spark.sql("""
    SELECT
        GENDER_ID AS GENDER_ID,
        NAME AS NAME,
        IS_ACTIVE AS IS_ACTIVE,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_GENDER_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PET_GENDER_PRE_1")

# COMMAND ----------
# DBTITLE 1, PET_GENDER_2


df_2=spark.sql("""
    SELECT
        PET_GENDER_ID AS PET_GENDER_ID,
        PET_GENDER_DESC AS PET_GENDER_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PET_GENDER""")

df_2.createOrReplaceTempView("PET_GENDER_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_GENDER_3


df_3=spark.sql("""
    SELECT
        PET_GENDER_ID AS PET_GENDER_ID,
        PET_GENDER_DESC AS PET_GENDER_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PET_GENDER_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PET_GENDER_3")

# COMMAND ----------
# DBTITLE 1, jnr_Pre_DWTable_4


df_4=spark.sql("""
    SELECT
        MASTER.GENDER_ID AS GENDER_ID,
        MASTER.NAME AS NAME,
        MASTER.IS_ACTIVE AS IS_ACTIVE,
        DETAIL.PET_GENDER_ID AS PET_GENDER_ID,
        DETAIL.PET_GENDER_DESC AS PET_GENDER_DESC,
        DETAIL.IS_ACTIVE AS IS_ACTIVE1,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PET_GENDER_PRE_1 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_PET_GENDER_3 DETAIL 
            ON MASTER.GENDER_ID = DETAIL.PET_GENDER_ID""")

df_4.createOrReplaceTempView("jnr_Pre_DWTable_4")

# COMMAND ----------
# DBTITLE 1, exp_FLAGS_5


df_5=spark.sql("""
    SELECT
        PET_GENDER_ID AS PET_GENDER_ID,
        PET_GENDER_DESC AS PET_GENDER_DESC,
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
        PET_GENDER_ID AS PET_GENDER_ID,
        PET_GENDER_DESC AS PET_GENDER_DESC,
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
        PET_GENDER_ID AS PET_GENDER_ID,
        PET_GENDER_DESC AS PET_GENDER_DESC,
        IS_ACTIVE AS IS_ACTIVE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        fil_FLAGS_6""")

df_7.createOrReplaceTempView("upd_FLAG_7")

# COMMAND ----------
# DBTITLE 1, PET_GENDER


spark.sql("""INSERT INTO PET_GENDER SELECT PET_GENDER_ID AS PET_GENDER_ID,
PET_GENDER_DESC AS PET_GENDER_DESC,
IS_ACTIVE AS IS_ACTIVE,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM upd_FLAG_7""")