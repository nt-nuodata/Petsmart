# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Flavor_0


df_0=spark.sql("""
    SELECT
        FlavorCd AS FlavorCd,
        FlavorDesc AS FlavorDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Flavor""")

df_0.createOrReplaceTempView("Flavor_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Flavor_1


df_1=spark.sql("""
    SELECT
        FlavorCd AS FlavorCd,
        FlavorDesc AS FlavorDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Flavor_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Flavor_1")

# COMMAND ----------
# DBTITLE 1, SKU_FLAVOR_2


df_2=spark.sql("""
    SELECT
        FLAVOR_CD AS FLAVOR_CD,
        FLAVOR_DESC AS FLAVOR_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_FLAVOR""")

df_2.createOrReplaceTempView("SKU_FLAVOR_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_FLAVOR_3


df_3=spark.sql("""
    SELECT
        FLAVOR_CD AS FLAVOR_CD,
        FLAVOR_DESC AS FLAVOR_DESC,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_FLAVOR_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_FLAVOR_3")

# COMMAND ----------
# DBTITLE 1, JNR_MasterOuterJoin_4


df_4=spark.sql("""
    SELECT
        DETAIL.FlavorCd AS FlavorCd,
        DETAIL.FlavorDesc AS FlavorDesc,
        MASTER.FLAVOR_CD AS M_FLAVOR_CD,
        MASTER.FLAVOR_DESC AS M_FLAVOR_DESC,
        MASTER.LOAD_TSTMP AS M_LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_FLAVOR_3 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_Flavor_1 DETAIL 
            ON MASTER.FLAVOR_CD = DETAIL.FlavorCd""")

df_4.createOrReplaceTempView("JNR_MasterOuterJoin_4")

# COMMAND ----------
# DBTITLE 1, EXP_CheckChanges_5


df_5=spark.sql("""
    SELECT
        FlavorCd AS FlavorCd,
        FlavorDesc AS FlavorDesc,
        IFF(ISNULL(M_FLAVOR_CD),
        DD_INSERT,
        IFF(M_FLAVOR_DESC <> FlavorDesc,
        DD_UPDATE,
        DD_REJECT)) AS UpdateStrategy,
        IFF(ISNULL(M_LOAD_TSTMP),
        current_timestamp,
        M_LOAD_TSTMP) AS LOAD_TSTMP_NOTNULL,
        current_timestamp AS Update_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MasterOuterJoin_4""")

df_5.createOrReplaceTempView("EXP_CheckChanges_5")

# COMMAND ----------
# DBTITLE 1, FIL_RemoveRejected_6


df_6=spark.sql("""
    SELECT
        FlavorCd AS FlavorCd,
        FlavorDesc AS FlavorDesc,
        UpdateStrategy AS UpdateStrategy,
        LOAD_TSTMP_NOTNULL AS LOAD_TSTMP_NOTNULL,
        Update_TSTMP AS Update_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_CheckChanges_5 
    WHERE
        UpdateStrategy <> DD_REJECT""")

df_6.createOrReplaceTempView("FIL_RemoveRejected_6")

# COMMAND ----------
# DBTITLE 1, UPD_SetStrategy_7


df_7=spark.sql("""
    SELECT
        FlavorCd AS FlavorCd,
        FlavorDesc AS FlavorDesc,
        UpdateStrategy AS UpdateStrategy,
        LOAD_TSTMP_NOTNULL AS LOAD_TSTMP_NOTNULL,
        Update_TSTMP AS Update_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_RemoveRejected_6""")

df_7.createOrReplaceTempView("UPD_SetStrategy_7")

# COMMAND ----------
# DBTITLE 1, SKU_FLAVOR


spark.sql("""INSERT INTO SKU_FLAVOR SELECT FlavorCd AS FLAVOR_CD,
FlavorDesc AS FLAVOR_DESC,
Update_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP_NOTNULL AS LOAD_TSTMP FROM UPD_SetStrategy_7""")