# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ContainerType_0


df_0=spark.sql("""
    SELECT
        ContainerTypeCd AS ContainerTypeCd,
        ContainerTypeDesc AS ContainerTypeDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ContainerType""")

df_0.createOrReplaceTempView("ContainerType_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ContainerType_1


df_1=spark.sql("""
    SELECT
        ContainerTypeCd AS ContainerTypeCd,
        ContainerTypeDesc AS ContainerTypeDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ContainerType_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ContainerType_1")

# COMMAND ----------
# DBTITLE 1, SKU_CONTAINER_2


df_2=spark.sql("""
    SELECT
        CONTAINER_CD AS CONTAINER_CD,
        CONTAINER_DESC AS CONTAINER_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_CONTAINER""")

df_2.createOrReplaceTempView("SKU_CONTAINER_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_CONTAINER_3


df_3=spark.sql("""
    SELECT
        CONTAINER_CD AS CONTAINER_CD,
        CONTAINER_DESC AS CONTAINER_DESC,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_CONTAINER_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_CONTAINER_3")

# COMMAND ----------
# DBTITLE 1, JNR_MasterOuterJoin_4


df_4=spark.sql("""
    SELECT
        DETAIL.ContainerTypeCd AS ContainerTypeCd,
        DETAIL.ContainerTypeDesc AS ContainerTypeDesc,
        MASTER.CONTAINER_CD AS M_CONTAINER_CD,
        MASTER.CONTAINER_DESC AS M_CONTAINER_DESC,
        MASTER.LOAD_TSTMP AS M_LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_CONTAINER_3 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_ContainerType_1 DETAIL 
            ON MASTER.CONTAINER_CD = DETAIL.ContainerTypeCd""")

df_4.createOrReplaceTempView("JNR_MasterOuterJoin_4")

# COMMAND ----------
# DBTITLE 1, EXP_CheckChanges_5


df_5=spark.sql("""
    SELECT
        ContainerTypeCd AS ContainerTypeCd,
        ContainerTypeDesc AS ContainerTypeDesc,
        IFF(ISNULL(M_LOAD_TSTMP),
        current_timestamp,
        M_LOAD_TSTMP) AS M_LOAD_TSTMP_NOTNULL,
        IFF(ISNULL(M_CONTAINER_CD),
        DD_INSERT,
        IFF(M_CONTAINER_DESC <> ContainerTypeDesc,
        DD_UPDATE,
        DD_REJECT)) AS UpdateStrategy,
        current_timestamp AS Update_Dt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MasterOuterJoin_4""")

df_5.createOrReplaceTempView("EXP_CheckChanges_5")

# COMMAND ----------
# DBTITLE 1, FIL_RemoveRejected_6


df_6=spark.sql("""
    SELECT
        ContainerTypeCd AS ContainerTypeCd,
        ContainerTypeDesc AS ContainerTypeDesc,
        M_LOAD_TSTMP_NOTNULL AS M_LOAD_TSTMP_NOTNULL,
        UpdateStrategy AS UpdateStrategy,
        Update_Dt AS Update_Dt,
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
        ContainerTypeCd AS ContainerTypeCd,
        ContainerTypeDesc AS ContainerTypeDesc,
        M_LOAD_TSTMP_NOTNULL AS M_LOAD_TSTMP_NOTNULL,
        UpdateStrategy AS UpdateStrategy,
        Update_Dt AS Update_Dt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_RemoveRejected_6""")

df_7.createOrReplaceTempView("UPD_SetStrategy_7")

# COMMAND ----------
# DBTITLE 1, SKU_CONTAINER


spark.sql("""INSERT INTO SKU_CONTAINER SELECT ContainerTypeCd AS CONTAINER_CD,
ContainerTypeDesc AS CONTAINER_DESC,
Update_Dt AS UPDATE_TSTMP,
M_LOAD_TSTMP_NOTNULL AS LOAD_TSTMP FROM UPD_SetStrategy_7""")