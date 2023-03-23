# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_HAZMAT_PRE_0


df_0=spark.sql("""
    SELECT
        RTV_DEPT_CD AS RTV_DEPT_CD,
        RTV_DESC AS RTV_DESC,
        HAZ_FLAG AS HAZ_FLAG,
        AEROSOL_FLAG AS AEROSOL_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_HAZMAT_PRE""")

df_0.createOrReplaceTempView("SKU_HAZMAT_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SKU_HAZMAT_PRE_1


df_1=spark.sql("""
    SELECT
        RTV_DEPT_CD AS RTV_DEPT_CD,
        RTV_DESC AS RTV_DESC,
        HAZ_FLAG AS HAZ_FLAG,
        AEROSOL_FLAG AS AEROSOL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_HAZMAT_PRE_0""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SKU_HAZMAT_PRE_1")

# COMMAND ----------
# DBTITLE 1, lkp_SKU_HAZMAT_2


df_2=spark.sql("""
    SELECT
        ADD_DT AS ADD_DT,
        ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_HAZMAT 
    RIGHT OUTER JOIN
        ASQ_Shortcut_to_SKU_HAZMAT_PRE_1 
            ON RTV_DEPT_CD = ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.RTV_DEPT_CD""")

df_2.createOrReplaceTempView("lkp_SKU_HAZMAT_2")

# COMMAND ----------
# DBTITLE 1, exp_SKU_HAZMAT_3


df_3=spark.sql("""
    SELECT
        lkp_SKU_HAZMAT_2.ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.RTV_DEPT_CD AS INSERT_UPDATE_FLAG,
        ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.RTV_DEPT_CD AS RTV_DEPT_CD,
        ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.RTV_DESC AS RTV_DESC,
        DECODE(ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.HAZ_FLAG,
        'N',
        0,
        'Y',
        1) AS o_HAZ_FLAG,
        DECODE(ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.AEROSOL_FLAG,
        'N',
        0,
        'Y',
        1) AS o_AEROSOL_FLAG,
        IFF(ISNULL(ADD_DT),
        current_timestamp,
        ADD_DT) AS ADD_DT,
        current_timestamp AS LOAD_DT,
        lkp_SKU_HAZMAT_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        lkp_SKU_HAZMAT_2 
    INNER JOIN
        ASQ_Shortcut_to_SKU_HAZMAT_PRE_1 
            ON lkp_SKU_HAZMAT_2.Monotonically_Increasing_Id = ASQ_Shortcut_to_SKU_HAZMAT_PRE_1.Monotonically_Increasing_Id""")

df_3.createOrReplaceTempView("exp_SKU_HAZMAT_3")

# COMMAND ----------
# DBTITLE 1, upd_SKU_HAZMAT_ins_upd_4


df_4=spark.sql("""
    SELECT
        RTV_DEPT_CD AS RTV_DEPT_CD,
        RTV_DESC AS RTV_DESC,
        o_HAZ_FLAG AS HAZ_FLAG,
        o_AEROSOL_FLAG AS AEROSOL_FLAG,
        ADD_DT AS ADD_DT,
        LOAD_DT AS LOAD_DT,
        INSERT_UPDATE_FLAG AS INSERT_UPDATE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        exp_SKU_HAZMAT_3""")

df_4.createOrReplaceTempView("upd_SKU_HAZMAT_ins_upd_4")

# COMMAND ----------
# DBTITLE 1, SKU_HAZMAT


spark.sql("""INSERT INTO SKU_HAZMAT SELECT RTV_DEPT_CD AS RTV_DEPT_CD,
RTV_DESC AS RTV_DESC,
HAZ_FLAG AS HAZ_FLAG,
AEROSOL_FLAG AS AEROSOL_FLAG,
ADD_DT AS ADD_DT,
LOAD_DT AS LOAD_DT FROM upd_SKU_HAZMAT_ins_upd_4""")