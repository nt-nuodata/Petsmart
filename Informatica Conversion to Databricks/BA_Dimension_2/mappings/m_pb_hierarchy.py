# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, PB_HIERARCHY_0

df_0=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PB_HIERARCHY""")

df_0.createOrReplaceTempView("PB_HIERARCHY_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PB_HIERARCHY_1

df_1=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PB_HIERARCHY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PB_HIERARCHY_1")

# COMMAND ----------

# DBTITLE 1, PB_HIERARCHY_PRE_2

df_2=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PB_HIERARCHY_PRE""")

df_2.createOrReplaceTempView("PB_HIERARCHY_PRE_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PB_HIERARCHY_PRE_3

df_3=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PB_HIERARCHY_PRE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PB_HIERARCHY_PRE_3")

# COMMAND ----------

# DBTITLE 1, JNR_PB_HIERARCHY_4

df_4=spark.sql("""
    SELECT
        DETAIL.BRAND_CD AS BRAND_CD,
        DETAIL.SAP_DEPT_ID AS SAP_DEPT_ID,
        DETAIL.BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        DETAIL.PB_MANAGER_ID AS PB_MANAGER_ID,
        DETAIL.PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        MASTER.BRAND_CD AS BRAND_CD_pre,
        MASTER.SAP_DEPT_ID AS SAP_DEPT_ID_pre,
        MASTER.BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID_pre,
        MASTER.PB_MANAGER_ID AS PB_MANAGER_ID_pre,
        MASTER.PB_DIRECTOR_ID AS PB_DIRECTOR_ID_pre,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PB_HIERARCHY_PRE_3 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_PB_HIERARCHY_1 DETAIL 
            ON MASTER.BRAND_CD = BRAND_CD 
            AND SAP_DEPT_ID_pre = SAP_DEPT_ID 
            AND BRAND_CLASSIFICATION_ID_pre = DETAIL.BRAND_CLASSIFICATION_ID""")

df_4.createOrReplaceTempView("JNR_PB_HIERARCHY_4")

# COMMAND ----------

# DBTITLE 1, EXP_PB_HIERARCHY_5

df_5=spark.sql("""
    SELECT
        IFF(v_LOAD_STRATEGY_FLAG = 'D',
        BRAND_CD,
        BRAND_CD_pre) AS BRAND_CD,
        IFF(v_LOAD_STRATEGY_FLAG = 'D',
        SAP_DEPT_ID,
        SAP_DEPT_ID_pre) AS SAP_DEPT_ID,
        IFF(v_LOAD_STRATEGY_FLAG = 'D',
        BRAND_CLASSIFICATION_ID,
        BRAND_CLASSIFICATION_ID_pre) AS BRAND_CLASSIFICATION_ID,
        IFF(v_LOAD_STRATEGY_FLAG = 'D',
        PB_MANAGER_ID,
        PB_MANAGER_ID_pre) AS PB_MANAGER_ID,
        IFF(v_LOAD_STRATEGY_FLAG = 'D',
        PB_DIRECTOR_ID,
        PB_DIRECTOR_ID_pre) AS PB_DIRECTOR_ID,
        IFF(v_LOAD_STRATEGY_FLAG = 'D',
        1,
        0) AS DELETE_FLAG,
        current_timestamp AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        v_LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_PB_HIERARCHY_4""")

df_5.createOrReplaceTempView("EXP_PB_HIERARCHY_5")

# COMMAND ----------

# DBTITLE 1, UPS_PB_HIERARCHY_6

df_6=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        PB_MANAGER_ID AS PB_MANAGER_ID,
        PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_PB_HIERARCHY_5""")

df_6.createOrReplaceTempView("UPS_PB_HIERARCHY_6")

# COMMAND ----------

# DBTITLE 1, PB_HIERARCHY

spark.sql("""INSERT INTO PB_HIERARCHY SELECT BRAND_CD AS BRAND_CD,
SAP_DEPT_ID AS SAP_DEPT_ID,
BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
PB_MANAGER_ID AS PB_MANAGER_ID,
PB_DIRECTOR_ID AS PB_DIRECTOR_ID,
DELETE_FLAG AS DELETE_FLAG,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPS_PB_HIERARCHY_6""")
