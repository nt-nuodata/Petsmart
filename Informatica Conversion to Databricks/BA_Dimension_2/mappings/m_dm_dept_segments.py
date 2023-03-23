# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, DM_DEPT_SEGMENTS_USER_0


df_0=spark.sql("""
    SELECT
        CONSUM_ID AS CONSUM_ID,
        CONSUM_DESC AS CONSUM_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DM_DEPT_SEGMENTS_USER""")

df_0.createOrReplaceTempView("DM_DEPT_SEGMENTS_USER_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DM_DEPT_SEGMENTS_USER_1


df_1=spark.sql("""
    SELECT
        CONSUM_ID AS CONSUM_ID,
        CONSUM_DESC AS CONSUM_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        DM_DEPT_SEGMENTS_USER_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_DM_DEPT_SEGMENTS_USER_1")

# COMMAND ----------
# DBTITLE 1, DM_DEPT_SEGMENTS


spark.sql("""INSERT INTO DM_DEPT_SEGMENTS SELECT SAP_DEPT_ID AS SAP_DEPT_ID,
CONSUM_ID AS CONSUM_ID,
CONSUM_DESC AS CONSUM_DESC,
SEGMENT_ID AS SEGMENT_ID,
SEGMENT_DESC AS SEGMENT_DESC FROM SQ_Shortcut_to_DM_DEPT_SEGMENTS_USER_1""")