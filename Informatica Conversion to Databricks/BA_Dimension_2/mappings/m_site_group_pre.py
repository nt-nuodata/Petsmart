# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SITEGROUP_0

df_0=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        SITE_GROUP_CD AS SITE_GROUP_CD,
        STORE_NBR AS STORE_NBR,
        SITE_GROUP_DESC AS SITE_GROUP_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITEGROUP""")

df_0.createOrReplaceTempView("SITEGROUP_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_To_SITEGROUP_1

df_1=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        SITE_GROUP_CD AS SITE_GROUP_CD,
        STORE_NBR AS STORE_NBR,
        SITE_GROUP_DESC AS SITE_GROUP_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITEGROUP_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_SITEGROUP_1")

# COMMAND ----------

# DBTITLE 1, EXP_TRIM_DESC_2

df_2=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        RTRIM(SITE_GROUP_CD) AS SITE_GROUP_CD_OUT,
        DELETE_IND AS DELETE_IND,
        RTRIM(SITE_GROUP_DESC) AS SITE_GROUP_DESC_OUT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_SITEGROUP_1""")

df_2.createOrReplaceTempView("EXP_TRIM_DESC_2")

# COMMAND ----------

# DBTITLE 1, SITE_GROUP_PRE

spark.sql("""INSERT INTO SITE_GROUP_PRE SELECT STORE_NBR AS STORE_NBR,
SITE_GROUP_CD_OUT AS SITE_GROUP_CD,
DELETE_IND AS DELETE_IND,
SITE_GROUP_DESC_OUT AS SITE_GROUP_DESC FROM EXP_TRIM_DESC_2""")
