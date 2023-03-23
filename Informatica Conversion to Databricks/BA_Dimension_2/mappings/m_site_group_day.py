# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SITE_GROUP_PRE_0

df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        SITE_GROUP_CD AS SITE_GROUP_CD,
        DELETE_IND AS DELETE_IND,
        SITE_GROUP_DESC AS SITE_GROUP_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_GROUP_PRE""")

df_0.createOrReplaceTempView("SITE_GROUP_PRE_0")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_to_SITE_GROUP_PRE_1

df_1=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        SITE_GROUP_CD AS SITE_GROUP_CD,
        DELETE_IND AS DELETE_IND,
        SITE_GROUP_DESC AS SITE_GROUP_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_GROUP_PRE_0""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SITE_GROUP_PRE_1")

# COMMAND ----------

# DBTITLE 1, AGG_SITE_GROUP_2

df_2=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        SITE_GROUP_CD AS SITE_GROUP_CD,
        first(DELETE_IND) AS first_DELETE_IND,
        first(SITE_GROUP_DESC) AS first_SITE_GROUP_DESC 
    FROM
        ASQ_Shortcut_to_SITE_GROUP_PRE_1 
    GROUP BY
        STORE_NBR,
        SITE_GROUP_CD""")

df_2.createOrReplaceTempView("AGG_SITE_GROUP_2")

# COMMAND ----------

# DBTITLE 1, EXP_LOAD_DT_3

df_3=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        SITE_GROUP_CD AS SITE_GROUP_CD,
        first_DELETE_IND AS first_DELETE_IND,
        first_SITE_GROUP_DESC AS first_SITE_GROUP_DESC,
        date_trunc('DAY',
        current_timestamp) AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        AGG_SITE_GROUP_2""")

df_3.createOrReplaceTempView("EXP_LOAD_DT_3")

# COMMAND ----------

# DBTITLE 1, SITE_GROUP_DAY

spark.sql("""INSERT INTO SITE_GROUP_DAY SELECT STORE_NBR AS STORE_NBR,
SITE_GROUP_CD AS SITE_GROUP_CD,
first_DELETE_IND AS DELETE_IND,
first_SITE_GROUP_DESC AS SITE_GROUP_DESC,
LOAD_DT AS LOAD_DT FROM EXP_LOAD_DT_3""")
