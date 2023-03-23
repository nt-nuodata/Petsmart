# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SAP_ATTRIBUTE_FILE_0


df_0=spark.sql("""
    SELECT
        DELETE_FLAG AS DELETE_FLAG,
        ARTICLE AS ARTICLE,
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATTRIBUTE_FILE""")

df_0.createOrReplaceTempView("SAP_ATTRIBUTE_FILE_0")

# COMMAND ----------
# DBTITLE 1, SQ_SAP_ATTRIBUTE_FILE_1


df_1=spark.sql("""
    SELECT
        ARTICLE AS ARTICLE,
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        DELETE_FLAG AS DELETE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_ATTRIBUTE_FILE_0""")

df_1.createOrReplaceTempView("SQ_SAP_ATTRIBUTE_FILE_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_COLUMNS_2


df_2=spark.sql("""
    SELECT
        ARTICLE AS ARTICLE,
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        DELETE_FLAG AS DELETE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_SAP_ATTRIBUTE_FILE_1""")

df_2.createOrReplaceTempView("EXP_LOAD_COLUMNS_2")

# COMMAND ----------
# DBTITLE 1, SAP_ATTRIBUTE_PRE


spark.sql("""INSERT INTO SAP_ATTRIBUTE_PRE SELECT ARTICLE AS SKU_NBR,
SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
DELETE_FLAG AS DELETE_FLAG FROM EXP_LOAD_COLUMNS_2""")