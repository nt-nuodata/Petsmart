# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ATT_VALUES_0

df_0=spark.sql("""
    SELECT
        SAP_ATT_PROD_ID AS SAP_ATT_PROD_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_LANGUAGE_CD AS SAP_ATT_LANGUAGE_CD,
        SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ATT_VALUES""")

df_0.createOrReplaceTempView("ATT_VALUES_0")

# COMMAND ----------

# DBTITLE 1, SQ_ATT_VALUES_1

df_1=spark.sql("""
    SELECT
        SAP_ATT_PROD_ID AS SAP_ATT_PROD_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_LANGUAGE_ID AS SAP_ATT_LANGUAGE_ID,
        SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ATT_VALUES_0""")

df_1.createOrReplaceTempView("SQ_ATT_VALUES_1")

# COMMAND ----------

# DBTITLE 1, EXP_ATT_VALUES_2

df_2=spark.sql("""
    SELECT
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_LANGUAGE_ID AS SAP_ATT_LANGUAGE_ID,
        IN_SAP_ATT_VALUE_DESC AS IN_SAP_ATT_VALUE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_ATT_VALUES_1""")

df_2.createOrReplaceTempView("EXP_ATT_VALUES_2")

# COMMAND ----------

# DBTITLE 1, SAP_ATT_VALUE_PRE

spark.sql("""INSERT INTO SAP_ATT_VALUE_PRE SELECT SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
SAP_ATT_LANGUAGE_ID AS LANGUAGE_CD,
IN_SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC FROM EXP_ATT_VALUES_2""")
