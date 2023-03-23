# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SAP_ATT_TYPE_PRE_0


df_0=spark.sql("""
    SELECT
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATT_TYPE_PRE""")

df_0.createOrReplaceTempView("SAP_ATT_TYPE_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SAP_ATT_TYPE_PRE_1


df_1=spark.sql("""
    SELECT
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_ATT_TYPE_PRE_0""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SAP_ATT_TYPE_PRE_1")

# COMMAND ----------
# DBTITLE 1, SAP_ATT_TYPE


spark.sql("""INSERT INTO SAP_ATT_TYPE SELECT SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC FROM ASQ_Shortcut_to_SAP_ATT_TYPE_PRE_1""")