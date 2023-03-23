# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SAP_ATT_VALUE_PRE_0

df_0=spark.sql("""
    SELECT
        SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
        SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
        LANGUAGE_CD AS LANGUAGE_CD,
        SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ATT_VALUE_PRE""")

df_0.createOrReplaceTempView("SAP_ATT_VALUE_PRE_0")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_to_SAP_ATT_VALUE_PRE_1

df_1=spark.sql(""" SELECT   SAP_ATT_VALUE_ID,
       SAP_ATT_CODE_ID,
       SAP_ATT_VALUE_DESC
FROM   SAP_ATT_VALUE_PRE  EXCEPT
SELECT   SAP_ATT_VALUE_ID,
       SAP_ATT_CODE_ID,
       SAP_ATT_VALUE_DESC
FROM  SAP_ATT_VALUE
""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SAP_ATT_VALUE_PRE_1")

# COMMAND ----------

# DBTITLE 1, SAP_ATT_VALUE

spark.sql("""INSERT INTO SAP_ATT_VALUE SELECT SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC FROM ASQ_Shortcut_to_SAP_ATT_VALUE_PRE_1""")
