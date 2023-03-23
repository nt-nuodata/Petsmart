# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, C003_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        KAPPL AS KAPPL,
        KSCHL AS KSCHL,
        KTOPL AS KTOPL,
        VKORG AS VKORG,
        KTGRM AS KTGRM,
        KVSL1 AS KVSL1,
        SAKN1 AS SAKN1,
        SAKN2 AS SAKN2,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        C003""")

df_0.createOrReplaceTempView("C003_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_C003_1

df_1=spark.sql("""
    SELECT
        C003.VKORG,
        C003.KTGRM,
        CAST(C003.SAKN1 AS NUMBER (5)) SAKN1 
    FROM
        SAPPR3.C003 
    WHERE
        KTOPL = 'PCOA' 
        AND KVSL1 = 'ERL'""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_C003_1")

# COMMAND ----------

# DBTITLE 1, GL_C003_PRE

spark.sql("""INSERT INTO GL_C003_PRE SELECT VKORG AS SALES_ORG,
KTGRM AS ACCT_ASSIGNMENT_GRP,
SAKN1 AS GL_ACCT_NBR FROM SQ_Shortcut_to_C003_1""")
