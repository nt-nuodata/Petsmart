# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, DD07V_0


df_0=spark.sql("""
    SELECT
        DOMNAME AS DOMNAME,
        VALPOS AS VALPOS,
        DDLANGUAGE AS DDLANGUAGE,
        DOMVALUE_L AS DOMVALUE_L,
        DOMVALUE_H AS DOMVALUE_H,
        DDTEXT AS DDTEXT,
        DOMVAL_LD AS DOMVAL_LD,
        DOMVAL_HD AS DOMVAL_HD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DD07V""")

df_0.createOrReplaceTempView("DD07V_0")

# COMMAND ----------
# DBTITLE 1, FF_DD07V
