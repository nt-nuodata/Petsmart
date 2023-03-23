# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, BrandDirector_0

df_0=spark.sql("""
    SELECT
        BrandDirectorId AS BrandDirectorId,
        BrandDirectorName AS BrandDirectorName,
        MerchVpId AS MerchVpId,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BrandDirector""")

df_0.createOrReplaceTempView("BrandDirector_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_BrandDirector_1

df_1=spark.sql("""
    SELECT
        BrandDirectorId AS BrandDirectorId,
        BrandDirectorName AS BrandDirectorName,
        MerchVpId AS MerchVpId,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        BrandDirector_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_BrandDirector_1")

# COMMAND ----------

# DBTITLE 1, PB_DIRECTOR_PRE

spark.sql("""INSERT INTO PB_DIRECTOR_PRE SELECT BrandDirectorId AS PB_DIRECTOR_ID,
BrandDirectorName AS PB_DIRECTOR_NAME FROM SQ_Shortcut_to_BrandDirector_1""")
