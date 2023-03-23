# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, BrandManager_0

df_0=spark.sql("""
    SELECT
        BrandManagerId AS BrandManagerId,
        BrandManagerName AS BrandManagerName,
        BrandDirectorId AS BrandDirectorId,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BrandManager""")

df_0.createOrReplaceTempView("BrandManager_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_BrandManager_1

df_1=spark.sql("""
    SELECT
        BrandManagerId AS BrandManagerId,
        BrandManagerName AS BrandManagerName,
        BrandDirectorId AS BrandDirectorId,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        BrandManager_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_BrandManager_1")

# COMMAND ----------

# DBTITLE 1, PB_MANAGER_PRE

spark.sql("""INSERT INTO PB_MANAGER_PRE SELECT BrandManagerId AS PB_MANAGER_ID,
BrandManagerName AS PB_MANAGER_NAME,
BrandDirectorId AS PB_DIRECTOR_ID FROM SQ_Shortcut_to_BrandManager_1""")
