# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, DM_PG_VENDOR_0

df_0=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DM_PG_VENDOR""")

df_0.createOrReplaceTempView("DM_PG_VENDOR_0")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_to_DM_PG_VENDOR_1

df_1=spark.sql("""
    SELECT
        DM_PG_VENDOR.VENDOR_ID,
        DM_PG_VENDOR.PURCH_GROUP_ID 
    FROM
        NCAST.DM_PG_VENDOR""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_DM_PG_VENDOR_1")

# COMMAND ----------

# DBTITLE 1, PURCH_GROUP_VENDOR

spark.sql("""INSERT INTO PURCH_GROUP_VENDOR SELECT PURCH_GROUP_ID AS PURCH_GROUP_ID,
VENDOR_ID AS VENDOR_ID FROM ASQ_Shortcut_to_DM_PG_VENDOR_1""")
