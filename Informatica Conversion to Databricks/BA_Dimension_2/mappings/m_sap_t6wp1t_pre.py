# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, T6WP1T_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        BWVOR AS BWVOR,
        SPRAS AS SPRAS,
        VTEXT AS VTEXT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T6WP1T""")

df_0.createOrReplaceTempView("T6WP1T_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_T6WP1T_1

df_1=spark.sql("""
    SELECT
        BWVOR AS BWVOR,
        VTEXT AS VTEXT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        T6WP1T_0 
    WHERE
        MANDT = '100' 
        AND SPRAS = 'E'""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_T6WP1T_1")

# COMMAND ----------

# DBTITLE 1, SAP_T6WP1T_PRE

spark.sql("""INSERT INTO SAP_T6WP1T_PRE SELECT BWVOR AS PROCUREMENT_RULE_CD,
VTEXT AS PROCUREMENT_RULE_DESC FROM SQ_Shortcut_to_T6WP1T_1""")
