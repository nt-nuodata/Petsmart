# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTPIM_MAP_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        ARTICLE AS ARTICLE,
        DATAB AS DATAB,
        DATBI AS DATBI,
        MAP_PRICE AS MAP_PRICE,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTPIM_MAP""")

df_0.createOrReplaceTempView("ZTPIM_MAP_0")

# COMMAND ----------

# DBTITLE 1, EXP_LOAD_TSTMP_1

df_1=spark.sql("""SELECT MANDT AS MANDT,
ARTICLE AS ARTICLE,
MAP_PRICE AS MAP_PRICE,
CREATED_BY AS CREATED_BY,
LAST_CHANGED_BY AS LAST_CHANGED_BY,
LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
DATAB AS DATAB,
DATBI AS DATBI,
SYSDATE AS LOAD_TSTMP,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_1.createOrReplaceTempView("EXP_LOAD_TSTMP_1")

# COMMAND ----------

# DBTITLE 1, SAP_ZTPIM_MAP_PRE

spark.sql("""INSERT INTO SAP_ZTPIM_MAP_PRE SELECT MANDT AS MANDT,
ARTICLE AS ARTICLE,
MAP_PRICE AS MAP_PRICE,
CREATED_BY AS CREATED_BY,
LAST_CHANGED_BY AS LAST_CHANGED_BY,
LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
DATAB AS DATAB,
DATBI AS DATBI,
LOAD_TSTMP AS LOAD_TSTMP FROM EXP_LOAD_TSTMP_1""")
