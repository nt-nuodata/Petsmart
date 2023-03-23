# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, T024_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        EKGRP AS EKGRP,
        EKNAM AS EKNAM,
        EKTEL AS EKTEL,
        LDEST AS LDEST,
        TELFX AS TELFX,
        ZZBNAME AS ZZBNAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T024""")

df_0.createOrReplaceTempView("T024_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_T024_1

df_1=spark.sql("""
    SELECT
        DISTINCT EKGRP,
        EKNAM 
    FROM
        SAPPR3.T024 
    WHERE
        MANDT = 100""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_T024_1")

# COMMAND ----------

# DBTITLE 1, FIL_PURCH_GROUP_ID_2

df_2=spark.sql("""
    SELECT
        EKGRP AS EKGRP,
        EKNAM AS EKNAM,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_T024_1 
    WHERE
        IS_NUMBER(EKGRP)""")

df_2.createOrReplaceTempView("FIL_PURCH_GROUP_ID_2")

# COMMAND ----------

# DBTITLE 1, PURCH_GROUP

spark.sql("""INSERT INTO PURCH_GROUP SELECT EKGRP AS PURCH_GROUP_ID,
EKNAM AS PURCH_GROUP_NAME FROM FIL_PURCH_GROUP_ID_2""")
