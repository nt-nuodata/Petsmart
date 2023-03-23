# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ZTB_LABEL_CHGS_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        EFFECTIVE_DATE AS EFFECTIVE_DATE,
        ARTICLE AS ARTICLE,
        SITE AS SITE,
        POG_TYPE AS POG_TYPE,
        LABEL_SIZE AS LABEL_SIZE,
        LABEL_TYPE AS LABEL_TYPE,
        EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
        SUPPRESS_IND AS SUPPRESS_IND,
        NUM_LABELS AS NUM_LABELS,
        CREATE_DATE AS CREATE_DATE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_LABEL_CHGS""")

df_0.createOrReplaceTempView("ZTB_LABEL_CHGS_0")

# COMMAND ----------
# DBTITLE 1, EXP_EFFECTIVE_DATE_1


df_1=spark.sql("""SELECT MANDT AS MANDT,
TO_CHAR(EFFECTIVE_DATE,'YYYYMMDD') AS EFFECTIVE_DATE,
ARTICLE AS ARTICLE,
SITE AS SITE,
POG_TYPE AS POG_TYPE,
LABEL_SIZE AS LABEL_SIZE,
LABEL_TYPE AS LABEL_TYPE,
IIF( NOT ISNULL(EXP_LABEL_TYPE) AND UPPER(EXP_LABEL_TYPE) != 'X',' ',UPPER(EXP_LABEL_TYPE) ) AS EXP_LABEL_TYPE,
SUPPRESS_IND AS SUPPRESS_IND,
NUM_LABELS AS NUM_LABELS,
TO_CHAR(CREATE_DATE,'YYYYMMDD') AS CREATE_DATE,
ENH_LBL_ID AS ENH_LBL_ID,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_1.createOrReplaceTempView("EXP_EFFECTIVE_DATE_1")

# COMMAND ----------
# DBTITLE 1, ZTB_LABEL_CHGS_PRE


spark.sql("""INSERT INTO ZTB_LABEL_CHGS_PRE SELECT MANDT AS MANDT,
EFFECTIVE_DATE AS EFFECTIVE_DATE,
ARTICLE AS ARTICLE,
SITE AS SITE,
POG_TYPE AS POG_TYPE,
LABEL_SIZE AS LABEL_SIZE,
LABEL_TYPE AS LABEL_TYPE,
EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
SUPPRESS_IND AS SUPPRESS_IND,
NUM_LABELS AS NUM_LABELS,
CREATE_DATE AS CREATE_DATE,
ENH_LBL_ID AS ENH_LBL_ID FROM EXP_EFFECTIVE_DATE_1""")