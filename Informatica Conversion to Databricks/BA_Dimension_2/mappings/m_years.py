# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, Months_0

df_0=spark.sql("""
    SELECT
        FiscalMo AS FiscalMo,
        FiscalHalf AS FiscalHalf,
        FiscalMoName AS FiscalMoName,
        FiscalMoNameAbbr AS FiscalMoNameAbbr,
        FiscalMoNbr AS FiscalMoNbr,
        FiscalQtr AS FiscalQtr,
        FiscalQtrNbr AS FiscalQtrNbr,
        FiscalYr AS FiscalYr,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Months""")

df_0.createOrReplaceTempView("Months_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_Months_1

df_1=spark.sql("""
    SELECT
        DISTINCT FiscalYr 
    FROM
        Months""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Months_1")

# COMMAND ----------

# DBTITLE 1, EXPTRANS_2

df_2=spark.sql("""
    SELECT
        FiscalYr AS FiscalYr,
        to_char(ADD_TO_DATE(current_timestamp,
        'YY',
        $$year),
        'YYYY') AS O_FiscalYr,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_Months_1""")

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------

# DBTITLE 1, FILTRANS_3

df_3=spark.sql("""
    SELECT
        FiscalYr AS FiscalYr,
        O_FiscalYr AS O_FiscalYr,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_2 
    WHERE
        FiscalYr = O_FiscalYr""")

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------

# DBTITLE 1, YEARS

spark.sql("""INSERT INTO YEARS SELECT FISCAL_YR AS FISCAL_YR FROM FILTRANS_3""")
