# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, T009B_ff_0

df_0=spark.sql("""
    SELECT
        Blank AS Blank,
        Fiscal_Year_Variant AS Fiscal_Year_Variant,
        Year AS Year,
        Calendar_month AS Calendar_month,
        calendar_day AS calendar_day,
        Posing_period AS Posing_period,
        Year_shift AS Year_shift,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T009B_ff""")

df_0.createOrReplaceTempView("T009B_ff_0")

# COMMAND ----------

# DBTITLE 1, SQ_T009B_ff_1

df_1=spark.sql("""
    SELECT
        Blank AS Blank,
        Fiscal_Year_Variant AS Fiscal_Year_Variant,
        Year AS Year,
        Calendar_month AS Calendar_month,
        calendar_day AS calendar_day,
        posing_period AS posing_period,
        year_shift AS year_shift,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        T009B_ff_0""")

df_1.createOrReplaceTempView("SQ_T009B_ff_1")

# COMMAND ----------

# DBTITLE 1, EXPTRANS1_2

df_2=spark.sql("""
    SELECT
        Fiscal_Year_Variant AS Fiscal_Year_Variant,
        Year AS Year,
        to_char(ADD_TO_DATE(current_timestamp,
        'YY',
        $$year),
        'YYYY') AS O_Year,
        Calendar_month AS Calendar_month,
        Calendar_day AS Calendar_day,
        Posting_period AS Posting_period,
        Year_shift AS Year_shift,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_T009B_ff_1""")

df_2.createOrReplaceTempView("EXPTRANS1_2")

# COMMAND ----------

# DBTITLE 1, FILTRANS_3

df_3=spark.sql("""
    SELECT
        Fiscal_Year_Variant AS Fiscal_Year_Variant,
        Year AS Year,
        Calendar_month AS Calendar_month,
        Calendar_day AS Calendar_day,
        Posting_period AS Posting_period,
        Year_shift AS Year_shift,
        MANDT AS MANDT,
        O_Year AS O_Year,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS1_2 
    WHERE
        (
            Fiscal_Year_Variant = 'P1' 
            AND Year = to_char(ADD_TO_DATE(current_timestamp, 'YY', $$year ),'YYYY') 
            and  to_char(Year_shift)='0'
        ) 
        or (
            Fiscal_Year_Variant='P1'  
            AND  Year=to_char(ADD_TO_DATE ( sysdate, 'YY', $$ year + 1), 'YYYY') 
            AND to_char(Year_shift) = '-1'
        ) 
        OR (
            Fiscal_Year_Variant = 'P1' 
            AND Year = to_char(ADD_TO_DATE(current_timestamp, 'YY', $$year+1 ),'YYYY') 
            and  to_char(Year_shift)='0'
        ) 
        or (
            Fiscal_Year_Variant='P1'  
            AND  Year=to_char(ADD_TO_DATE ( sysdate, 'YY', $$ year + 2), 'YYYY') 
            AND to_char(Year_shift) = '-1'
        )""")

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------

# DBTITLE 1, T009B_Pre

spark.sql("""INSERT INTO T009B_Pre SELECT MANDT AS MANDT,
Fiscal_Year_Variant AS PERIV,
Year AS BDATJ,
Calendar_month AS BUMON,
Calendar_day AS BUTAG,
Posting_period AS POPER,
Year_shift AS RELJR FROM FILTRANS_3""")
