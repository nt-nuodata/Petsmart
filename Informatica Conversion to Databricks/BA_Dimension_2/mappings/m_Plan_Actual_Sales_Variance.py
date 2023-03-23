# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, PLAN_STORE_DAY_0

df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        PLAN_VRSN_NM AS PLAN_VRSN_NM,
        PROFIT_CTR_NM AS PROFIT_CTR_NM,
        PROFIT_CTR_SUB_ID AS PROFIT_CTR_SUB_ID,
        CURRENCY_TYPE_ID AS CURRENCY_TYPE_ID,
        CURRENCY_TYPE_DESC AS CURRENCY_TYPE_DESC,
        WEEK_DT AS WEEK_DT,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_YR AS FISCAL_YR,
        STORE_NBR AS STORE_NBR,
        COUNTRY_CD AS COUNTRY_CD,
        PROFIT_CTR_SUB_NM AS PROFIT_CTR_SUB_NM,
        PLAN_VRSN_DT AS PLAN_VRSN_DT,
        PLAN_SALES_AMT AS PLAN_SALES_AMT,
        PLAN_MARGIN_AMT AS PLAN_MARGIN_AMT,
        PLAN_DISCOUNT_AMT AS PLAN_DISCOUNT_AMT,
        PLAN_ITEM_RMVL_AMT AS PLAN_ITEM_RMVL_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PLAN_STORE_DAY""")

df_0.createOrReplaceTempView("PLAN_STORE_DAY_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PLAN_STORE_DAY_1

df_1=spark.sql("""
    SELECT
        pa11.DAY_DT,
        pa11.PLAN_SALES,
        pa12.Actual_Sales,
        (((pa12.Actual_Sales - pa11.PLAN_SALES) / pa11.PLAN_SALES) * 100) PLAN_VARIANCE 
    FROM
        (SELECT
            a11.DAY_DT,
            sum(a11.PLAN_SALES_AMT) PLAN_SALES 
        FROM
            PLAN_STORE_DAY a11 
        JOIN
            SITE_PROFILE_RPT a12 
                ON (
                    a11.LOCATION_ID = a12.LOCATION_ID
                ) 
        WHERE
            a11.DAY_DT = (
                current_date - 1
            ) 
            AND a11.CURRENCY_TYPE_ID = 1 
            AND upper(a11.PLAN_VRSN_NM) IN (
                'CURRENT', 'PLAN', 'F1'
            ) 
            AND rtrim(a11.PROFIT_CTR_NM) IN (
                'Store Ops'
            ) 
            AND a12.STORE_OPEN_CLOSE_FLAG NOT IN (
                'C'
            ) 
        GROUP BY
            a11.DAY_DT) pa11 FULL 
    OUTER JOIN
        (
            SELECT
                a13.DAY_DT,
                sum((a13.NET_SALES_AMT * a13.EXCH_RATE_PCT)) ACTUAL_SALES 
            FROM
                SALES_DAY_STORE_RPT a13 
            JOIN
                SITE_PROFILE_RPT a14 
                    ON (
                        a13.LOCATION_ID = a14.LOCATION_ID
                    ) 
            WHERE
                a13.DAY_DT = (
                    current_date - 1
                ) 
                AND a14.STORE_OPEN_CLOSE_FLAG NOT IN (
                    'C'
                ) 
                AND a14.LOCATION_TYPE_ID IN (
                    6, 8, 15
                ) 
            GROUP BY
                a13.DAY_DT
        ) pa12 
            ON pa11.DAY_DT = pa12.DAY_DT""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PLAN_STORE_DAY_1")

# COMMAND ----------

# DBTITLE 1, EXP_BATCH_DATE_2

df_2=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        current_timestamp AS BATCH_DATE,
        PLAN_SALES_AMT AS PLAN_SALES_AMT,
        ACTUAL_SALES_AMT AS ACTUAL_SALES_AMT,
        PLAN_VARIANCE AS PLAN_VARIANCE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PLAN_STORE_DAY_1""")

df_2.createOrReplaceTempView("EXP_BATCH_DATE_2")

# COMMAND ----------

# DBTITLE 1, UPDTRANS_3

df_3=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BATCH_DATE AS BATCH_DATE,
        PLAN_SALES_AMT AS PLAN_SALES_AMT,
        ACTUAL_SALES_AMT AS ACTUAL_SALES_AMT,
        PLAN_VARIANCE AS PLAN_VARIANCE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_BATCH_DATE_2""")

df_3.createOrReplaceTempView("UPDTRANS_3")

# COMMAND ----------

# DBTITLE 1, BATCH_LOAD_AUD_LOG

spark.sql("""INSERT INTO BATCH_LOAD_AUD_LOG SELECT DAY_DT AS DAY_DT,
BATCH_DATE AS BATCH_DATE,
AOS AS AOS,
ISPU AS ISPU,
SFS AS SFS,
STR AS STR,
WEB AS WEB,
STX_COUNT AS STX_COUNT,
EDW_COUNT AS EDW_COUNT,
EDW_SALES AS EDW_SALES,
STX_SALES AS STX_SALES,
PLAN_SALES_AMT AS PLAN_SALES,
ACTUAL_SALES_AMT AS ACTUAL_SALES,
SALES_VARIANCE AS SALES_VARIANCE,
PLAN_VARIANCE AS PLAN_VARIANCE FROM UPDTRANS_3""")
