# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ARTICLE_VENDOR_0


df_0=spark.sql("""
    SELECT
        DELETE_FLAG AS DELETE_FLAG,
        ARTICLE_ID AS ARTICLE_ID,
        VENDOR_ID AS VENDOR_ID,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        COUNTRY_CD AS COUNTRY_CD,
        DELIV_EFF_DT AS DELIV_EFF_DT,
        DELIV_END_DT AS DELIV_END_DT,
        REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
        ROUNDING_PROFILE_CD AS ROUNDING_PROFILE_CD,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ARTICLE_VENDOR""")

df_0.createOrReplaceTempView("ARTICLE_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_ARTICLE_VENDOR_1


df_1=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        VENDOR_ID AS VENDOR_ID,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        COUNTRY_CD AS COUNTRY_CD,
        DELIV_EFF_DT AS DELIV_EFF_DT,
        DELIV_END_DT AS DELIV_END_DT,
        REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
        ROUNDING_PROFILE AS ROUNDING_PROFILE,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        DELETE_FLAG AS DELETE_FLAG,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ARTICLE_VENDOR_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_ARTICLE_VENDOR_1")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_VENDOR_TRANS_2


df_2=spark.sql("""
    SELECT
        IFF(SUBSTR(VENDOR_ID,
        1,
        1) = 'V',
        TO_FLOAT(SUBSTR(VENDOR_ID,
        2,
        4)) + 900000,
        TO_FLOAT(VENDOR_ID)) AS VENDOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_ARTICLE_VENDOR_1""")

df_2.createOrReplaceTempView("EXP_COMMON_VENDOR_TRANS_2")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_DATE_TRANS_3


df_3=spark.sql("""
    SELECT
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE(DELETE_DT,
        'MMDDYYYY')) AS o_MMDDYYYY_W_DEFAULT_TIME,
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE(DELETE_DT,
        'YYYYMMDD')) AS o_YYYYMMDD_W_DEFAULT_TIME,
        TO_DATE(('9999-12-31.' || i_TIME_ONLY),
        'YYYY-MM-DD.HH24MISS') AS o_TIME_W_DEFAULT_DATE,
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE((DELETE_DT || '.' || i_TIME_ONLY),
        'MMDDYYYY.HH24:MI:SS')) AS o_MMDDYYYY_W_TIME,
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE((DELETE_DT || '.' || i_TIME_ONLY),
        'YYYYMMDD.HH24:MI:SS')) AS o_YYYYMMDD_W_TIME,
        date_trunc('DAY',
        SESSSTARTTIME) AS o_CURRENT_DATE,
        ADD_TO_DATE(v_CURRENT_DATE,
        'DD',
        -1) AS o_CURRENT_DATE_MINUS1,
        TO_DATE('0001-01-01',
        'YYYY-MM-DD') AS o_DEFAULT_EFF_DATE,
        TO_DATE('9999-12-31',
        'YYYY-MM-DD') AS o_DEFAULT_END_DATE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_ARTICLE_VENDOR_1""")

df_3.createOrReplaceTempView("EXP_COMMON_DATE_TRANS_3")

# COMMAND ----------
# DBTITLE 1, AGGTRANS_4


df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        VENDOR_ID AS VENDOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        COUNTRY_CD AS COUNTRY_CD,
        MAX(i_From_Date) AS o_From_DT,
        o_To_Date AS o_To_Date,
        IFF(isnull(in_REGULAR_VENDOR_CD),
        ' ',
        in_REGULAR_VENDOR_CD) AS out_REGULAR_VENDOR_CD,
        ROUNDING_PROFILE AS ROUNDING_PROFILE,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD 
    FROM
        SQ_Shortcut_To_ARTICLE_VENDOR_1 
    GROUP BY
        ARTICLE_ID,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        VENDOR_ID AS VENDOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        COUNTRY_CD AS COUNTRY_CD,
        MAX(i_From_Date) AS o_From_DT,
        o_To_Date AS o_To_Date,
        IFF(isnull(in_REGULAR_VENDOR_CD),
        ' ',
        in_REGULAR_VENDOR_CD) AS out_REGULAR_VENDOR_CD,
        ROUNDING_PROFILE AS ROUNDING_PROFILE,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD 
    FROM
        EXP_COMMON_DATE_TRANS_3 
    GROUP BY
        ARTICLE_ID,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        VENDOR_ID AS VENDOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        COUNTRY_CD AS COUNTRY_CD,
        MAX(i_From_Date) AS o_From_DT,
        o_To_Date AS o_To_Date,
        IFF(isnull(in_REGULAR_VENDOR_CD),
        ' ',
        in_REGULAR_VENDOR_CD) AS out_REGULAR_VENDOR_CD,
        ROUNDING_PROFILE AS ROUNDING_PROFILE,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD 
    FROM
        EXP_COMMON_VENDOR_TRANS_2 
    GROUP BY
        ARTICLE_ID,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        VENDOR_ID AS VENDOR_ID,
        DELETE_FLAG AS DELETE_FLAG,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        COUNTRY_CD AS COUNTRY_CD,
        MAX(i_From_Date) AS o_From_DT,
        o_To_Date AS o_To_Date,
        IFF(isnull(in_REGULAR_VENDOR_CD),
        ' ',
        in_REGULAR_VENDOR_CD) AS out_REGULAR_VENDOR_CD,
        ROUNDING_PROFILE AS ROUNDING_PROFILE,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD 
    FROM
        EXP_COMMON_DATE_TRANS_3 
    GROUP BY
        ARTICLE_ID,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

# COMMAND ----------
# DBTITLE 1, SKU_VENDOR_PRE


spark.sql("""INSERT INTO SKU_VENDOR_PRE SELECT ARTICLE_ID AS SKU_NBR,
VENDOR_ID AS VENDOR_ID,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
DELETE_FLAG AS DELETE_IND,
UNIT_NUMERATOR AS UNIT_NUMERATOR,
UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
o_From_DT AS DELIV_EFF_DT,
o_To_Date AS DELIV_END_DT,
out_REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
COUNTRY_CD AS COUNTRY_CD,
ROUNDING_PROFILE AS ROUNDING_PROFILE_CD,
VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR FROM AGGTRANS_4""")