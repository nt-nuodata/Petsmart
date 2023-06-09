# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, VENDOR_PO_COND_A044_FLAT_0

df_0=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        COND_TYPE_CD AS COND_TYPE_CD,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        VENDOR_ID AS VENDOR_ID,
        COND_END_DT AS COND_END_DT,
        COND_START_DT AS COND_START_DT,
        COND_REC_NBR AS COND_REC_NBR,
        COND_RATE_AMT AS COND_RATE_AMT,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PO_COND_A044_FLAT""")

df_0.createOrReplaceTempView("VENDOR_PO_COND_A044_FLAT_0")

# COMMAND ----------

# DBTITLE 1, SQ_VENDOR_PO_COND_A044_FILE_1

df_1=spark.sql("""
    SELECT
        COND_TYPE_CD AS COND_TYPE_CD,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        VENDOR_ID AS VENDOR_ID,
        COND_END_DT AS COND_END_DT,
        DELETE_IND AS DELETE_IND,
        COND_START_DT AS COND_START_DT,
        COND_REC_NBR AS COND_REC_NBR,
        COND_RATE_AMT AS COND_RATE_AMT,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PO_COND_A044_FLAT_0""")

df_1.createOrReplaceTempView("SQ_VENDOR_PO_COND_A044_FILE_1")

# COMMAND ----------

# DBTITLE 1, FILTRANS_2

df_2=spark.sql("""
    SELECT
        COND_TYPE_CD AS COND_TYPE_CD,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        VENDOR_ID AS VENDOR_ID,
        COND_END_DT AS COND_END_DT,
        DELETE_IND AS DELETE_IND,
        COND_START_DT AS COND_START_DT,
        COND_REC_NBR AS COND_REC_NBR,
        COND_RATE_AMT AS COND_RATE_AMT,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_VENDOR_PO_COND_A044_FILE_1 
    WHERE
        COND_TYPE_CD <> 'ZCCT'""")

df_2.createOrReplaceTempView("FILTRANS_2")

# COMMAND ----------

# DBTITLE 1, EXP_VENDOR_SUBRANGE_CD_3

df_3=spark.sql("""
    SELECT
        IFF(ISNULL(VENDOR_SUBRANGE_CD),
        ' ',
        VENDOR_SUBRANGE_CD) AS VENDOR_SUBRANGE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FILTRANS_2""")

df_3.createOrReplaceTempView("EXP_VENDOR_SUBRANGE_CD_3")

# COMMAND ----------

# DBTITLE 1, EXPTRANS_4

df_4=spark.sql("""
    SELECT
        (CAST(VENDOR_ID AS DECIMAL (38,
        0))) AS o_VENDOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FILTRANS_2""")

df_4.createOrReplaceTempView("EXPTRANS_4")

# COMMAND ----------

# DBTITLE 1, EXP_COMMON_DATE_TRANS_5

df_5=spark.sql("""
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
        FILTRANS_2""")

df_5.createOrReplaceTempView("EXP_COMMON_DATE_TRANS_5")

# COMMAND ----------

# DBTITLE 1, VENDOR_PO_COND_PRE

spark.sql("""INSERT INTO VENDOR_PO_COND_PRE SELECT PO_COND_CD AS PO_COND_CD,
PURCH_ORG_CD AS PURCH_ORG_CD,
VENDOR_ID AS VENDOR_ID,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
o_YYYYMMDD_W_DEFAULT_TIME AS PO_COND_END_DT,
DELETE_IND AS DELETE_IND,
PO_COND_EFF_DT AS PO_COND_EFF_DT,
PO_COND_REC_NBR AS PO_COND_REC_NBR,
PO_COND_RATE_AMT AS PO_COND_RATE_AMT FROM EXP_COMMON_DATE_TRANS_5""")

spark.sql("""INSERT INTO VENDOR_PO_COND_PRE SELECT PO_COND_CD AS PO_COND_CD,
PURCH_ORG_CD AS PURCH_ORG_CD,
VENDOR_ID AS VENDOR_ID,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
o_YYYYMMDD_W_DEFAULT_TIME AS PO_COND_END_DT,
DELETE_IND AS DELETE_IND,
PO_COND_EFF_DT AS PO_COND_EFF_DT,
PO_COND_REC_NBR AS PO_COND_REC_NBR,
PO_COND_RATE_AMT AS PO_COND_RATE_AMT FROM EXP_COMMON_DATE_TRANS_5""")

spark.sql("""INSERT INTO VENDOR_PO_COND_PRE SELECT PO_COND_CD AS PO_COND_CD,
PURCH_ORG_CD AS PURCH_ORG_CD,
VENDOR_ID AS VENDOR_ID,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
o_YYYYMMDD_W_DEFAULT_TIME AS PO_COND_END_DT,
DELETE_IND AS DELETE_IND,
PO_COND_EFF_DT AS PO_COND_EFF_DT,
PO_COND_REC_NBR AS PO_COND_REC_NBR,
PO_COND_RATE_AMT AS PO_COND_RATE_AMT FROM FILTRANS_2""")

spark.sql("""INSERT INTO VENDOR_PO_COND_PRE SELECT PO_COND_CD AS PO_COND_CD,
PURCH_ORG_CD AS PURCH_ORG_CD,
VENDOR_ID AS VENDOR_ID,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
o_YYYYMMDD_W_DEFAULT_TIME AS PO_COND_END_DT,
DELETE_IND AS DELETE_IND,
PO_COND_EFF_DT AS PO_COND_EFF_DT,
PO_COND_REC_NBR AS PO_COND_REC_NBR,
PO_COND_RATE_AMT AS PO_COND_RATE_AMT FROM EXPTRANS_4""")

spark.sql("""INSERT INTO VENDOR_PO_COND_PRE SELECT PO_COND_CD AS PO_COND_CD,
PURCH_ORG_CD AS PURCH_ORG_CD,
VENDOR_ID AS VENDOR_ID,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
o_YYYYMMDD_W_DEFAULT_TIME AS PO_COND_END_DT,
DELETE_IND AS DELETE_IND,
PO_COND_EFF_DT AS PO_COND_EFF_DT,
PO_COND_REC_NBR AS PO_COND_REC_NBR,
PO_COND_RATE_AMT AS PO_COND_RATE_AMT FROM EXP_VENDOR_SUBRANGE_CD_3""")
