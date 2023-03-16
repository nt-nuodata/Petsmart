# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SQL_TRANSFORM_DUMMY_SOURCE_0


df_0=spark.sql("""
    SELECT
        START_TSTMP AS START_TSTMP,
        TABLE_NAME AS TABLE_NAME,
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SQL_TRANSFORM_DUMMY_SOURCE""")

df_0.createOrReplaceTempView("SQL_TRANSFORM_DUMMY_SOURCE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SQL_TRANSFORM_DUMMY_SOURCE_1


df_1=spark.sql("""
    SELECT
        CURRENT_TIMESTAMP AS START_TSTMP,
        'MA_STU_CHG_PRE' AS TABLE_NAME,
        0 AS BEGIN_ROW_CNT""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SQL_TRANSFORM_DUMMY_SOURCE_1")

# COMMAND ----------
# DBTITLE 1, SQL_INS_and_DUPS_CHECK_2


df_2=spark.sql("""
    SELECT
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        START_TSTMP AS START_TSTMP,
        TABLE_NAME AS TABLE_NAME 
    FROM
        SQ_Shortcut_to_SQL_TRANSFORM_DUMMY_SOURCE_1""")

df_2.createOrReplaceTempView("SQL_INS_and_DUPS_CHECK_2")

# COMMAND ----------
# DBTITLE 1, EXP_GET_SESSION_INFO_3


df_3=spark.sql("""
    SELECT
        TO_CHAR(START_TSTMP_output,
        'MM/DD/YYYY HH24:MI:SS') AS START_TSTMP,
        TO_CHAR(current_timestamp,
        'MM/DD/YYYY HH24:MI:SS') AS END_TSTMP,
        $PMWorkflowName AS WORKFLOW_NAME,
        $PMSessionName AS SESSION_NAME,
        $PMMappingName AS MAPPING_NAME,
        TABLE_NAME AS TABLE_NAME,
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        INSERT_ROW_CNT AS INSERT_ROW_CNT,
        DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
        IFF(DUPLICATE_ROW_CNT > 0,
        'There are duplicate records in the table',
        SQLError) AS SQL_TRANSFORM_ERROR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_INS_and_DUPS_CHECK_2""")

df_3.createOrReplaceTempView("EXP_GET_SESSION_INFO_3")

# COMMAND ----------
# DBTITLE 1, AGG_4


df_4=spark.sql("""
    SELECT
        START_TSTMP AS START_TSTMP,
        MAX(i_END_TSTMP) AS END_TSTMP,
        WORKFLOW_NAME AS WORKFLOW_NAME,
        SESSION_NAME AS SESSION_NAME,
        MAPPING_NAME AS MAPPING_NAME,
        TABLE_NAME AS TABLE_NAME,
        TO_CHAR(MAX(i_BEGIN_ROW_CNT)) AS BEGIN_ROW_CNT,
        TO_CHAR(SUM(i_INSERT_ROW_CNT)) AS INSERT_ROW_CNT,
        MAX(i_SQL_TRANSFORM_ERROR) AS SQL_TRANSFORM_ERROR,
        TO_CHAR(SUM(i_DUPLICATE_ROW_CNT)) AS DUPLICATE_ROW_CNT 
    FROM
        EXP_GET_SESSION_INFO_3 
    GROUP BY
        START_TSTMP,
        WORKFLOW_NAME,
        SESSION_NAME,
        MAPPING_NAME,
        TABLE_NAME""")

df_4.createOrReplaceTempView("AGG_4")

# COMMAND ----------
# DBTITLE 1, EXP_CREATE_INS_SQL_5


df_5=spark.sql("""
    SELECT
        START_TSTMP AS START_TSTMP,
        END_TSTMP AS END_TSTMP,
        WORKFLOW_NAME AS WORKFLOW_NAME,
        SESSION_NAME AS SESSION_NAME,
        MAPPING_NAME AS MAPPING_NAME,
        TABLE_NAME AS TABLE_NAME,
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        INSERT_ROW_CNT AS INSERT_ROW_CNT,
        DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
        SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
        'INSERT INTO SQL_TRANSFORM_LOG VALUES (TO_DATE(' || CHR(39) || START_TSTMP || CHR(39) || ',' || CHR(39) || 'MM/DD/YYYY HH24:MI:SS' || CHR(39) || '),TO_DATE(' || CHR(39) || END_TSTMP || CHR(39) || ',' || CHR(39) || 'MM/DD/YYYY HH24:MI:SS' || CHR(39) || '), ' || CHR(39) || WORKFLOW_NAME || CHR(39) || ', ' || CHR(39) || SESSION_NAME || CHR(39) || ', ' || CHR(39) || MAPPING_NAME || CHR(39) || ', ' || CHR(39) || TABLE_NAME || CHR(39) || ', ' || CHR(39) || BEGIN_ROW_CNT || CHR(39) || ', ' || CHR(39) || INSERT_ROW_CNT || CHR(39) || ', ' || CHR(39) || DUPLICATE_ROW_CNT || CHR(39) || ',  ' || CHR(39) || SQL_TRANSFORM_ERROR || CHR(39) || ')' AS INSERT_SQL,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        AGG_4""")

df_5.createOrReplaceTempView("EXP_CREATE_INS_SQL_5")

# COMMAND ----------
# DBTITLE 1, SQL_INS_to_SQL_TRANSFORM_LOG_6


df_6=spark.sql("""
    SELECT
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
        END_TSTMP AS END_TSTMP,
        INSERT_ROW_CNT AS INSERT_ROW_CNT,
        INSERT_SQL AS INSERT_SQL,
        MAPPING_NAME AS MAPPING_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        SESSION_NAME AS SESSION_NAME,
        SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
        START_TSTMP AS START_TSTMP,
        TABLE_NAME AS TABLE_NAME,
        WORKFLOW_NAME AS WORKFLOW_NAME 
    FROM
        EXP_CREATE_INS_SQL_5""")

df_6.createOrReplaceTempView("SQL_INS_to_SQL_TRANSFORM_LOG_6")

# COMMAND ----------
# DBTITLE 1, EXP_ABORT_SESSION_7


df_7=spark.sql("""
    SELECT
        DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
        SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
        IFF((CAST(DUPLICATE_ROW_CNT_output AS DECIMAL (38,
        0))) > 0,
        ABORT('There are duplicates rows in the table'),
        IIF()) AS ABORT_SESSION,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_INS_to_SQL_TRANSFORM_LOG_6""")

df_7.createOrReplaceTempView("EXP_ABORT_SESSION_7")

# COMMAND ----------
# DBTITLE 1, MA_SALES_TRANS_UPC_8


df_8=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        MA_SALES_AMT AS MA_SALES_AMT,
        MA_SALES_QTY AS MA_SALES_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_SALES_TRANS_UPC""")

df_8.createOrReplaceTempView("MA_SALES_TRANS_UPC_8")

# COMMAND ----------
# DBTITLE 1, DAYS_9


df_9=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
        DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
        CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
        CAL_WK AS CAL_WK,
        CAL_WK_NBR AS CAL_WK_NBR,
        CAL_MO AS CAL_MO,
        CAL_MO_NBR AS CAL_MO_NBR,
        CAL_MO_NAME AS CAL_MO_NAME,
        CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
        CAL_QTR AS CAL_QTR,
        CAL_QTR_NBR AS CAL_QTR_NBR,
        CAL_HALF AS CAL_HALF,
        CAL_YR AS CAL_YR,
        FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
        FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_WK_NBR AS FISCAL_WK_NBR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_YR AS FISCAL_YR,
        LYR_WEEK_DT AS LYR_WEEK_DT,
        LWK_WEEK_DT AS LWK_WEEK_DT,
        WEEK_DT AS WEEK_DT,
        EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
        EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
        ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
        ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
        CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
        CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
        CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
        CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
        MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
        MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
        MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
        MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
        PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
        PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DAYS""")

df_9.createOrReplaceTempView("DAYS_9")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_RESTATE_HIST_10


df_10=spark.sql("""
    SELECT
        LOAD_DT AS LOAD_DT,
        MA_EVENT_ID AS MA_EVENT_ID,
        OFFER_ID AS OFFER_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        SOURCE_VENDOR AS SOURCE_VENDOR,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        EM_COMMENT AS EM_COMMENT,
        EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        EM_VENDOR_ID AS EM_VENDOR_ID,
        EM_VENDOR_NAME AS EM_VENDOR_NAME,
        EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_RESTATE_HIST""")

df_10.createOrReplaceTempView("MA_EVENT_RESTATE_HIST_10")

# COMMAND ----------
# DBTITLE 1, SQL_TRANSFORM_DUMMY_TARGET


spark.sql("""INSERT INTO SQL_TRANSFORM_DUMMY_TARGET SELECT DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
ABORT_SESSION AS ABORT_SESSION FROM EXP_ABORT_SESSION_7""")