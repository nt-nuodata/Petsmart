# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SALES_TRANS_UPC_OFFER_0


df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        OFFER_ID AS OFFER_ID,
        SCAN_SEQ_NBR AS SCAN_SEQ_NBR,
        UPC_SEQ_NBR AS UPC_SEQ_NBR,
        OFFER_AMT AS OFFER_AMT,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_UPC_OFFER""")

df_0.createOrReplaceTempView("SALES_TRANS_UPC_OFFER_0")

# COMMAND ----------
# DBTITLE 1, TRANS_AGG_PRE_1


df_1=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        TXN_KEY_GID AS TXN_KEY_GID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        TIME_ID AS TIME_ID,
        REGISTER_NBR AS REGISTER_NBR,
        TRANSACTION_NBR AS TRANSACTION_NBR,
        ORDER_NBR AS ORDER_NBR,
        ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
        ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
        DS_ORDER_SEQ_NBR AS DS_ORDER_SEQ_NBR,
        TXN_CNT AS TXN_CNT,
        CUST_TRANS_ID AS CUST_TRANS_ID,
        CASHIER_NBR AS CASHIER_NBR,
        CUST_CAPTURE_TYPE AS CUST_CAPTURE_TYPE,
        COUNTRY_CD AS COUNTRY_CD,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        DS_CHANNEL AS DS_CHANNEL,
        DS_ASSIST_SITE_NBR AS DS_ASSIST_SITE_NBR,
        DS_CURRENCY_CD AS DS_CURRENCY_CD,
        SALES_MID_VOID_REASON_CD AS SALES_MID_VOID_REASON_CD,
        ORDER_CREATION_DEVICE_TYPE AS ORDER_CREATION_DEVICE_TYPE,
        ORDER_CREATION_DEVICE_WIDTH AS ORDER_CREATION_DEVICE_WIDTH,
        TXN_SEGMENT AS TXN_SEGMENT,
        PAYMENT_DEVICE_TYPE AS PAYMENT_DEVICE_TYPE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TRANS_AGG_PRE""")

df_1.createOrReplaceTempView("TRANS_AGG_PRE_1")

# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_CTRL_2


df_2=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRIMARY_SAP_DEPT_ID AS PRIMARY_SAP_DEPT_ID,
        STORE_DEPT_NBR AS STORE_DEPT_NBR,
        START_EFF_DT AS START_EFF_DT,
        END_EFF_DT AS END_EFF_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_CTRL""")

df_2.createOrReplaceTempView("SERVICES_MARGIN_CTRL_2")

# COMMAND ----------
# DBTITLE 1, SALES_TRANS_CUST_3


df_3=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        CUST_ACCT_ID AS CUST_ACCT_ID,
        CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_CUST""")

df_3.createOrReplaceTempView("SALES_TRANS_CUST_3")

# COMMAND ----------
# DBTITLE 1, SALES_TRANS_UPC_4


df_4=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        TRANS_TSTMP AS TRANS_TSTMP,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        PRODUCT_ID AS PRODUCT_ID,
        SERVICE_BULK_SKU_NBR AS SERVICE_BULK_SKU_NBR,
        PET_ID AS PET_ID,
        CUST_TRANS_ID AS CUST_TRANS_ID,
        KEYED_FLAG AS KEYED_FLAG,
        UPC_ADD_FLAG AS UPC_ADD_FLAG,
        NON_TAX_FLAG AS NON_TAX_FLAG,
        SPECIAL_SALES_FLAG AS SPECIAL_SALES_FLAG,
        DROP_SHIP_FLAG AS DROP_SHIP_FLAG,
        REASON_ID AS REASON_ID,
        CASHIER_NBR AS CASHIER_NBR,
        UPC_SEQ_NBR AS UPC_SEQ_NBR,
        UNIT_PRICE_AMT AS UNIT_PRICE_AMT,
        SALES_AMT AS SALES_AMT,
        SALES_COST AS SALES_COST,
        SALES_QTY AS SALES_QTY,
        RETURN_AMT AS RETURN_AMT,
        RETURN_COST AS RETURN_COST,
        RETURN_QTY AS RETURN_QTY,
        CLEARANCE_AMT AS CLEARANCE_AMT,
        CLEARANCE_QTY AS CLEARANCE_QTY,
        CLEARANCE_RETURN_AMT AS CLEARANCE_RETURN_AMT,
        CLEARANCE_RETURN_QTY AS CLEARANCE_RETURN_QTY,
        SPECIAL_SRVC_AMT AS SPECIAL_SRVC_AMT,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        DATE_LOADED AS DATE_LOADED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_UPC""")

df_4.createOrReplaceTempView("SALES_TRANS_UPC_4")

# COMMAND ----------
# DBTITLE 1, SALES_TRANS_SPECIAL_5


df_5=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        CUST_TRANS_ID AS CUST_TRANS_ID,
        PRODUCT_ID AS PRODUCT_ID,
        SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        SPECIAL_SALES_QTY AS SPECIAL_SALES_QTY,
        SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        SPECIAL_RETURN_QTY AS SPECIAL_RETURN_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        DATE_LOADED AS DATE_LOADED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_SPECIAL""")

df_5.createOrReplaceTempView("SALES_TRANS_SPECIAL_5")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_SOURCE_6


df_6=spark.sql("""
    SELECT
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        MA_EVENT_SOURCE_DESC AS MA_EVENT_SOURCE_DESC,
        TARGET_TABLE AS TARGET_TABLE,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_SOURCE""")

df_6.createOrReplaceTempView("MA_EVENT_SOURCE_6")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_TYPE_7


df_7=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_TYPE_DESC AS MA_EVENT_TYPE_DESC,
        REF_MA_EVENT_TYPE_ID AS REF_MA_EVENT_TYPE_ID,
        EM_TPR_TYPE AS EM_TPR_TYPE,
        PETPERK_ONLY_FLAG AS PETPERK_ONLY_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_TYPE""")

df_7.createOrReplaceTempView("MA_EVENT_TYPE_7")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_8


df_8=spark.sql("""
    SELECT
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
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        COMPANY_ID AS COMPANY_ID,
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
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT""")

df_8.createOrReplaceTempView("MA_EVENT_8")

# COMMAND ----------
# DBTITLE 1, STX_UPC_PRE_9


df_9=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        TXN_KEY_GID AS TXN_KEY_GID,
        UPC_ID AS UPC_ID,
        SEQ_NBR AS SEQ_NBR,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        TXN_TYPE_ID AS TXN_TYPE_ID,
        ADOPTION_GROUP_ID AS ADOPTION_GROUP_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        UPC_KEYED_FLAG AS UPC_KEYED_FLAG,
        UPC_NOTONFILE_FLAG AS UPC_NOTONFILE_FLAG,
        ADJ_REASON_ID AS ADJ_REASON_ID,
        ORIG_UPC_TAX_STATUS_ID AS ORIG_UPC_TAX_STATUS_ID,
        REGULATED_ANIMAL_PERMIT_NBR AS REGULATED_ANIMAL_PERMIT_NBR,
        CARE_SHEET_GIVEN_FLAG AS CARE_SHEET_GIVEN_FLAG,
        RETURN_REASON_ID AS RETURN_REASON_ID,
        RETURN_DESC AS RETURN_DESC,
        SPECIAL_ORDER_NBR AS SPECIAL_ORDER_NBR,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        TP_MASTER_INVOICE_NBR AS TP_MASTER_INVOICE_NBR,
        TRAINING_START_DT AS TRAINING_START_DT,
        NON_TAX_FLAG AS NON_TAX_FLAG,
        NON_DISCOUNT_FLAG AS NON_DISCOUNT_FLAG,
        SPECIAL_SALES_FLAG AS SPECIAL_SALES_FLAG,
        UPC_SEQ_NBR AS UPC_SEQ_NBR,
        UNIT_PRICE_AMT AS UNIT_PRICE_AMT,
        SALES_AMT AS SALES_AMT,
        SALES_QTY AS SALES_QTY,
        SALES_COST AS SALES_COST,
        RETURN_AMT AS RETURN_AMT,
        RETURN_QTY AS RETURN_QTY,
        RETURN_COST AS RETURN_COST,
        SERVICE_AMT AS SERVICE_AMT,
        DROP_SHIP_FLAG AS DROP_SHIP_FLAG,
        PET_ID AS PET_ID,
        SERVICE_BULK_SKU_NBR AS SERVICE_BULK_SKU_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STX_UPC_PRE""")

df_9.createOrReplaceTempView("STX_UPC_PRE_9")

# COMMAND ----------
# DBTITLE 1, SALES_TRANS_DISCOUNT_10


df_10=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        OFFER_ID AS OFFER_ID,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        TRANS_TSTMP AS TRANS_TSTMP,
        PRODUCT_ID AS PRODUCT_ID,
        DISC_CPN_UPC_ID AS DISC_CPN_UPC_ID,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        CUST_TRANS_ID AS CUST_TRANS_ID,
        CASHIER_NBR AS CASHIER_NBR,
        UPC_SEQ_NBR AS UPC_SEQ_NBR,
        OFFER_CHARGEBACK_ID AS OFFER_CHARGEBACK_ID,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        DISCOUNT_QTY AS DISCOUNT_QTY,
        DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        DISCOUNT_RETURN_QTY AS DISCOUNT_RETURN_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        DATE_LOADED AS DATE_LOADED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_DISCOUNT""")

df_10.createOrReplaceTempView("SALES_TRANS_DISCOUNT_10")

# COMMAND ----------
# DBTITLE 1, SQL_TRANSFORM_DUMMY_SOURCE_11


df_11=spark.sql("""
    SELECT
        START_TSTMP AS START_TSTMP,
        TABLE_NAME AS TABLE_NAME,
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SQL_TRANSFORM_DUMMY_SOURCE""")

df_11.createOrReplaceTempView("SQL_TRANSFORM_DUMMY_SOURCE_11")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH_12


df_12=spark.sql("""
    SELECT
        CURRENT_TIMESTAMP AS START_TSTMP,
        'MA_SALES_PRE' AS TABLE_NAME,
        COUNT(*) AS BEGIN_ROW_CNT 
    FROM
        MA_SALES_PRE""")

df_12.createOrReplaceTempView("SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH_12")

# COMMAND ----------
# DBTITLE 1, SQL_INS_and_DUPS_CHECK_13


df_13=spark.sql("""
    SELECT
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        START_TSTMP AS START_TSTMP,
        TABLE_NAME AS TABLE_NAME 
    FROM
        SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH_12""")

df_13.createOrReplaceTempView("SQL_INS_and_DUPS_CHECK_13")

# COMMAND ----------
# DBTITLE 1, EXP_GET_SESSION_INFO_14


df_14=spark.sql("""
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
        SQL_INS_and_DUPS_CHECK_13""")

df_14.createOrReplaceTempView("EXP_GET_SESSION_INFO_14")

# COMMAND ----------
# DBTITLE 1, AGG_15


df_15=spark.sql("""
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
        EXP_GET_SESSION_INFO_14 
    GROUP BY
        START_TSTMP,
        WORKFLOW_NAME,
        SESSION_NAME,
        MAPPING_NAME,
        TABLE_NAME""")

df_15.createOrReplaceTempView("AGG_15")

# COMMAND ----------
# DBTITLE 1, EXP_CREATE_INS_SQL_16


df_16=spark.sql("""
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
        AGG_15""")

df_16.createOrReplaceTempView("EXP_CREATE_INS_SQL_16")

# COMMAND ----------
# DBTITLE 1, SQL_INS_to_SQL_TRANSFORM_LOG_17


df_17=spark.sql("""
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
        EXP_CREATE_INS_SQL_16""")

df_17.createOrReplaceTempView("SQL_INS_to_SQL_TRANSFORM_LOG_17")

# COMMAND ----------
# DBTITLE 1, EXP_ABORT_SESSION_18


df_18=spark.sql("""
    SELECT
        DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
        SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
        IFF((CAST(DUPLICATE_ROW_CNT_output AS DECIMAL (38,
        0))) > 0,
        ABORT('There are duplicates rows in the table'),
        IIF()) AS ABORT_SESSION,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_INS_to_SQL_TRANSFORM_LOG_17""")

df_18.createOrReplaceTempView("EXP_ABORT_SESSION_18")

# COMMAND ----------
# DBTITLE 1, CURRENCY_DAY_19


df_19=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        CURRENCY_ID AS CURRENCY_ID,
        DATE_RATE_START AS DATE_RATE_START,
        CURRENCY_TYPE AS CURRENCY_TYPE,
        DATE_RATE_ENDED AS DATE_RATE_ENDED,
        EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        RATIO_TO AS RATIO_TO,
        RATIO_FROM AS RATIO_FROM,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        CURRENCY_NBR AS CURRENCY_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CURRENCY_DAY""")

df_19.createOrReplaceTempView("CURRENCY_DAY_19")

# COMMAND ----------
# DBTITLE 1, SALES_TRANS_TXN_20


df_20=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        TXN_WAS_POST_VOIDED_FLAG AS TXN_WAS_POST_VOIDED_FLAG,
        SALES_MID_VOID_REASON_CD AS SALES_MID_VOID_REASON_CD,
        CUST_TRANS_ID AS CUST_TRANS_ID,
        TRANS_TSTMP AS TRANS_TSTMP,
        TXN_END_TSTMP AS TXN_END_TSTMP,
        REGISTER_NBR AS REGISTER_NBR,
        TRANSACTION_NBR AS TRANSACTION_NBR,
        TXN_CONTROL_ID AS TXN_CONTROL_ID,
        ORDER_NBR AS ORDER_NBR,
        ORDER_SEQ_NBR AS ORDER_SEQ_NBR,
        ORDER_CHANNEL AS ORDER_CHANNEL,
        ORDER_ASSIST_LOCATION_ID AS ORDER_ASSIST_LOCATION_ID,
        ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
        ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
        ORDER_CREATION_DEVICE_TYPE AS ORDER_CREATION_DEVICE_TYPE,
        ORDER_CREATION_DEVICE_WIDTH AS ORDER_CREATION_DEVICE_WIDTH,
        TXN_SEGMENT AS TXN_SEGMENT,
        PAYMENT_DEVICE_TYPE AS PAYMENT_DEVICE_TYPE,
        BP_SOURCE_CD AS BP_SOURCE_CD,
        TRANS_FLAG AS TRANS_FLAG,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        ZIP_CODE AS ZIP_CODE,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        CASHIER_NBR AS CASHIER_NBR,
        MANAGER_NBR AS MANAGER_NBR,
        TAX_EXEMPT_ID AS TAX_EXEMPT_ID,
        COMM_TILL_FLG AS COMM_TILL_FLG,
        SPECIAL_ORD_NBR AS SPECIAL_ORD_NBR,
        PETPERK_OVERRIDE_NBR AS PETPERK_OVERRIDE_NBR,
        PETPERK_EMAIL_IND AS PETPERK_EMAIL_IND,
        PETPERK_FIRST_NAME_IND AS PETPERK_FIRST_NAME_IND,
        PETPERK_LAST_NAME_IND AS PETPERK_LAST_NAME_IND,
        PETPERK_PHONE_NBR_IND AS PETPERK_PHONE_NBR_IND,
        LOYALTY_NBR AS LOYALTY_NBR,
        LOYALTY_REDEMPTION_ID AS LOYALTY_REDEMPTION_ID,
        LUID AS LUID,
        POINTS_REDEEMED AS POINTS_REDEEMED,
        BASE_POINTS_EARNED AS BASE_POINTS_EARNED,
        BONUS_POINTS_EARNED AS BONUS_POINTS_EARNED,
        POINT_BALANCE AS POINT_BALANCE,
        POINTS_DEDUCTED AS POINTS_DEDUCTED,
        CDC_DCOL_RAW_TXT AS CDC_DCOL_RAW_TXT,
        CDC_EMAIL_ID AS CDC_EMAIL_ID,
        CDC_FIRST_NAME_ID AS CDC_FIRST_NAME_ID,
        CDC_LAST_NAME_ID AS CDC_LAST_NAME_ID,
        CDC_PHONE_NBR_ID AS CDC_PHONE_NBR_ID,
        PHONE_TYPE AS PHONE_TYPE,
        OPT_OUT_EMAIL_FLAG AS OPT_OUT_EMAIL_FLAG,
        OPT_OUT_TEXT_FLAG AS OPT_OUT_TEXT_FLAG,
        DIGITAL_RECEIPT_ANSWER_CD AS DIGITAL_RECEIPT_ANSWER_CD,
        OFFLINE_CUST_LKP_IND AS OFFLINE_CUST_LKP_IND,
        POS_OFFLINE_REASON_ID AS POS_OFFLINE_REASON_ID,
        SALES_AMT AS SALES_AMT,
        SALES_COST AS SALES_COST,
        SALES_QTY AS SALES_QTY,
        RETURN_AMT AS RETURN_AMT,
        RETURN_COST AS RETURN_COST,
        RETURN_QTY AS RETURN_QTY,
        SPECIAL_SRVC_AMT AS SPECIAL_SRVC_AMT,
        SPECIAL_SRVC_QTY AS SPECIAL_SRVC_QTY,
        NET_COUPON_AMT AS NET_COUPON_AMT,
        NET_COUPON_QTY AS NET_COUPON_QTY,
        NET_SALES_AMT AS NET_SALES_AMT,
        NET_SALES_COST AS NET_SALES_COST,
        NET_SALES_QTY AS NET_SALES_QTY,
        NET_DISC_AMT AS NET_DISC_AMT,
        NET_DISC_QTY AS NET_DISC_QTY,
        NET_MERCH_DISC_AMT AS NET_MERCH_DISC_AMT,
        NET_MERCH_DISC_QTY AS NET_MERCH_DISC_QTY,
        NET_SPECIAL_SALES_AMT AS NET_SPECIAL_SALES_AMT,
        NET_SPECIAL_SALES_QTY AS NET_SPECIAL_SALES_QTY,
        NET_SALES_TAX_AMT AS NET_SALES_TAX_AMT,
        NET_PAYMENT_AMT AS NET_PAYMENT_AMT,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        DATE_LOADED AS DATE_LOADED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_TXN""")

df_20.createOrReplaceTempView("SALES_TRANS_TXN_20")

# COMMAND ----------
# DBTITLE 1, SUPPLY_CHAIN_21


df_21=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        DIRECT_VENDOR_ID AS DIRECT_VENDOR_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SUPPLY_CHAIN""")

df_21.createOrReplaceTempView("SUPPLY_CHAIN_21")

# COMMAND ----------
# DBTITLE 1, CR_CUST_ACCT_TYPE_22


df_22=spark.sql("""
    SELECT
        CUST_ACCT_TYPE_CD AS CUST_ACCT_TYPE_CD,
        CUST_ACCT_TYPE_DESC AS CUST_ACCT_TYPE_DESC,
        CUST_ACCT_RANK AS CUST_ACCT_RANK,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CR_CUST_ACCT_TYPE""")

df_22.createOrReplaceTempView("CR_CUST_ACCT_TYPE_22")

# COMMAND ----------
# DBTITLE 1, SALES_CUST_CAPTURE_23


df_23=spark.sql("""
    SELECT
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        SALES_CUST_CAPTURE_TX AS SALES_CUST_CAPTURE_TX,
        LOYALTY_CAPTURE_FLAG AS LOYALTY_CAPTURE_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_CUST_CAPTURE""")

df_23.createOrReplaceTempView("SALES_CUST_CAPTURE_23")

# COMMAND ----------
# DBTITLE 1, SALES_TRANS_CLEARANCE_24


df_24=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        UPC_ID AS UPC_ID,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        POS_TXN_SEQ_NBR AS POS_TXN_SEQ_NBR,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        PRODUCT_ID AS PRODUCT_ID,
        ORIGINAL_UNIT_PRICE_AMT AS ORIGINAL_UNIT_PRICE_AMT,
        UNIT_PRICE_AMT AS UNIT_PRICE_AMT,
        CLEARANCE_METHOD_ID AS CLEARANCE_METHOD_ID,
        CLEARANCE_TYPE_ID AS CLEARANCE_TYPE_ID,
        CLEARANCE_AMT AS CLEARANCE_AMT,
        CLEARANCE_QTY AS CLEARANCE_QTY,
        CLEARANCE_RETURN_AMT AS CLEARANCE_RETURN_AMT,
        CLEARANCE_RETURN_QTY AS CLEARANCE_RETURN_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_CLEARANCE""")

df_24.createOrReplaceTempView("SALES_TRANS_CLEARANCE_24")

# COMMAND ----------
# DBTITLE 1, UPC_25


df_25=spark.sql("""
    SELECT
        UPC_ID AS UPC_ID,
        UPC_CD AS UPC_CD,
        UPC_ADD_DT AS UPC_ADD_DT,
        UPC_DELETE_DT AS UPC_DELETE_DT,
        UPC_REFRESH_DT AS UPC_REFRESH_DT,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        UPC""")

df_25.createOrReplaceTempView("UPC_25")

# COMMAND ----------
# DBTITLE 1, DAYS_26


df_26=spark.sql("""
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

df_26.createOrReplaceTempView("DAYS_26")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_27


df_27=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_TYPE AS SKU_TYPE,
        PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
        STATUS_ID AS STATUS_ID,
        SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
        SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
        SKU_DESC AS SKU_DESC,
        ALT_DESC AS ALT_DESC,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        COUNTRY_CD AS COUNTRY_CD,
        IMPORT_FLAG AS IMPORT_FLAG,
        HTS_CODE_ID AS HTS_CODE_ID,
        CONTENTS AS CONTENTS,
        CONTENTS_UNITS AS CONTENTS_UNITS,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
        SIZE_DESC AS SIZE_DESC,
        BUM_QTY AS BUM_QTY,
        UOM_CD AS UOM_CD,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        BUYER_ID AS BUYER_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_COST_AMT AS PURCH_COST_AMT,
        NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        BRAND_CD AS BRAND_CD,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        STATELINE_FLAG AS STATELINE_FLAG,
        SIGN_TYPE_CD AS SIGN_TYPE_CD,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        INIT_MKDN_DT AS INIT_MKDN_DT,
        DISC_START_DT AS DISC_START_DT,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        FIRST_INV_DT AS FIRST_INV_DT,
        LAST_INV_DT AS LAST_INV_DT,
        LOAD_DT AS LOAD_DT,
        BASE_NBR AS BASE_NBR,
        BP_COLOR_ID AS BP_COLOR_ID,
        BP_SIZE_ID AS BP_SIZE_ID,
        BP_BREED_ID AS BP_BREED_ID,
        BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
        BP_AEROSOL_FLAG AS BP_AEROSOL_FLAG,
        BP_HAZMAT_FLAG AS BP_HAZMAT_FLAG,
        CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
        NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
        NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
        RTV_DEPT_CD AS RTV_DEPT_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
        COMPONENT_FLAG AS COMPONENT_FLAG,
        ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
        ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
        ZDISCO_PID_DT AS ZDISCO_PID_DT,
        ZDISCO_START_DT AS ZDISCO_START_DT,
        ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
        ZDISCO_DC_DT AS ZDISCO_DC_DT,
        ZDISCO_STR_DT AS ZDISCO_STR_DT,
        ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
        ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE""")

df_27.createOrReplaceTempView("SKU_PROFILE_27")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE_28


df_28=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        STORE_NAME AS STORE_NAME,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        COMPANY_ID AS COMPANY_ID,
        REGION_ID AS REGION_ID,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        STATE_CD AS STATE_CD,
        COUNTRY_CD AS COUNTRY_CD,
        POSTAL_CD AS POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
        SITE_SALES_FLAG AS SITE_SALES_FLAG,
        EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
        EQUINE_SITE_ID AS EQUINE_SITE_ID,
        EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
        GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        PROMO_LABEL_CD AS PROMO_LABEL_CD,
        PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
        LOCATION_NBR AS LOCATION_NBR,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
        PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
        SITE_LOGIN_ID AS SITE_LOGIN_ID,
        SITE_MANAGER_ID AS SITE_MANAGER_ID,
        SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
        HOTEL_FLAG AS HOTEL_FLAG,
        DAYCAMP_FLAG AS DAYCAMP_FLAG,
        VET_FLAG AS VET_FLAG,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
        REGION_VP_NAME AS REGION_VP_NAME,
        REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
        ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
        SITE_COUNTY AS SITE_COUNTY,
        SITE_FAX_NO AS SITE_FAX_NO,
        SFT_OPEN_DT AS SFT_OPEN_DT,
        DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
        DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
        RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
        TRADE_AREA AS TRADE_AREA,
        FDLPS_NAME AS FDLPS_NAME,
        FDLPS_EMAIL AS FDLPS_EMAIL,
        OVERSITE_MGR_NAME AS OVERSITE_MGR_NAME,
        OVERSITE_MGR_EMAIL AS OVERSITE_MGR_EMAIL,
        SAFETY_DIRECTOR_NAME AS SAFETY_DIRECTOR_NAME,
        SAFETY_DIRECTOR_EMAIL AS SAFETY_DIRECTOR_EMAIL,
        RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
        RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
        AREA_DIRECTOR_NAME AS AREA_DIRECTOR_NAME,
        AREA_DIRECTOR_EMAIL AS AREA_DIRECTOR_EMAIL,
        DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
        DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
        ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
        ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
        ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
        ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
        REGIONAL_DC_SAFETY_MGR_NAME AS REGIONAL_DC_SAFETY_MGR_NAME,
        REGIONAL_DC_SAFETY_MGR_EMAIL AS REGIONAL_DC_SAFETY_MGR_EMAIL,
        DC_PEOPLE_SUPERVISOR_NAME AS DC_PEOPLE_SUPERVISOR_NAME,
        DC_PEOPLE_SUPERVISOR_EMAIL AS DC_PEOPLE_SUPERVISOR_EMAIL,
        PEOPLE_MANAGER_NAME AS PEOPLE_MANAGER_NAME,
        PEOPLE_MANAGER_EMAIL AS PEOPLE_MANAGER_EMAIL,
        ASSET_PROT_DIR_NAME AS ASSET_PROT_DIR_NAME,
        ASSET_PROT_DIR_EMAIL AS ASSET_PROT_DIR_EMAIL,
        SR_REG_ASSET_PROT_MGR_NAME AS SR_REG_ASSET_PROT_MGR_NAME,
        SR_REG_ASSET_PROT_MGR_EMAIL AS SR_REG_ASSET_PROT_MGR_EMAIL,
        REG_ASSET_PROT_MGR_NAME AS REG_ASSET_PROT_MGR_NAME,
        REG_ASSET_PROT_MGR_EMAIL AS REG_ASSET_PROT_MGR_EMAIL,
        ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
        TP_START_DT AS TP_START_DT,
        OPEN_DT AS OPEN_DT,
        GR_OPEN_DT AS GR_OPEN_DT,
        CLOSE_DT AS CLOSE_DT,
        HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE""")

df_28.createOrReplaceTempView("SITE_PROFILE_28")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_RESTATE_HIST_29


df_29=spark.sql("""
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

df_29.createOrReplaceTempView("MA_EVENT_RESTATE_HIST_29")

# COMMAND ----------
# DBTITLE 1, SQL_TRANSFORM_DUMMY_TARGET


spark.sql("""INSERT INTO SQL_TRANSFORM_DUMMY_TARGET SELECT DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
ABORT_SESSION AS ABORT_SESSION FROM EXP_ABORT_SESSION_18""")