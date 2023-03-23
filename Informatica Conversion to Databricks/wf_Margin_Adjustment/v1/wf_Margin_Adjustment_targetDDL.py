# Databricks notebook source
# COMMAND ----------
CREATE TABLE IF NOT EXISTS DELTA_TRAINING.SQL_TRANSFORM_DUMMY_TARGET(DUPLICATE_ROW_CNT STRING,
SQL_TRANSFORM_ERROR STRING,
ABORT_SESSION STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_EVENT(MA_EVENT_ID INT,
OFFER_ID BIGINT,
SAP_DEPT_ID INT,
PRODUCT_ID INT,
COUNTRY_CD STRING,
START_DT DATE,
END_DT DATE,
MA_EVENT_TYPE_ID INT,
MA_EVENT_SOURCE_ID INT,
LOCATION_ID INT,
MOVEMENT_ID INT,
VALUATION_CLASS_CD STRING,
GL_ACCT_NBR INT,
LOCATION_TYPE_ID INT,
ROYALTY_BRAND_ID INT,
BRAND_CD STRING,
MA_FORMULA_CD STRING,
FISCAL_MO INT,
SAP_CATEGORY_ID INT,
FROM_LOCATION_ID INT,
SOURCE_VENDOR_ID BIGINT,
COMPANY_ID INT,
MA_EVENT_DESC STRING,
EM_VENDOR_FUNDING_ID BIGINT,
EM_COMMENT STRING,
EM_BILL_ALT_VENDOR_FLAG STRING,
EM_ALT_VENDOR_ID STRING,
EM_ALT_VENDOR_NAME STRING,
EM_ALT_VENDOR_COUNTRY_CD STRING,
EM_VENDOR_ID BIGINT,
EM_VENDOR_NAME STRING,
EM_VENDOR_COUNTRY_CD STRING,
VENDOR_NAME_TXT STRING,
MA_PCT_IND INT,
MA_AMT INT,
MA_MAX_AMT INT,
UPDATE_DT DATE,
LOAD_DT DATE) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_EVENT_RESTATE_HIST(LOAD_DT DATE,
MA_EVENT_ID INT,
OFFER_ID INT,
SAP_DEPT_ID INT,
PRODUCT_ID INT,
COUNTRY_CD STRING,
START_DT DATE,
END_DT DATE,
MA_EVENT_TYPE_ID INT,
MA_EVENT_SOURCE_ID INT,
LOCATION_ID INT,
MOVEMENT_ID INT,
VALUATION_CLASS_CD STRING,
GL_ACCT_NBR INT,
LOCATION_TYPE_ID INT,
ROYALTY_BRAND_ID INT,
BRAND_CD STRING,
MA_FORMULA_CD STRING,
FISCAL_MO INT,
SAP_CATEGORY_ID INT,
FROM_LOCATION_ID INT,
SOURCE_VENDOR_ID BIGINT,
COMPANY_ID INT,
MA_EVENT_DESC STRING,
EM_VENDOR_FUNDING_ID BIGINT,
EM_COMMENT STRING,
EM_BILL_ALT_VENDOR_FLAG STRING,
EM_ALT_VENDOR_ID STRING,
EM_ALT_VENDOR_NAME STRING,
EM_ALT_VENDOR_COUNTRY_CD STRING,
EM_VENDOR_ID BIGINT,
EM_VENDOR_NAME STRING,
EM_VENDOR_COUNTRY_CD STRING,
VENDOR_NAME_TXT STRING,
MA_PCT_IND INT,
MA_AMT INT,
MA_MAX_AMT INT,
INS_UPD_DEL_FLAG STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.SERVICES_MARGIN_RATE_PRE(WEEK_DT DATE,
LOCATION_ID INT,
SAP_DEPT_ID INT,
MARGIN_RATE INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_DC_COST_ADJ_CTRL(FISCAL_MO INT,
MA_EVENT_TYPE_ID INT,
EST_DC_COST_USD INT,
ACT_DC_COST_USD INT,
DC_COST_ADJ_PCT INT,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_OB_FREIGHT_CTRL(FROM_LOCATION_ID INT,
FISCAL_MO INT,
R12_NET_SALES_COST INT,
R12_OB_FREIGHT_COST INT,
R12_OB_FREIGHT_PCT INT,
ACT_NET_SALES_COST INT,
ACT_OB_FREIGHT_COST INT,
ACT_OB_FREIGHT_PCT INT,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_SALES_PRE(DAY_DT TIMESTAMP,
LOCATION_ID INT,
SALES_INSTANCE_ID BIGINT,
UPC_ID BIGINT,
TP_INVOICE_NBR BIGINT,
PARENT_UPC_ID BIGINT,
COMBO_TYPE_CD STRING,
POS_TXN_SEQ_NBR INT,
MA_EVENT_ID INT,
PRODUCT_ID INT,
SALES_CUST_CAPTURE_CD STRING,
MA_SALES_AMT INT,
MA_SALES_QTY BIGINT,
EXCH_RATE_PCT INT,
RESTATE_FLAG INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.USR_MA_DC_COST_CTRL(MA_EVENT_TYPE_ID INT,
SAP_CATEGORY_ID INT,
LOCATION_TYPE_ID INT,
COMPANY_ID INT,
FISCAL_MO INT,
DC_COST_PCT INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_EVENT_AMS_PRE(MA_EVENT_ID INT,
OFFER_ID BIGINT,
START_DT DATE,
END_DT DATE,
MA_EVENT_TYPE_ID INT,
MA_EVENT_SOURCE_ID INT,
MA_EVENT_DESC STRING,
MA_PCT_IND INT,
MA_AMT INT,
MA_MAX_AMT INT,
OFFER_AS_DISC_IND INT,
VENDOR_FUNDED_IND INT,
VENDOR_NAME_TXT STRING,
INSERT_FLAG INT,
DELETE_FLAG INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_FISCAL_MO_CTRL(FISCAL_MO INT,
RESTATE_DT DATE,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_EVENT_PRE(MA_EVENT_ID INT,
PRODUCT_ID INT,
COUNTRY_CD STRING,
START_DT TIMESTAMP,
END_DT TIMESTAMP,
MA_EVENT_TYPE_ID INT,
MA_EVENT_SOURCE_ID INT,
LOCATION_ID INT,
MA_EVENT_DESC STRING,
EM_VENDOR_FUNDING_ID BIGINT,
EM_COMMENT STRING,
EM_BILL_ALT_VENDOR_FLAG STRING,
EM_ALT_VENDOR_ID STRING,
EM_ALT_VENDOR_NAME STRING,
EM_ALT_VENDOR_COUNTRY_CD STRING,
EM_VENDOR_ID BIGINT,
EM_VENDOR_NAME STRING,
EM_VENDOR_COUNTRY_CD STRING,
MA_AMT INT,
MA_MAX_AMT INT,
UPDATE_DT TIMESTAMP,
LOAD_DT TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_MOVEMENT_PRE(DAY_DT TIMESTAMP,
PRODUCT_ID INT,
LOCATION_ID INT,
MOVEMENT_ID INT,
PO_NBR BIGINT,
PO_LINE_NBR INT,
MA_EVENT_ID INT,
STO_TYPE_ID INT,
MA_TRANS_AMT INT,
MA_TRANS_COST INT,
MA_TRANS_QTY BIGINT,
EXCH_RATE_PCT INT,
UPDATE_DT TIMESTAMP,
LOAD_DT TIMESTAMP,
INS_UPD_FLAG STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.SERVICES_MARGIN_RATE(WEEK_DT DATE,
LOCATION_ID INT,
SAP_DEPT_ID INT,
MARGIN_RATE INT,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_STU_CHG_PRE(DAY_DT DATE,
SALES_INSTANCE_ID BIGINT,
PRODUCT_ID INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.USR_MA_DEF_ALLOW_CTRL(FISCAL_MO INT,
SOURCE_VENDOR_ID BIGINT,
DEFECTIVE_ALLOWANCE_PCT INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_MOVEMENT_DAY(DAY_DT TIMESTAMP,
PRODUCT_ID INT,
LOCATION_ID INT,
MOVEMENT_ID INT,
PO_NBR BIGINT,
PO_LINE_NBR INT,
MA_EVENT_ID INT,
STO_TYPE_ID INT,
MA_TRANS_AMT INT,
MA_TRANS_COST INT,
MA_TRANS_QTY BIGINT,
SALES_ADJ_AMT INT,
EXCH_RATE_PCT INT,
UPDATE_DT TIMESTAMP,
LOAD_DT TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.MA_CASH_DISCOUNT_CTRL(FISCAL_MO INT,
SOURCE_VENDOR_ID BIGINT,
EST_CASH_DISCOUNT_PCT INT,
ACT_NET_SALES_COST INT,
ACT_CASH_DISCOUNT_GL_AMT INT,
ACT_CASH_DISCOUNT_PCT INT,
OVRD_CASH_DISCOUNT_PCT INT,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.USR_MA_MOVEMENT_CTRL(MOVEMENT_ID INT,
VALUATION_CLASS_CD STRING,
LOCATION_TYPE_ID INT,
FISCAL_MO INT,
MA_EVENT_TYPE_ID INT,
MA_EVENT_SOURCE_ID INT,
GL_ACCT_NBR INT,
MA_AMT INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.USR_MA_CASH_DISCOUNT_OVRD_CTRL(FISCAL_MO INT,
SOURCE_VENDOR_ID BIGINT,
OVRD_CASH_DISCOUNT_PCT INT) USING DELTA;