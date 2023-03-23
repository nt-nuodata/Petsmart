# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, REPL_SCHED_DAY_0

df_0=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        REPL_SCHED_TRANS_FLAG AS REPL_SCHED_TRANS_FLAG,
        PROGRAM_VARIANT AS PROGRAM_VARIANT,
        REPL_SCHED_DAY_ABBR AS REPL_SCHED_DAY_ABBR,
        SITE_GROUP_CD AS SITE_GROUP_CD,
        DELETE_IND AS DELETE_IND,
        PICK_TYPE_CD AS PICK_TYPE_CD,
        REPL_INTERVAL_AMT AS REPL_INTERVAL_AMT,
        LAST_PULL_DT AS LAST_PULL_DT,
        CONVERT_REQ_FLAG AS CONVERT_REQ_FLAG,
        ACT_PULL_DT AS ACT_PULL_DT,
        NEXT_PULL_DT AS NEXT_PULL_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REPL_SCHED_DAY""")

df_0.createOrReplaceTempView("REPL_SCHED_DAY_0")

# COMMAND ----------

# DBTITLE 1, REPLENISHMENT_DAY_1

df_1=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        DELETE_IND AS DELETE_IND,
        SAFETY_QTY AS SAFETY_QTY,
        SERVICE_LVL_RT AS SERVICE_LVL_RT,
        REORDER_POINT_QTY AS REORDER_POINT_QTY,
        PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
        TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
        PRESENT_QTY AS PRESENT_QTY,
        PROMO_QTY AS PROMO_QTY,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REPLENISHMENT_DAY""")

df_1.createOrReplaceTempView("REPLENISHMENT_DAY_1")

# COMMAND ----------

# DBTITLE 1, POG_SKU_PRO_2

df_2=spark.sql("""
    SELECT
        POG_ID AS POG_ID,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_CAPACITY_QTY AS SKU_CAPACITY_QTY,
        SKU_FACINGS_QTY AS SKU_FACINGS_QTY,
        SKU_HEIGHT_IN AS SKU_HEIGHT_IN,
        SKU_DEPTH_IN AS SKU_DEPTH_IN,
        SKU_WIDTH_IN AS SKU_WIDTH_IN,
        UNIT_OF_MEASURE AS UNIT_OF_MEASURE,
        TRAY_PACK_NBR AS TRAY_PACK_NBR,
        POG_STATUS AS POG_STATUS,
        LAST_CHNG_DT AS LAST_CHNG_DT,
        PQ_CHNG_DT AS PQ_CHNG_DT,
        LIST_START_DT AS LIST_START_DT,
        LIST_END_DT AS LIST_END_DT,
        PROMO_START_DT AS PROMO_START_DT,
        PROMO_END_DT AS PROMO_END_DT,
        POG_PROMO_QTY AS POG_PROMO_QTY,
        DATE_POG_ADDED AS DATE_POG_ADDED,
        DATE_POG_REFRESHED AS DATE_POG_REFRESHED,
        DATE_POG_DELETED AS DATE_POG_DELETED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        POG_SKU_PRO""")

df_2.createOrReplaceTempView("POG_SKU_PRO_2")

# COMMAND ----------

# DBTITLE 1, SKU_VENDOR_DAY_3

df_3=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        VENDOR_ID AS VENDOR_ID,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        DELETE_IND AS DELETE_IND,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        DELIV_EFF_DT AS DELIV_EFF_DT,
        DELIV_END_DT AS DELIV_END_DT,
        REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
        ROUNDING_PROFILE_CD AS ROUNDING_PROFILE_CD,
        COUNTRY_CD AS COUNTRY_CD,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_VENDOR_DAY""")

df_3.createOrReplaceTempView("SKU_VENDOR_DAY_3")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_EMPLOYEE_PROFILE_WK_PRE1_4

df_4=spark.sql("""
    SELECT
        CURRENT_TIMESTAMP AS START_TSTMP,
        'REPLENISHMENT_PROFILE' AS TABLE_NAME,
        COUNT(*) AS BEGIN_ROW_CNT 
    FROM
        REPLENISHMENT_PROFILE""")

df_4.createOrReplaceTempView("SQ_Shortcut_to_EMPLOYEE_PROFILE_WK_PRE1_4")

# COMMAND ----------

# DBTITLE 1, SQL_INS_and_DUPS_CHECK_5

df_5=spark.sql("""
    SELECT
        BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        START_TSTMP AS START_TSTMP,
        TABLE_NAME AS TABLE_NAME 
    FROM
        SQ_Shortcut_to_EMPLOYEE_PROFILE_WK_PRE1_4""")

df_5.createOrReplaceTempView("SQL_INS_and_DUPS_CHECK_5")

# COMMAND ----------

# DBTITLE 1, EXP_GET_SESSION_INFO_6

df_6=spark.sql("""
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
        SQL_INS_and_DUPS_CHECK_5""")

df_6.createOrReplaceTempView("EXP_GET_SESSION_INFO_6")

# COMMAND ----------

# DBTITLE 1, AGG_7

df_7=spark.sql("""
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
        EXP_GET_SESSION_INFO_6 
    GROUP BY
        START_TSTMP,
        WORKFLOW_NAME,
        SESSION_NAME,
        MAPPING_NAME,
        TABLE_NAME""")

df_7.createOrReplaceTempView("AGG_7")

# COMMAND ----------

# DBTITLE 1, EXP_CREATE_INS_SQL_8

df_8=spark.sql("""
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
        AGG_7""")

df_8.createOrReplaceTempView("EXP_CREATE_INS_SQL_8")

# COMMAND ----------

# DBTITLE 1, SQL_INS_to_SQL_TRANSFORM_LOG_9

df_9=spark.sql("""
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
        EXP_CREATE_INS_SQL_8""")

df_9.createOrReplaceTempView("SQL_INS_to_SQL_TRANSFORM_LOG_9")

# COMMAND ----------

# DBTITLE 1, EXP_ABORT_SESSION_10

df_10=spark.sql("""
    SELECT
        DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
        SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
        IFF((CAST(DUPLICATE_ROW_CNT_output AS DECIMAL (38,
        0))) > 0,
        ABORT('There are duplicates rows in the table'),
        IIF()) AS ABORT_SESSION,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_INS_to_SQL_TRANSFORM_LOG_9""")

df_10.createOrReplaceTempView("EXP_ABORT_SESSION_10")

# COMMAND ----------

# DBTITLE 1, SQL_TRANSFORM_DUMMY_TARGET

spark.sql("""INSERT INTO SQL_TRANSFORM_DUMMY_TARGET SELECT DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
ABORT_SESSION AS ABORT_SESSION FROM EXP_ABORT_SESSION_10""")
