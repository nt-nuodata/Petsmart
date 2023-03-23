# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_SITE_PROFILE_PRE_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        DP_PURCH_GROUP_ID AS DP_PURCH_GROUP_ID,
        ACTIVITY_CLASS_CD AS ACTIVITY_CLASS_CD,
        FIRST_DPR_DT AS FIRST_DPR_DT,
        LAST_DPR_DT AS LAST_DPR_DT,
        FIRST_ON_HAND_DT AS FIRST_ON_HAND_DT,
        LAST_ON_HAND_DT AS LAST_ON_HAND_DT,
        FIRST_ON_ORDER_DT AS FIRST_ON_ORDER_DT,
        LAST_ON_ORDER_DT AS LAST_ON_ORDER_DT,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_SITE_PROFILE_PRE""")

df_0.createOrReplaceTempView("SKU_SITE_PROFILE_PRE_0")

# COMMAND ----------
# DBTITLE 1, QUERY_ARGUMENTS_1


df_1=spark.sql("""
    SELECT
        JOB_NAME AS JOB_NAME,
        LAST_CHANGE_DT AS LAST_CHANGE_DT,
        LAST_CHANGE_USER_ID AS LAST_CHANGE_USER_ID,
        TABLE_NAME AS TABLE_NAME,
        SQL_DESC AS SQL_DESC,
        SQL_TX AS SQL_TX,
        SQL_TX2 AS SQL_TX2,
        SQL_TX3 AS SQL_TX3,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        QUERY_ARGUMENTS""")

df_1.createOrReplaceTempView("QUERY_ARGUMENTS_1")

# COMMAND ----------
# DBTITLE 1, ASQ_PLSQL_2


df_2=spark.sql("""
    SELECT
        'M_SKU_SITE_PROFILE_PLSQL'""")

df_2.createOrReplaceTempView("ASQ_PLSQL_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_mplt_GENERIC_SQL_15


df_3=spark.sql("""SELECT JOB_NAME AS MAP_NAME FROM ASQ_PLSQL_2""")

df_3.createOrReplaceTempView("Shortcut_to_mplt_GENERIC_SQL_Input")

# COMMAND ----------
# DBTITLE 1, INP_MPLT_GENERIC_SQL


df_4=spark.sql("""SELECT MAP_NAME,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM Shortcut_to_mplt_GENERIC_SQL_Input""")

df_4.createOrReplaceTempView("INP_MPLT_GENERIC_SQL_4")

# COMMAND ----------
# DBTITLE 1, EXP_START_TIME_5


df_5=spark.sql("""
    SELECT
        MAP_NAME AS MAP_NAME,
        var_SESS_START_TIME AS SESS_START_TIME,
        'INSERT INTO QUERY_LOG  VALUES(' || CHR(39) || MAP_NAME || CHR(39) || ',TO_DATE(' || CHR(39) || var_SESS_START_TIME || CHR(39) || ',' || CHR(39) || 'YYYY-MM-DD HH:MI:SS AM' || CHR(39) || '),NULL, 0,0,' || CHR(39) || 'JOB_STARTED' || CHR(39) || ', 0, 0, 0, 0, ' || CHR(39) || 'Netezza User' || CHR(39) || ')' AS SQL_QUERY_LOG_INSERT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        INP_MPLT_GENERIC_SQL""")

df_5.createOrReplaceTempView("EXP_START_TIME_5")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_LOG_INSERT_6


df_6=spark.sql("""
    SELECT
        MAP_NAME AS MAP_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        SESS_START_TIME AS SESS_START_TIME,
        SQL_QUERY_LOG_INSERT AS SQL_QUERY_LOG_INSERT 
    FROM
        EXP_START_TIME_5""")

df_6.createOrReplaceTempView("SQL_QUERY_LOG_INSERT_6")

# COMMAND ----------
# DBTITLE 1, EXP_ONE_ROW_FILTER_7


df_7=spark.sql("""
    SELECT
        MAP_NAME_output AS MAP_NAME_output,
        SESS_START_TIME_output AS SESS_START_TIME_output,
        var_COUNTER AS out_COUNTER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_QUERY_LOG_INSERT_6""")

df_7.createOrReplaceTempView("EXP_ONE_ROW_FILTER_7")

# COMMAND ----------
# DBTITLE 1, FIL_ONE_ROW_8


df_8=spark.sql("""
    SELECT
        MAP_NAME_output AS MAP_NAME_output,
        SESS_START_TIME_output AS SESS_START_TIME_output,
        out_COUNTER AS out_COUNTER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ONE_ROW_FILTER_7 
    WHERE
        out_COUNTER = 2""")

df_8.createOrReplaceTempView("FIL_ONE_ROW_8")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_ARG_DATA_FETCH_9


df_9=spark.sql("""
    SELECT
        MAP_NAME_output AS MAP_NAME_output,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        SESS_START_TIME_output AS SESS_START_TIME_output 
    FROM
        FIL_ONE_ROW_8""")

df_9.createOrReplaceTempView("SQL_QUERY_ARG_DATA_FETCH_9")

# COMMAND ----------
# DBTITLE 1, EXP_TXT_CONCAT_10


df_10=spark.sql("""
    SELECT
        SQLError AS SQLError,
        IN_MAP_NAME_output AS IN_MAP_NAME_output,
        SESS_START_TIME_output AS SESS_START_TIME_output,
        out_SQL_TX || DECODE(TRUE,
        ISNULL(out_SQL_TX2),
        ' ',
        out_SQL_TX2) || DECODE(TRUE,
        ISNULL(out_SQL_TX3),
        ' ',
        out_SQL_TX3) AS out_SQL_TXT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_QUERY_ARG_DATA_FETCH_9""")

df_10.createOrReplaceTempView("EXP_TXT_CONCAT_10")

# COMMAND ----------
# DBTITLE 1, SQL_RUN_SQL_FROM_QUERY_ARG_11


df_11=spark.sql("""
    SELECT
        IN_MAP_NAME_output AS IN_MAP_NAME_output,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        SESS_START_TIME_output AS SESS_START_TIME_output,
        SQLError AS SQLError,
        out_SQL_TXT AS out_SQL_TXT 
    FROM
        EXP_TXT_CONCAT_10""")

df_11.createOrReplaceTempView("SQL_RUN_SQL_FROM_QUERY_ARG_11")

# COMMAND ----------
# DBTITLE 1, EXP_ERROR_MSG_12


df_12=spark.sql("""
    SELECT
        DECODE(TRUE,
        ISNULL(SQLError) 
        AND ISNULL(SQL_Error_output),
        'NO ERRORS ENCOUNTERED',
        'error  = ' || SQLError) AS out_SQL_Error,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_RUN_SQL_FROM_QUERY_ARG_11""")

df_12.createOrReplaceTempView("EXP_ERROR_MSG_12")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_LOG_UPDATE_13


df_13=spark.sql("""SELECT Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
out_SQL_Error AS out_SQL_Error FROM EXP_ERROR_MSG_12 UNION ALL SELECT MAP_NAME_output AS MAP_NAME_output,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
NumRowsAffected AS NumRowsAffected,
SESS_START_TIME_output AS SESS_START_TIME_output FROM SQL_RUN_SQL_FROM_QUERY_ARG_11""")

df_13.createOrReplaceTempView("SQL_QUERY_LOG_UPDATE_13")

# COMMAND ----------
# DBTITLE 1, EXP_OUTPUT_14


df_14=spark.sql("""
    SELECT
        MAP_NAME AS MAP_NAME,
        DECODE(TRUE,
        Sql_Error_output = 'NO ERRORS ENCOUNTERED',
        'SUCCEEDED',
        ABORT('FAILURE IN SQL')) AS out_MPLT_STATUS,
        Sql_Error_output AS out_MPLT_SQL_ERROR,
        SESSSTARTTIME AS out_JOB_START_DATE,
        SQLError AS SQLError,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQL_QUERY_LOG_UPDATE_13""")

df_14.createOrReplaceTempView("EXP_OUTPUT_14")

# COMMAND ----------
# DBTITLE 1, OUT_MPLT_GENERIC_SQL


df_15=spark.sql("""SELECT MAP_NAME AS MAP_NAME1,
MPLT_STATUS AS MPLT_STATUS,
MPLT_SQL_ERROR AS MPLT_SQL_ERROR,
JOB_START_DATE AS JOB_START_DATE,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM EXP_OUTPUT_14""")

df_15.createOrReplaceTempView("Shortcut_to_mplt_GENERIC_SQL_15")

# COMMAND ----------
# DBTITLE 1, QUERY_LOG_TXT


spark.sql("""INSERT INTO QUERY_LOG_TXT SELECT MAP_NAME1 AS JOB_NAME,
JOB_START_DATE AS QUERY_START_TSTMP,
QUERY_END_TSTMP AS QUERY_END_TSTMP,
RESULT_NBR AS RESULT_NBR,
ERROR_NBR AS ERROR_NBR,
MPLT_STATUS AS ERROR_TX,
ROWS_AFFECTED AS ROWS_AFFECTED,
ROWS_INSERTED AS ROWS_INSERTED,
ROWS_UPDATED AS ROWS_UPDATED,
ROWS_DELETED AS ROWS_DELETED,
EXEC_USER AS EXEC_USER FROM Shortcut_to_mplt_GENERIC_SQL_15""")