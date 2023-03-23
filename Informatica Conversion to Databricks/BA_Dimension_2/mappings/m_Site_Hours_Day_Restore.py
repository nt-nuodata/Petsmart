# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SITE_HOURS_DAY_PRE_0

df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_NBR AS LOCATION_NBR,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        BUSINESS_AREA AS BUSINESS_AREA,
        OPEN_TSTMP AS OPEN_TSTMP,
        CLOSE_TSTMP AS CLOSE_TSTMP,
        IS_CLOSED AS IS_CLOSED,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_HOURS_DAY_PRE""")

df_0.createOrReplaceTempView("SITE_HOURS_DAY_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_SITE_HOURS_DAY_PRE_1

df_1=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_NBR AS LOCATION_NBR,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        BUSINESS_AREA AS BUSINESS_AREA,
        OPEN_TSTMP AS OPEN_TSTMP,
        CLOSE_TSTMP AS CLOSE_TSTMP,
        IS_CLOSED AS IS_CLOSED,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_HOURS_DAY_PRE_0""")

df_1.createOrReplaceTempView("SQ_SITE_HOURS_DAY_PRE_1")

# COMMAND ----------

# DBTITLE 1, Lkp_Site_Profile_2

df_2=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        TIME_ZONE AS TIME_ZONE,
        STORE_NBR AS STORE_NBR,
        SQ_SITE_HOURS_DAY_PRE_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_RPT 
    RIGHT OUTER JOIN
        SQ_SITE_HOURS_DAY_PRE_1 
            ON SITE_PROFILE_RPT.LOCATION_NBR = LOCATION_NBR1 
            AND SITE_PROFILE_RPT.LOCATION_TYPE_ID = SQ_SITE_HOURS_DAY_PRE_1.LOCATION_TYPE_ID""")

df_2.createOrReplaceTempView("Lkp_Site_Profile_2")

# COMMAND ----------

# DBTITLE 1, Exp_Site_Hours_Day_Pre_3

df_3=spark.sql("""
    SELECT
        SQ_SITE_HOURS_DAY_PRE_1.DAY_DT AS DAY_DT,
        Lkp_Site_Profile_2.LOCATION_ID AS LOCATION_ID,
        SQ_SITE_HOURS_DAY_PRE_1.BUSINESS_AREA AS BUSINESS_AREA,
        SQ_SITE_HOURS_DAY_PRE_1.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        Lkp_Site_Profile_2.TIME_ZONE AS TIME_ZONE,
        SQ_SITE_HOURS_DAY_PRE_1.OPEN_TSTMP AS OPEN_TSTMP,
        SQ_SITE_HOURS_DAY_PRE_1.CLOSE_TSTMP AS CLOSE_TSTMP,
        SQ_SITE_HOURS_DAY_PRE_1.IS_CLOSED AS IS_CLOSED,
        Lkp_Site_Profile_2.STORE_NBR AS STORE_NBR,
        MD5(TO_CHAR(SQ_SITE_HOURS_DAY_PRE_1.LOCATION_TYPE_ID) || Lkp_Site_Profile_2.TIME_ZONE || TO_CHAR(SQ_SITE_HOURS_DAY_PRE_1.OPEN_TSTMP,
        'YYYY-MM-DD HH24:MI:SS') || TO_CHAR(SQ_SITE_HOURS_DAY_PRE_1.CLOSE_TSTMP,
        'YYYY-MM-DD HH24:MI:SS') || TO_CHAR(SQ_SITE_HOURS_DAY_PRE_1.IS_CLOSED) || TO_CHAR(Lkp_Site_Profile_2.STORE_NBR)) AS _md5PRE,
        SQ_SITE_HOURS_DAY_PRE_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_SITE_HOURS_DAY_PRE_1 
    INNER JOIN
        Lkp_Site_Profile_2 
            ON SQ_SITE_HOURS_DAY_PRE_1.Monotonically_Increasing_Id = Lkp_Site_Profile_2.Monotonically_Increasing_Id""")

df_3.createOrReplaceTempView("Exp_Site_Hours_Day_Pre_3")

# COMMAND ----------

# DBTITLE 1, SITE_HOURS_DAY_4

df_4=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        BUSINESS_AREA AS BUSINESS_AREA,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        CLOSE_FLAG AS CLOSE_FLAG,
        TIME_ZONE AS TIME_ZONE,
        OPEN_TSTMP AS OPEN_TSTMP,
        CLOSE_TSTMP AS CLOSE_TSTMP,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_HOURS_DAY""")

df_4.createOrReplaceTempView("SITE_HOURS_DAY_4")

# COMMAND ----------

# DBTITLE 1, SQ_SITE_HOURS_DAY_5

df_5=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        BUSINESS_AREA AS BUSINESS_AREA,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        CLOSE_FLAG AS CLOSE_FLAG,
        TIME_ZONE AS TIME_ZONE,
        OPEN_TSTMP AS OPEN_TSTMP,
        CLOSE_TSTMP AS CLOSE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_HOURS_DAY_4""")

df_5.createOrReplaceTempView("SQ_SITE_HOURS_DAY_5")

# COMMAND ----------

# DBTITLE 1, Exp_Site_Hours_Day_6

df_6=spark.sql("""
    SELECT
        MD5(TO_CHAR(LOCATION_TYPE_ID) || TIME_ZONE || TO_CHAR(OPEN_TSTMP,
        'YYYY-MM-DD HH24:MI:SS') || TO_CHAR(CLOSE_TSTMP,
        'YYYY-MM-DD HH24:MI:SS') || TO_CHAR(CLOSE_FLAG) || TO_CHAR(STORE_NBR)) AS _md5FINAL,
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        BUSINESS_AREA AS BUSINESS_AREA,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_SITE_HOURS_DAY_5""")

df_6.createOrReplaceTempView("Exp_Site_Hours_Day_6")

# COMMAND ----------

# DBTITLE 1, Jnr_Site_Hours_Day_7

df_7=spark.sql("""
    SELECT
        MASTER.DAY_DT AS DAY_DT,
        MASTER.LOCATION_ID AS LOCATION_ID,
        MASTER.BUSINESS_AREA AS BUSINESS_AREA,
        MASTER.LOAD_TSTMP AS LOAD_TSTMP,
        DETAIL.DAY_DT AS DAY_DT1,
        DETAIL.LOCATION_ID AS LOCATION_ID1,
        DETAIL.BUSINESS_AREA AS BUSINESS_AREA1,
        DETAIL.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        DETAIL.TIME_ZONE AS TIME_ZONE,
        DETAIL.OPEN_TSTMP AS OPEN_TSTMP,
        DETAIL.CLOSE_TSTMP AS CLOSE_TSTMP,
        DETAIL.IS_CLOSED AS IS_CLOSED,
        DETAIL.STORE_NBR AS STORE_NBR,
        MASTER._md5FINAL AS _md5FINAL,
        DETAIL._md5PRE AS _md5PRE,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_Site_Hours_Day_6 MASTER 
    LEFT JOIN
        Exp_Site_Hours_Day_Pre_3 DETAIL 
            ON MASTER.DAY_DT = DAY_DT1 
            AND LOCATION_ID = LOCATION_ID1 
            AND BUSINESS_AREA = DETAIL.BUSINESS_AREA""")

df_7.createOrReplaceTempView("Jnr_Site_Hours_Day_7")

# COMMAND ----------

# DBTITLE 1, Fil_Site_Hours_Day_8

df_8=spark.sql("""
    SELECT
        DAY_DT1 AS DAY_DT1,
        LOCATION_ID1 AS LOCATION_ID1,
        BUSINESS_AREA1 AS BUSINESS_AREA1,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        TIME_ZONE AS TIME_ZONE,
        OPEN_TSTMP AS OPEN_TSTMP,
        CLOSE_TSTMP AS CLOSE_TSTMP,
        IS_CLOSED AS IS_CLOSED,
        STORE_NBR AS STORE_NBR,
        _md5FINAL AS _md5FINAL,
        _md5PRE AS _md5PRE,
        DAY_DT AS DAY_DT,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_Site_Hours_Day_7 
    WHERE
        (
            NOT ISNULL(DAY_DT1) 
            AND ISNULL(DAY_DT)
        ) 
        OR (
            NOT ISNULL(DAY_DT1) 
            AND NOT ISNULL(DAY_DT) 
            AND _md5FINAL <> _md5PRE
        )""")

df_8.createOrReplaceTempView("Fil_Site_Hours_Day_8")

# COMMAND ----------

# DBTITLE 1, Exp_StoreHours_9

df_9=spark.sql("""
    SELECT
        DAY_DT1 AS DAY_DT1,
        LOCATION_ID1 AS LOCATION_ID1,
        BUSINESS_AREA1 AS BUSINESS_AREA1,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        TIME_ZONE AS TIME_ZONE,
        OPEN_TSTMP AS OPEN_TSTMP,
        CLOSE_TSTMP AS CLOSE_TSTMP,
        IS_CLOSED AS IS_CLOSED,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(DAY_DT),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(DAY_DT),
        DD_INSERT,
        DD_UPDATE) AS LoadStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_Site_Hours_Day_8""")

df_9.createOrReplaceTempView("Exp_StoreHours_9")

# COMMAND ----------

# DBTITLE 1, Upd_Site_Hours_Day_10

df_10=spark.sql("""
    SELECT
        DAY_DT1 AS DAY_DT,
        LOCATION_ID1 AS LOCATION_ID,
        BUSINESS_AREA1 AS BUSINESS_AREA,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        TIME_ZONE AS TIME_ZONE,
        OPEN_TSTMP AS OPEN_TSTMP,
        CLOSE_TSTMP AS CLOSE_TSTMP,
        IS_CLOSED AS IS_CLOSED,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        LoadStrategy AS LoadStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_StoreHours_9""")

df_10.createOrReplaceTempView("Upd_Site_Hours_Day_10")

# COMMAND ----------

# DBTITLE 1, SITE_HOURS_DAY

spark.sql("""INSERT INTO SITE_HOURS_DAY SELECT DAY_DT AS DAY_DT,
LOCATION_ID AS LOCATION_ID,
BUSINESS_AREA AS BUSINESS_AREA,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
STORE_NBR AS STORE_NBR,
IS_CLOSED AS CLOSE_FLAG,
TIME_ZONE AS TIME_ZONE,
OPEN_TSTMP AS OPEN_TSTMP,
CLOSE_TSTMP AS CLOSE_TSTMP,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM Upd_Site_Hours_Day_10""")
