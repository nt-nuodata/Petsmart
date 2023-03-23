# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, UDH_SITE_EAGLE_STATUS_0


df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        EAGLE_LEVEL AS EAGLE_LEVEL,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        UDH_SITE_EAGLE_STATUS""")

df_0.createOrReplaceTempView("UDH_SITE_EAGLE_STATUS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1


df_1=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        EAGLE_LEVEL AS EAGLE_LEVEL,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UDH_SITE_EAGLE_STATUS_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1")

# COMMAND ----------
# DBTITLE 1, LKP_SITE_PROFILE_2


df_2=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        STORE_NBR AS STORE_NBR,
        SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1.STORE_NBR AS STORE_NBR1,
        SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1.EAGLE_LEVEL AS EAGLE_LEVEL,
        SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1.LOAD_DT AS LOAD_DT,
        SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1 
            ON STORE_NBR = SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1.STORE_NBR""")

df_2.createOrReplaceTempView("LKP_SITE_PROFILE_2")

# COMMAND ----------
# DBTITLE 1, SITE_EAGLE_STATUS_3


df_3=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        EAGLE_LEVEL AS EAGLE_LEVEL,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_EAGLE_STATUS""")

df_3.createOrReplaceTempView("SITE_EAGLE_STATUS_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_EAGLE_STATUS_4


df_4=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        EAGLE_LEVEL AS EAGLE_LEVEL,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_EAGLE_STATUS_3""")

df_4.createOrReplaceTempView("SQ_Shortcut_to_SITE_EAGLE_STATUS_4")

# COMMAND ----------
# DBTITLE 1, JNR_LEFTOUTERJOIN_5


df_5=spark.sql("""
    SELECT
        MASTER.LOCATION_ID AS NEW_LOCATION_ID,
        MASTER.EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
        MASTER.LOAD_DT AS NEW_LOAD_DT,
        DETAIL.LOCATION_ID AS OLD_LOCATION_ID,
        DETAIL.EAGLE_LEVEL AS OLD_EAGLE_LEVEL,
        DETAIL.UPDATE_TSTMP AS UPDATE_TSTMP,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_SITE_PROFILE_2 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_SITE_EAGLE_STATUS_4 DETAIL 
            ON MASTER.LOCATION_ID = DETAIL.LOCATION_ID""")

df_5.createOrReplaceTempView("JNR_LEFTOUTERJOIN_5")

# COMMAND ----------
# DBTITLE 1, EXP_STRATEGY_6


df_6=spark.sql("""
    SELECT
        NEW_LOCATION_ID AS NEW_LOCATION_ID,
        NEW_EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
        v_STRATEGY AS STRATEGY,
        current_timestamp AS UPDATE_TSTMP,
        IFF(v_STRATEGY = 'INSERT',
        current_timestamp,
        LOAD_TSTMP) AS INSERT_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_LEFTOUTERJOIN_5""")

df_6.createOrReplaceTempView("EXP_STRATEGY_6")

# COMMAND ----------
# DBTITLE 1, FILT_EXISTING_RECORDS_7


df_7=spark.sql("""
    SELECT
        NEW_LOCATION_ID AS NEW_LOCATION_ID,
        NEW_EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
        STRATEGY AS STRATEGY,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        INSERT_TSTMP AS INSERT_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_STRATEGY_6 
    WHERE
        STRATEGY <> 'REJECT'""")

df_7.createOrReplaceTempView("FILT_EXISTING_RECORDS_7")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_8


df_8=spark.sql("""
    SELECT
        NEW_LOCATION_ID AS NEW_LOCATION_ID,
        NEW_EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
        STRATEGY AS STRATEGY,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        INSERT_TSTMP AS INSERT_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FILT_EXISTING_RECORDS_7""")

df_8.createOrReplaceTempView("UPDTRANS_8")

# COMMAND ----------
# DBTITLE 1, SITE_EAGLE_STATUS


spark.sql("""INSERT INTO SITE_EAGLE_STATUS SELECT NEW_LOCATION_ID AS LOCATION_ID,
NEW_EAGLE_LEVEL AS EAGLE_LEVEL,
UPDATE_TSTMP AS UPDATE_TSTMP,
INSERT_TSTMP AS LOAD_TSTMP FROM UPDTRANS_8""")