# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, CARRIER_PROFILE_0


df_0=spark.sql("""
    SELECT
        CARRIER_ID AS CARRIER_ID,
        SCAC_CD AS SCAC_CD,
        SCM_CARRIER_ID AS SCM_CARRIER_ID,
        SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
        WMS_SHIP_VIA AS WMS_SHIP_VIA,
        WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
        PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CARRIER_PROFILE""")

df_0.createOrReplaceTempView("CARRIER_PROFILE_0")

# COMMAND ----------
# DBTITLE 1, CARRIER_PROFILE_PRE_1


df_1=spark.sql("""
    SELECT
        CARRIER_ID AS CARRIER_ID,
        SCAC_CD AS SCAC_CD,
        SCM_CARRIER_ID AS SCM_CARRIER_ID,
        SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
        WMS_SHIP_VIA AS WMS_SHIP_VIA,
        WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
        PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
        UPDATE_FLAG AS UPDATE_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CARRIER_PROFILE_PRE""")

df_1.createOrReplaceTempView("CARRIER_PROFILE_PRE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_CARRIER_PROFILE_PRE_2


df_2=spark.sql("""SELECT CARRIER_ID,
       SCAC_CD,
       SCM_CARRIER_ID,
       SCM_CARRIER_NAME,
       WMS_SHIP_VIA,
       WMS_SHIP_VIA_DESC,
       PRIMARY_CARRIER_IND,
       CURRENT_DATE AS UPDATE_DT,
           CURRENT_DATE AS LOAD_DT,
       UPDATE_FLAG
  FROM CARRIER_PROFILE_PRE
 WHERE UPDATE_FLAG = 0
UNION ALL
SELECT P.CARRIER_ID,
       P.SCAC_CD,
       P.SCM_CARRIER_ID,
       P.SCM_CARRIER_NAME,
       P.WMS_SHIP_VIA,
       P.WMS_SHIP_VIA_DESC,
       P.PRIMARY_CARRIER_IND,
       CURRENT_DATE AS UPDATE_DT,
           NVL(C.LOAD_DT,CURRENT_DATE) as LOAD_DT,
       UPDATE_FLAG
  FROM CARRIER_PROFILE_PRE P,
       CARRIER_PROFILE C
 WHERE UPDATE_FLAG = 1
   AND P.CARRIER_ID = C.CARRIER_ID
   AND (NVL(P.SCAC_CD, ' ')              <> NVL(C.SCAC_CD, ' ')
        OR NVL(P.SCM_CARRIER_ID, ' ')    <> NVL(C.SCM_CARRIER_ID, ' ')
        OR NVL(P.SCM_CARRIER_NAME, ' ')  <> NVL(C.SCM_CARRIER_NAME, ' ')
        OR NVL(P.WMS_SHIP_VIA, ' ')      <> NVL(C.WMS_SHIP_VIA, ' ')
        OR NVL(P.WMS_SHIP_VIA_DESC, ' ') <> NVL(C.WMS_SHIP_VIA_DESC, ' ')
        OR P.PRIMARY_CARRIER_IND         <> C.PRIMARY_CARRIER_IND)""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_CARRIER_PROFILE_PRE_2")

# COMMAND ----------
# DBTITLE 1, UPD_ins_upd_3


df_3=spark.sql("""
    SELECT
        CARRIER_ID AS CARRIER_ID,
        SCAC_CD AS SCAC_CD,
        SCM_CARRIER_ID AS SCM_CARRIER_ID,
        SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
        WMS_SHIP_VIA AS WMS_SHIP_VIA,
        WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
        PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        UPDATE_FLAG AS UPDATE_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_CARRIER_PROFILE_PRE_2""")

df_3.createOrReplaceTempView("UPD_ins_upd_3")

# COMMAND ----------
# DBTITLE 1, CARRIER_PROFILE


spark.sql("""INSERT INTO CARRIER_PROFILE SELECT CARRIER_ID AS CARRIER_ID,
SCAC_CD AS SCAC_CD,
SCM_CARRIER_ID AS SCM_CARRIER_ID,
SCM_CARRIER_NAME AS SCM_CARRIER_NAME,
WMS_SHIP_VIA AS WMS_SHIP_VIA,
WMS_SHIP_VIA_DESC AS WMS_SHIP_VIA_DESC,
PRIMARY_CARRIER_IND AS PRIMARY_CARRIER_IND,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPD_ins_upd_3""")