# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, VENDOR_PO_COND_PRE_0


df_0=spark.sql("""
    SELECT
        PO_COND_CD AS PO_COND_CD,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        VENDOR_ID AS VENDOR_ID,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        PO_COND_END_DT AS PO_COND_END_DT,
        DELETE_IND AS DELETE_IND,
        PO_COND_EFF_DT AS PO_COND_EFF_DT,
        PO_COND_REC_NBR AS PO_COND_REC_NBR,
        PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PO_COND_PRE""")

df_0.createOrReplaceTempView("VENDOR_PO_COND_PRE_0")

# COMMAND ----------
# DBTITLE 1, VENDOR_PO_COND_1


df_1=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        PO_COND_CD AS PO_COND_CD,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        PO_COND_EFF_DT AS PO_COND_EFF_DT,
        PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
        PO_COND_RATE_AMT_HIST AS PO_COND_RATE_AMT_HIST,
        PO_COND_END_DT AS PO_COND_END_DT,
        DELETE_IND AS DELETE_IND,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PO_COND""")

df_1.createOrReplaceTempView("VENDOR_PO_COND_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_VENDOR_SITE_PO_COND_PRE_2


df_2=spark.sql("""
    SELECT
        COND.VENDOR_ID,
        COND.PO_COND_CD,
        COND.PO_COND_EFF_DT,
        COND.PO_COND_RATE_AMT,
        NVL(VPC.PO_COND_RATE_AMT,
        0) AS PRIOR_PO_COND_RATE_AMT,
        COND.PO_COND_END_DT,
        COND.DELETE_IND,
        CURRENT_DATE,
        VPC.VENDOR_ID OLD_VENDOR_ID,
        COND.VENDOR_SUBRANGE_CD 
    FROM
        VENDOR_PO_COND_PRE COND 
    LEFT OUTER JOIN
        VENDOR_PO_COND VPC 
            ON COND.VENDOR_ID = VPC.VENDOR_ID 
            AND COND.PO_COND_CD = VPC.PO_COND_CD 
            AND COND.VENDOR_SUBRANGE_CD = VPC.VENDOR_SUBRANGE_CD 
    WHERE
        COND.DELETE_IND <> 'X'""")

df_2.createOrReplaceTempView("ASQ_Shortcut_To_VENDOR_SITE_PO_COND_PRE_2")

# COMMAND ----------
# DBTITLE 1, UPD_VENDOR_PO_COND_3


df_3=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        PO_COND_CD AS PO_COND_CD,
        PO_COND_EFF_DT AS PO_COND_EFF_DT,
        PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
        PO_COND_RATE_AMT_HIST AS PO_COND_RATE_AMT_HIST,
        PO_COND_END_DT AS PO_COND_END_DT,
        DELETE_IND AS DELETE_IND,
        LOAD_DT AS LOAD_DT,
        VENDOR_ID_OLD AS VENDOR_ID_OLD,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_To_VENDOR_SITE_PO_COND_PRE_2""")

df_3.createOrReplaceTempView("UPD_VENDOR_PO_COND_3")

# COMMAND ----------
# DBTITLE 1, VENDOR_PO_COND


spark.sql("""INSERT INTO VENDOR_PO_COND SELECT VENDOR_ID AS VENDOR_ID,
PO_COND_CD AS PO_COND_CD,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
PO_COND_EFF_DT AS PO_COND_EFF_DT,
PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
PO_COND_RATE_AMT_HIST AS PO_COND_RATE_AMT_HIST,
PO_COND_END_DT AS PO_COND_END_DT,
DELETE_IND AS DELETE_IND,
LOAD_DT AS LOAD_DT FROM UPD_VENDOR_PO_COND_3""")