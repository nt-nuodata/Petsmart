# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, VENDOR_SITE_PO_COND_PRE_0


df_0=spark.sql("""
    SELECT
        PO_COND_CD AS PO_COND_CD,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        PO_COND_END_DT AS PO_COND_END_DT,
        DELETE_IND AS DELETE_IND,
        PO_COND_EFF_DT AS PO_COND_EFF_DT,
        PO_COND_REC_NBR AS PO_COND_REC_NBR,
        PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_SITE_PO_COND_PRE""")

df_0.createOrReplaceTempView("VENDOR_SITE_PO_COND_PRE_0")

# COMMAND ----------
# DBTITLE 1, PO_COND_1


df_1=spark.sql("""
    SELECT
        PO_COND_CD AS PO_COND_CD,
        PO_COND_DESC AS PO_COND_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PO_COND""")

df_1.createOrReplaceTempView("PO_COND_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_PO_COND_2


df_2=spark.sql("""SELECT  VPC.PO_COND_CD,  'UNKNOWN' AS PO_COND_DESC
FROM  VENDOR_PO_COND_PRE VPC UNION ALL  SELECT  VSPC.PO_COND_CD,  'UNKNOWN' AS PO_COND_DESC FROM  VENDOR_SITE_PO_COND_PRE VSPC EXCEPT SELECT  PC.PO_COND_CD,  'UNKNOWN' AS PO_COND_DESC  FROM  PO_COND PC""")

df_2.createOrReplaceTempView("ASQ_Shortcut_to_PO_COND_2")

# COMMAND ----------
# DBTITLE 1, VENDOR_PO_COND_PRE_3


df_3=spark.sql("""
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

df_3.createOrReplaceTempView("VENDOR_PO_COND_PRE_3")

# COMMAND ----------
# DBTITLE 1, PO_COND


spark.sql("""INSERT INTO PO_COND SELECT PO_COND_CD AS PO_COND_CD,
PO_COND_DESC AS PO_COND_DESC FROM ASQ_Shortcut_to_PO_COND_2""")