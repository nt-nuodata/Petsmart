# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, UPC_Flat_0


df_0=spark.sql("""
    SELECT
        UPC_ID AS UPC_ID,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        UPC_Flat""")

df_0.createOrReplaceTempView("UPC_Flat_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UPC_Flat_1


df_1=spark.sql("""
    SELECT
        UPC_ID AS UPC_ID,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UPC_Flat_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_UPC_Flat_1")

# COMMAND ----------
# DBTITLE 1, LKP_UPC_2


df_2=spark.sql("""
    SELECT
        SQ_Shortcut_to_UPC_Flat_1.UPC_ID AS in_UPC_ID,
        UPC_ID AS UPC_ID,
        UPC_ADD_DT AS UPC_ADD_DT,
        UPC_DELETE_DT AS UPC_DELETE_DT,
        UPC_REFRESH_DT AS UPC_REFRESH_DT,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SQ_Shortcut_to_UPC_Flat_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UPC 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_UPC_Flat_1 
            ON UPC_ID = SQ_Shortcut_to_UPC_Flat_1.UPC_ID""")

df_2.createOrReplaceTempView("LKP_UPC_2")

# COMMAND ----------
# DBTITLE 1, EXP_UPC_3


df_3=spark.sql("""
    SELECT
        LKP_UPC_2.UPC_ID AS UPC_ID_FROM_LKP,
        SQ_Shortcut_to_UPC_Flat_1.UPC_ID AS UPC_ID,
        IFF(SQ_Shortcut_to_UPC_Flat_1.UPC_ID > 0,
        UPC_ADD_DT,
        DATE_ADDED_REFRESHED_VAR) AS DATE_UPC_ADDED,
        TO_DATE('12-31-9999',
        'mm-dd-yyyy') AS DATE_UPC_DELETED,
        date_trunc('DAY',
        SESSSTARTTIME) AS DATE_UPC_REFRESHED,
        SQ_Shortcut_to_UPC_Flat_1.PRODUCT_ID AS PRODUCT_ID,
        SQ_Shortcut_to_UPC_Flat_1.SKU_NBR AS SKU_NBR,
        SQ_Shortcut_to_UPC_Flat_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_UPC_Flat_1 
    INNER JOIN
        LKP_UPC_2 
            ON SQ_Shortcut_to_UPC_Flat_1.Monotonically_Increasing_Id = LKP_UPC_2.Monotonically_Increasing_Id""")

df_3.createOrReplaceTempView("EXP_UPC_3")

# COMMAND ----------
# DBTITLE 1, EXP_UPC_CD_4


df_4=spark.sql("""
    SELECT
        UPC_TXT_RAW || CHECK_DIGIT AS UPC_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_UPC_Flat_1""")

df_4.createOrReplaceTempView("EXP_UPC_CD_4")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_5


df_5=spark.sql("""
    SELECT
        UPC_ID_FROM_LKP AS UPC_ID_FROM_LKP,
        UPC_ID AS UPC_ID,
        UPC_CD AS UPC_CD,
        UPC_ADD_DT AS UPC_ADD_DT,
        UPC_DELETE_DT AS UPC_DELETE_DT,
        UPC_REFRESH_DT AS UPC_REFRESH_DT,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_UPC_CD_4""")

df_5.createOrReplaceTempView("UPDTRANS_5")

df_5=spark.sql("""
    SELECT
        UPC_ID_FROM_LKP AS UPC_ID_FROM_LKP,
        UPC_ID AS UPC_ID,
        UPC_CD AS UPC_CD,
        UPC_ADD_DT AS UPC_ADD_DT,
        UPC_DELETE_DT AS UPC_DELETE_DT,
        UPC_REFRESH_DT AS UPC_REFRESH_DT,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_UPC_3""")

df_5.createOrReplaceTempView("UPDTRANS_5")

# COMMAND ----------
# DBTITLE 1, UPC


spark.sql("""INSERT INTO UPC SELECT UPC_ID AS UPC_ID,
UPC_CD AS UPC_CD,
UPC_ADD_DT AS UPC_ADD_DT,
UPC_DELETE_DT AS UPC_DELETE_DT,
UPC_REFRESH_DT AS UPC_REFRESH_DT,
PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR FROM UPDTRANS_5""")