# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_UOM_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        UOM_CD AS UOM_CD,
        UOM_NUMERATOR AS UOM_NUMERATOR,
        UOM_DENOMINATOR AS UOM_DENOMINATOR,
        LENGTH_AMT AS LENGTH_AMT,
        WIDTH_AMT AS WIDTH_AMT,
        HEIGHT_AMT AS HEIGHT_AMT,
        DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
        VOLUME_AMT AS VOLUME_AMT,
        VOLUME_UOM_CD AS VOLUME_UOM_CD,
        WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
        WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        SCM_VOLUME_UOM_CD AS SCM_VOLUME_UOM_CD,
        SCM_VOLUME_AMT AS SCM_VOLUME_AMT,
        SCM_WEIGHT_UOM_CD AS SCM_WEIGHT_UOM_CD,
        SCM_WEIGHT_NET_AMT AS SCM_WEIGHT_NET_AMT,
        DELETE_DT AS DELETE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_UOM""")

df_0.createOrReplaceTempView("SKU_UOM_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_SKU_UOM_1


df_1=spark.sql("""
    SELECT
        PC.PRODUCT_ID AS PRODUCT_ID,
        NVL(CS.UOM_NUMERATOR,
        1) UNIT_NUMERATOR,
        NVL(CS.VOLUME_AMT,
        0) VOLUME_AMT,
        NVL(CS.WEIGHT_GROSS_AMT,
        0) WEIGHT_GROSS_AMT 
    FROM
        (SELECT
            PRODUCT_ID 
        FROM
            SKU_UOM 
        WHERE
            UOM_CD = 'PC') PC 
    LEFT OUTER JOIN
        (
            SELECT
                PRODUCT_ID,
                UOM_NUMERATOR,
                VOLUME_AMT,
                WEIGHT_GROSS_AMT 
            FROM
                SKU_UOM 
            WHERE
                UOM_CD = 'CS'
        ) CS 
            ON PC.PRODUCT_ID = CS.PRODUCT_ID 
    ORDER BY
        1""")

df_1.createOrReplaceTempView("ASQ_Shortcut_To_SKU_UOM_1")

# COMMAND ----------
# DBTITLE 1, SKU_CASE_DIM


spark.sql("""INSERT INTO SKU_CASE_DIM SELECT PRODUCT_ID AS PRODUCT_ID,
UOM_NUMERATOR AS CASE_UNIT_CNT,
VOLUME_AMT AS CASE_VOLUME_AMT,
WEIGHT_GROSS_AMT AS CASE_WEIGHT_GROSS_AMT FROM ASQ_Shortcut_To_SKU_UOM_1""")