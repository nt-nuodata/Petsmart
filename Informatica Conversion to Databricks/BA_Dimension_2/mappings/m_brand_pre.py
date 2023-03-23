# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, Brand_0

df_0=spark.sql("""
    SELECT
        BrandCd AS BrandCd,
        BrandName AS BrandName,
        BrandTypeCd AS BrandTypeCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        BrandClassificationCd AS BrandClassificationCd,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Brand""")

df_0.createOrReplaceTempView("Brand_0")

# COMMAND ----------

# DBTITLE 1, SQ_ArtMast_Brand_1

df_1=spark.sql("""
    SELECT
        BrandCd AS BrandCd,
        BrandName AS BrandName,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Brand_0 
    WHERE
        Brand.BrandClassificationCd IS NOT NULL""")

df_1.createOrReplaceTempView("SQ_ArtMast_Brand_1")

# COMMAND ----------

# DBTITLE 1, BRAND_PRE

spark.sql("""INSERT INTO BRAND_PRE SELECT BrandCd AS BRAND_CD,
BrandName AS BRAND_NAME FROM SQ_ArtMast_Brand_1""")
