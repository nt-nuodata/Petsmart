# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, DD07V_0


df_0=spark.sql("""
    SELECT
        DOMNAME AS DOMNAME,
        VALPOS AS VALPOS,
        DDLANGUAGE AS DDLANGUAGE,
        DOMVALUE_L AS DOMVALUE_L,
        DOMVALUE_H AS DOMVALUE_H,
        DDTEXT AS DDTEXT,
        DOMVAL_LD AS DOMVAL_LD,
        DOMVAL_HD AS DOMVAL_HD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DD07V""")

df_0.createOrReplaceTempView("DD07V_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DD07V_1


df_1=spark.sql("""
    SELECT
        CAST(VALPOS AS NUMBER (4)) ARTICLE_CATEGORY_ID,
        DOMVALUE_L ARTICLE_CATEGORY_CD,
        DDTEXT ARTICLE_CATEGORY_DESC 
    FROM
        SAPPR3.DD07V 
    WHERE
        DOMNAME = 'ATTYP' 
        AND DDLANGUAGE = 'E' 
        AND DOMVALUE_L <> ' '""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_DD07V_1")

# COMMAND ----------
# DBTITLE 1, ARTICLE_CATEGORY


spark.sql("""INSERT INTO ARTICLE_CATEGORY SELECT ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
ARTICLE_CATEGORY_DESC AS ARTICLE_CATEGORY_DESC FROM SQ_Shortcut_to_DD07V_1""")