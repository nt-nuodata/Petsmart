# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, Lkp_Sku_Profile_0

df_0=spark.sql("""SELECT  FROM SKU_PROFILE """)

df_0.createOrReplaceTempView("Lkp_Sku_Profile_0")

# COMMAND ----------

# DBTITLE 1, SKU_PIM_ATTR_PRE_1

df_1=spark.sql("""
    SELECT
        ARTICLE_NBR AS ARTICLE_NBR,
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        SLICE_IND AS SLICE_IND,
        SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        DEL_IND AS DEL_IND,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_PRE""")

df_1.createOrReplaceTempView("SKU_PIM_ATTR_PRE_1")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_PRE_2

df_2=spark.sql("""
    SELECT
        ARTICLE_NBR AS ARTICLE_NBR,
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        SLICE_IND AS SLICE_IND,
        SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        DEL_IND AS DEL_IND,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_PRE_1""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_PRE_2")

# COMMAND ----------

# DBTITLE 1, Exp_SkuNbr_3

df_3=spark.sql("""SELECT :LKP.LKP_SKU_PROFILE(TO_INTEGER(ARTICLE_NBR)) AS PRODUCT_ID,
PIM_ATTR_ID AS PIM_ATTR_ID,
PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
SLICE_IND AS SLICE_IND,
SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
iif(DEL_IND = 'X', 1, 0) AS DEL_IND1,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM SQ_Shortcut_to_SKU_PIM_ATTR_PRE_2""")

df_3.createOrReplaceTempView("Exp_SkuNbr_3")

# COMMAND ----------

# DBTITLE 1, SKU_PIM_ATTR_4

df_4=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        SLICE_IND AS SLICE_IND,
        SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR""")

df_4.createOrReplaceTempView("SKU_PIM_ATTR_4")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_5

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        SLICE_IND AS SLICE_IND,
        SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_5")

# COMMAND ----------

# DBTITLE 1, Jnr_SKU_PIM_Attr_6

df_6=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.PIM_ATTR_ID AS PIM_ATTR_ID,
        DETAIL.PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        DETAIL.SLICE_IND AS SLICE_IND1,
        DETAIL.SLICE_SEQ_NBR AS SLICE_SEQ_NBR1,
        DETAIL.DEL_IND1 AS DEL_IND,
        MASTER.PRODUCT_ID AS PRODUCT_ID1,
        MASTER.SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        MASTER.SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        MASTER.SLICE_IND AS SLICE_IND,
        MASTER.SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        MASTER.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_PIM_ATTR_5 MASTER 
    LEFT JOIN
        Exp_SkuNbr_3 DETAIL 
            ON MASTER.PRODUCT_ID = PRODUCT_ID 
            AND SKU_PIM_ATTR_TYPE_ID = PIM_ATTR_ID 
            AND SKU_PIM_ATTR_TYPE_VALUE_ID = DETAIL.PIM_ATTR_VAL_ID""")

df_6.createOrReplaceTempView("Jnr_SKU_PIM_Attr_6")

# COMMAND ----------

# DBTITLE 1, Fil_SKU_PIM_Attr_7

df_7=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        SLICE_IND1 AS SLICE_IND1,
        SLICE_SEQ_NBR1 AS SLICE_SEQ_NBR1,
        DEL_IND AS DEL_IND,
        PRODUCT_ID1 AS PRODUCT_ID1,
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        SLICE_IND AS SLICE_IND,
        SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_SKU_PIM_Attr_6 
    WHERE
        (
            NOT ISNULL(PRODUCT_ID) 
            AND ISNULL(PRODUCT_ID1) 
            AND DEL_IND <> 1
        ) 
        OR (
            NOT ISNULL(PRODUCT_ID) 
            AND NOT ISNULL(PRODUCT_ID1) 
            AND (
                SLICE_IND1 <> SLICE_IND 
                OR SLICE_SEQ_NBR1 <> SLICE_SEQ_NBR 
                OR DEL_IND = 1
            )
        )""")

df_7.createOrReplaceTempView("Fil_SKU_PIM_Attr_7")

# COMMAND ----------

# DBTITLE 1, Exp_SKU_PIM_Attr_8

df_8=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        SLICE_IND AS SLICE_IND,
        SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        DEL_IND AS DEL_IND,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        DD_INSERT,
        IFF(DEL_IND = 1,
        DD_DELETE,
        DD_UPDATE)) AS LoadStrategyFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_SKU_PIM_Attr_7""")

df_8.createOrReplaceTempView("Exp_SKU_PIM_Attr_8")

# COMMAND ----------

# DBTITLE 1, Ups_SKU_PIM_Attr_9

df_9=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        SLICE_IND AS SLICE_IND,
        SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
        DEL_IND AS DEL_IND,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        LoadStrategyFlag AS LoadStrategyFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_SKU_PIM_Attr_8""")

df_9.createOrReplaceTempView("Ups_SKU_PIM_Attr_9")

# COMMAND ----------

# DBTITLE 1, SKU_PIM_ATTR

spark.sql("""INSERT INTO SKU_PIM_ATTR SELECT PRODUCT_ID AS PRODUCT_ID,
PIM_ATTR_ID AS SKU_PIM_ATTR_TYPE_ID,
PIM_ATTR_VAL_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
SLICE_IND AS SLICE_IND,
SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM Ups_SKU_PIM_Attr_9""")
