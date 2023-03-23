# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR_PRE_0


df_0=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        DEL_IND AS DEL_IND,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_TXS_ATTR_PRE""")

df_0.createOrReplaceTempView("SKU_TXS_ATTR_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1


df_1=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        DEL_IND AS DEL_IND,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_TXS_ATTR_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1")

# COMMAND ----------
# DBTITLE 1, Lkp_Sku_Profile_2


df_2=spark.sql("""
    SELECT
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1 
            ON SKU_PROFILE.SKU_NBR = SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.SKU_NBR""")

df_2.createOrReplaceTempView("Lkp_Sku_Profile_2")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_TXS_Attr_Pre_3


df_3=spark.sql("""
    SELECT
        Lkp_Sku_Profile_2.PRODUCT_ID AS PRODUCT_ID,
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.DEL_IND AS DEL_IND,
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.UPDATE_USER AS UPDATE_USER,
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.LOAD_USER AS LOAD_USER,
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1 
    INNER JOIN
        Lkp_Sku_Profile_2 
            ON SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1.Monotonically_Increasing_Id = Lkp_Sku_Profile_2.Monotonically_Increasing_Id""")

df_3.createOrReplaceTempView("Exp_SKU_TXS_Attr_Pre_3")

# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR_4


df_4=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_TXS_ATTR""")

df_4.createOrReplaceTempView("SKU_TXS_ATTR_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_5


df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_TXS_ATTR_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_5")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_TXS_Attr_6


df_6=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        DETAIL.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        DETAIL.DEL_IND AS DEL_IND,
        DETAIL.UPDATE_USER AS UPDATE_USER,
        DETAIL.LOAD_USER AS LOAD_USER,
        MASTER.PRODUCT_ID AS PRODUCT_ID1,
        MASTER.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID1,
        MASTER.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID1,
        MASTER.UPDATE_USER AS UPDATE_USER1,
        MASTER.LOAD_USER AS LOAD_USER1,
        MASTER.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_TXS_ATTR_5 MASTER 
    LEFT JOIN
        Exp_SKU_TXS_Attr_Pre_3 DETAIL 
            ON MASTER.PRODUCT_ID = PRODUCT_ID 
            AND SKU_TXS_ATTR_TYPE_ID1 = DETAIL.SKU_TXS_ATTR_TYPE_ID""")

df_6.createOrReplaceTempView("Jnr_SKU_TXS_Attr_6")

# COMMAND ----------
# DBTITLE 1, Fil_SKU_TXS_Attr_7


df_7=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        DEL_IND AS DEL_IND,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        PRODUCT_ID1 AS PRODUCT_ID1,
        SKU_TXS_ATTR_TYPE_ID1 AS SKU_TXS_ATTR_TYPE_ID1,
        SKU_TXS_ATTR_TYPE_VALUE_ID1 AS SKU_TXS_ATTR_TYPE_VALUE_ID1,
        UPDATE_USER1 AS UPDATE_USER1,
        LOAD_USER1 AS LOAD_USER1,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_SKU_TXS_Attr_6 
    WHERE
        NOT ISNULL(PRODUCT_ID) 
        AND ISNULL(PRODUCT_ID1) 
        OR (
            NOT ISNULL(PRODUCT_ID) 
            AND NOT ISNULL(PRODUCT_ID1) 
            OR (
                DEL_IND = 'X' 
                OR DEL_IND = '1' 
                OR SKU_TXS_ATTR_TYPE_VALUE_ID <> SKU_TXS_ATTR_TYPE_VALUE_ID1 
                OR UPDATE_USER <> UPDATE_USER1 
                OR LOAD_USER <> LOAD_USER1
            )
        )""")

df_7.createOrReplaceTempView("Fil_SKU_TXS_Attr_7")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_TXS_Attr_8


df_8=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        DD_INSERT,
        IFF(DEL_IND = 'X' 
        OR DEL_IND = '1',
        DD_DELETE,
        DD_UPDATE)) AS LoadStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_SKU_TXS_Attr_7""")

df_8.createOrReplaceTempView("Exp_SKU_TXS_Attr_8")

# COMMAND ----------
# DBTITLE 1, Ups_SKU_TXS_Attr_9


df_9=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        LoadStrategy AS LoadStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_SKU_TXS_Attr_8""")

df_9.createOrReplaceTempView("Ups_SKU_TXS_Attr_9")

# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR


spark.sql("""INSERT INTO SKU_TXS_ATTR SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
UPDATE_USER AS UPDATE_USER,
LOAD_USER AS LOAD_USER,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM Ups_SKU_TXS_Attr_9""")