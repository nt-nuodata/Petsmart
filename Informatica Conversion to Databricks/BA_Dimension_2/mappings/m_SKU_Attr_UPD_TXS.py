# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR_TYPE_VALUES_0


df_0=spark.sql("""
    SELECT
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
        UPDATE_USER AS UPDATE_USER,
        LOAD_USER AS LOAD_USER,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_TXS_ATTR_TYPE_VALUES""")

df_0.createOrReplaceTempView("SKU_TXS_ATTR_TYPE_VALUES_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_1


df_1=spark.sql("""
    SELECT
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_TXS_ATTR_TYPE_VALUES_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_1")

# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR_2


df_2=spark.sql("""
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

df_2.createOrReplaceTempView("SKU_TXS_ATTR_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_3


df_3=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_TXS_ATTR_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_3")

# COMMAND ----------
# DBTITLE 1, Fil_SKU_TXS_Attr_Type_4


df_4=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_TXS_ATTR_3 
    WHERE
        SKU_TXS_ATTR_TYPE_ID IN (
            4, 5, 6, 7, 8, 9, 10
        )""")

df_4.createOrReplaceTempView("Fil_SKU_TXS_Attr_Type_4")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_TXS_5


df_5=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        DETAIL.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        MASTER.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID1,
        MASTER.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID1,
        MASTER.SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_1 MASTER 
    INNER JOIN
        Fil_SKU_TXS_Attr_Type_4 DETAIL 
            ON MASTER.SKU_TXS_ATTR_TYPE_ID = SKU_TXS_ATTR_TYPE_ID 
            AND SKU_TXS_ATTR_TYPE_VALUE_ID1 = DETAIL.SKU_TXS_ATTR_TYPE_VALUE_ID""")

df_5.createOrReplaceTempView("Jnr_SKU_TXS_5")

# COMMAND ----------
# DBTITLE 1, Srt_SKU_TXS_Attr_6


df_6=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_SKU_TXS_5 
    ORDER BY
        PRODUCT_ID ASC""")

df_6.createOrReplaceTempView("Srt_SKU_TXS_Attr_6")

# COMMAND ----------
# DBTITLE 1, Agg_SKU_TXS_Attr_7


df_7=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_ID = 4) AS SKU_ATTR4_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_ID = 5) AS SKU_ATTR5_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_ID = 6) AS SKU_ATTR6_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_ID = 7) AS SKU_ATTR7_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_ID = 8) AS SKU_ATTR8_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_ID = 9) AS SKU_ATTR9_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_ID,
        SKU_TXS_ATTR_TYPE_ID = 10) AS SKU_ATTR10_ID,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKU_TXS_ATTR_TYPE_ID = 4) AS SKU_ATTR4_DESC,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKU_TXS_ATTR_TYPE_ID = 5) AS SKU_ATTR5_DESC,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKU_TXS_ATTR_TYPE_ID = 6) AS SKU_ATTR6_DESC,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKU_TXS_ATTR_TYPE_ID = 7) AS SKU_ATTR7_DESC,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKU_TXS_ATTR_TYPE_ID = 8) AS SKU_ATTR8_DESC,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKU_TXS_ATTR_TYPE_ID = 9) AS SKU_ATTR9_DESC,
        FIRST(SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKU_TXS_ATTR_TYPE_ID = 10) AS SKU_ATTR10_DESC 
    FROM
        Srt_SKU_TXS_Attr_6 
    GROUP BY
        PRODUCT_ID""")

df_7.createOrReplaceTempView("Agg_SKU_TXS_Attr_7")

# COMMAND ----------
# DBTITLE 1, SKU_ATTR_8


df_8=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_ATTR1_ID AS SKU_ATTR1_ID,
        SKU_ATTR1_DESC AS SKU_ATTR1_DESC,
        SKU_ATTR2_ID AS SKU_ATTR2_ID,
        SKU_ATTR2_DESC AS SKU_ATTR2_DESC,
        SKU_ATTR3_ID AS SKU_ATTR3_ID,
        SKU_ATTR3_DESC AS SKU_ATTR3_DESC,
        SKU_ATTR4_ID AS SKU_ATTR4_ID,
        SKU_ATTR4_DESC AS SKU_ATTR4_DESC,
        SKU_ATTR5_ID AS SKU_ATTR5_ID,
        SKU_ATTR5_DESC AS SKU_ATTR5_DESC,
        SKU_ATTR6_ID AS SKU_ATTR6_ID,
        SKU_ATTR6_DESC AS SKU_ATTR6_DESC,
        SKU_ATTR7_ID AS SKU_ATTR7_ID,
        SKU_ATTR7_DESC AS SKU_ATTR7_DESC,
        SKU_ATTR8_ID AS SKU_ATTR8_ID,
        SKU_ATTR8_DESC AS SKU_ATTR8_DESC,
        SKU_ATTR9_ID AS SKU_ATTR9_ID,
        SKU_ATTR9_DESC AS SKU_ATTR9_DESC,
        SKU_ATTR10_ID AS SKU_ATTR10_ID,
        SKU_ATTR10_DESC AS SKU_ATTR10_DESC,
        SKU_ATTR11_ID AS SKU_ATTR11_ID,
        SKU_ATTR11_DESC AS SKU_ATTR11_DESC,
        SKU_ATTR12_ID AS SKU_ATTR12_ID,
        SKU_ATTR12_DESC AS SKU_ATTR12_DESC,
        SKU_ATTR13_ID AS SKU_ATTR13_ID,
        SKU_ATTR13_DESC AS SKU_ATTR13_DESC,
        SKU_ATTR14_ID AS SKU_ATTR14_ID,
        SKU_ATTR14_DESC AS SKU_ATTR14_DESC,
        SKU_ATTR15_ID AS SKU_ATTR15_ID,
        SKU_ATTR15_DESC AS SKU_ATTR15_DESC,
        SKU_ATTR16_ID AS SKU_ATTR16_ID,
        SKU_ATTR16_DESC AS SKU_ATTR16_DESC,
        SKU_ATTR17_ID AS SKU_ATTR17_ID,
        SKU_ATTR17_DESC AS SKU_ATTR17_DESC,
        SKU_ATTR18_ID AS SKU_ATTR18_ID,
        SKU_ATTR18_DESC AS SKU_ATTR18_DESC,
        SKU_ATTR19_ID AS SKU_ATTR19_ID,
        SKU_ATTR19_DESC AS SKU_ATTR19_DESC,
        SKU_ATTR20_ID AS SKU_ATTR20_ID,
        SKU_ATTR20_DESC AS SKU_ATTR20_DESC,
        SKU_ATTR21_ID AS SKU_ATTR21_ID,
        SKU_ATTR21_DESC AS SKU_ATTR21_DESC,
        SKU_ATTR22_ID AS SKU_ATTR22_ID,
        SKU_ATTR22_DESC AS SKU_ATTR22_DESC,
        SKU_ATTR23_ID AS SKU_ATTR23_ID,
        SKU_ATTR23_DESC AS SKU_ATTR23_DESC,
        SKU_ATTR24_ID AS SKU_ATTR24_ID,
        SKU_ATTR24_DESC AS SKU_ATTR24_DESC,
        SKU_ATTR25_ID AS SKU_ATTR25_ID,
        SKU_ATTR25_DESC AS SKU_ATTR25_DESC,
        SKU_ATTR26_ID AS SKU_ATTR26_ID,
        SKU_ATTR26_DESC AS SKU_ATTR26_DESC,
        SKU_ATTR27_ID AS SKU_ATTR27_ID,
        SKU_ATTR27_DESC AS SKU_ATTR27_DESC,
        SKU_ATTR28_ID AS SKU_ATTR28_ID,
        SKU_ATTR28_DESC AS SKU_ATTR28_DESC,
        SKU_ATTR29_ID AS SKU_ATTR29_ID,
        SKU_ATTR29_DESC AS SKU_ATTR29_DESC,
        SKU_ATTR30_ID AS SKU_ATTR30_ID,
        SKU_ATTR30_DESC AS SKU_ATTR30_DESC,
        SKU_ATTR31_ID AS SKU_ATTR31_ID,
        SKU_ATTR31_DESC AS SKU_ATTR31_DESC,
        SKU_ATTR32_ID AS SKU_ATTR32_ID,
        SKU_ATTR32_DESC AS SKU_ATTR32_DESC,
        SKU_ATTR33_ID AS SKU_ATTR33_ID,
        SKU_ATTR33_DESC AS SKU_ATTR33_DESC,
        SKU_ATTR34_ID AS SKU_ATTR34_ID,
        SKU_ATTR34_DESC AS SKU_ATTR34_DESC,
        SKU_ATTR35_ID AS SKU_ATTR35_ID,
        SKU_ATTR35_DESC AS SKU_ATTR35_DESC,
        SKU_ATTR36_ID AS SKU_ATTR36_ID,
        SKU_ATTR36_DESC AS SKU_ATTR36_DESC,
        SKU_ATTR37_ID AS SKU_ATTR37_ID,
        SKU_ATTR37_DESC AS SKU_ATTR37_DESC,
        SKU_ATTR38_ID AS SKU_ATTR38_ID,
        SKU_ATTR38_DESC AS SKU_ATTR38_DESC,
        SKU_ATTR39_ID AS SKU_ATTR39_ID,
        SKU_ATTR39_DESC AS SKU_ATTR39_DESC,
        SKU_ATTR40_ID AS SKU_ATTR40_ID,
        SKU_ATTR40_DESC AS SKU_ATTR40_DESC,
        SKU_ATTR41_ID AS SKU_ATTR41_ID,
        SKU_ATTR41_DESC AS SKU_ATTR41_DESC,
        SKU_ATTR42_ID AS SKU_ATTR42_ID,
        SKU_ATTR42_DESC AS SKU_ATTR42_DESC,
        SKU_ATTR43_ID AS SKU_ATTR43_ID,
        SKU_ATTR43_DESC AS SKU_ATTR43_DESC,
        SKU_ATTR44_ID AS SKU_ATTR44_ID,
        SKU_ATTR44_DESC AS SKU_ATTR44_DESC,
        SKU_ATTR45_ID AS SKU_ATTR45_ID,
        SKU_ATTR45_DESC AS SKU_ATTR45_DESC,
        SKU_ATTR46_ID AS SKU_ATTR46_ID,
        SKU_ATTR46_DESC AS SKU_ATTR46_DESC,
        SKU_ATTR47_ID AS SKU_ATTR47_ID,
        SKU_ATTR47_DESC AS SKU_ATTR47_DESC,
        SKU_ATTR48_ID AS SKU_ATTR48_ID,
        SKU_ATTR48_DESC AS SKU_ATTR48_DESC,
        SKU_ATTR49_ID AS SKU_ATTR49_ID,
        SKU_ATTR49_DESC AS SKU_ATTR49_DESC,
        SKU_ATTR50_ID AS SKU_ATTR50_ID,
        SKU_ATTR50_DESC AS SKU_ATTR50_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_ATTR""")

df_8.createOrReplaceTempView("SKU_ATTR_8")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_ATTR_9


df_9=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_ATTR_8""")

df_9.createOrReplaceTempView("SQ_Shortcut_to_SKU_ATTR_9")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_Attr_10


df_10=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        MASTER.PRODUCT_ID AS PRODUCT_ID1,
        DETAIL.SKU_ATTR4_ID AS SKU_ATTR4_ID,
        DETAIL.SKU_ATTR5_ID AS SKU_ATTR5_ID,
        DETAIL.SKU_ATTR6_ID AS SKU_ATTR6_ID,
        DETAIL.SKU_ATTR7_ID AS SKU_ATTR7_ID,
        DETAIL.SKU_ATTR8_ID AS SKU_ATTR8_ID,
        DETAIL.SKU_ATTR9_ID AS SKU_ATTR9_ID,
        DETAIL.SKU_ATTR10_ID AS SKU_ATTR10_ID,
        DETAIL.SKU_ATTR4_DESC AS SKU_ATTR4_DESC,
        DETAIL.SKU_ATTR5_DESC AS SKU_ATTR5_DESC,
        DETAIL.SKU_ATTR6_DESC AS SKU_ATTR6_DESC,
        DETAIL.SKU_ATTR7_DESC AS SKU_ATTR7_DESC,
        DETAIL.SKU_ATTR8_DESC AS SKU_ATTR8_DESC,
        DETAIL.SKU_ATTR9_DESC AS SKU_ATTR9_DESC,
        DETAIL.SKU_ATTR10_DESC AS SKU_ATTR10_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_ATTR_9 MASTER 
    RIGHT JOIN
        Agg_SKU_TXS_Attr_7 DETAIL 
            ON MASTER.PRODUCT_ID = DETAIL.PRODUCT_ID""")

df_10.createOrReplaceTempView("Jnr_SKU_Attr_10")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_Attr_11


df_11=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_ATTR4_ID AS SKU_ATTR4_ID,
        SKU_ATTR5_ID AS SKU_ATTR5_ID,
        SKU_ATTR6_ID AS SKU_ATTR6_ID,
        SKU_ATTR7_ID AS SKU_ATTR7_ID,
        SKU_ATTR8_ID AS SKU_ATTR8_ID,
        SKU_ATTR9_ID AS SKU_ATTR9_ID,
        SKU_ATTR10_ID AS SKU_ATTR10_ID,
        SKU_ATTR4_DESC AS SKU_ATTR4_DESC,
        SKU_ATTR5_DESC AS SKU_ATTR5_DESC,
        SKU_ATTR6_DESC AS SKU_ATTR6_DESC,
        SKU_ATTR7_DESC AS SKU_ATTR7_DESC,
        SKU_ATTR8_DESC AS SKU_ATTR8_DESC,
        SKU_ATTR9_DESC AS SKU_ATTR9_DESC,
        SKU_ATTR10_DESC AS SKU_ATTR10_DESC,
        current_timestamp AS UPDATE_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_SKU_Attr_10""")

df_11.createOrReplaceTempView("Exp_SKU_Attr_11")

# COMMAND ----------
# DBTITLE 1, Ups_SKU_Attr_12


df_12=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_ATTR4_ID AS SKU_ATTR4_ID,
        SKU_ATTR5_ID AS SKU_ATTR5_ID,
        SKU_ATTR6_ID AS SKU_ATTR6_ID,
        SKU_ATTR7_ID AS SKU_ATTR7_ID,
        SKU_ATTR8_ID AS SKU_ATTR8_ID,
        SKU_ATTR9_ID AS SKU_ATTR9_ID,
        SKU_ATTR10_ID AS SKU_ATTR10_ID,
        SKU_ATTR4_DESC AS SKU_ATTR4_DESC,
        SKU_ATTR5_DESC AS SKU_ATTR5_DESC,
        SKU_ATTR6_DESC AS SKU_ATTR6_DESC,
        SKU_ATTR7_DESC AS SKU_ATTR7_DESC,
        SKU_ATTR8_DESC AS SKU_ATTR8_DESC,
        SKU_ATTR9_DESC AS SKU_ATTR9_DESC,
        SKU_ATTR10_DESC AS SKU_ATTR10_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_SKU_Attr_11""")

df_12.createOrReplaceTempView("Ups_SKU_Attr_12")

# COMMAND ----------
# DBTITLE 1, SKU_ATTR


spark.sql("""INSERT INTO SKU_ATTR SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_ATTR1_ID AS SKU_ATTR1_ID,
SKU_ATTR1_DESC AS SKU_ATTR1_DESC,
SKU_ATTR2_ID AS SKU_ATTR2_ID,
SKU_ATTR2_DESC AS SKU_ATTR2_DESC,
SKU_SAP_KOI_ID AS SKU_ATTR3_ID,
SKU_SAP_KOI_DESC AS SKU_ATTR3_DESC,
SKU_ATTR4_ID AS SKU_ATTR4_ID,
SKU_ATTR4_DESC AS SKU_ATTR4_DESC,
SKU_ATTR5_ID AS SKU_ATTR5_ID,
SKU_ATTR5_DESC AS SKU_ATTR5_DESC,
SKU_ATTR6_ID AS SKU_ATTR6_ID,
SKU_ATTR6_DESC AS SKU_ATTR6_DESC,
SKU_ATTR7_ID AS SKU_ATTR7_ID,
SKU_ATTR7_DESC AS SKU_ATTR7_DESC,
SKU_ATTR8_ID AS SKU_ATTR8_ID,
SKU_ATTR8_DESC AS SKU_ATTR8_DESC,
SKU_ATTR9_ID AS SKU_ATTR9_ID,
SKU_ATTR9_DESC AS SKU_ATTR9_DESC,
SKU_ATTR10_ID AS SKU_ATTR10_ID,
SKU_ATTR10_DESC AS SKU_ATTR10_DESC,
SKU_SAP_PROD_GROUP_ID AS SKU_ATTR11_ID,
SKU_SAP_PROD_GROUP_DESC AS SKU_ATTR11_DESC,
SKU_SAP_FAMILY_ID AS SKU_ATTR12_ID,
SKU_SAP_FAMILY_DESC AS SKU_ATTR12_DESC,
SKU_SAP_PPU_GROUP_ID AS SKU_ATTR13_ID,
SKU_SAP_PPU_GROUP_DESC AS SKU_ATTR13_DESC,
SKU_SAP_PRVT_LABEL_ID AS SKU_ATTR14_ID,
SKU_SAP_PRVT_LABEL_DESC AS SKU_ATTR14_DESC,
SKU_SAP_PRVT_LABEL_TIER_ID AS SKU_ATTR15_ID,
SKU_SAP_PRVT_LABEL_TIER_DESC AS SKU_ATTR15_DESC,
SKU_SAP_PRVT_LABEL_PARENT_TIER_ID AS SKU_ATTR16_ID,
SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC AS SKU_ATTR16_DESC,
SKU_SAP_MAP_ID AS SKU_ATTR17_ID,
SKU_SAP_MAP_DESC AS SKU_ATTR17_DESC,
SKU_ATTR18_ID AS SKU_ATTR18_ID,
SKU_ATTR18_DESC AS SKU_ATTR18_DESC,
SKU_ATTR19_ID AS SKU_ATTR19_ID,
SKU_ATTR19_DESC AS SKU_ATTR19_DESC,
SKU_ATTR20_ID AS SKU_ATTR20_ID,
SKU_ATTR20_DESC AS SKU_ATTR20_DESC,
SKU_ATTR21_ID AS SKU_ATTR21_ID,
SKU_ATTR21_DESC AS SKU_ATTR21_DESC,
SKU_ATTR22_ID AS SKU_ATTR22_ID,
SKU_ATTR22_DESC AS SKU_ATTR22_DESC,
SKU_ATTR23_ID AS SKU_ATTR23_ID,
SKU_ATTR23_DESC AS SKU_ATTR23_DESC,
SKU_ATTR24_ID AS SKU_ATTR24_ID,
SKU_ATTR24_DESC AS SKU_ATTR24_DESC,
SKU_ATTR25_ID AS SKU_ATTR25_ID,
SKU_ATTR25_DESC AS SKU_ATTR25_DESC,
SKU_ATTR26_ID AS SKU_ATTR26_ID,
SKU_ATTR26_DESC AS SKU_ATTR26_DESC,
SKU_ATTR27_ID AS SKU_ATTR27_ID,
SKU_ATTR27_DESC AS SKU_ATTR27_DESC,
SKU_ATTR28_ID AS SKU_ATTR28_ID,
SKU_ATTR28_DESC AS SKU_ATTR28_DESC,
SKU_ATTR29_ID AS SKU_ATTR29_ID,
SKU_ATTR29_DESC AS SKU_ATTR29_DESC,
SKU_ATTR30_ID AS SKU_ATTR30_ID,
SKU_ATTR30_DESC AS SKU_ATTR30_DESC,
SKU_ATTR31_ID AS SKU_ATTR31_ID,
SKU_ATTR31_DESC AS SKU_ATTR31_DESC,
SKU_ATTR32_ID AS SKU_ATTR32_ID,
SKU_ATTR32_DESC AS SKU_ATTR32_DESC,
SKU_ATTR33_ID AS SKU_ATTR33_ID,
SKU_ATTR33_DESC AS SKU_ATTR33_DESC,
SKU_ATTR34_ID AS SKU_ATTR34_ID,
SKU_ATTR34_DESC AS SKU_ATTR34_DESC,
SKU_ATTR35_ID AS SKU_ATTR35_ID,
SKU_ATTR35_DESC AS SKU_ATTR35_DESC,
SKU_ATTR36_ID AS SKU_ATTR36_ID,
SKU_ATTR36_DESC AS SKU_ATTR36_DESC,
SKU_ATTR37_ID AS SKU_ATTR37_ID,
SKU_ATTR37_DESC AS SKU_ATTR37_DESC,
SKU_ATTR38_ID AS SKU_ATTR38_ID,
SKU_ATTR38_DESC AS SKU_ATTR38_DESC,
SKU_ATTR39_ID AS SKU_ATTR39_ID,
SKU_ATTR39_DESC AS SKU_ATTR39_DESC,
SKU_ATTR40_ID AS SKU_ATTR40_ID,
SKU_ATTR40_DESC AS SKU_ATTR40_DESC,
SKU_ATTR41_ID AS SKU_ATTR41_ID,
SKU_ATTR41_DESC AS SKU_ATTR41_DESC,
SKU_ATTR42_ID AS SKU_ATTR42_ID,
SKU_ATTR42_DESC AS SKU_ATTR42_DESC,
SKU_ATTR43_ID AS SKU_ATTR43_ID,
SKU_ATTR43_DESC AS SKU_ATTR43_DESC,
SKU_ATTR44_ID AS SKU_ATTR44_ID,
SKU_ATTR44_DESC AS SKU_ATTR44_DESC,
SKU_ATTR45_ID AS SKU_ATTR45_ID,
SKU_ATTR45_DESC AS SKU_ATTR45_DESC,
SKU_ATTR46_ID AS SKU_ATTR46_ID,
SKU_ATTR46_DESC AS SKU_ATTR46_DESC,
SKU_ATTR47_ID AS SKU_ATTR47_ID,
SKU_ATTR47_DESC AS SKU_ATTR47_DESC,
SKU_ATTR48_ID AS SKU_ATTR48_ID,
SKU_ATTR48_DESC AS SKU_ATTR48_DESC,
SKU_ATTR49_ID AS SKU_ATTR49_ID,
SKU_ATTR49_DESC AS SKU_ATTR49_DESC,
SKU_ATTR50_ID AS SKU_ATTR50_ID,
SKU_ATTR50_DESC AS SKU_ATTR50_DESC,
o_UPDATE_TSTMP AS UPDATE_TSTMP,
o_LOAD_TSTMP AS LOAD_TSTMP FROM Ups_SKU_Attr_12""")