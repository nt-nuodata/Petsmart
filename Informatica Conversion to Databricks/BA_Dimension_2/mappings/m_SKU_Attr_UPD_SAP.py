# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SKU_SAP_ATTR_0

df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_SAP_PROD_GROUP_ID AS SKU_SAP_PROD_GROUP_ID,
        SKU_SAP_PROD_GROUP_DESC AS SKU_SAP_PROD_GROUP_DESC,
        SKU_SAP_FAMILY_ID AS SKU_SAP_FAMILY_ID,
        SKU_SAP_FAMILY_DESC AS SKU_SAP_FAMILY_DESC,
        SKU_SAP_PPU_GROUP_ID AS SKU_SAP_PPU_GROUP_ID,
        SKU_SAP_PPU_GROUP_DESC AS SKU_SAP_PPU_GROUP_DESC,
        SKU_SAP_PRVT_LABEL_ID AS SKU_SAP_PRVT_LABEL_ID,
        SKU_SAP_PRVT_LABEL_DESC AS SKU_SAP_PRVT_LABEL_DESC,
        SKU_SAP_PRVT_LABEL_TIER_ID AS SKU_SAP_PRVT_LABEL_TIER_ID,
        SKU_SAP_PRVT_LABEL_TIER_DESC AS SKU_SAP_PRVT_LABEL_TIER_DESC,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_ID AS SKU_SAP_PRVT_LABEL_PARENT_TIER_ID,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC AS SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC,
        SKU_SAP_MAP_ID AS SKU_SAP_MAP_ID,
        SKU_SAP_MAP_DESC AS SKU_SAP_MAP_DESC,
        SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
        SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_SAP_ATTR""")

df_0.createOrReplaceTempView("SKU_SAP_ATTR_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_SAP_ATTR_1

df_1=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_SAP_PROD_GROUP_ID AS SKU_SAP_PROD_GROUP_ID,
        SKU_SAP_PROD_GROUP_DESC AS SKU_SAP_PROD_GROUP_DESC,
        SKU_SAP_FAMILY_ID AS SKU_SAP_FAMILY_ID,
        SKU_SAP_FAMILY_DESC AS SKU_SAP_FAMILY_DESC,
        SKU_SAP_PPU_GROUP_ID AS SKU_SAP_PPU_GROUP_ID,
        SKU_SAP_PPU_GROUP_DESC AS SKU_SAP_PPU_GROUP_DESC,
        SKU_SAP_PRVT_LABEL_ID AS SKU_SAP_PRVT_LABEL_ID,
        SKU_SAP_PRVT_LABEL_DESC AS SKU_SAP_PRVT_LABEL_DESC,
        SKU_SAP_PRVT_LABEL_TIER_ID AS SKU_SAP_PRVT_LABEL_TIER_ID,
        SKU_SAP_PRVT_LABEL_TIER_DESC AS SKU_SAP_PRVT_LABEL_TIER_DESC,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_ID AS SKU_SAP_PRVT_LABEL_PARENT_TIER_ID,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC AS SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC,
        SKU_SAP_MAP_ID AS SKU_SAP_MAP_ID,
        SKU_SAP_MAP_DESC AS SKU_SAP_MAP_DESC,
        SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
        SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_SAP_ATTR_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_SAP_ATTR_1")

# COMMAND ----------

# DBTITLE 1, SKU_ATTR_2

df_2=spark.sql("""
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

df_2.createOrReplaceTempView("SKU_ATTR_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_ATTR_3

df_3=spark.sql("""
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
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_ATTR_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_ATTR_3")

# COMMAND ----------

# DBTITLE 1, JNR_SKU_TGT_4

df_4=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.SKU_SAP_PROD_GROUP_ID AS SKU_SAP_PROD_GROUP_ID,
        DETAIL.SKU_SAP_PROD_GROUP_DESC AS SKU_SAP_PROD_GROUP_DESC,
        DETAIL.SKU_SAP_FAMILY_ID AS SKU_SAP_FAMILY_ID,
        DETAIL.SKU_SAP_FAMILY_DESC AS SKU_SAP_FAMILY_DESC,
        DETAIL.SKU_SAP_PPU_GROUP_ID AS SKU_SAP_PPU_GROUP_ID,
        DETAIL.SKU_SAP_PPU_GROUP_DESC AS SKU_SAP_PPU_GROUP_DESC,
        DETAIL.SKU_SAP_PRVT_LABEL_ID AS SKU_SAP_PRVT_LABEL_ID,
        DETAIL.SKU_SAP_PRVT_LABEL_DESC AS SKU_SAP_PRVT_LABEL_DESC,
        DETAIL.SKU_SAP_PRVT_LABEL_TIER_ID AS SKU_SAP_PRVT_LABEL_TIER_ID,
        DETAIL.SKU_SAP_PRVT_LABEL_TIER_DESC AS SKU_SAP_PRVT_LABEL_TIER_DESC,
        DETAIL.SKU_SAP_PRVT_LABEL_PARENT_TIER_ID AS SKU_SAP_PRVT_LABEL_PARENT_TIER_ID,
        DETAIL.SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC AS SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC,
        DETAIL.SKU_SAP_MAP_ID AS SKU_SAP_MAP_ID,
        DETAIL.SKU_SAP_MAP_DESC AS SKU_SAP_MAP_DESC,
        DETAIL.SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
        DETAIL.SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
        MASTER.PRODUCT_ID AS PRODUCT_ID1,
        MASTER.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_ATTR_3 MASTER 
    INNER JOIN
        SQ_Shortcut_to_SKU_SAP_ATTR_1 DETAIL 
            ON MASTER.PRODUCT_ID = DETAIL.PRODUCT_ID""")

df_4.createOrReplaceTempView("JNR_SKU_TGT_4")

# COMMAND ----------

# DBTITLE 1, EXP_UPDT_TSTMP_5

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_SAP_PROD_GROUP_ID AS SKU_SAP_PROD_GROUP_ID,
        SKU_SAP_PROD_GROUP_DESC AS SKU_SAP_PROD_GROUP_DESC,
        SKU_SAP_FAMILY_ID AS SKU_SAP_FAMILY_ID,
        SKU_SAP_FAMILY_DESC AS SKU_SAP_FAMILY_DESC,
        SKU_SAP_PPU_GROUP_ID AS SKU_SAP_PPU_GROUP_ID,
        SKU_SAP_PPU_GROUP_DESC AS SKU_SAP_PPU_GROUP_DESC,
        SKU_SAP_PRVT_LABEL_ID AS SKU_SAP_PRVT_LABEL_ID,
        SKU_SAP_PRVT_LABEL_DESC AS SKU_SAP_PRVT_LABEL_DESC,
        SKU_SAP_PRVT_LABEL_TIER_ID AS SKU_SAP_PRVT_LABEL_TIER_ID,
        SKU_SAP_PRVT_LABEL_TIER_DESC AS SKU_SAP_PRVT_LABEL_TIER_DESC,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_ID AS SKU_SAP_PRVT_LABEL_PARENT_TIER_ID,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC AS SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC,
        SKU_SAP_MAP_ID AS SKU_SAP_MAP_ID,
        SKU_SAP_MAP_DESC AS SKU_SAP_MAP_DESC,
        SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
        SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
        PRODUCT_ID1 AS PRODUCT_ID1,
        IFF(ISNULL(LOAD_TSTMP),
        SYSTIMESTAMP(),
        LOAD_TSTMP) AS o_LOAD_TSTMP,
        SYSTIMESTAMP() AS o_UPDATE_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_SKU_TGT_4""")

df_5.createOrReplaceTempView("EXP_UPDT_TSTMP_5")

# COMMAND ----------

# DBTITLE 1, UPD_SKU_ATTR_6

df_6=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_SAP_PROD_GROUP_ID AS SKU_SAP_PROD_GROUP_ID,
        SKU_SAP_PROD_GROUP_DESC AS SKU_SAP_PROD_GROUP_DESC,
        SKU_SAP_FAMILY_ID AS SKU_SAP_FAMILY_ID,
        SKU_SAP_FAMILY_DESC AS SKU_SAP_FAMILY_DESC,
        SKU_SAP_PPU_GROUP_ID AS SKU_SAP_PPU_GROUP_ID,
        SKU_SAP_PPU_GROUP_DESC AS SKU_SAP_PPU_GROUP_DESC,
        SKU_SAP_PRVT_LABEL_ID AS SKU_SAP_PRVT_LABEL_ID,
        SKU_SAP_PRVT_LABEL_DESC AS SKU_SAP_PRVT_LABEL_DESC,
        SKU_SAP_PRVT_LABEL_TIER_ID AS SKU_SAP_PRVT_LABEL_TIER_ID,
        SKU_SAP_PRVT_LABEL_TIER_DESC AS SKU_SAP_PRVT_LABEL_TIER_DESC,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_ID AS SKU_SAP_PRVT_LABEL_PARENT_TIER_ID,
        SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC AS SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC,
        SKU_SAP_MAP_ID AS SKU_SAP_MAP_ID,
        SKU_SAP_MAP_DESC AS SKU_SAP_MAP_DESC,
        SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
        SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
        PRODUCT_ID1 AS PRODUCT_ID1,
        o_LOAD_TSTMP AS o_LOAD_TSTMP,
        o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_UPDT_TSTMP_5""")

df_6.createOrReplaceTempView("UPD_SKU_ATTR_6")

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
o_LOAD_TSTMP AS LOAD_TSTMP FROM UPD_SKU_ATTR_6""")
