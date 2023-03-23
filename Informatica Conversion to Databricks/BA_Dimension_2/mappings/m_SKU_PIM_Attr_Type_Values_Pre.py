# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, PIMWebstyle_0


df_0=spark.sql("""
    SELECT
        PIMWebstyle AS PIMWebstyle,
        PIMProductTitle AS PIMProductTitle,
        CopyRequiredFlg AS CopyRequiredFlg,
        SpecificationReqdFlg AS SpecificationReqdFlg,
        Keywords AS Keywords,
        ShortDesc AS ShortDesc,
        ImageReqFlg AS ImageReqFlg,
        ProductTitleStatus AS ProductTitleStatus,
        SliceAttrId1 AS SliceAttrId1,
        SliceAttrId2 AS SliceAttrId2,
        DelInd AS DelInd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PIMWebstyle""")

df_0.createOrReplaceTempView("PIMWebstyle_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PIMWebstyle_1


df_1=spark.sql("""
    SELECT
        PIMWebstyle AS PIMWebstyle,
        PIMProductTitle AS PIMProductTitle,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PIMWebstyle_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PIMWebstyle_1")

# COMMAND ----------
# DBTITLE 1, Exp_WebStyle_2


df_2=spark.sql("""
    SELECT
        0 AS PIM_ATTR_ID2,
        PIMWebstyle AS PIMWebstyle,
        PIMProductTitle AS PIMProductTitle,
        NULL AS ENTRY_POSITION2,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PIMWebstyle_1""")

df_2.createOrReplaceTempView("Exp_WebStyle_2")

# COMMAND ----------
# DBTITLE 1, PIMAttrValues_3


df_3=spark.sql("""
    SELECT
        PIMAttrID AS PIMAttrID,
        PIMAttrValID AS PIMAttrValID,
        PIMAttrValDesc AS PIMAttrValDesc,
        EntryPosition AS EntryPosition,
        DelInd AS DelInd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PIMAttrValues""")

df_3.createOrReplaceTempView("PIMAttrValues_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PIMAttrValues_4


df_4=spark.sql("""
    SELECT
        PIMAttrID AS PIMAttrID,
        PIMAttrValID AS PIMAttrValID,
        PIMAttrValDesc AS PIMAttrValDesc,
        EntryPosition AS EntryPosition,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PIMAttrValues_3""")

df_4.createOrReplaceTempView("SQ_Shortcut_to_PIMAttrValues_4")

# COMMAND ----------
# DBTITLE 1, Union_5


df_5=spark.sql("""SELECT DelInd AS DEL_IND,
EntryPosition AS ENTRY_POSITION,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PIMAttrID AS PIM_ATTR_ID,
PIMAttrValDesc AS PIM_ATTR_VAL_DESC,
PIMAttrValID AS PIM_ATTR_VAL_ID FROM SQ_Shortcut_to_PIMAttrValues_4 UNION ALL SELECT DelInd AS DEL_IND,
ENTRY_POSITION2 AS ENTRY_POSITION,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PIMProductTitle AS PIM_ATTR_VAL_DESC,
PIMWebstyle AS PIM_ATTR_VAL_ID,
PIM_ATTR_ID2 AS PIM_ATTR_ID FROM Exp_WebStyle_2""")

df_5.createOrReplaceTempView("Union_5")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_PIM_Attr_Values_Pre_6


df_6=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
        DEL_IND AS DEL_IND,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Union_5""")

df_6.createOrReplaceTempView("Exp_SKU_PIM_Attr_Values_Pre_6")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_VALUES_PRE


spark.sql("""INSERT INTO SKU_PIM_ATTR_TYPE_VALUES_PRE SELECT PIM_ATTR_ID AS PIM_ATTR_ID,
PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
ENTRY_POSITION AS ENTRY_POSITION,
DEL_IND AS DEL_IND,
LOAD_TSTMP AS LOAD_TSTMP FROM Exp_SKU_PIM_Attr_Values_Pre_6""")