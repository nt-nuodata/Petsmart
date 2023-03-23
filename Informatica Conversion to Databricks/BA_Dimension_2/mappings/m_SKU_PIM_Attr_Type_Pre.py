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
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PIMWebstyle_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PIMWebstyle_1")

# COMMAND ----------
# DBTITLE 1, Agg_WebStyle_2


df_2=spark.sql("""SELECT MAX(PIMWebstyle) AS PIMWebstyle1 FROM SQ_Shortcut_to_PIMWebstyle_1 GROUP BY """)

df_2.createOrReplaceTempView("Agg_WebStyle_2")

# COMMAND ----------
# DBTITLE 1, Exp_WebStyle_3


df_3=spark.sql("""
    SELECT
        0 AS PIM_ATTR_ID,
        'Web Style' AS PIM_ATTR_TAG,
        'Web Style' AS PIM_ATTR_NAME,
        'Web Style' AS PIM_ATTR_DISPLAY_NAME,
        NULL AS SLICING_ATTR_IND2,
        NULL AS MULTI_VAL_ASSIGN_IND2,
        NULL AS INTERNAL_AUD_IND2,
        NULL AS SIZE_ATTR_IND2,
        NULL AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Agg_WebStyle_2""")

df_3.createOrReplaceTempView("Exp_WebStyle_3")

# COMMAND ----------
# DBTITLE 1, PIMAttribute_4


df_4=spark.sql("""
    SELECT
        PIMAttrID AS PIMAttrID,
        PIMAttrTag AS PIMAttrTag,
        PIMAttrName AS PIMAttrName,
        PIMAttrDisplayName AS PIMAttrDisplayName,
        SlicingAttrInd AS SlicingAttrInd,
        MultiValAssignInd AS MultiValAssignInd,
        InternalAudInd AS InternalAudInd,
        SizeAttrInd AS SizeAttrInd,
        DelInd AS DelInd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PIMAttribute""")

df_4.createOrReplaceTempView("PIMAttribute_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PIMAttribute_5


df_5=spark.sql("""
    SELECT
        PIMAttrID AS PIMAttrID,
        PIMAttrTag AS PIMAttrTag,
        PIMAttrName AS PIMAttrName,
        PIMAttrDisplayName AS PIMAttrDisplayName,
        SlicingAttrInd AS SlicingAttrInd,
        MultiValAssignInd AS MultiValAssignInd,
        InternalAudInd AS InternalAudInd,
        SizeAttrInd AS SizeAttrInd,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PIMAttribute_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_PIMAttribute_5")

# COMMAND ----------
# DBTITLE 1, Union_6


df_6=spark.sql("""SELECT DelInd AS DEL_IND,
InternalAudInd AS INTERNAL_AUD_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
MultiValAssignInd AS MULTI_VAL_ASSIGN_IND,
PIMAttrDisplayName AS PIM_ATTR_DISPLAY_NAME,
PIMAttrID AS PIM_ATTR_ID,
PIMAttrName AS PIM_ATTR_NAME,
PIMAttrTag AS PIM_ATTR_TAG,
SizeAttrInd AS SIZE_ATTR_IND,
SlicingAttrInd AS SLICING_ATTR_IND FROM SQ_Shortcut_to_PIMAttribute_5 UNION ALL SELECT DelInd AS DEL_IND,
INTERNAL_AUD_IND2 AS INTERNAL_AUD_IND,
MULTI_VAL_ASSIGN_IND2 AS MULTI_VAL_ASSIGN_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
PIM_ATTR_ID AS PIM_ATTR_ID,
PIM_ATTR_NAME AS PIM_ATTR_NAME,
PIM_ATTR_TAG AS PIM_ATTR_TAG,
SIZE_ATTR_IND2 AS SIZE_ATTR_IND,
SLICING_ATTR_IND2 AS SLICING_ATTR_IND FROM Exp_WebStyle_3""")

df_6.createOrReplaceTempView("Union_6")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_PIM_Attr_Type_Pre_7


df_7=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_TAG AS PIM_ATTR_TAG,
        PIM_ATTR_NAME AS PIM_ATTR_NAME,
        PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        DEL_IND AS DEL_IND,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Union_6""")

df_7.createOrReplaceTempView("Exp_SKU_PIM_Attr_Type_Pre_7")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_PRE


spark.sql("""INSERT INTO SKU_PIM_ATTR_TYPE_PRE SELECT PIM_ATTR_ID AS PIM_ATTR_ID,
PIM_ATTR_TAG AS PIM_ATTR_TAG,
PIM_ATTR_NAME AS PIM_ATTR_NAME,
PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
SLICING_ATTR_IND AS SLICING_ATTR_IND,
MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
SIZE_ATTR_IND AS SIZE_ATTR_IND,
DEL_IND AS DEL_IND,
LOAD_TSTMP AS LOAD_TSTMP FROM Exp_SKU_PIM_Attr_Type_Pre_7""")