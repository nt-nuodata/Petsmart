# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, PIMArticleAttr_0

df_0=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        PIMAttrID AS PIMAttrID,
        PIMAttrValID AS PIMAttrValID,
        SliceInd AS SliceInd,
        SliceSeqNbr AS SliceSeqNbr,
        DelInd AS DelInd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PIMArticleAttr""")

df_0.createOrReplaceTempView("PIMArticleAttr_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PIMArticleAttr_1

df_1=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        PIMAttrID AS PIMAttrID,
        PIMAttrValID AS PIMAttrValID,
        SliceInd AS SliceInd,
        SliceSeqNbr AS SliceSeqNbr,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PIMArticleAttr_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_PIMArticleAttr_1")

# COMMAND ----------

# DBTITLE 1, PIMArticleAssignment_2

df_2=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        PIMCategoryNbr AS PIMCategoryNbr,
        PIMWebStyle AS PIMWebStyle,
        CopySt AS CopySt,
        PIMArticleAttrSt AS PIMArticleAttrSt,
        PIMArtSlicingAttrSt AS PIMArtSlicingAttrSt,
        PIMImageSt AS PIMImageSt,
        LiveOnSiteDt AS LiveOnSiteDt,
        CantoEmbargoDt AS CantoEmbargoDt,
        ImageRefreshRequestReason AS ImageRefreshRequestReason,
        ImageRefreshComments AS ImageRefreshComments,
        DelInd AS DelInd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PIMArticleAssignment""")

df_2.createOrReplaceTempView("PIMArticleAssignment_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PIMArticleAssignment_3

df_3=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        PIMWebStyle AS PIMWebStyle,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PIMArticleAssignment_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PIMArticleAssignment_3")

# COMMAND ----------

# DBTITLE 1, FILTRANS_4

df_4=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        PIMWebStyle AS PIMWebStyle,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PIMArticleAssignment_3 
    WHERE
        PIMWebStyle <> 0 
        AND NOT ISNULL(LTRIM(RTRIM(ArticleNbr)))""")

df_4.createOrReplaceTempView("FILTRANS_4")

# COMMAND ----------

# DBTITLE 1, Exp_PIMWebStyle_5

df_5=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        PIMWebStyle AS PIMWebStyle,
        0 AS PIMATTRID2,
        NULL AS SLICEIND2,
        NULL AS SLICESEQNBR2,
        DelInd AS DelInd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FILTRANS_4""")

df_5.createOrReplaceTempView("Exp_PIMWebStyle_5")

# COMMAND ----------

# DBTITLE 1, Union_6

df_6=spark.sql("""SELECT ArticleNbr AS ARTICLENBR,
DelInd AS DELIND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PIMAttrID AS PIMATTRID,
PIMAttrValID AS PIMATTRVALID,
SliceInd AS SLICEIND,
SliceSeqNbr AS SLICESEQNBR FROM SQ_Shortcut_to_PIMArticleAttr_1 UNION ALL SELECT ArticleNbr AS ARTICLENBR,
DelInd AS DELIND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PIMATTRID2 AS PIMATTRID,
PIMWebStyle AS PIMATTRVALID,
SLICEIND2 AS SLICEIND,
SLICESEQNBR2 AS SLICESEQNBR FROM Exp_PIMWebStyle_5""")

df_6.createOrReplaceTempView("Union_6")

# COMMAND ----------

# DBTITLE 1, Exp_Load_Tstmp_7

df_7=spark.sql("""
    SELECT
        ARTICLENBR AS ARTICLENBR,
        PIMATTRID AS PIMATTRID,
        PIMATTRVALID AS PIMATTRVALID,
        SLICEIND AS SLICEIND,
        SLICESEQNBR AS SLICESEQNBR,
        DELIND AS DELIND,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Union_6""")

df_7.createOrReplaceTempView("Exp_Load_Tstmp_7")

# COMMAND ----------

# DBTITLE 1, SKU_PIM_ATTR_PRE

spark.sql("""INSERT INTO SKU_PIM_ATTR_PRE SELECT ARTICLENBR AS ARTICLE_NBR,
PIMATTRID AS PIM_ATTR_ID,
PIMATTRVALID AS PIM_ATTR_VAL_ID,
SLICEIND AS SLICE_IND,
SLICESEQNBR AS SLICE_SEQ_NBR,
DELIND AS DEL_IND,
LOAD_TSTMP AS LOAD_TSTMP FROM Exp_Load_Tstmp_7""")
