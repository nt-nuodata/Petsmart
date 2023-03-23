# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, VENDOR_SUBRANGE_PRE_0

df_0=spark.sql("""
    SELECT
        VENDOR_NBR AS VENDOR_NBR,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        VENDOR_SUBRANGE_DESC AS VENDOR_SUBRANGE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_SUBRANGE_PRE""")

df_0.createOrReplaceTempView("VENDOR_SUBRANGE_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_1

df_1=spark.sql("""
    SELECT
        VENDOR_NBR AS VENDOR_NBR,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
        VENDOR_SUBRANGE_DESC AS VENDOR_SUBRANGE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_SUBRANGE_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_1")

# COMMAND ----------

# DBTITLE 1, EXP_Subrange_2

df_2=spark.sql("""
    SELECT
        SubRangeCD AS SubRangeCD,
        VendorNbr AS VendorNbr,
        SubRangeDesc AS SubRangeDesc,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_1""")

df_2.createOrReplaceTempView("EXP_Subrange_2")

# COMMAND ----------

# DBTITLE 1, LKP_Vendor_Profile_3

df_3=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        EXP_Subrange_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE 
    RIGHT OUTER JOIN
        EXP_Subrange_2 
            ON VENDOR_TYPE_ID = in_Vendor_Type_Id 
            AND VENDOR_PROFILE.VENDOR_NBR = EXP_Subrange_2.VendorNbr""")

df_3.createOrReplaceTempView("LKP_Vendor_Profile_3")

# COMMAND ----------

# DBTITLE 1, EXP_Final_4

df_4=spark.sql("""
    SELECT
        LKP_Vendor_Profile_3.VENDOR_ID AS VENDOR_ID,
        ltrim(rtrim(SubRangeCD)) AS lkp_SubRangeCD1,
        IFF(ISNULL(SubRangeCD),
        ' ',
        SubRangeCD) AS SubRangeCD,
        EXP_Subrange_2.SubRangeDesc AS SubRangeDesc,
        LKP_Vendor_Profile_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_Vendor_Profile_3 
    INNER JOIN
        EXP_Subrange_2 
            ON LKP_Vendor_Profile_3.Monotonically_Increasing_Id = EXP_Subrange_2.Monotonically_Increasing_Id""")

df_4.createOrReplaceTempView("EXP_Final_4")

# COMMAND ----------

# DBTITLE 1, LKP_Vendor_Subrange1_5

df_5=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        EXP_Final_4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            VENDOR_SUBRANGE.VENDOR_ID AS VENDOR_ID,
            Trim(VENDOR_SUBRANGE.VENDOR_SUBRANGE_CD) AS VENDOR_SUBRANGE_CD 
        FROM
            VENDOR_SUBRANGE) AS VENDOR_SUBRANGE 
    RIGHT OUTER JOIN
        EXP_Final_4 
            ON VENDOR_ID = EXP_Final_4.VENDOR_ID 
            AND VENDOR_SUBRANGE.VENDOR_SUBRANGE_CD = SubRangeCD""")

df_5.createOrReplaceTempView("LKP_Vendor_Subrange1_5")

# COMMAND ----------

# DBTITLE 1, EXP_UPD_6

df_6=spark.sql("""
    SELECT
        LKP_Vendor_Subrange1_5.VENDOR_ID AS VENDOR_ID1,
        EXP_Final_4.VENDOR_ID AS VENDOR_ID,
        EXP_Final_4.SubRangeCD AS SubRangeCD,
        EXP_Final_4.SubRangeDesc AS SubRangeDesc,
        EXP_Final_4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Final_4 
    INNER JOIN
        LKP_Vendor_Subrange1_5 
            ON EXP_Final_4.Monotonically_Increasing_Id = LKP_Vendor_Subrange1_5.Monotonically_Increasing_Id""")

df_6.createOrReplaceTempView("EXP_UPD_6")

# COMMAND ----------

# DBTITLE 1, UPD_UPDATE_7

df_7=spark.sql("""
    SELECT
        VENDOR_ID1 AS VENDOR_ID3,
        SubRangeCD AS SubRangeCd3,
        SubRangeDesc AS SubRangeDesc3,
        VENDOR_ID1 AS VENDOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_UPD_6""")

df_7.createOrReplaceTempView("UPD_UPDATE_7")

# COMMAND ----------

# DBTITLE 1, VENDOR_SUBRANGE

spark.sql("""INSERT INTO VENDOR_SUBRANGE SELECT VENDOR_ID3 AS VENDOR_ID,
SubRangeCd3 AS VENDOR_SUBRANGE_CD,
SubRangeDesc3 AS VENDOR_SUBRANGE_DESC FROM UPD_UPDATE_7""")
