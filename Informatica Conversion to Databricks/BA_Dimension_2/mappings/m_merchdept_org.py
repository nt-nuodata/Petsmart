# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, CAMerchDirector_0


df_0=spark.sql("""
    SELECT
        CAMerchDirectorId AS CAMerchDirectorId,
        CAMerchVpId AS CAMerchVpId,
        CAMerchDirectorName AS CAMerchDirectorName,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CAMerchDirector""")

df_0.createOrReplaceTempView("CAMerchDirector_0")

# COMMAND ----------
# DBTITLE 1, CAMerchBuyer_1


df_1=spark.sql("""
    SELECT
        CAMerchBuyerId AS CAMerchBuyerId,
        CAMerchDirectorId AS CAMerchDirectorId,
        CAMerchBuyerName AS CAMerchBuyerName,
        CountryCd AS CountryCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CAMerchBuyer""")

df_1.createOrReplaceTempView("CAMerchBuyer_1")

# COMMAND ----------
# DBTITLE 1, MerchBusinessUnit_2


df_2=spark.sql("""
    SELECT
        MerchBusinessUnitId AS MerchBusinessUnitId,
        MerchBusinessUnitDesc AS MerchBusinessUnitDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchBusinessUnit""")

df_2.createOrReplaceTempView("MerchBusinessUnit_2")

# COMMAND ----------
# DBTITLE 1, MerchSvp_3


df_3=spark.sql("""
    SELECT
        MerchSvpId AS MerchSvpId,
        MerchBusinessUnitId AS MerchBusinessUnitId,
        MerchSvpDesc AS MerchSvpDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchSvp""")

df_3.createOrReplaceTempView("MerchSvp_3")

# COMMAND ----------
# DBTITLE 1, MerchSegment_4


df_4=spark.sql("""
    SELECT
        MerchSegmentId AS MerchSegmentId,
        MerchGroupId AS MerchGroupId,
        MerchSegmentDesc AS MerchSegmentDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchSegment""")

df_4.createOrReplaceTempView("MerchSegment_4")

# COMMAND ----------
# DBTITLE 1, MerchGroup_5


df_5=spark.sql("""
    SELECT
        MerchGroupId AS MerchGroupId,
        MerchGroupDesc AS MerchGroupDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchGroup""")

df_5.createOrReplaceTempView("MerchGroup_5")

# COMMAND ----------
# DBTITLE 1, MerchVp_6


df_6=spark.sql("""
    SELECT
        MerchVpId AS MerchVpId,
        MerchSvpId AS MerchSvpId,
        MerchVpName AS MerchVpName,
        MerchDesc AS MerchDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchVp""")

df_6.createOrReplaceTempView("MerchVp_6")

# COMMAND ----------
# DBTITLE 1, MerchBuyer_7


df_7=spark.sql("""
    SELECT
        MerchBuyerId AS MerchBuyerId,
        MerchDirectorId AS MerchDirectorId,
        MerchBuyerName AS MerchBuyerName,
        CountryCd AS CountryCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchBuyer""")

df_7.createOrReplaceTempView("MerchBuyer_7")

# COMMAND ----------
# DBTITLE 1, MerchDirector_8


df_8=spark.sql("""
    SELECT
        MerchDirectorId AS MerchDirectorId,
        MerchVpId AS MerchVpId,
        MerchDirectorName AS MerchDirectorName,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchDirector""")

df_8.createOrReplaceTempView("MerchDirector_8")

# COMMAND ----------
# DBTITLE 1, MerchDept_9


df_9=spark.sql("""
    SELECT
        MerchDeptCd AS MerchDeptCd,
        MerchDeptDesc AS MerchDeptDesc,
        MerchDivisionCd AS MerchDivisionCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        DivisionId AS DivisionId,
        DivSpeciesId AS DivSpeciesId,
        MerchBuyerId AS MerchBuyerId,
        PlannerCD AS PlannerCD,
        MerchSegmentId AS MerchSegmentId,
        CAMerchBuyerId AS CAMerchBuyerId,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MerchDept""")

df_9.createOrReplaceTempView("MerchDept_9")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MerchDept_10


df_10=spark.sql("""
    SELECT
        a17.MERCHBUSINESSUNITID BUS_UNIT_ID,
        a112.MERCHBUSinessUNITDESC BUS_UNIT_DESC,
        a12.MERCHBUYERID BUYER_ID,
        a14.MERCHBUYERNaMe BUYER_NM,
        CASE 
            WHEN a12.CAMERCHBUYERID IS NULL THEN a12.MERCHBUYERID 
            ELSE a12.CAMERCHBUYERID 
        END AS CA_BUYER_ID,
        CASE 
            WHEN a12.CAMERCHBUYERID IS NULL THEN a14.MERCHBUYERNaMe 
            ELSE a18.CAMERCHBUYERNAME 
        END AS CA_BUYER_NM,
        CASE 
            WHEN a12.CAMERCHBUYERID IS NULL THEN a14.MERCHDIRECTORID 
            ELSE a18.CAMERCHDIRECTORID 
        END AS CA_DIRECTOR_ID,
        CASE 
            WHEN a12.CAMERCHBUYERID IS NULL THEN a15.MERCHDIRECTORNAME 
            ELSE a110.CAMERCHDIRECTORNAME 
        END AS CA_DIRECTOR_NM,
        a12.MERCHDEPTCD SAP_DEPT_ID,
        a12.MERCHDEPTDESC SAP_DEPT_DESC,
        a14.MERCHDIRECTORID DIRECTOR_ID,
        a15.MERCHDIRECTORNAME DIRECTOR_NM,
        a19.MERCHGROUPID GROUP_ID,
        a111.MERCHGROUPDESC GROUP_DESC,
        a12.MERCHSEGMENTID SEGMENT_ID,
        a19.MERCHSEGMENTDESC SEGMENT_DESC,
        a16.MERCHSVPID SVP_ID,
        a17.MERCHSVPDESC SVP_NM,
        a15.MERCHVPID VP_ID,
        a16.MERCHDESC VP_DESC,
        a16.MERCHVPNAME VP_NM,
        a40.MERCHVPID AS CA_VP_ID,
        a40.MERCHVPNAME AS CA_VP_NM 
    FROM
        MERCHDEPT a12 
    LEFT JOIN
        MERCHBUYER a14 
            ON (
                a12.MERCHBUYERID = a14.MERCHBUYERID
            ) 
    JOIN
        MERCHDIRECTOR a15 
            ON (
                a14.MERCHDIRECTORID = a15.MERCHDIRECTORID
            ) 
    JOIN
        MERCHVP a16 
            ON (
                a15.MERCHVPID = a16.MERCHVPID
            ) 
    JOIN
        MERCHSVP a17 
            ON (
                a16.MERCHSVPID = a17.MERCHSVPID
            ) 
    LEFT OUTER JOIN
        CAMERCHBUYER a18 
            ON (
                a12.CAMERCHBUYERID = a18.CAMERCHBUYERID
            ) 
    LEFT OUTER JOIN
        MERCHSEGMENT a19 
            ON (
                a12.MERCHSEGMENTID = a19.MERCHSEGMENTID
            ) 
    LEFT OUTER JOIN
        CAMERCHDIRECTOR a110 
            ON (
                a18.CAMERCHDIRECTORID = a110.CAMERCHDIRECTORID
            ) 
    LEFT OUTER JOIN
        MERCHVP a40 
            ON (
                a40.MERCHVPID = a110.CAMERCHVPID
            ) 
    LEFT OUTER JOIN
        MERCHGROUP a111 
            ON (
                a19.MERCHGROUPID = a111.MERCHGROUPID
            ) 
    JOIN
        MERCHBUSINESSUNIT a112 
            ON (
                a17.MERCHBUSINESSUNITID = a112.MERCHBUSINESSUNITID
            ) 
    ORDER BY
        a12.MERCHDEPTCD""")

df_10.createOrReplaceTempView("SQ_Shortcut_to_MerchDept_10")

# COMMAND ----------
# DBTITLE 1, EXP_CA_FLG_11


df_11=spark.sql("""
    SELECT
        BUS_UNIT_ID AS BUS_UNIT_ID,
        BUS_UNIT_DESC AS BUS_UNIT_DESC,
        BUYER_ID AS BUYER_ID,
        BUYER_NM AS BUYER_NM,
        CA_BUYER_ID AS CA_BUYER_ID,
        CA_BUYER_NM AS CA_BUYER_NM,
        CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
        CA_DIRECTOR_NM AS CA_DIRECTOR_NM,
        IFF(BUYER_ID <> CA_BUYER_ID,
        'Y',
        'N') AS CA_MANAGED_FLG,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        DIRECTOR_ID AS DIRECTOR_ID,
        DIRECTOR_NM AS DIRECTOR_NM,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        SVP_ID AS SVP_ID,
        SVP_NM AS SVP_NM,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        VP_NM AS VP_NM,
        CA_VP_ID AS CA_VP_ID,
        CA_VP_NM AS CA_VP_NM,
        current_timestamp AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MerchDept_10""")

df_11.createOrReplaceTempView("EXP_CA_FLG_11")

# COMMAND ----------
# DBTITLE 1, MERCHDEPT_ORG


spark.sql("""INSERT INTO MERCHDEPT_ORG SELECT SAP_DEPT_ID AS SAP_DEPT_ID,
SAP_DEPT_DESC AS SAP_DEPT_DESC,
BUS_UNIT_ID AS BUS_UNIT_ID,
BUS_UNIT_DESC AS BUS_UNIT_DESC,
BUYER_ID AS BUYER_ID,
BUYER_NM AS BUYER_NM,
CA_BUYER_ID AS CA_BUYER_ID,
CA_BUYER_NM AS CA_BUYER_NM,
CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
CA_DIRECTOR_NM AS CA_DIRECTOR_NM,
CA_MANAGED_FLG AS CA_MANAGED_FLG,
DIRECTOR_ID AS DIRECTOR_ID,
DIRECTOR_NM AS DIRECTOR_NM,
GROUP_ID AS GROUP_ID,
GROUP_DESC AS GROUP_DESC,
PRICING_ROLE_ID AS PRICING_ROLE_ID,
PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
SEGMENT_ID AS SEGMENT_ID,
SEGMENT_DESC AS SEGMENT_DESC,
SVP_ID AS SVP_ID,
SVP_NM AS SVP_NM,
VP_ID AS VP_ID,
VP_DESC AS VP_DESC,
VP_NM AS VP_NM,
CA_VP_ID AS CA_VP_ID,
CA_VP_NM AS CA_VP_NM,
LOAD_DT AS LOAD_DT FROM EXP_CA_FLG_11""")