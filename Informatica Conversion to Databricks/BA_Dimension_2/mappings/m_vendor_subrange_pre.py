# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SubRange_0

df_0=spark.sql("""
    SELECT
        SubRangeCD AS SubRangeCD,
        VendorNbr AS VendorNbr,
        SubRangeDesc AS SubRangeDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SubRange""")

df_0.createOrReplaceTempView("SubRange_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SubRange_1

df_1=spark.sql("""
    SELECT
        SubRangeCD AS SubRangeCD,
        VendorNbr AS VendorNbr,
        SubRangeDesc AS SubRangeDesc,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SubRange_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SubRange_1")

# COMMAND ----------

# DBTITLE 1, Vendor_2

df_2=spark.sql("""
    SELECT
        VendorNbr AS VendorNbr,
        VendorAccountGroup AS VendorAccountGroup,
        VendorName AS VendorName,
        CountryCD AS CountryCD,
        StreetAddress AS StreetAddress,
        AddressLine1 AS AddressLine1,
        AddressLine2 AS AddressLine2,
        City AS City,
        State AS State,
        Zip AS Zip,
        PhoneNbr1 AS PhoneNbr1,
        PhoneNbr2 AS PhoneNbr2,
        FaxNbr AS FaxNbr,
        POBoxNbr AS POBoxNbr,
        POBoxPostalCd AS POBoxPostalCd,
        LanguageCd AS LanguageCd,
        IndustryCd AS IndustryCd,
        TimeZone AS TimeZone,
        Region AS Region,
        CentralDeletionFlag AS CentralDeletionFlag,
        CentralPostingBlock AS CentralPostingBlock,
        CentralPurchasingBlock AS CentralPurchasingBlock,
        TaxNbr1 AS TaxNbr1,
        TaxNbr2 AS TaxNbr2,
        CustomerReferenceNbr AS CustomerReferenceNbr,
        CompanyIDforTradingPartner AS CompanyIDforTradingPartner,
        PayByEDIFlag AS PayByEDIFlag,
        ReturnToVendorEligibilityFlag AS ReturnToVendorEligibilityFlag,
        AlternatePayeeVendorCd AS AlternatePayeeVendorCd,
        VIPCD AS VIPCD,
        InactiveFlag AS InactiveFlag,
        IsVendorSubRangeRelevant AS IsVendorSubRangeRelevant,
        IsSiteLevelRelevant AS IsSiteLevelRelevant,
        IsOneTimeVendor AS IsOneTimeVendor,
        IsAlternatePayeeAllowed AS IsAlternatePayeeAllowed,
        StandardCarrierAccessCd AS StandardCarrierAccessCd,
        BlueGreenFlag AS BlueGreenFlag,
        IDOAFlag AS IDOAFlag,
        CarrierConfirmationExpectedFlag AS CarrierConfirmationExpectedFlag,
        TransportationChainCd AS TransportationChainCd,
        TransitTimeDays AS TransitTimeDays,
        TMSTransportationModeID AS TMSTransportationModeID,
        SCACCd AS SCACCd,
        PerformanceRatingID AS PerformanceRatingID,
        TenderMethodID AS TenderMethodID,
        CarrierContactName AS CarrierContactName,
        CarrierContactEmailAddress AS CarrierContactEmailAddress,
        CarrierGroupName AS CarrierGroupName,
        ShipmentStatusRequiredFlag AS ShipmentStatusRequiredFlag,
        ApplyFixedCostAllFlag AS ApplyFixedCostAllFlag,
        PickupInServiceFlag AS PickupInServiceFlag,
        PickupCutoffFlag AS PickupCutoffFlag,
        ResponseTime AS ResponseTime,
        ABSMinimumCharge AS ABSMinimumCharge,
        CMMaxHours AS CMMaxHours,
        CMFreeMilesi AS CMFreeMilesi,
        CMCapableFlag AS CMCapableFlag,
        CMDiscount AS CMDiscount,
        CMDiscountFirstLegFlag AS CMDiscountFirstLegFlag,
        CMVariableRate AS CMVariableRate,
        PackageDiscount AS PackageDiscount,
        Notes AS Notes,
        VendorAddDT AS VendorAddDT,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Vendor""")

df_2.createOrReplaceTempView("Vendor_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_Vendor_3

df_3=spark.sql("""
    SELECT
        VendorNbr AS VendorNbr,
        VendorAccountGroup AS VendorAccountGroup,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Vendor_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_Vendor_3")

# COMMAND ----------

# DBTITLE 1, JNR_Subrange_4

df_4=spark.sql("""
    SELECT
        DETAIL.VendorNbr AS VendorNbr,
        DETAIL.VendorAccountGroup AS VendorAccountGroup,
        MASTER.SubRangeCD AS SubRangeCD,
        MASTER.VendorNbr AS VendorNbr1,
        MASTER.SubRangeDesc AS SubRangeDesc,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SubRange_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_Vendor_3 DETAIL 
            ON MASTER.VendorNbr = DETAIL.VendorNbr""")

df_4.createOrReplaceTempView("JNR_Subrange_4")

# COMMAND ----------

# DBTITLE 1, EXP_DEFAULT_5

df_5=spark.sql("""
    SELECT
        in_SubRangeCD AS in_SubRangeCD,
        IFF(ISNULL(SubRangeCD),
        ' ',
        SubRangeCD) AS SubRangeCD,
        in_SubRangeDesc AS in_SubRangeDesc,
        IFF(ISNULL(SubRangeDesc),
        ' ',
        SubRangeDesc) AS SubRangeDesc,
        LTRIM(VendorNbr,
        '0') AS VendorNbr,
        VendorAccountGroup AS VendorAccountGroup,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_Subrange_4""")

df_5.createOrReplaceTempView("EXP_DEFAULT_5")

# COMMAND ----------

# DBTITLE 1, LKP_Vendor_Type_6

df_6=spark.sql("""
    SELECT
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        EXP_DEFAULT_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_TYPE 
    RIGHT OUTER JOIN
        EXP_DEFAULT_5 
            ON VENDOR_TYPE.VENDOR_ACCOUNT_GROUP_CD = EXP_DEFAULT_5.VendorAccountGroup""")

df_6.createOrReplaceTempView("LKP_Vendor_Type_6")

# COMMAND ----------

# DBTITLE 1, EXP_Vendor_type_7

df_7=spark.sql("""
    SELECT
        EXP_DEFAULT_5.VendorNbr AS VendorNbr,
        IFF(VendorAccountGroup = ' ',
        21,
        VENDOR_TYPE_ID) AS VENDOR_TYPE_ID,
        EXP_DEFAULT_5.SubRangeCD AS SubRangeCD,
        EXP_DEFAULT_5.SubRangeDesc AS SubRangeDesc,
        EXP_DEFAULT_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DEFAULT_5 
    INNER JOIN
        LKP_Vendor_Type_6 
            ON EXP_DEFAULT_5.Monotonically_Increasing_Id = LKP_Vendor_Type_6.Monotonically_Increasing_Id""")

df_7.createOrReplaceTempView("EXP_Vendor_type_7")

# COMMAND ----------

# DBTITLE 1, VENDOR_SUBRANGE_PRE

spark.sql("""INSERT INTO VENDOR_SUBRANGE_PRE SELECT VendorNbr AS VENDOR_NBR,
VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
SubRangeCD AS VENDOR_SUBRANGE_CD,
SubRangeDesc AS VENDOR_SUBRANGE_DESC FROM EXP_Vendor_type_7""")
