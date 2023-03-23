# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Vendor_0


df_0=spark.sql("""
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

df_0.createOrReplaceTempView("Vendor_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Vendor_1


df_1=spark.sql("""
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
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Vendor_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Vendor_1")

# COMMAND ----------
# DBTITLE 1, LKP_Company_vendor_Relation_2


df_2=spark.sql("""
    SELECT
        VendorNbr AS VendorNbr,
        EDIEligibilityFlag AS EDIEligibilityFlag,
        SQ_Shortcut_to_Vendor_1.VendorNbr AS VendorNbr1,
        SQ_Shortcut_to_Vendor_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            max(Company_Vendor_Relation.EDIEligibilityFlag) AS EDIEligibilityFlag,
            Company_Vendor_Relation.VendorNbr AS VendorNbr 
        FROM
            Company_Vendor_Relation 
        GROUP BY
            Company_Vendor_Relation.VendorNbr) AS Company_Vendor_Relation 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_Vendor_1 
            ON VendorNbr = SQ_Shortcut_to_Vendor_1.VendorNbr""")

df_2.createOrReplaceTempView("LKP_Company_vendor_Relation_2")

# COMMAND ----------
# DBTITLE 1, LKP_LFA1_PRE_3


df_3=spark.sql("""SELECT LIFNR AS LIFNR,
WERKS AS WERKS,
SQ_Shortcut_to_Vendor_1.VendorNbr AS VendorNbr,
SQ_Shortcut_to_Vendor_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM (SELECT LFA1_PRE.WERKS as WERKS, TRIM(LFA1_PRE.LIFNR) as LIFNR FROM LFA1_PRE--) AS LFA1_PRE RIGHT OUTER JOIN SQ_Shortcut_to_Vendor_1
 ON LIFNR = SQ_Shortcut_to_Vendor_1.VendorNbr""")

df_3.createOrReplaceTempView("LKP_LFA1_PRE_3")

# COMMAND ----------
# DBTITLE 1, EXP_VENDOR1_4


df_4=spark.sql("""
    SELECT
        VendorNbr AS VendorNbr,
        VendorAccountGroup AS VendorAccountGroup,
        VendorName AS VendorName,
        CountryCD AS CountryCD,
        StreetAddress AS StreetAddress,
        City AS City,
        State AS State,
        Zip AS Zip,
        PhoneNbr1 AS PhoneNbr1,
        IndustryCd AS IndustryCd,
        Region AS Region,
        CentralDeletionFlag AS CentralDeletionFlag,
        CentralPostingBlock AS CentralPostingBlock,
        CentralPurchasingBlock AS CentralPurchasingBlock,
        FaxNbr AS FaxNbr,
        InactiveFlag AS InactiveFlag,
        VendorAddDT AS VendorAddDT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_Vendor_1""")

df_4.createOrReplaceTempView("EXP_VENDOR1_4")

# COMMAND ----------
# DBTITLE 1, LKP_Purchasing_Organization_to_vendor_5


df_5=spark.sql("""
    SELECT
        VendorNbr AS VendorNbr,
        PurchasingOrganizationCd AS PurchasingOrganizationCd,
        PurchasingGroupId AS PurchasingGroupId,
        SuperiorVendorNbr AS SuperiorVendorNbr,
        SalesPersonName AS SalesPersonName,
        PhoneNbr AS PhoneNbr,
        OrderCurrency AS OrderCurrency,
        LeadTime AS LeadTime,
        IncoTermCd AS IncoTermCd,
        PaymentTermCd AS PaymentTermCd,
        USRTVTypeCd AS USRTVTypeCd,
        CARTVTypeCd AS CARTVTypeCd,
        RTVFreightTypeCd AS RTVFreightTypeCd,
        RTVEligFlag AS RTVEligFlag,
        VIPCD AS VIPCD,
        WEBFlag AS WEBFlag,
        CashDiscFlag AS CashDiscFlag,
        BlockedFlag AS BlockedFlag,
        BlockedDate AS BlockedDate,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        EXP_VENDOR1_4.VendorNbr AS VendorNbr1,
        EXP_VENDOR1_4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PurchasingOrganization_to_Vendor 
    RIGHT OUTER JOIN
        EXP_VENDOR1_4 
            ON VendorNbr = EXP_VENDOR1_4.VendorNbr""")

df_5.createOrReplaceTempView("LKP_Purchasing_Organization_to_vendor_5")

# COMMAND ----------
# DBTITLE 1, EXP_FINAL_6


df_6=spark.sql("""
    SELECT
        EXP_VENDOR1_4.VendorNbr AS VendorNbr,
        EXP_VENDOR1_4.VendorAccountGroup AS i_VendorAccountGroup,
        IIF() AS VendorAccountGroup,
        EXP_VENDOR1_4.VendorName AS VendorName,
        LKP_LFA1_PRE_3.WERKS AS SITE,
        LKP_Company_vendor_Relation_2.EDIEligibilityFlag AS in_EDIEligibilityFlag,
        IFF(IN(VendorAccountGroup,
        'ZMER',
        'ZEXP') = 1,
        IFF(EDIEligibilityFlag = 'X',
        'Y',
        'N'),
        ' ') AS EDIEligibilityFlag1,
        EXP_VENDOR1_4.CentralDeletionFlag AS CentralDeletionFlag,
        EXP_VENDOR1_4.CentralPostingBlock AS CentralPostingBlock,
        EXP_VENDOR1_4.CentralPurchasingBlock AS CentralPurchasingBlock,
        LKP_Purchasing_Organization_to_vendor_5.VIPCD AS VIPCD,
        EXP_VENDOR1_4.InactiveFlag AS InactiveFlag,
        LKP_Purchasing_Organization_to_vendor_5.IncoTermCd AS IncoTermCd,
        LKP_Purchasing_Organization_to_vendor_5.PaymentTermCd AS PaymentTermCd,
        LKP_Purchasing_Organization_to_vendor_5.RTVFreightTypeCd AS RTVFreightTypeCd,
        EXP_VENDOR1_4.CountryCD AS CountryCD,
        EXP_VENDOR1_4.StreetAddress AS StreetAddress,
        EXP_VENDOR1_4.City AS City,
        EXP_VENDOR1_4.State AS State,
        EXP_VENDOR1_4.Zip AS Zip,
        EXP_VENDOR1_4.FaxNbr AS FaxNbr,
        EXP_VENDOR1_4.IndustryCd AS IndustryCd,
        LKP_Purchasing_Organization_to_vendor_5.RTVEligFlag AS ReturnToVendorEligibilityFlag,
        EXP_VENDOR1_4.VendorAddDT AS VendorAddDT,
        EXP_VENDOR1_4.PhoneNbr1 AS PhoneNbr,
        LKP_Purchasing_Organization_to_vendor_5.SalesPersonName AS Contact,
        LKP_Purchasing_Organization_to_vendor_5.EXP_VENDOR1_4.PhoneNbr1 AS Contact_Phone,
        LKP_Purchasing_Organization_to_vendor_5.PurchasingGroupId AS PurchasingGroupId,
        LKP_Purchasing_Organization_to_vendor_5.SuperiorVendorNbr AS SuperiorVendorNbr,
        LKP_Purchasing_Organization_to_vendor_5.USRTVTypeCd AS USRTVTypeCd,
        LKP_Purchasing_Organization_to_vendor_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_Purchasing_Organization_to_vendor_5 
    INNER JOIN
        EXP_VENDOR1_4 
            ON LKP_Purchasing_Organization_to_vendor_5.Monotonically_Increasing_Id = EXP_VENDOR1_4.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_LFA1_PRE_3 
            ON LKP_Purchasing_Organization_to_vendor_5.Monotonically_Increasing_Id = LKP_LFA1_PRE_3.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_Company_vendor_Relation_2 
            ON LKP_Purchasing_Organization_to_vendor_5.Monotonically_Increasing_Id = LKP_Company_vendor_Relation_2.Monotonically_Increasing_Id""")

df_6.createOrReplaceTempView("EXP_FINAL_6")

# COMMAND ----------
# DBTITLE 1, FLTR_7


df_7=spark.sql("""
    SELECT
        VendorNbr AS VendorNbr,
        VendorAccountGroup AS VendorAccountGroup,
        VendorName AS VendorName,
        SITE AS SITE,
        EDIEligibilityFlag AS EDIEligibilityFlag,
        CentralDeletionFlag AS CentralDeletionFlag,
        CentralPostingBlock AS CentralPostingBlock,
        CentralPurchasingBlock AS CentralPurchasingBlock,
        VIPCD AS VIPCD,
        InactiveFlag AS InactiveFlag,
        IncoTermCd AS IncoTermCd,
        PaymentTermCd AS PaymentTermCd,
        RTVFreightTypeCd AS RTVFreightTypeCd,
        CountryCD AS CountryCD,
        StreetAddress AS StreetAddress,
        City AS City,
        State AS State,
        Zip AS Zip,
        FaxNbr AS FaxNbr,
        IndustryCd AS IndustryCd,
        ReturnToVendorEligibilityFlag AS ReturnToVendorEligibilityFlag,
        VendorAddDT AS VendorAddDT,
        PhoneNbr AS PhoneNbr,
        Contact AS Contact,
        Contact_Phone AS Contact_Phone,
        PurchasingGroupId AS PurchasingGroupId,
        SuperiorVendorNbr AS SuperiorVendorNbr,
        USRTVTypeCd AS USRTVTypeCd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_FINAL_6 
    WHERE
        1""")

df_7.createOrReplaceTempView("FLTR_7")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE_PRE


spark.sql("""INSERT INTO VENDOR_PROFILE_PRE SELECT VendorAccountGroup AS VENDOR_TYPE_ID,
VendorNbr AS VENDOR_NBR,
VendorName AS VENDOR_NAME,
VENDOR_GROUP AS VENDOR_GROUP,
SITE AS SITE,
PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
PurchasingGroupId AS PURCH_GROUP_ID,
SuperiorVendorNbr AS SUPERIOR_VENDOR_NBR,
EDIEligibilityFlag AS EDI_ELIG_FLAG,
CentralPurchasingBlock AS PURCHASE_BLOCK,
CentralPostingBlock AS POSTING_BLOCK,
CentralDeletionFlag AS DELETION_FLAG,
VIPCD AS VIP_CD,
InactiveFlag AS INACTIVE_FLAG,
PaymentTermCd AS PAYMENT_TERM_CD,
IncoTermCd AS INCO_TERM_CD,
StreetAddress AS ADDRESS,
City AS CITY,
State AS STATE,
CountryCD AS COUNTRY_CD,
Zip AS ZIP,
Contact AS CONTACT,
Contact_Phone AS CONTACT_PHONE,
PhoneNbr AS PHONE,
FaxNbr AS FAX,
ReturnToVendorEligibilityFlag AS RTV_ELIG_FLAG,
USRTVTypeCd AS RTV_TYPE_CD,
RTVFreightTypeCd AS RTV_FREIGHT_TYPE_CD,
IndustryCd AS INDUSTRY_CD,
VendorAddDT AS ADD_DT FROM FLTR_7""")