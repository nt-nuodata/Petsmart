# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, BrandDirector_0


df_0=spark.sql("""
    SELECT
        BrandDirectorId AS BrandDirectorId,
        BrandDirectorName AS BrandDirectorName,
        MerchVpId AS MerchVpId,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BrandDirector""")

df_0.createOrReplaceTempView("BrandDirector_0")

# COMMAND ----------
# DBTITLE 1, BrandManager_1


df_1=spark.sql("""
    SELECT
        BrandManagerId AS BrandManagerId,
        BrandManagerName AS BrandManagerName,
        BrandDirectorId AS BrandDirectorId,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BrandManager""")

df_1.createOrReplaceTempView("BrandManager_1")

# COMMAND ----------
# DBTITLE 1, DeptBrand_2


df_2=spark.sql("""
    SELECT
        DeptBrandCd AS DeptBrandCd,
        BrandManagerId AS BrandManagerId,
        BrandCd AS BrandCd,
        MerchDeptCd AS MerchDeptCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DeptBrand""")

df_2.createOrReplaceTempView("DeptBrand_2")

# COMMAND ----------
# DBTITLE 1, SQ_ADH_BrandHierarchy_3


df_3=spark.sql("""SELECT DeptBrand.MerchDeptCd AS MerchDeptCd,
DeptBrand.DeptBrandCd AS DeptBrandCd,
DeptBrand.UpdateTstmp AS UpdateTstmp3,
DeptBrand.LoadTstmp AS LoadTstmp3,
DeptBrand.BrandManagerId AS BrandManagerId1,
DeptBrand.BrandCd AS BrandCd1,
BrandManager.BrandManagerId AS BrandManagerId,
BrandManager.BrandDirectorId AS BrandDirectorId1,
BrandManager.BrandManagerName AS BrandManagerName,
BrandManager.UpdateTstmp AS UpdateTstmp2,
BrandManager.LoadTstmp AS LoadTstmp2,
BrandDirector.MerchVpId AS MerchVpId,
BrandDirector.BrandDirectorName AS BrandDirectorName,
BrandDirector.LoadTstmp AS LoadTstmp1,
BrandDirector.BrandDirectorId AS BrandDirectorId,
BrandDirector.UpdateTstmp AS UpdateTstmp1,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM DeptBrand.BrandManagerId = BrandManager.BrandManagerId
AND BrandManager.BrandDirectorId = BrandDirector.BrandDirectorId""")

df_3.createOrReplaceTempView("SQ_ADH_BrandHierarchy_3")

# COMMAND ----------
# DBTITLE 1, ArtAttribute_4


df_4=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        AttTypeID AS AttTypeID,
        AttCodeID AS AttCodeID,
        AttValueID AS AttValueID,
        DeleteFlag AS DeleteFlag,
        DeleteTstmp AS DeleteTstmp,
        UpdateTstmp AS UpdateTstmp,
        LoadTstmp AS LoadTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ArtAttribute""")

df_4.createOrReplaceTempView("ArtAttribute_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ArtAttribute_5


df_5=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        AttTypeID AS AttTypeID,
        AttCodeID AS AttCodeID,
        AttValueID AS AttValueID,
        DeleteFlag AS DeleteFlag,
        DeleteTstmp AS DeleteTstmp,
        UpdateTstmp AS UpdateTstmp,
        LoadTstmp AS LoadTstmp,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ArtAttribute_4 
    WHERE
        ArtAttribute.AttCodeID = 'PBRD' 
        AND ArtAttribute.AttValueID = 'NBE'""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_ArtAttribute_5")

# COMMAND ----------
# DBTITLE 1, Brand_6


df_6=spark.sql("""
    SELECT
        BrandCd AS BrandCd,
        BrandName AS BrandName,
        BrandTypeCd AS BrandTypeCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        BrandClassificationCd AS BrandClassificationCd,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Brand""")

df_6.createOrReplaceTempView("Brand_6")

# COMMAND ----------
# DBTITLE 1, ArtMas_7


df_7=spark.sql("""
    SELECT
        ArticleNbr AS ArticleNbr,
        ArticleDesc AS ArticleDesc,
        AlternateDesc AS AlternateDesc,
        ArticleTypeCd AS ArticleTypeCd,
        MerchCategoryCd AS MerchCategoryCd,
        ArticleCategoryCd AS ArticleCategoryCd,
        ArticleStatusCd AS ArticleStatusCd,
        PrimaryVendorCd AS PrimaryVendorCd,
        BrandCd AS BrandCd,
        ProcRule AS ProcRule,
        FlavorCd AS FlavorCd,
        ColorCd AS ColorCd,
        SizeCd AS SizeCd,
        RtvCd AS RtvCd,
        CreateDt AS CreateDt,
        CreatedBy AS CreatedBy,
        UpdateDt AS UpdateDt,
        UpdatedBy AS UpdatedBy,
        BaseUomCd AS BaseUomCd,
        BaseUomIsoCd AS BaseUomIsoCd,
        DocNbr AS DocNbr,
        DocSheetCnt AS DocSheetCnt,
        WeightNetAmt AS WeightNetAmt,
        ContainerReqmtCd AS ContainerReqmtCd,
        TransportGroupCd AS TransportGroupCd,
        DivisionCd AS DivisionCd,
        GrGiSlipPrintCnt AS GrGiSlipPrintCnt,
        SupplySourceCd AS SupplySourceCd,
        WeightAllowedPkgAmt AS WeightAllowedPkgAmt,
        VolumeAllowedPkgAmt AS VolumeAllowedPkgAmt,
        WeightToleranceAmt AS WeightToleranceAmt,
        VolumeToleranceAmt AS VolumeToleranceAmt,
        VariableOrderUnitFlag AS VariableOrderUnitFlag,
        VolumeFillAmt AS VolumeFillAmt,
        StackingFactorAmt AS StackingFactorAmt,
        ShelfLifeRemCnt AS ShelfLifeRemCnt,
        ShelfLifeTotalCnt AS ShelfLifeTotalCnt,
        StoragePct AS StoragePct,
        ValidFromDt AS ValidFromDt,
        DeleteDt AS DeleteDt,
        XSiteStatusCd AS XSiteStatusCd,
        XSiteValidFromDt AS XSiteValidFromDt,
        XDistValidFromDt AS XDistValidFromDt,
        TaxClassCd AS TaxClassCd,
        ContentUnitCd AS ContentUnitCd,
        NetContentsAmt AS NetContentsAmt,
        ContentMetricUnitCd AS ContentMetricUnitCd,
        NetContentsMetricAmt AS NetContentsMetricAmt,
        CompPriceUnitAmt AS CompPriceUnitAmt,
        GrossContentsAmt AS GrossContentsAmt,
        ItemCategory AS ItemCategory,
        FiberShare1Pct AS FiberShare1Pct,
        FiberShare2Pct AS FiberShare2Pct,
        FiberShare3Pct AS FiberShare3Pct,
        FiberShare4Pct AS FiberShare4Pct,
        FiberShare5Pct AS FiberShare5Pct,
        TempSKU AS TempSKU,
        CopySKU AS CopySKU,
        OldArticleNbr AS OldArticleNbr,
        MandatorySkuFlag AS MandatorySkuFlag,
        BasicMaterial AS BasicMaterial,
        RxFlag AS RxFlag,
        Seasonality AS Seasonality,
        iDocNumber AS iDocNumber,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ArtMas""")

df_7.createOrReplaceTempView("ArtMas_7")

# COMMAND ----------
# DBTITLE 1, SQ_ArtMast_Brand_8


df_8=spark.sql("""SELECT ArtMas.BrandCd AS BrandCd,
ArtMas.ArticleNbr AS ArticleNbr,
Brand.BrandClassificationCd AS BrandClassificationCd,
Brand.BrandCd AS BrandCd1,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM ArtMas.BrandCd = Brand.BrandCd WHERE 
Brand.BrandClassificationCd is not null""")

df_8.createOrReplaceTempView("SQ_ArtMast_Brand_8")

# COMMAND ----------
# DBTITLE 1, Exp_LPAD_ArtAttibute_9


df_9=spark.sql("""
    SELECT
        LPAD(ArticleNbr,
        18,
        '0') AS ArticleNbr1,
        BrandCd AS BrandCd,
        BrandCd1 AS BrandCd1,
        BrandClassificationCd AS BrandClassificationCd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_ArtMast_Brand_8""")

df_9.createOrReplaceTempView("Exp_LPAD_ArtAttibute_9")

# COMMAND ----------
# DBTITLE 1, Jnr_Brand_Art_10


df_10=spark.sql("""
    SELECT
        MASTER.ArticleNbr1 AS ArticleNbr,
        MASTER.BrandCd AS BrandCd,
        MASTER.BrandCd1 AS BrandCd1,
        MASTER.BrandClassificationCd AS BrandClassificationCd,
        DETAIL.ArticleNbr AS ArticleNbr1,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_LPAD_ArtAttibute_9 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_ArtAttribute_5 DETAIL 
            ON MASTER.ArticleNbr1 = DETAIL.ArticleNbr""")

df_10.createOrReplaceTempView("Jnr_Brand_Art_10")

# COMMAND ----------
# DBTITLE 1, Exp_Brand_Classification_11


df_11=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        IFF(ISNULL(ArticleNbr1),
        BrandClassificationCd,
        5) AS BRAND_CLASSIFICATION_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_Brand_Art_10""")

df_11.createOrReplaceTempView("Exp_Brand_Classification_11")

# COMMAND ----------
# DBTITLE 1, SRTTRANS_12


df_12=spark.sql("""
    SELECT
        BRAND_CD AS BrandCd,
        BRAND_CLASSIFICATION_ID AS BrandClassificationCd,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_Brand_Classification_11 
    ORDER BY
        BrandCd ASC,
        BrandClassificationCd ASC""")

df_12.createOrReplaceTempView("SRTTRANS_12")

# COMMAND ----------
# DBTITLE 1, Jnr_ADH_EDW__Brand_13


df_13=spark.sql("""
    SELECT
        DETAIL.BrandManagerId AS BrandManagerId,
        DETAIL.BrandDirectorId1 AS BrandDirectorId1,
        DETAIL.MerchDeptCd AS MerchDeptCd,
        DETAIL.BrandCd1 AS BrandCd1,
        MASTER.BrandCd AS BRAND_CD,
        MASTER.BrandClassificationCd AS BRAND_CLASSIFICATION_ID,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SRTTRANS_12 MASTER 
    INNER JOIN
        SQ_ADH_BrandHierarchy_3 DETAIL 
            ON MASTER.BrandCd = DETAIL.BrandCd1""")

df_13.createOrReplaceTempView("Jnr_ADH_EDW__Brand_13")

# COMMAND ----------
# DBTITLE 1, PB_HIERARCHY_PRE


spark.sql("""INSERT INTO PB_HIERARCHY_PRE SELECT BRAND_CD AS BRAND_CD,
MerchDeptCd AS SAP_DEPT_ID,
BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
BrandManagerId AS PB_MANAGER_ID,
BrandDirectorId1 AS PB_DIRECTOR_ID FROM Jnr_ADH_EDW__Brand_13""")