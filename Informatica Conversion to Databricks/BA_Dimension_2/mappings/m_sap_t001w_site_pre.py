# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, T005F_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        SPRAS AS SPRAS,
        LAND1 AS LAND1,
        REGIO AS REGIO,
        COUNC AS COUNC,
        BEZEI AS BEZEI,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T005F""")

df_0.createOrReplaceTempView("T005F_0")

# COMMAND ----------
# DBTITLE 1, T001W_1


df_1=spark.sql("""
    SELECT
        MANDT AS MANDT,
        WERKS AS WERKS,
        NAME1 AS NAME1,
        BWKEY AS BWKEY,
        KUNNR AS KUNNR,
        LIFNR AS LIFNR,
        FABKL AS FABKL,
        NAME2 AS NAME2,
        STRAS AS STRAS,
        PFACH AS PFACH,
        PSTLZ AS PSTLZ,
        ORT01 AS ORT01,
        EKORG AS EKORG,
        VKORG AS VKORG,
        CHAZV AS CHAZV,
        KKOWK AS KKOWK,
        KORDB AS KORDB,
        BEDPL AS BEDPL,
        LAND1 AS LAND1,
        REGIO AS REGIO,
        COUNC AS COUNC,
        CITYC AS CITYC,
        ADRNR AS ADRNR,
        IWERK AS IWERK,
        TXJCD AS TXJCD,
        VTWEG AS VTWEG,
        SPART AS SPART,
        SPRAS AS SPRAS,
        WKSOP AS WKSOP,
        AWSLS AS AWSLS,
        CHAZV_OLD AS CHAZV_OLD,
        VLFKZ AS VLFKZ,
        BZIRK AS BZIRK,
        ZONE1 AS ZONE1,
        TAXIW AS TAXIW,
        BZQHL AS BZQHL,
        LET01 AS LET01,
        LET02 AS LET02,
        LET03 AS LET03,
        TXNAM_MA1 AS TXNAM_MA1,
        TXNAM_MA2 AS TXNAM_MA2,
        TXNAM_MA3 AS TXNAM_MA3,
        BETOL AS BETOL,
        J_1BBRANCH AS J_1BBRANCH,
        VTBFI AS VTBFI,
        FPRFW AS FPRFW,
        ACHVM AS ACHVM,
        DVSART AS DVSART,
        NODETYPE AS NODETYPE,
        NSCHEMA AS NSCHEMA,
        PKOSA AS PKOSA,
        MISCH AS MISCH,
        MGVUPD AS MGVUPD,
        VSTEL AS VSTEL,
        MGVLAUPD AS MGVLAUPD,
        ZZBROCK_CO AS ZZBROCK_CO,
        ZZPOST_SITE AS ZZPOST_SITE,
        MGVLAREVAL AS MGVLAREVAL,
        SOURCING AS SOURCING,
        OILIVAL AS OILIVAL,
        OIHVTYPE AS OIHVTYPE,
        OIHCREDIPI AS OIHCREDIPI,
        STORETYPE AS STORETYPE,
        DEP_STORE AS DEP_STORE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T001W""")

df_1.createOrReplaceTempView("T001W_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_T001W_2


df_2=spark.sql("""
    SELECT
        SITE.MANDT,
        SITE.WERKS,
        SITE.NAME1,
        SITE.BWKEY,
        SITE.KUNNR,
        SITE.LIFNR,
        SITE.FABKL,
        SITE.NAME2,
        SITE.STRAS,
        SITE.PFACH,
        SITE.PSTLZ,
        SITE.ORT01,
        SITE.EKORG,
        SITE.VKORG,
        SITE.CHAZV,
        SITE.KKOWK,
        SITE.KORDB,
        SITE.BEDPL,
        SITE.LAND1,
        SITE.REGIO,
        SITE.COUNC,
        SITE.CITYC,
        SITE.ADRNR,
        SITE.IWERK,
        SITE.TXJCD,
        SITE.VTWEG,
        SITE.SPART,
        SITE.SPRAS,
        SITE.WKSOP,
        SITE.AWSLS,
        SITE.CHAZV_OLD,
        SITE.VLFKZ,
        SITE.BZIRK,
        SITE.ZONE1,
        SITE.TAXIW,
        SITE.BZQHL,
        SITE.LET01,
        SITE.LET02,
        SITE.LET03,
        SITE.TXNAM_MA1,
        SITE.TXNAM_MA2,
        SITE.TXNAM_MA3,
        SITE.BETOL,
        SITE.J_1BBRANCH,
        SITE.VTBFI,
        SITE.FPRFW,
        SITE.ACHVM,
        SITE.DVSART,
        SITE.NODETYPE,
        SITE.NSCHEMA,
        SITE.PKOSA,
        SITE.MISCH,
        SITE.MGVUPD,
        SITE.VSTEL,
        SITE.MGVLAUPD,
        SITE.ZZBROCK_CO,
        SITE.ZZPOST_SITE,
        SITE.MGVLAREVAL,
        SITE.SOURCING,
        SITE.OILIVAL,
        SITE.OIHVTYPE,
        SITE.OIHCREDIPI,
        SITE.STORETYPE,
        SITE.DEP_STORE,
        COUNTY.BEZEI 
    FROM
        sappr3.t001w SITE,
        sappr3.t005f COUNTY 
    WHERE
        site.mandt = county.mandt 
        AND site.SPRAS = county.SPRAS 
        AND site.land1 = county.land1 
        AND site.regio = county.regio 
        AND site.counc = county.counc""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_T001W_2")

# COMMAND ----------
# DBTITLE 1, SAP_T001W_SITE_PRE


spark.sql("""INSERT INTO SAP_T001W_SITE_PRE SELECT MANDT AS MANDT,
WERKS AS WERKS,
NAME1 AS NAME1,
BWKEY AS BWKEY,
KUNNR AS KUNNR,
LIFNR AS LIFNR,
FABKL AS FABKL,
NAME2 AS NAME2,
STRAS AS STRAS,
PFACH AS PFACH,
PSTLZ AS PSTLZ,
ORT01 AS ORT01,
EKORG AS EKORG,
VKORG AS VKORG,
CHAZV AS CHAZV,
KKOWK AS KKOWK,
KORDB AS KORDB,
BEDPL AS BEDPL,
LAND1 AS LAND1,
REGIO AS REGIO,
COUNC AS COUNC,
CITYC AS CITYC,
ADRNR AS ADRNR,
IWERK AS IWERK,
TXJCD AS TXJCD,
VTWEG AS VTWEG,
SPART AS SPART,
SPRAS AS SPRAS,
WKSOP AS WKSOP,
AWSLS AS AWSLS,
CHAZV_OLD AS CHAZV_OLD,
VLFKZ AS VLFKZ,
BZIRK AS BZIRK,
ZONE1 AS ZONE1,
TAXIW AS TAXIW,
BZQHL AS BZQHL,
LET01 AS LET01,
LET02 AS LET02,
LET03 AS LET03,
TXNAM_MA1 AS TXNAM_MA1,
TXNAM_MA2 AS TXNAM_MA2,
TXNAM_MA3 AS TXNAM_MA3,
BETOL AS BETOL,
J_1BBRANCH AS J_1BBRANCH,
VTBFI AS VTBFI,
FPRFW AS FPRFW,
ACHVM AS ACHVM,
DVSART AS DVSART,
NODETYPE AS NODETYPE,
NSCHEMA AS NSCHEMA,
PKOSA AS PKOSA,
MISCH AS MISCH,
MGVUPD AS MGVUPD,
VSTEL AS VSTEL,
MGVLAUPD AS MGVLAUPD,
ZZBROCK_CO AS ZZBROCK_CO,
ZZPOST_SITE AS ZZPOST_SITE,
MGVLAREVAL AS MGVLAREVAL,
SOURCING AS SOURCING,
OILIVAL AS OILIVAL,
OIHVTYPE AS OIHVTYPE,
OIHCREDIPI AS OIHCREDIPI,
STORETYPE AS STORETYPE,
DEP_STORE AS DEP_STORE,
BEZEI AS BEZEI FROM SQ_Shortcut_to_T001W_2""")