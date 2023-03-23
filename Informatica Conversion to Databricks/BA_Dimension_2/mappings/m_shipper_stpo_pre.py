# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, STPO_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        STLTY AS STLTY,
        STLNR AS STLNR,
        STLKN AS STLKN,
        STPOZ AS STPOZ,
        DATUV AS DATUV,
        TECHV AS TECHV,
        AENNR AS AENNR,
        LKENZ AS LKENZ,
        VGKNT AS VGKNT,
        VGPZL AS VGPZL,
        ANDAT AS ANDAT,
        ANNAM AS ANNAM,
        AEDAT AS AEDAT,
        AENAM AS AENAM,
        IDNRK AS IDNRK,
        PSWRK AS PSWRK,
        POSTP AS POSTP,
        POSNR AS POSNR,
        SORTF AS SORTF,
        MEINS AS MEINS,
        MENGE AS MENGE,
        FMENG AS FMENG,
        AUSCH AS AUSCH,
        AVOAU AS AVOAU,
        NETAU AS NETAU,
        SCHGT AS SCHGT,
        BEIKZ AS BEIKZ,
        ERSKZ AS ERSKZ,
        RVREL AS RVREL,
        SANFE AS SANFE,
        SANIN AS SANIN,
        SANKA AS SANKA,
        SANKO AS SANKO,
        SANVS AS SANVS,
        STKKZ AS STKKZ,
        REKRI AS REKRI,
        REKRS AS REKRS,
        CADPO AS CADPO,
        NFMAT AS NFMAT,
        NLFZT AS NLFZT,
        VERTI AS VERTI,
        ALPOS AS ALPOS,
        EWAHR AS EWAHR,
        EKGRP AS EKGRP,
        LIFZT AS LIFZT,
        LIFNR AS LIFNR,
        PREIS AS PREIS,
        PEINH AS PEINH,
        WAERS AS WAERS,
        SAKTO AS SAKTO,
        ROANZ AS ROANZ,
        ROMS1 AS ROMS1,
        ROMS2 AS ROMS2,
        ROMS3 AS ROMS3,
        ROMEI AS ROMEI,
        ROMEN AS ROMEN,
        RFORM AS RFORM,
        UPSKZ AS UPSKZ,
        VALKZ AS VALKZ,
        LTXSP AS LTXSP,
        POTX1 AS POTX1,
        POTX2 AS POTX2,
        OBJTY AS OBJTY,
        MATKL AS MATKL,
        WEBAZ AS WEBAZ,
        DOKAR AS DOKAR,
        DOKNR AS DOKNR,
        DOKVR AS DOKVR,
        DOKTL AS DOKTL,
        CSSTR AS CSSTR,
        CLASS AS CLASS,
        KLART AS KLART,
        POTPR AS POTPR,
        AWAKZ AS AWAKZ,
        INSKZ AS INSKZ,
        VCEKZ AS VCEKZ,
        VSTKZ AS VSTKZ,
        VACKZ AS VACKZ,
        EKORG AS EKORG,
        CLOBK AS CLOBK,
        CLMUL AS CLMUL,
        CLALT AS CLALT,
        CVIEW AS CVIEW,
        KNOBJ AS KNOBJ,
        LGORT AS LGORT,
        KZKUP AS KZKUP,
        INTRM AS INTRM,
        TPEKZ AS TPEKZ,
        STVKN AS STVKN,
        DVDAT AS DVDAT,
        DVNAM AS DVNAM,
        DSPST AS DSPST,
        ALPST AS ALPST,
        ALPRF AS ALPRF,
        ALPGR AS ALPGR,
        KZNFP AS KZNFP,
        NFGRP AS NFGRP,
        NFEAG AS NFEAG,
        KNDVB AS KNDVB,
        KNDBZ AS KNDBZ,
        KSTTY AS KSTTY,
        KSTNR AS KSTNR,
        KSTKN AS KSTKN,
        KSTPZ AS KSTPZ,
        CLSZU AS CLSZU,
        KZCLB AS KZCLB,
        AEHLP AS AEHLP,
        PRVBE AS PRVBE,
        NLFZV AS NLFZV,
        NLFMV AS NLFMV,
        IDPOS AS IDPOS,
        IDHIS AS IDHIS,
        IDVAR AS IDVAR,
        ALEKZ AS ALEKZ,
        ITMID AS ITMID,
        GUID AS GUID,
        ITSOB AS ITSOB,
        RFPNT AS RFPNT,
        GUIDX AS GUIDX,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STPO""")

df_0.createOrReplaceTempView("STPO_0")

# COMMAND ----------

# DBTITLE 1, SQ_STPO_1

df_1=spark.sql("""
    SELECT
        STPO.STLTY,
        STPO.STLNR,
        STPO.STLKN,
        STPO.STPOZ,
        STPO.DATUV,
        STPO.ANDAT,
        STPO.ANNAM,
        STPO.AEDAT,
        STPO.AENAM,
        STPO.IDNRK,
        STPO.POSNR,
        STPO.MEINS,
        STPO.MENGE,
        STPO.POTX1,
        STPO.MATKL,
        STPO.STVKN,
        STPO.ALEKZ,
        STPO.GUIDX 
    FROM
        SAPPR3.STPO""")

df_1.createOrReplaceTempView("SQ_STPO_1")

# COMMAND ----------

# DBTITLE 1, SHIPPER_STPO_PRE

spark.sql("""INSERT INTO SHIPPER_STPO_PRE SELECT STLTY AS BOM_CATEGORY_ID,
STLNR AS BILL_OF_MATERIAL,
STLKN AS ITEM_NODE,
STPOZ AS INTERNAL_CNTR,
DATUV AS VALID_FROM_DT,
ANDAT AS CREATED_ON_DT,
ANNAM AS CREATED_BY_NAME,
AEDAT AS CHANGED_ON_DT,
AENAM AS CHANGED_BY_NAME,
IDNRK AS BOM_COMPONENT,
POSNR AS ITEM_NBR,
MEINS AS COMPONENT_UNIT,
MENGE AS COMPONENT_QTY,
POTX1 AS LINE_1_ITEM_TEXT,
MATKL AS MERCH_CATEGORY_ID,
STVKN AS INHERITED_NODE_NBR,
ALEKZ AS ALE_IND,
GUIDX AS ITEM_CHG_STATUS_ID FROM SQ_STPO_1""")
