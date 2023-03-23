# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, LFM1_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        LIFNR AS LIFNR,
        EKORG AS EKORG,
        ERDAT AS ERDAT,
        ERNAM AS ERNAM,
        SPERM AS SPERM,
        LOEVM AS LOEVM,
        LFABC AS LFABC,
        WAERS AS WAERS,
        VERKF AS VERKF,
        TELF1 AS TELF1,
        MINBW AS MINBW,
        ZTERM AS ZTERM,
        INCO1 AS INCO1,
        INCO2 AS INCO2,
        WEBRE AS WEBRE,
        KZABS AS KZABS,
        KALSK AS KALSK,
        KZAUT AS KZAUT,
        EXPVZ AS EXPVZ,
        ZOLLA AS ZOLLA,
        MEPRF AS MEPRF,
        EKGRP AS EKGRP,
        BOLRE AS BOLRE,
        UMSAE AS UMSAE,
        XERSY AS XERSY,
        PLIFZ AS PLIFZ,
        MRPPP AS MRPPP,
        LFRHY AS LFRHY,
        LIBES AS LIBES,
        LIPRE AS LIPRE,
        LISER AS LISER,
        ZZRTV_ELIGIBLE AS ZZRTV_ELIGIBLE,
        ZZUSVEND_RTVTYPE AS ZZUSVEND_RTVTYPE,
        ZZCAVEND_RTVTYPE AS ZZCAVEND_RTVTYPE,
        ZZFREIGHTYPE AS ZZFREIGHTYPE,
        ZZWEB_FLAG AS ZZWEB_FLAG,
        PRFRE AS PRFRE,
        NRGEW AS NRGEW,
        BOIND AS BOIND,
        BLIND AS BLIND,
        KZRET AS KZRET,
        SKRIT AS SKRIT,
        BSTAE AS BSTAE,
        RDPRF AS RDPRF,
        MEGRU AS MEGRU,
        VENSL AS VENSL,
        BOPNR AS BOPNR,
        XERSR AS XERSR,
        EIKTO AS EIKTO,
        ABUEB AS ABUEB,
        PAPRF AS PAPRF,
        AGREL AS AGREL,
        XNBWY AS XNBWY,
        VSBED AS VSBED,
        LEBRE AS LEBRE,
        ZZCASH_DISC_FLAG AS ZZCASH_DISC_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LFM1""")

df_0.createOrReplaceTempView("LFM1_0")

# COMMAND ----------
# DBTITLE 1, SQ_LFM1_1


df_1=spark.sql("""
    SELECT
        LIFNR AS LIFNR,
        ERDAT AS ERDAT,
        VERKF AS VERKF,
        TELF1 AS TELF1,
        ZZRTV_ELIGIBLE AS ZZRTV_ELIGIBLE,
        ZZUSVEND_RTVTYPE AS ZZUSVEND_RTVTYPE,
        ZZFREIGHTYPE AS ZZFREIGHTYPE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LFM1_0""")

df_1.createOrReplaceTempView("SQ_LFM1_1")

# COMMAND ----------
# DBTITLE 1, LFM1_PRE


spark.sql("""INSERT INTO LFM1_PRE SELECT LIFNR AS LIFNR,
ERDAT AS ERDAT,
VERKF AS VERKF,
TELF1 AS TELF1,
ZZRTV_ELIGIBLE AS ZZRTV_ELIGIBLE,
ZZUSVEND_RTVTYPE AS ZZUSVEND_RTVTYPE,
ZZFREIGHTYPE AS ZZFREIGHTYPE FROM SQ_LFM1_1""")