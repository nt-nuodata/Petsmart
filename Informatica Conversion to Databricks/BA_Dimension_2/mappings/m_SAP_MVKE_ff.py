# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MVKE_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        MATNR AS MATNR,
        VKORG AS VKORG,
        VTWEG AS VTWEG,
        LVORM AS LVORM,
        VERSG AS VERSG,
        BONUS AS BONUS,
        PROVG AS PROVG,
        SKTOF AS SKTOF,
        VMSTA AS VMSTA,
        VMSTD AS VMSTD,
        AUMNG AS AUMNG,
        LFMNG AS LFMNG,
        EFMNG AS EFMNG,
        SCMNG AS SCMNG,
        SCHME AS SCHME,
        VRKME AS VRKME,
        MTPOS AS MTPOS,
        DWERK AS DWERK,
        PRODH AS PRODH,
        PMATN AS PMATN,
        KONDM AS KONDM,
        KTGRM AS KTGRM,
        MVGR1 AS MVGR1,
        MVGR2 AS MVGR2,
        MVGR3 AS MVGR3,
        MVGR4 AS MVGR4,
        MVGR5 AS MVGR5,
        SSTUF AS SSTUF,
        PFLKS AS PFLKS,
        LSTFL AS LSTFL,
        LSTVZ AS LSTVZ,
        LSTAK AS LSTAK,
        LDVFL AS LDVFL,
        LDBFL AS LDBFL,
        LDVZL AS LDVZL,
        LDBZL AS LDBZL,
        VDVFL AS VDVFL,
        VDBFL AS VDBFL,
        VDVZL AS VDVZL,
        VDBZL AS VDBZL,
        PRAT1 AS PRAT1,
        PRAT2 AS PRAT2,
        PRAT3 AS PRAT3,
        PRAT4 AS PRAT4,
        PRAT5 AS PRAT5,
        PRAT6 AS PRAT6,
        PRAT7 AS PRAT7,
        PRAT8 AS PRAT8,
        PRAT9 AS PRAT9,
        PRATA AS PRATA,
        RDPRF AS RDPRF,
        MEGRU AS MEGRU,
        LFMAX AS LFMAX,
        RJART AS RJART,
        PBIND AS PBIND,
        VAVME AS VAVME,
        MATKC AS MATKC,
        PVMSO AS PVMSO,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MVKE""")

df_0.createOrReplaceTempView("MVKE_0")

# COMMAND ----------
# DBTITLE 1, MVKE_ff
