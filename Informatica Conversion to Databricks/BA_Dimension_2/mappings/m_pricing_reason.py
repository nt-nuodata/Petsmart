# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, TWBNT_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        SPRAS AS SPRAS,
        PV_GRUND AS PV_GRUND,
        PV_GRTXT AS PV_GRTXT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TWBNT""")

df_0.createOrReplaceTempView("TWBNT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TWBNT_1


df_1=spark.sql("""
    SELECT
        TRIM(PV_GRUND) PRICING_REASON_CD,
        PV_GRTXT PRICING_REASON_DESC 
    FROM
        SAPPR3.TWBNT 
    WHERE
        MANDT = '100' 
        AND SPRAS = 'E'""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_TWBNT_1")

# COMMAND ----------
# DBTITLE 1, PRICING_REASON_2


df_2=spark.sql("""
    SELECT
        PRICING_REASON_CD AS PRICING_REASON_CD,
        PRICING_REASON_DESC AS PRICING_REASON_DESC,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PRICING_REASON""")

df_2.createOrReplaceTempView("PRICING_REASON_2")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_PRICING_REASON_3


df_3=spark.sql("""
    SELECT
        PRICING_REASON_CD AS PRICING_REASON_CD,
        PRICING_REASON_DESC AS PRICING_REASON_DESC,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PRICING_REASON_2""")

df_3.createOrReplaceTempView("ASQ_Shortcut_to_PRICING_REASON_3")

# COMMAND ----------
# DBTITLE 1, JNR_PRICING_REASON_4


df_4=spark.sql("""
    SELECT
        DETAIL.PV_GRUND AS PRICING_REASON_CD_sap,
        DETAIL.PV_GRTXT AS PRICING_REASON_DESC_sap,
        MASTER.PRICING_REASON_CD AS PRICING_REASON_CD_edw,
        MASTER.PRICING_REASON_DESC AS PRICING_REASON_DESC_edw,
        MASTER.LOAD_DT AS LOAD_DT_edw,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_to_PRICING_REASON_3 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_TWBNT_1 DETAIL 
            ON MASTER.PRICING_REASON_CD = DETAIL.PV_GRUND""")

df_4.createOrReplaceTempView("JNR_PRICING_REASON_4")

# COMMAND ----------
# DBTITLE 1, EXP_PRICING_REASON_5


df_5=spark.sql("""
    SELECT
        IFF(IsNull(PRICING_REASON_CD_sap),
        PRICING_REASON_CD_edw,
        PRICING_REASON_CD_sap) AS PRICING_REASON_CD_out,
        PRICING_REASON_DESC_sap AS PRICING_REASON_DESC_sap,
        date_trunc('DAY',
        current_timestamp) AS UPDATE_DT,
        IFF(IsNull(LOAD_DT_edw),
        date_trunc('DAY',
        current_timestamp),
        LOAD_DT_edw) AS LOAD_DT_out,
        IFF(IsNull(PRICING_REASON_CD_sap),
        DD_DELETE,
        IFF(IsNull(PRICING_REASON_CD_edw),
        DD_INSERT,
        IFF(IFF(IsNull(PRICING_REASON_DESC_sap),
        'NULL',
        PRICING_REASON_DESC_sap) <> IFF(IsNull(PRICING_REASON_DESC_edw),
        'NULL',
        PRICING_REASON_DESC_edw),
        DD_UPDATE,
        DD_REJECT))) AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_PRICING_REASON_4""")

df_5.createOrReplaceTempView("EXP_PRICING_REASON_5")

# COMMAND ----------
# DBTITLE 1, FTR_PRICING_REASON_6


df_6=spark.sql("""
    SELECT
        PRICING_REASON_CD AS PRICING_REASON_CD,
        PRICING_REASON_DESC AS PRICING_REASON_DESC,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        UPDATE_STRATEGY AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_PRICING_REASON_5 
    WHERE
        UPDATE_STRATEGY <> DD_REJECT""")

df_6.createOrReplaceTempView("FTR_PRICING_REASON_6")

# COMMAND ----------
# DBTITLE 1, UPD_PRICING_REASON_7


df_7=spark.sql("""
    SELECT
        PRICING_REASON_CD AS PRICING_REASON_CD,
        PRICING_REASON_DESC AS PRICING_REASON_DESC,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        UPDATE_STRATEGY AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FTR_PRICING_REASON_6""")

df_7.createOrReplaceTempView("UPD_PRICING_REASON_7")

# COMMAND ----------
# DBTITLE 1, PRICING_REASON


spark.sql("""INSERT INTO PRICING_REASON SELECT PRICING_REASON_CD AS PRICING_REASON_CD,
PRICING_REASON_DESC AS PRICING_REASON_DESC,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPD_PRICING_REASON_7""")