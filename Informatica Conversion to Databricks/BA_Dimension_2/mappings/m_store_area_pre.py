# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, STORE_AREA_0


df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        AREA_SQ_FT_AMT_2 AS AREA_SQ_FT_AMT_2,
        AREA_SQ_FT_AMT_10 AS AREA_SQ_FT_AMT_10,
        AREA_SQ_FT_AMT_11 AS AREA_SQ_FT_AMT_11,
        AREA_SQ_FT_AMT_12 AS AREA_SQ_FT_AMT_12,
        AREA_SQ_FT_AMT_13 AS AREA_SQ_FT_AMT_13,
        AREA_SQ_FT_AMT_14 AS AREA_SQ_FT_AMT_14,
        AREA_SQ_FT_AMT_15 AS AREA_SQ_FT_AMT_15,
        AREA_SQ_FT_AMT_16 AS AREA_SQ_FT_AMT_16,
        AREA_SQ_FT_AMT_17 AS AREA_SQ_FT_AMT_17,
        AREA_SQ_FT_AMT_3 AS AREA_SQ_FT_AMT_3,
        AREA_SQ_FT_AMT_4 AS AREA_SQ_FT_AMT_4,
        AREA_SQ_FT_AMT_5 AS AREA_SQ_FT_AMT_5,
        AREA_SQ_FT_AMT_6 AS AREA_SQ_FT_AMT_6,
        AREA_SQ_FT_AMT_7 AS AREA_SQ_FT_AMT_7,
        AREA_SQ_FT_AMT_8 AS AREA_SQ_FT_AMT_8,
        AREA_SQ_FT_AMT_9 AS AREA_SQ_FT_AMT_9,
        AREA_SQ_FT_AMT_STORE_1 AS AREA_SQ_FT_AMT_STORE_1,
        AREA_SQ_FT_AMT_18 AS AREA_SQ_FT_AMT_18,
        AREA_SQ_FT_AMT_19 AS AREA_SQ_FT_AMT_19,
        AREA_SQ_FT_AMT_20 AS AREA_SQ_FT_AMT_20,
        AREA_SQ_FT_AMT_21 AS AREA_SQ_FT_AMT_21,
        AREA_SQ_FT_AMT_22 AS AREA_SQ_FT_AMT_22,
        AREA_SQ_FT_AMT_23 AS AREA_SQ_FT_AMT_23,
        AREA_SQ_FT_AMT_24 AS AREA_SQ_FT_AMT_24,
        AREA_SQ_FT_AMT_25 AS AREA_SQ_FT_AMT_25,
        AREA_SQ_FT_AMT_26 AS AREA_SQ_FT_AMT_26,
        AREA_SQ_FT_AMT_27 AS AREA_SQ_FT_AMT_27,
        AREA_SQ_FT_AMT_28 AS AREA_SQ_FT_AMT_28,
        AREA_SQ_FT_AMT_29 AS AREA_SQ_FT_AMT_29,
        AREA_SQ_FT_AMT_30 AS AREA_SQ_FT_AMT_30,
        AREA_SQ_FT_AMT_31 AS AREA_SQ_FT_AMT_31,
        AREA_SQ_FT_AMT_32 AS AREA_SQ_FT_AMT_32,
        AREA_SQ_FT_AMT_33 AS AREA_SQ_FT_AMT_33,
        AREA_SQ_FT_AMT_34 AS AREA_SQ_FT_AMT_34,
        AREA_SQ_FT_AMT_35 AS AREA_SQ_FT_AMT_35,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STORE_AREA""")

df_0.createOrReplaceTempView("STORE_AREA_0")

# COMMAND ----------
# DBTITLE 1, SQ_STORE_AREA_1


df_1=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        AREA_SQ_FT_AMT_2 AS AREA_SQ_FT_AMT_2,
        AREA_SQ_FT_AMT_10 AS AREA_SQ_FT_AMT_10,
        AREA_SQ_FT_AMT_11 AS AREA_SQ_FT_AMT_11,
        AREA_SQ_FT_AMT_12 AS AREA_SQ_FT_AMT_12,
        AREA_SQ_FT_AMT_13 AS AREA_SQ_FT_AMT_13,
        AREA_SQ_FT_AMT_14 AS AREA_SQ_FT_AMT_14,
        AREA_SQ_FT_AMT_15 AS AREA_SQ_FT_AMT_15,
        AREA_SQ_FT_AMT_16 AS AREA_SQ_FT_AMT_16,
        AREA_SQ_FT_AMT_17 AS AREA_SQ_FT_AMT_17,
        AREA_SQ_FT_AMT_3 AS AREA_SQ_FT_AMT_3,
        AREA_SQ_FT_AMT_4 AS AREA_SQ_FT_AMT_4,
        AREA_SQ_FT_AMT_5 AS AREA_SQ_FT_AMT_5,
        AREA_SQ_FT_AMT_6 AS AREA_SQ_FT_AMT_6,
        AREA_SQ_FT_AMT_7 AS AREA_SQ_FT_AMT_7,
        AREA_SQ_FT_AMT_8 AS AREA_SQ_FT_AMT_8,
        AREA_SQ_FT_AMT_9 AS AREA_SQ_FT_AMT_9,
        AREA_SQ_FT_AMT_STORE_1 AS AREA_SQ_FT_AMT_STORE_1,
        AREA_SQ_FT_AMT_18 AS AREA_SQ_FT_AMT_18,
        AREA_SQ_FT_AMT_19 AS AREA_SQ_FT_AMT_19,
        AREA_SQ_FT_AMT_20 AS AREA_SQ_FT_AMT_20,
        AREA_SQ_FT_AMT_21 AS AREA_SQ_FT_AMT_21,
        AREA_SQ_FT_AMT_22 AS AREA_SQ_FT_AMT_22,
        AREA_SQ_FT_AMT_23 AS AREA_SQ_FT_AMT_23,
        AREA_SQ_FT_AMT_24 AS AREA_SQ_FT_AMT_24,
        AREA_SQ_FT_AMT_25 AS AREA_SQ_FT_AMT_25,
        AREA_SQ_FT_AMT_26 AS AREA_SQ_FT_AMT_26,
        AREA_SQ_FT_AMT_27 AS AREA_SQ_FT_AMT_27,
        AREA_SQ_FT_AMT_28 AS AREA_SQ_FT_AMT_28,
        AREA_SQ_FT_AMT_29 AS AREA_SQ_FT_AMT_29,
        AREA_SQ_FT_AMT_30 AS AREA_SQ_FT_AMT_30,
        AREA_SQ_FT_AMT_31 AS AREA_SQ_FT_AMT_31,
        AREA_SQ_FT_AMT_32 AS AREA_SQ_FT_AMT_32,
        AREA_SQ_FT_AMT_33 AS AREA_SQ_FT_AMT_33,
        AREA_SQ_FT_AMT_34 AS AREA_SQ_FT_AMT_34,
        AREA_SQ_FT_AMT_35 AS AREA_SQ_FT_AMT_35,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        STORE_AREA_0""")

df_1.createOrReplaceTempView("SQ_STORE_AREA_1")

# COMMAND ----------
# DBTITLE 1, EXP_STORE_AREA_PRE_2


df_2=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        SESSSTARTTIME AS LOC_AREA_EFF_DT,
        AREA_SQ_FT_AMT_2 AS AREA_SQ_FT_AMT_2,
        AREA_SQ_FT_AMT_10 AS AREA_SQ_FT_AMT_10,
        AREA_SQ_FT_AMT_11 AS AREA_SQ_FT_AMT_11,
        AREA_SQ_FT_AMT_12 AS AREA_SQ_FT_AMT_12,
        AREA_SQ_FT_AMT_13 AS AREA_SQ_FT_AMT_13,
        AREA_SQ_FT_AMT_14 AS AREA_SQ_FT_AMT_14,
        AREA_SQ_FT_AMT_15 AS AREA_SQ_FT_AMT_15,
        AREA_SQ_FT_AMT_16 AS AREA_SQ_FT_AMT_16,
        AREA_SQ_FT_AMT_17 AS AREA_SQ_FT_AMT_17,
        AREA_SQ_FT_AMT_3 AS AREA_SQ_FT_AMT_3,
        AREA_SQ_FT_AMT_4 AS AREA_SQ_FT_AMT_4,
        AREA_SQ_FT_AMT_5 AS AREA_SQ_FT_AMT_5,
        AREA_SQ_FT_AMT_6 AS AREA_SQ_FT_AMT_6,
        AREA_SQ_FT_AMT_7 AS AREA_SQ_FT_AMT_7,
        AREA_SQ_FT_AMT_8 AS AREA_SQ_FT_AMT_8,
        AREA_SQ_FT_AMT_9 AS AREA_SQ_FT_AMT_9,
        AREA_SQ_FT_AMT_STORE_1 AS AREA_SQ_FT_AMT_STORE_1,
        AREA_SQ_FT_AMT_18 AS AREA_SQ_FT_AMT_18,
        AREA_SQ_FT_AMT_19 AS AREA_SQ_FT_AMT_19,
        AREA_SQ_FT_AMT_20 AS AREA_SQ_FT_AMT_20,
        AREA_SQ_FT_AMT_21 AS AREA_SQ_FT_AMT_21,
        AREA_SQ_FT_AMT_22 AS AREA_SQ_FT_AMT_22,
        AREA_SQ_FT_AMT_23 AS AREA_SQ_FT_AMT_23,
        AREA_SQ_FT_AMT_24 AS AREA_SQ_FT_AMT_24,
        AREA_SQ_FT_AMT_25 AS AREA_SQ_FT_AMT_25,
        AREA_SQ_FT_AMT_26 AS AREA_SQ_FT_AMT_26,
        AREA_SQ_FT_AMT_27 AS AREA_SQ_FT_AMT_27,
        AREA_SQ_FT_AMT_28 AS AREA_SQ_FT_AMT_28,
        AREA_SQ_FT_AMT_29 AS AREA_SQ_FT_AMT_29,
        AREA_SQ_FT_AMT_30 AS AREA_SQ_FT_AMT_30,
        AREA_SQ_FT_AMT_31 AS AREA_SQ_FT_AMT_31,
        AREA_SQ_FT_AMT_32 AS AREA_SQ_FT_AMT_32,
        AREA_SQ_FT_AMT_33 AS AREA_SQ_FT_AMT_33,
        AREA_SQ_FT_AMT_34 AS AREA_SQ_FT_AMT_34,
        AREA_SQ_FT_AMT_35 AS AREA_SQ_FT_AMT_35,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_STORE_AREA_1""")

df_2.createOrReplaceTempView("EXP_STORE_AREA_PRE_2")

# COMMAND ----------
# DBTITLE 1, STORE_AREA_PRE


spark.sql("""INSERT INTO STORE_AREA_PRE SELECT STORE_NBR AS STORE_NBR,
LOC_AREA_EFF_DT AS LOC_AREA_EFF_DT,
AREA_SQ_FT_AMT_STORE_1 AS AREA_1_SQ_FT_AMT,
AREA_SQ_FT_AMT_2 AS AREA_2_SQ_FT_AMT,
AREA_SQ_FT_AMT_3 AS AREA_3_SQ_FT_AMT,
AREA_SQ_FT_AMT_4 AS AREA_4_SQ_FT_AMT,
AREA_SQ_FT_AMT_5 AS AREA_5_SQ_FT_AMT,
AREA_SQ_FT_AMT_6 AS AREA_6_SQ_FT_AMT,
AREA_SQ_FT_AMT_7 AS AREA_7_SQ_FT_AMT,
AREA_SQ_FT_AMT_8 AS AREA_8_SQ_FT_AMT,
AREA_SQ_FT_AMT_9 AS AREA_9_SQ_FT_AMT,
AREA_SQ_FT_AMT_10 AS AREA_10_SQ_FT_AMT,
AREA_SQ_FT_AMT_11 AS AREA_11_SQ_FT_AMT,
AREA_SQ_FT_AMT_12 AS AREA_12_SQ_FT_AMT,
AREA_SQ_FT_AMT_13 AS AREA_13_SQ_FT_AMT,
AREA_SQ_FT_AMT_14 AS AREA_14_SQ_FT_AMT,
AREA_SQ_FT_AMT_15 AS AREA_15_SQ_FT_AMT,
AREA_SQ_FT_AMT_16 AS AREA_16_SQ_FT_AMT,
AREA_SQ_FT_AMT_17 AS AREA_17_SQ_FT_AMT,
AREA_SQ_FT_AMT_18 AS AREA_18_SQ_FT_AMT,
AREA_SQ_FT_AMT_19 AS AREA_19_SQ_FT_AMT,
AREA_SQ_FT_AMT_20 AS AREA_20_SQ_FT_AMT,
AREA_SQ_FT_AMT_21 AS AREA_21_SQ_FT_AMT,
AREA_SQ_FT_AMT_22 AS AREA_22_SQ_FT_AMT,
AREA_SQ_FT_AMT_23 AS AREA_23_SQ_FT_AMT,
AREA_SQ_FT_AMT_24 AS AREA_24_SQ_FT_AMT,
AREA_SQ_FT_AMT_25 AS AREA_25_SQ_FT_AMT,
AREA_SQ_FT_AMT_26 AS AREA_26_SQ_FT_AMT,
AREA_SQ_FT_AMT_27 AS AREA_27_SQ_FT_AMT,
AREA_SQ_FT_AMT_28 AS AREA_28_SQ_FT_AMT,
AREA_SQ_FT_AMT_29 AS AREA_29_SQ_FT_AMT,
AREA_SQ_FT_AMT_30 AS AREA_30_SQ_FT_AMT,
AREA_SQ_FT_AMT_31 AS AREA_31_SQ_FT_AMT,
AREA_SQ_FT_AMT_32 AS AREA_32_SQ_FT_AMT,
AREA_SQ_FT_AMT_33 AS AREA_33_SQ_FT_AMT,
AREA_SQ_FT_AMT_34 AS AREA_34_SQ_FT_AMT,
AREA_SQ_FT_AMT_35 AS AREA_35_SQ_FT_AMT FROM EXP_STORE_AREA_PRE_2""")