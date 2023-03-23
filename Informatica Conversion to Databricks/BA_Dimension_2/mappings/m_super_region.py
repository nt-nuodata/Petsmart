# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, REGION_0


df_0=spark.sql("""
    SELECT
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REGION""")

df_0.createOrReplaceTempView("REGION_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_REGION_1


df_1=spark.sql("""
    SELECT
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        REGION_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_REGION_1")

# COMMAND ----------
# DBTITLE 1, EXP_SUPER_REGION_2


df_2=spark.sql("""
    SELECT
        IFF(IN(REGION_ID,
        200,
        300,
        850,
        400),
        1,
        IFF(IN(REGION_ID,
        775,
        800,
        950,
        500),
        4,
        IFF(IN(REGION_ID,
        650),
        5,
        REGION_ID))) AS SUPER_REGION_ID,
        IFF(IN(REGION_ID,
        200,
        300,
        850,
        400),
        'West',
        IFF(IN(REGION_ID,
        775,
        800,
        950,
        500),
        'East',
        IFF(IN(REGION_ID,
        700,
        750,
        600,
        825),
        'NA',
        IFF(IN(REGION_ID,
        650),
        'Canada',
        REGION_DESC)))) AS SUPER_REGION_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_REGION_1""")

df_2.createOrReplaceTempView("EXP_SUPER_REGION_2")

# COMMAND ----------
# DBTITLE 1, AGG_SUPER_REGION_3


df_3=spark.sql("""
    SELECT
        SUPER_REGION_ID AS SUPER_REGION_ID,
        SUPER_REGION_DESC AS SUPER_REGION_DESC 
    FROM
        EXP_SUPER_REGION_2 
    GROUP BY
        SUPER_REGION_ID,
        SUPER_REGION_DESC""")

df_3.createOrReplaceTempView("AGG_SUPER_REGION_3")

# COMMAND ----------
# DBTITLE 1, SUPER_REGION


spark.sql("""INSERT INTO SUPER_REGION SELECT SUPER_REGION_ID AS SUPER_REGION_ID,
SUPER_REGION_DESC AS SUPER_REGION_DESC FROM AGG_SUPER_REGION_3""")