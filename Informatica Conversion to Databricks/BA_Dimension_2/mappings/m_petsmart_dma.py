# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, LOYALTY_PRE_0

df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LOYALTY_PRE""")

df_0.createOrReplaceTempView("LOYALTY_PRE_0")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_To_LOYALTY_PRE_Insert_1

df_1=spark.sql("""
    SELECT
        DISTINCT RTRIM(LP.PETSMART_DMA_CD),
        MAX(RTRIM(LP.PETSMART_DMA_DESC)) 
    FROM
        LOYALTY_PRE LP 
    WHERE
        NOT EXISTS (
            SELECT
                * 
            FROM
                LOYALTY_PRE LP,
                PETSMART_DMA PD 
            WHERE
                RTRIM(LP.PETSMART_DMA_CD) = RTRIM(PD.PETSMART_DMA_CD)
        ) 
    GROUP BY
        LP.PETSMART_DMA_CD 
    ORDER BY
        MAX(RTRIM(LP.PETSMART_DMA_DESC))""")

df_1.createOrReplaceTempView("ASQ_Shortcut_To_LOYALTY_PRE_Insert_1")

# COMMAND ----------

# DBTITLE 1, UPD_INSERT_2

df_2=spark.sql("""
    SELECT
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_To_LOYALTY_PRE_Insert_1""")

df_2.createOrReplaceTempView("UPD_INSERT_2")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_To_LOYALTY_PRE_Update_3

df_3=spark.sql("""
    SELECT
        DISTINCT RTRIM(LP.PETSMART_DMA_CD),
        MAX(RTRIM(LP.PETSMART_DMA_DESC)) 
    FROM
        LOYALTY_PRE LP 
    GROUP BY
        LP.PETSMART_DMA_CD 
    ORDER BY
        MAX(RTRIM(LP.PETSMART_DMA_DESC))""")

df_3.createOrReplaceTempView("ASQ_Shortcut_To_LOYALTY_PRE_Update_3")

# COMMAND ----------

# DBTITLE 1, UPD_UPDATE_EXISTING_4

df_4=spark.sql("""
    SELECT
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_To_LOYALTY_PRE_Update_3""")

df_4.createOrReplaceTempView("UPD_UPDATE_EXISTING_4")

# COMMAND ----------

# DBTITLE 1, PETSMART_DMA

spark.sql("""INSERT INTO PETSMART_DMA SELECT PETSMART_DMA_CD AS PETSMART_DMA_CD,
PETSMART_DMA_DESC AS PETSMART_DMA_DESC FROM UPD_INSERT_2""")

# COMMAND ----------

# DBTITLE 1, PETSMART_DMA

spark.sql("""INSERT INTO PETSMART_DMA SELECT PETSMART_DMA_CD AS PETSMART_DMA_CD,
PETSMART_DMA_DESC AS PETSMART_DMA_DESC FROM UPD_UPDATE_EXISTING_4""")
