# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, Inco_Term_0

df_0=spark.sql("""
    SELECT
        IncoTermCd AS IncoTermCd,
        IncoTermDesc AS IncoTermDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Inco_Term""")

df_0.createOrReplaceTempView("Inco_Term_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_Inco_Term_1

df_1=spark.sql("""
    SELECT
        IncoTermCd AS IncoTermCd,
        IncoTermDesc AS IncoTermDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Inco_Term_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Inco_Term_1")

# COMMAND ----------

# DBTITLE 1, LKPTRANS_2

df_2=spark.sql("""
    SELECT
        INCO_TERM_CD AS INCO_TERM_CD,
        INCO_TERM_DESC AS INCO_TERM_DESC,
        SQ_Shortcut_to_Inco_Term_1.IncoTermCd AS IncoTermCd,
        SQ_Shortcut_to_Inco_Term_1.IncoTermDesc AS IncoTermDesc,
        SQ_Shortcut_to_Inco_Term_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_INCO_TERM 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_Inco_Term_1 
            ON INCO_TERM_CD = SQ_Shortcut_to_Inco_Term_1.IncoTermCd""")

df_2.createOrReplaceTempView("LKPTRANS_2")

# COMMAND ----------

# DBTITLE 1, UPDTRANS_3

df_3=spark.sql("""
    SELECT
        INCO_TERM_CD AS INCO_TERM_CD,
        INCO_TERM_DESC AS INCO_TERM_DESC,
        IncoTermCd AS IncoTermCd,
        IncoTermDesc AS IncoTermDesc,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKPTRANS_2""")

df_3.createOrReplaceTempView("UPDTRANS_3")

# COMMAND ----------

# DBTITLE 1, VENDOR_INCO_TERM

spark.sql("""INSERT INTO VENDOR_INCO_TERM SELECT IncoTermCd AS INCO_TERM_CD,
IncoTermDesc AS INCO_TERM_DESC FROM UPDTRANS_3""")
