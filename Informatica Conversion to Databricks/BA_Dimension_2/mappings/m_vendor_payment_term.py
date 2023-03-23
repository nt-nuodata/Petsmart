# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Payment_Terms_0


df_0=spark.sql("""
    SELECT
        PaymentTermCd AS PaymentTermCd,
        PaymentTermDesc AS PaymentTermDesc,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Payment_Terms""")

df_0.createOrReplaceTempView("Payment_Terms_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Payment_Terms_1


df_1=spark.sql("""
    SELECT
        PaymentTermCd AS PaymentTermCd,
        PaymentTermDesc AS PaymentTermDesc,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Payment_Terms_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Payment_Terms_1")

# COMMAND ----------
# DBTITLE 1, LKPTRANS_2


df_2=spark.sql("""
    SELECT
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        PAYMENT_TERM_DESC AS PAYMENT_TERM_DESC,
        SQ_Shortcut_to_Payment_Terms_1.PaymentTermCd AS PaymentTermCd,
        SQ_Shortcut_to_Payment_Terms_1.PaymentTermDesc AS PaymentTermDesc,
        SQ_Shortcut_to_Payment_Terms_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PAYMENT_TERM 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_Payment_Terms_1 
            ON PAYMENT_TERM_CD = SQ_Shortcut_to_Payment_Terms_1.PaymentTermCd""")

df_2.createOrReplaceTempView("LKPTRANS_2")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_3


df_3=spark.sql("""
    SELECT
        PaymentTermCd AS PaymentTermCd,
        PaymentTermDesc AS PaymentTermDesc,
        IFF(PaymentTermDesc = '',
        'UnKnown',
        PaymentTermDesc) AS PaymentTermDesc_Default,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKPTRANS_2""")

df_3.createOrReplaceTempView("EXPTRANS_3")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_4


df_4=spark.sql("""
    SELECT
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        PaymentTermCd AS PaymentTermCd,
        PaymentTermDesc AS PaymentTermDesc,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKPTRANS_2""")

df_4.createOrReplaceTempView("UPDTRANS_4")

df_4=spark.sql("""
    SELECT
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        PaymentTermCd AS PaymentTermCd,
        PaymentTermDesc AS PaymentTermDesc,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_3""")

df_4.createOrReplaceTempView("UPDTRANS_4")

# COMMAND ----------
# DBTITLE 1, VENDOR_PAYMENT_TERM


spark.sql("""INSERT INTO VENDOR_PAYMENT_TERM SELECT PaymentTermCd AS PAYMENT_TERM_CD,
PaymentTermDesc AS PAYMENT_TERM_DESC FROM UPDTRANS_4""")