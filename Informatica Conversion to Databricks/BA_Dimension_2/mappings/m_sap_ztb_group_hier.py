# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTB_GROUP_HIER_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        DEPARTMENT AS DEPARTMENT,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_GROUP_HIER""")

df_0.createOrReplaceTempView("ZTB_GROUP_HIER_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_ZTB_GROUP_HIER_1

df_1=spark.sql("""
    SELECT
        MANDT AS MANDT,
        DEPARTMENT AS DEPARTMENT,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ZTB_GROUP_HIER_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_GROUP_HIER_1")

# COMMAND ----------

# DBTITLE 1, EXP_Department_2

df_2=spark.sql("""
    SELECT
        MANDT AS MANDT,
        LTRIM(RTRIM(DEPARTMENT)) AS DEPARTMENT,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ZTB_GROUP_HIER_1""")

df_2.createOrReplaceTempView("EXP_Department_2")

# COMMAND ----------

# DBTITLE 1, FIL_MANDT_3

df_3=spark.sql("""
    SELECT
        MANDT AS MANDT,
        DEPARTMENT AS DEPARTMENT,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Department_2 
    WHERE
        MANDT = '100'""")

df_3.createOrReplaceTempView("FIL_MANDT_3")

# COMMAND ----------

# DBTITLE 1, MERCHDEPT_ORG_4

df_4=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        BUS_UNIT_ID AS BUS_UNIT_ID,
        BUS_UNIT_DESC AS BUS_UNIT_DESC,
        BUYER_ID AS BUYER_ID,
        BUYER_NM AS BUYER_NM,
        CA_BUYER_ID AS CA_BUYER_ID,
        CA_BUYER_NM AS CA_BUYER_NM,
        CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
        CA_DIRECTOR_NM AS CA_DIRECTOR_NM,
        CA_MANAGED_FLG AS CA_MANAGED_FLG,
        DIRECTOR_ID AS DIRECTOR_ID,
        DIRECTOR_NM AS DIRECTOR_NM,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        PRICING_ROLE_ID AS PRICING_ROLE_ID,
        PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        SVP_ID AS SVP_ID,
        SVP_NM AS SVP_NM,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        VP_NM AS VP_NM,
        CA_VP_ID AS CA_VP_ID,
        CA_VP_NM AS CA_VP_NM,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MERCHDEPT_ORG""")

df_4.createOrReplaceTempView("MERCHDEPT_ORG_4")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_MERCHDEPT_ORG_5

df_5=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        BUS_UNIT_ID AS BUS_UNIT_ID,
        BUS_UNIT_DESC AS BUS_UNIT_DESC,
        BUYER_ID AS BUYER_ID,
        BUYER_NM AS BUYER_NM,
        CA_BUYER_ID AS CA_BUYER_ID,
        CA_BUYER_NM AS CA_BUYER_NM,
        CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
        CA_DIRECTOR_NM AS CA_DIRECTOR_NM,
        CA_MANAGED_FLG AS CA_MANAGED_FLG,
        DIRECTOR_ID AS DIRECTOR_ID,
        DIRECTOR_NM AS DIRECTOR_NM,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        PRICING_ROLE_ID AS PRICING_ROLE_ID,
        PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        SVP_ID AS SVP_ID,
        SVP_NM AS SVP_NM,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        VP_NM AS VP_NM,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MERCHDEPT_ORG_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_MERCHDEPT_ORG_5")

# COMMAND ----------

# DBTITLE 1, EXP_Id_6

df_6=spark.sql("""
    SELECT
        TO_CHAR(SAP_DEPT_ID) AS SAP_DEPT_ID,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_MERCHDEPT_ORG_5""")

df_6.createOrReplaceTempView("EXP_Id_6")

# COMMAND ----------

# DBTITLE 1, JNR_Department_7

df_7=spark.sql("""
    SELECT
        MASTER.SAP_DEPT_ID AS SAP_DEPT_ID,
        MASTER.GROUP_ID AS GROUP_ID1,
        MASTER.GROUP_DESC AS GROUP_DESC1,
        MASTER.SEGMENT_ID AS SEGMENT_ID1,
        MASTER.SEGMENT_DESC AS SEGMENT_DESC1,
        MASTER.VP_ID AS VP_ID1,
        MASTER.VP_DESC AS VP_DESC1,
        DETAIL.MANDT AS MANDT,
        DETAIL.DEPARTMENT AS DEPARTMENT,
        DETAIL.SEGMENT_ID AS SEGMENT_ID,
        DETAIL.SEGMENT_DESC AS SEGMENT_DESC,
        DETAIL.GROUP_ID AS GROUP_ID,
        DETAIL.GROUP_DESC AS GROUP_DESC,
        DETAIL.VP_ID AS VP_ID,
        DETAIL.VP_DESC AS VP_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Id_6 MASTER 
    LEFT JOIN
        FIL_MANDT_3 DETAIL 
            ON MASTER.SAP_DEPT_ID = DETAIL.DEPARTMENT""")

df_7.createOrReplaceTempView("JNR_Department_7")

# COMMAND ----------

# DBTITLE 1, EXP_Strategy_8

df_8=spark.sql("""
    SELECT
        '100' AS MANDT,
        SAP_DEPT_ID AS DEPARTMENT,
        TO_CHAR(SEGMENT_ID1) AS SEGMENT_ID,
        SUBSTR(SEGMENT_DESC1,
        0,
        20) AS SEGMENT_DESC,
        TO_CHAR(GROUP_ID1) AS GROUP_ID,
        SUBSTR(GROUP_DESC1,
        0,
        20) AS GROUP_DESC,
        TO_CHAR(VP_ID1) AS VP_ID,
        VP_DESC1 AS VP_DESC,
        IFF(ISNULL(SAP_DEPT_ID),
        DD_DELETE,
        IFF(ISNULL(DEPARTMENT),
        DD_INSERT,
        IFF(IFF(ISNULL(TO_CHAR(SEGMENT_ID1)),
        'NULL',
        TO_CHAR(SEGMENT_ID1)) <> IFF(ISNULL(SEGMENT_ID),
        'NULL',
        SEGMENT_ID) 
        OR IFF(ISNULL(SUBSTR(SEGMENT_DESC1,
        0,
        20)),
        'NULL',
        SUBSTR(SEGMENT_DESC1,
        0,
        20)) <> IFF(ISNULL(SEGMENT_DESC),
        'NULL',
        SEGMENT_DESC) 
        OR IFF(ISNULL(TO_CHAR(GROUP_ID1)),
        'NULL',
        TO_CHAR(GROUP_ID1)) <> IFF(ISNULL(GROUP_ID),
        'NULL',
        GROUP_ID) 
        OR IFF(ISNULL(SUBSTR(GROUP_DESC1,
        0,
        20)),
        'NULL',
        SUBSTR(GROUP_DESC1,
        0,
        20)) <> IFF(ISNULL(GROUP_DESC),
        'NULL',
        GROUP_DESC) 
        OR IFF(ISNULL(TO_CHAR(VP_ID1)),
        'NULL',
        TO_CHAR(VP_ID1)) <> IFF(ISNULL(VP_ID),
        'NULL',
        VP_ID) 
        OR IFF(ISNULL(VP_DESC1),
        'NULL',
        VP_DESC1) <> IFF(ISNULL(VP_DESC),
        'NULL',
        VP_DESC),
        DD_UPDATE,
        DD_REJECT))) AS UpdateStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_Department_7""")

df_8.createOrReplaceTempView("EXP_Strategy_8")

# COMMAND ----------

# DBTITLE 1, FIL_Strategy_9

df_9=spark.sql("""
    SELECT
        MANDT AS MANDT,
        DEPARTMENT AS DEPARTMENT,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        UpdateStrategy AS UpdateStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Strategy_8 
    WHERE
        UpdateStrategy <> DD_REJECT""")

df_9.createOrReplaceTempView("FIL_Strategy_9")

# COMMAND ----------

# DBTITLE 1, UPD_Strategy_10

df_10=spark.sql("""
    SELECT
        MANDT AS MANDT,
        DEPARTMENT AS DEPARTMENT,
        SEGMENT_ID AS SEGMENT_ID,
        SEGMENT_DESC AS SEGMENT_DESC,
        GROUP_ID AS GROUP_ID,
        GROUP_DESC AS GROUP_DESC,
        VP_ID AS VP_ID,
        VP_DESC AS VP_DESC,
        UpdateStrategy AS UpdateStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_Strategy_9""")

df_10.createOrReplaceTempView("UPD_Strategy_10")

# COMMAND ----------

# DBTITLE 1, ZTB_GROUP_HIER

spark.sql("""INSERT INTO ZTB_GROUP_HIER SELECT MANDT AS MANDT,
DEPARTMENT AS DEPARTMENT,
SEGMENT_ID AS SEGMENT_ID,
SEGMENT_DESC AS SEGMENT_DESC,
GROUP_ID AS GROUP_ID,
GROUP_DESC AS GROUP_DESC,
VP_ID AS VP_ID,
VP_DESC AS VP_DESC FROM UPD_Strategy_10""")
