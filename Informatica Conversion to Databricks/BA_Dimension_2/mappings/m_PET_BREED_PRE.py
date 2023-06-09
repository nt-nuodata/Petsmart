# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, OMS_MASTER_ORDER_0

df_0=spark.sql("""
    SELECT
        PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
        PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
        TC_COMPANY_ID AS TC_COMPANY_ID,
        ORDER_NBR AS ORDER_NBR,
        TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID,
        TC_PO_LINE_ID AS TC_PO_LINE_ID,
        ORDER_FULFILLMENT_OPTION AS ORDER_FULFILLMENT_OPTION,
        ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
        ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
        ORDER_CHANNEL AS ORDER_CHANNEL,
        CREATED_SOURCE AS CREATED_SOURCE,
        ENTERED_BY AS ENTERED_BY,
        ROLE_NAME AS ROLE_NAME,
        PO_CREATED_DTTM AS PO_CREATED_DTTM,
        PO_LAST_UPDATED_DTTM AS PO_LAST_UPDATED_DTTM,
        POL_CREATED_DTTM AS POL_CREATED_DTTM,
        POL_LAST_UPDATED_DTTM AS POL_LAST_UPDATED_DTTM,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        OMS_MASTER_ORDER""")

df_0.createOrReplaceTempView("OMS_MASTER_ORDER_0")

# COMMAND ----------

# DBTITLE 1, SQ_Dummy_Source_1

df_1=spark.sql("""
    SELECT
        current_timestamp 
    FROM
        dual""")

df_1.createOrReplaceTempView("SQ_Dummy_Source_1")

# COMMAND ----------

# DBTITLE 1, exp_set_HTTP_Params_Pet_Breed_2

df_2=spark.sql("""
    SELECT
        'true' AS requestall,
        'application/xml' AS header,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Dummy_Source_1""")

df_2.createOrReplaceTempView("exp_set_HTTP_Params_Pet_Breed_2")

# COMMAND ----------

# DBTITLE 1, HTTP_Get_Pet_Breed_3

df_3=spark.sql("""
    SELECT
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
        header AS header,
        requestall AS requestall 
    FROM
        exp_set_HTTP_Params_Pet_Breed_2""")

df_3.createOrReplaceTempView("HTTP_Get_Pet_Breed_3")

# COMMAND ----------

# DBTITLE 1, exp_Cleanup_XML_4

df_4=spark.sql("""
    SELECT
        REPLACESTR(1,
        rtrim(REPLACECHR(0,
        HTTPOUT,
        '][',
        '')),
        ' xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://schemas.datacontract.org/2004/07/PetSmart.WebApi.Public.ViewModels.Pet"',
        '') AS HTTPOUT1,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        HTTP_Get_Pet_Breed_3""")

df_4.createOrReplaceTempView("exp_Cleanup_XML_4")

# COMMAND ----------

# DBTITLE 1, xml_Parse_Pet_Breed_5

df_5=spark.sql("""
    SELECT
        HTTPOUT1 AS HTTPOUT1,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        exp_Cleanup_XML_4""")

df_5.createOrReplaceTempView("xml_Parse_Pet_Breed_5")

# COMMAND ----------

# DBTITLE 1, exp_TRANSFORM_6

df_6=spark.sql("""
    SELECT
        BreedId AS BreedId,
        Description1 AS Description1,
        IsAggressive AS IsAggressive,
        SpeciesId AS SpeciesId,
        SpeciesName AS SpeciesName,
        IsActive AS IsActive,
        IsFirstParty AS IsFirstParty,
        (CAST(LTRIM(RTRIM(BreedId)) AS DECIMAL (38,
        0))) AS BREED_ID,
        LTRIM(RTRIM(Description)) AS DESCRIPTION,
        IFF(upper(LTRIM(RTRIM(IsAggressive))) = 'TRUE',
        1,
        0) AS IS_AGGRESSIVE,
        (CAST(LTRIM(RTRIM(SpeciesId)) AS DECIMAL (38,
        0))) AS SPECIES_ID,
        LTRIM(RTRIM(SpeciesName)) AS SPECIES_NAME,
        IFF(upper(LTRIM(RTRIM(IsActive))) = 'TRUE',
        1,
        0) AS IS_ACTIVE,
        LTRIM(RTRIM(IsFirstParty)) AS IS_FIRST_PARTY,
        SESSSTARTTIME AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        xml_Parse_Pet_Breed_5""")

df_6.createOrReplaceTempView("exp_TRANSFORM_6")

# COMMAND ----------

# DBTITLE 1, PET_BREED_PRE

spark.sql("""INSERT INTO PET_BREED_PRE SELECT BREED_ID AS BREED_ID,
DESCRIPTION AS DESCRIPTION,
IS_AGGRESSIVE AS IS_AGGRESSIVE,
SPECIES_ID AS SPECIES_ID,
SPECIES_NAME AS SPECIES_NAME,
IS_ACTIVE AS IS_ACTIVE,
IS_FIRST_PARTY AS IS_FIRST_PARTY,
LOAD_TSTMP AS LOAD_TSTMP FROM exp_TRANSFORM_6""")
