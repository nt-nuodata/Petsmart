# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_WAGE_TYPE_CTRL_0


df_0=spark.sql("""
    SELECT
        PS_WAGE_TYPE_CD AS PS_WAGE_TYPE_CD,
        START_EFF_DT AS START_EFF_DT,
        END_EFF_DT AS END_EFF_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_WAGE_TYPE_CTRL""")

df_0.createOrReplaceTempView("SERVICES_MARGIN_WAGE_TYPE_CTRL_0")

# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_CTRL_1


df_1=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRIMARY_SAP_DEPT_ID AS PRIMARY_SAP_DEPT_ID,
        STORE_DEPT_NBR AS STORE_DEPT_NBR,
        START_EFF_DT AS START_EFF_DT,
        END_EFF_DT AS END_EFF_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_CTRL""")

df_1.createOrReplaceTempView("SERVICES_MARGIN_CTRL_1")

# COMMAND ----------
# DBTITLE 1, EMPL_EMPL_LOC_WK_2


df_2=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        EMPLOYEE_ID AS EMPLOYEE_ID,
        EARN_ID AS EARN_ID,
        STORE_DEPT_NBR AS STORE_DEPT_NBR,
        JOB_CODE AS JOB_CODE,
        FULLPT_FLAG AS FULLPT_FLAG,
        HOURS_WORKED AS HOURS_WORKED,
        EARNINGS_AMT AS EARNINGS_AMT,
        EARNINGS_LOC_AMT AS EARNINGS_LOC_AMT,
        PAY_FREQ_CD AS PAY_FREQ_CD,
        CURRENCY_NBR AS CURRENCY_NBR,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EMPL_EMPL_LOC_WK""")

df_2.createOrReplaceTempView("EMPL_EMPL_LOC_WK_2")

# COMMAND ----------
# DBTITLE 1, EARNINGS_ID_3


df_3=spark.sql("""
    SELECT
        EARN_ID AS EARN_ID,
        PS_TAX_COMPANY_CD AS PS_TAX_COMPANY_CD,
        PS_WAGE_TYPE_GID AS PS_WAGE_TYPE_GID,
        PS_WAGE_TYPE_CD AS PS_WAGE_TYPE_CD,
        PS_COUNTRY_GROUP_CD AS PS_COUNTRY_GROUP_CD,
        PS_WAGE_TYPE_DESC AS PS_WAGE_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        EARNINGS_ID""")

df_3.createOrReplaceTempView("EARNINGS_ID_3")

# COMMAND ----------
# DBTITLE 1, DAYS_4


df_4=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
        DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
        CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
        CAL_WK AS CAL_WK,
        CAL_WK_NBR AS CAL_WK_NBR,
        CAL_MO AS CAL_MO,
        CAL_MO_NBR AS CAL_MO_NBR,
        CAL_MO_NAME AS CAL_MO_NAME,
        CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
        CAL_QTR AS CAL_QTR,
        CAL_QTR_NBR AS CAL_QTR_NBR,
        CAL_HALF AS CAL_HALF,
        CAL_YR AS CAL_YR,
        FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
        FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_WK_NBR AS FISCAL_WK_NBR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_YR AS FISCAL_YR,
        LYR_WEEK_DT AS LYR_WEEK_DT,
        LWK_WEEK_DT AS LWK_WEEK_DT,
        WEEK_DT AS WEEK_DT,
        EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
        EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
        ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
        ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
        CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
        CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
        CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
        CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
        MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
        MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
        MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
        MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
        PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
        PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DAYS""")

df_4.createOrReplaceTempView("DAYS_4")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE_RPT_5


df_5=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        LOCATION_TYPE_DESC AS LOCATION_TYPE_DESC,
        STORE_NBR AS STORE_NBR,
        STORE_NAME AS STORE_NAME,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
        LOCATION_NBR AS LOCATION_NBR,
        COMPANY_ID AS COMPANY_ID,
        COMPANY_DESC AS COMPANY_DESC,
        SUPER_REGION_ID AS SUPER_REGION_ID,
        SUPER_REGION_DESC AS SUPER_REGION_DESC,
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        SITE_COUNTY AS SITE_COUNTY,
        STATE_CD AS STATE_CD,
        STATE_NAME AS STATE_NAME,
        POSTAL_CD AS POSTAL_CD,
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        SITE_FAX_NO AS SITE_FAX_NO,
        SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        SFT_OPEN_DT AS SFT_OPEN_DT,
        OPEN_DT AS OPEN_DT,
        GR_OPEN_DT AS GR_OPEN_DT,
        CLOSE_DT AS CLOSE_DT,
        SITE_SALES_FLAG AS SITE_SALES_FLAG,
        SALES_CURR_FLAG AS SALES_CURR_FLAG,
        SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        FIRST_MEASURED_SALE_DT AS FIRST_MEASURED_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        COMP_CURR_FLAG AS COMP_CURR_FLAG,
        COMP_EFF_DT AS COMP_EFF_DT,
        COMP_END_DT AS COMP_END_DT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        TP_START_DT AS TP_START_DT,
        HOTEL_FLAG AS HOTEL_FLAG,
        HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
        DAYCAMP_FLAG AS DAYCAMP_FLAG,
        VET_FLAG AS VET_FLAG,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        TIME_ZONE AS TIME_ZONE,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        TRADE_AREA AS TRADE_AREA,
        DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
        PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        PROMO_LABEL_CD AS PROMO_LABEL_CD,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
        EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
        EQUINE_SITE_ID AS EQUINE_SITE_ID,
        EQUINE_SITE_DESC AS EQUINE_SITE_DESC,
        EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        SITE_LOGIN_ID AS SITE_LOGIN_ID,
        SITE_MANAGER_ID AS SITE_MANAGER_ID,
        SITE_MANAGER_NAME AS SITE_MANAGER_NAME,
        MGR_ID AS MGR_ID,
        MGR_DESC AS MGR_DESC,
        DVL_ID AS DVL_ID,
        DVL_DESC AS DVL_DESC,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        TOTAL_SALES_RANKING_CD AS TOTAL_SALES_RANKING_CD,
        MERCH_SALES_RANKING_CD AS MERCH_SALES_RANKING_CD,
        SERVICES_SALES_RANKING_CD AS SERVICES_SALES_RANKING_CD,
        SALON_SALES_RANKING_CD AS SALON_SALES_RANKING_CD,
        TRAINING_SALES_RANKING_CD AS TRAINING_SALES_RANKING_CD,
        HOTEL_DDC_SALES_RANKING_CD AS HOTEL_DDC_SALES_RANKING_CD,
        CONSUMABLES_SALES_RANKING_CD AS CONSUMABLES_SALES_RANKING_CD,
        HARDGOODS_SALES_RANKING_CD AS HARDGOODS_SALES_RANKING_CD,
        SPECIALTY_SALES_RANKING_CD AS SPECIALTY_SALES_RANKING_CD,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
        DC_AREA_DIRECTOR_NAME AS DC_AREA_DIRECTOR_NAME,
        DC_AREA_DIRECTOR_EMAIL AS DC_AREA_DIRECTOR_EMAIL,
        DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
        DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
        REGION_VP_NAME AS REGION_VP_NAME,
        RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
        REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
        ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
        ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
        LP_SAFETY_DIRECTOR_NAME AS LP_SAFETY_DIRECTOR_NAME,
        LP_SAFETY_DIRECTOR_EMAIL AS LP_SAFETY_DIRECTOR_EMAIL,
        SR_LP_SAFETY_MGR_NAME AS SR_LP_SAFETY_MGR_NAME,
        SR_LP_SAFETY_MGR_EMAIL AS SR_LP_SAFETY_MGR_EMAIL,
        REGIONAL_LP_SAFETY_MGR_NAME AS REGIONAL_LP_SAFETY_MGR_NAME,
        REGIONAL_LP_SAFETY_MGR_EMAIL AS REGIONAL_LP_SAFETY_MGR_EMAIL,
        RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
        RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
        DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
        DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
        ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
        ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
        ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
        ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
        HR_MANAGER_NAME AS HR_MANAGER_NAME,
        HR_MANAGER_EMAIL AS HR_MANAGER_EMAIL,
        HR_SUPERVISOR_NAME1 AS HR_SUPERVISOR_NAME1,
        HR_SUPERVISOR_EMAIL1 AS HR_SUPERVISOR_EMAIL1,
        HR_SUPERVISOR_NAME2 AS HR_SUPERVISOR_NAME2,
        HR_SUPERVISOR_EMAIL2 AS HR_SUPERVISOR_EMAIL2,
        LEARN_SOLUTION_MGR_NAME AS LEARN_SOLUTION_MGR_NAME,
        LEARN_SOLUTION_MGR_EMAIL AS LEARN_SOLUTION_MGR_EMAIL,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_RPT""")

df_5.createOrReplaceTempView("SITE_PROFILE_RPT_5")

# COMMAND ----------
# DBTITLE 1, SALES_TRANS_SKU_6


df_6=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        SALES_INSTANCE_ID_DIST_KEY AS SALES_INSTANCE_ID_DIST_KEY,
        PRODUCT_ID AS PRODUCT_ID,
        SALES_INSTANCE_ID AS SALES_INSTANCE_ID,
        LOCATION_ID AS LOCATION_ID,
        SALES_TYPE_ID AS SALES_TYPE_ID,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        TXN_WAS_POST_VOIDED_FLAG AS TXN_WAS_POST_VOIDED_FLAG,
        ORDER_NBR AS ORDER_NBR,
        ORDER_SEQ_NBR AS ORDER_SEQ_NBR,
        ORDER_CHANNEL AS ORDER_CHANNEL,
        ORDER_ASSIST_LOCATION_ID AS ORDER_ASSIST_LOCATION_ID,
        ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
        ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
        ORDER_CREATION_DEVICE_TYPE AS ORDER_CREATION_DEVICE_TYPE,
        ORDER_CREATION_DEVICE_WIDTH AS ORDER_CREATION_DEVICE_WIDTH,
        TXN_SEGMENT AS TXN_SEGMENT,
        PAYMENT_DEVICE_TYPE AS PAYMENT_DEVICE_TYPE,
        TRANS_TSTMP AS TRANS_TSTMP,
        LOYALTY_NBR AS LOYALTY_NBR,
        LOYALTY_REDEMPTION_ID AS LOYALTY_REDEMPTION_ID,
        LUID AS LUID,
        CUST_TRANS_ID AS CUST_TRANS_ID,
        CUSTOMER_EID AS CUSTOMER_EID,
        CUSTOMER_GID AS CUSTOMER_GID,
        SALES_CUSTOMER_LINK_EXCL_TYPE_ID AS SALES_CUSTOMER_LINK_EXCL_TYPE_ID,
        SPECIAL_SALES_FLAG AS SPECIAL_SALES_FLAG,
        RECEIPTLESS_RETURN_FLAG AS RECEIPTLESS_RETURN_FLAG,
        TRAINING_START_DT AS TRAINING_START_DT,
        TRAINER_NAME AS TRAINER_NAME,
        SALES_AMT AS SALES_AMT,
        SALES_COST AS SALES_COST,
        SALES_QTY AS SALES_QTY,
        SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        SPECIAL_SALES_QTY AS SPECIAL_SALES_QTY,
        RETURN_AMT AS RETURN_AMT,
        RETURN_COST AS RETURN_COST,
        RETURN_QTY AS RETURN_QTY,
        CLEARANCE_AMT AS CLEARANCE_AMT,
        CLEARANCE_QTY AS CLEARANCE_QTY,
        CLEARANCE_RETURN_AMT AS CLEARANCE_RETURN_AMT,
        CLEARANCE_RETURN_QTY AS CLEARANCE_RETURN_QTY,
        SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        SPECIAL_RETURN_QTY AS SPECIAL_RETURN_QTY,
        SPECIAL_SRVC_AMT AS SPECIAL_SRVC_AMT,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        DISCOUNT_QTY AS DISCOUNT_QTY,
        DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        DISCOUNT_RETURN_QTY AS DISCOUNT_RETURN_QTY,
        POS_COUPON_AMT AS POS_COUPON_AMT,
        POS_COUPON_QTY AS POS_COUPON_QTY,
        POS_COUPON_ALLOC_AMT AS POS_COUPON_ALLOC_AMT,
        POS_COUPON_ALLOC_QTY AS POS_COUPON_ALLOC_QTY,
        NET_SALES_AMT AS NET_SALES_AMT,
        NET_SALES_COST AS NET_SALES_COST,
        NET_SALES_QTY AS NET_SALES_QTY,
        MA_SALES_AMT AS MA_SALES_AMT,
        MA_SALES_QTY AS MA_SALES_QTY,
        MA_TRANS_AMT AS MA_TRANS_AMT,
        MA_TRANS_COST AS MA_TRANS_COST,
        MA_TRANS_QTY AS MA_TRANS_QTY,
        NET_MARGIN_AMT AS NET_MARGIN_AMT,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_TRANS_SKU""")

df_6.createOrReplaceTempView("SALES_TRANS_SKU_6")

# COMMAND ----------
# DBTITLE 1, SQ_SERVICES_MARGIN_RATE_PRE_7


df_7=spark.sql("""
    SELECT
        W.WEEK_DT,
        W.LOCATION_ID,
        W.PRIMARY_SAP_DEPT_ID,
        CASE 
            WHEN SUM(NVL(S.GROSS_SALES_LOC_AMT,
            0)) = 0 THEN 0 
            ELSE ROUND(SUM(NVL(W.SUM_EARNINGS_LOC_AMT,
            0)) / SUM(NVL(S.GROSS_SALES_LOC_AMT,
            0)),
            4) 
        END AS MARGIN_RATE 
    FROM
        (SELECT
            WEEK_DT,
            LOCATION_ID,
            PRIMARY_SAP_DEPT_ID,
            SUM(EARNINGS_LOC_AMT) AS SUM_EARNINGS_LOC_AMT 
        FROM
            (SELECT
                E.WEEK_DT,
                E.LOCATION_ID,
                C.PRIMARY_SAP_DEPT_ID,
                SUM(EARNINGS_LOC_AMT) AS EARNINGS_LOC_AMT 
            FROM
                EMPL_EMPL_LOC_WK E 
            JOIN
                EARNINGS_ID I 
                    ON E.EARN_ID = I.EARN_ID 
            JOIN
                (
                    SELECT
                        DISTINCT START_EFF_DT,
                        END_EFF_DT,
                        STORE_DEPT_NBR,
                        PRIMARY_SAP_DEPT_ID 
                    FROM
                        SERVICES_MARGIN_CTRL
                ) C 
                    ON E.STORE_DEPT_NBR = C.STORE_DEPT_NBR 
                    AND E.WEEK_DT BETWEEN C.START_EFF_DT AND C.END_EFF_DT 
            JOIN
                SERVICES_MARGIN_WAGE_TYPE_CTRL WC 
                    ON I.PS_WAGE_TYPE_CD = WC.PS_WAGE_TYPE_CD 
                    AND E.WEEK_DT BETWEEN WC.START_EFF_DT AND WC.END_EFF_DT 
            WHERE
                E.WEEK_DT >= CURRENT_DATE - 30 
            GROUP BY
                E.WEEK_DT,
                E.LOCATION_ID,
                C.PRIMARY_SAP_DEPT_ID 
            UNION
            ALL SELECT
                E.WEEK_DT,
                E.LOCATION_ID,
                C.PRIMARY_SAP_DEPT_ID,
                SUM(ACT_EARNINGS_LOC_AMT) AS EARNINGS_LOC_AMT 
            FROM
                PS2_ADJUSTED_LABOR_WK E 
            JOIN
                EARNINGS_ID I 
                    ON E.EARN_ID = I.EARN_ID 
            JOIN
                (
                    SELECT
                        DISTINCT START_EFF_DT,
                        END_EFF_DT,
                        STORE_DEPT_NBR,
                        PRIMARY_SAP_DEPT_ID 
                    FROM
                        SERVICES_MARGIN_CTRL
                ) C 
                    ON E.STORE_DEPT_NBR = C.STORE_DEPT_NBR 
                    AND E.WEEK_DT BETWEEN C.START_EFF_DT AND C.END_EFF_DT 
            JOIN
                SERVICES_MARGIN_WAGE_TYPE_CTRL WC 
                    ON I.PS_WAGE_TYPE_CD = WC.PS_WAGE_TYPE_CD 
                    AND E.WEEK_DT BETWEEN WC.START_EFF_DT AND WC.END_EFF_DT 
            WHERE
                E.WEEK_DT >= CURRENT_DATE - 30 
                AND ACT_EARNINGS_LOC_AMT <> 0 
            GROUP BY
                E.WEEK_DT,
                E.LOCATION_ID,
                C.PRIMARY_SAP_DEPT_ID) LBR 
            GROUP BY
                WEEK_DT,
                LOCATION_ID,
                PRIMARY_SAP_DEPT_ID
        ) W 
    JOIN
        (
            SELECT
                D.WEEK_DT,
                S.LOCATION_ID,
                C.PRIMARY_SAP_DEPT_ID,
                SUM(SALES_AMT) AS GROSS_SALES_LOC_AMT 
            FROM
                SALES_TRANS_SKU S 
            JOIN
                SKU_PROFILE_RPT K 
                    ON S.PRODUCT_ID = K.PRODUCT_ID 
            JOIN
                DAYS D 
                    ON S.DAY_DT = D.DAY_DT 
            JOIN
                (
                    SELECT
                        DISTINCT START_EFF_DT,
                        END_EFF_DT,
                        SAP_DEPT_ID,
                        PRIMARY_SAP_DEPT_ID 
                    FROM
                        SERVICES_MARGIN_CTRL
                ) C 
                    ON S.DAY_DT BETWEEN C.START_EFF_DT AND C.END_EFF_DT 
                    AND K.SAP_DEPT_ID = C.SAP_DEPT_ID 
            WHERE
                D.WEEK_DT >= CURRENT_DATE - 30 
            GROUP BY
                D.WEEK_DT,
                S.LOCATION_ID,
                C.PRIMARY_SAP_DEPT_ID) S 
                    ON W.WEEK_DT = S.WEEK_DT 
                    AND W.LOCATION_ID = S.LOCATION_ID 
                    AND W.PRIMARY_SAP_DEPT_ID = S.PRIMARY_SAP_DEPT_ID 
            GROUP BY
                W.WEEK_DT,
                W.LOCATION_ID,
                W.PRIMARY_SAP_DEPT_ID""")

df_7.createOrReplaceTempView("SQ_SERVICES_MARGIN_RATE_PRE_7")

# COMMAND ----------
# DBTITLE 1, EXP_RATE_8


df_8=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        PRIMARY_SAP_DEPT_ID AS PRIMARY_SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_SERVICES_MARGIN_RATE_PRE_7""")

df_8.createOrReplaceTempView("EXP_RATE_8")

# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_RATE_PRE


spark.sql("""INSERT INTO SERVICES_MARGIN_RATE_PRE SELECT WEEK_DT AS WEEK_DT,
LOCATION_ID AS LOCATION_ID,
PRIMARY_SAP_DEPT_ID AS SAP_DEPT_ID,
MARGIN_RATE AS MARGIN_RATE FROM EXP_RATE_8""")