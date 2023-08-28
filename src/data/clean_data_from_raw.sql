USE telecom;


SELECT 'create table telecom.clean_cdr' as '';
CREATE TABLE IF NOT EXISTS clean_cdr(
SELECT
    SERV_ID                                                                     AS serv_id,
    ACC_NBR                                                                     AS client_nbr,
    CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(END_TIME,' ',-1),':',1),UNSIGNED)   AS time,
    DAYOFWEEK(SUBSTRING_INDEX(END_TIME,' ',1))                                  AS day_of_week,
    CALLING_NBR                                                                 AS calling_nbr,
    CALLING_AREA_CODE                                                           AS calling_area_code,
    DURATION                                                                    AS duration,
    CALLED_AREA_CODE                                                            AS called_area_code,
    CALLED_NBR                                                                  AS called_nbr,
    CELL_ID                                                                     AS cell_id
FROM serv_acct_item_0838_201308
WHERE
    ACCT_ITEM_TYPE_CODE != -1                                                  AND
    (START_TIME LIKE "____/%/% %:__:__" AND END_TIME LIKE "____/%/% %:__:__")  AND
    (CALLING_NBR NOT LIKE 'NotMobile_%' AND CALLED_NBR NOT LIKE 'NotMobile_%') AND
    (CALLING_AREA_COdE != '-1' AND CALLED_AREA_CODE != '-1')                   AND
    (CELL_ID != '-1' AND CELL_ID != '0')                                       AND
    (CALLING_NBR in (SELECT MSISDN FROM tb_asz_cdma_0838_201308) OR CALLED_NBR in (SELECT MSISDN FROM tb_asz_cdma_0838_201308)) AND
    (CALLING_AREA_CODE = '0838' OR CALLED_AREA_CODE = '0838')
);


SELECT 'create table telecom.target_node' as '';
CREATE TABLE IF NOT EXISTS target_node(
SELECT DISTINCT subquery.client_nbr
FROM(
    (
        SELECT calling_nbr AS client_nbr
        FROM clean_cdr
        WHERE calling_nbr in (SELECT MSISDN FROM tb_asz_cdma_0838_201308)
    )
    UNION
    (
        SELECT called_nbr AS client_nbr
        FROM clean_cdr
        WHERE called_nbr in (SELECT MSISDN FROM tb_asz_cdma_0838_201308)
    )
) AS subquery
);


SELECT 'create table telecom.clean_user_info' as '';
CREATE TABLE IF NOT EXISTS clean_user_info(
SELECT
    SERV_ID                                                                        AS serv_id,
    MSISDN                                                                         AS client_nbr,
    CONCAT(SUBSTR(CERT_NBR, 1, 2), SUBSTR(CERT_NBR, 4, 2), SUBSTR(CERT_NBR, 7, 2)) AS born_area_code,
    CI_DISTRICT                                                                    AS register_district,
    2013 - CONVERT(SUBSTR(CERT_NBR,10,4), UNSIGNED)                                AS age,
    HS_CDMA_BRAND                                                                  AS phone_brand,
    HS_CDMA_LAYER                                                                  AS phone_level,
    HS_CDMA_TER_PRICE                                                              AS phone_price,
    CONVERT(HS_CDMA_IS_EVDO, UNSIGNED)                                             AS evdo_support_flag,
    VO_CDMA_MOUOUT_LOCAL_M1                                                        AS mou_local,
    VO_CDMA_MOU_DIST_M1                                                            AS mou_dist,
    VO_NET_VOL_M1                                                                  AS network_vol,
    CONVERT(PD_EVDO_FLAG_M1, UNSIGNED)                                             AS evdo_use_flag,
    CONVERT(PD_1X_FLAG_M1, UNSIGNED)                                               AS onex_use_flag,
    VO_EVDO_VOL_M1                                                                 AS evdo_vol,
    VO_1X_VOL_M1                                                                   AS onex_vol,
    MB_ARPU_CDMA_ALL_M1                                                            AS arpu,
    CONVERT(PL_E9_FLAG, UNSIGNED)                                                  AS e9_service_flag,
    CONVERT(PL_E6_FLAG, UNSIGNED)                                                  AS e6_service_flag,
    CONVERT(IS_E9ZX, UNSIGNED)                                                     AS e9_service_premium_flag,
    CASE IS_8CARD WHEN '是' THEN 1 ELSE 0 END                                      AS 8card_service_flag,
    CASE IS_INTELLIGENT WHEN '是' THEN 1 ELSE 0 END                                AS smart_phone_flag,
    CASE IS_CARDPHONE WHEN '是' THEN 1 ELSE 0 END                                  AS card_phone_flag,
    CASE WHEN
            (
                DETAIL_NAME = '农村公众（家庭及个人）' OR
                DETAIL_NAME = '农村客户'
            )
            OR
            PL_IVPN_CAT = '乡情v网' OR
            BSS_ORG_ZJ_FLAG = '1'
    THEN 1 ELSE 0 END AS rural_flag,
    CASE WHEN
        (
            DETAIL_NAME IS NOT NULL AND
            DETAIL_NAME != '个人客户' AND
            DETAIL_NAME != '家庭客户' AND
            DETAIL_NAME != '农村公众（家庭及个人）' AND
            DETAIL_NAME != '农村客户' AND
            DETAIL_NAME != '城市个人客户' AND
            DETAIL_NAME != '城市家庭客户'
        )
        OR
        (
            (CI_IVPN_FLAG=1 OR VPN_FLAG=1) AND
            (PL_IVPN_CAT IS NULL OR PL_IVPN_CAT = '虚拟网' OR PL_IVPN_CAT='家庭V网')
        )
        OR
        (
            MB_ENPR_FLAG_M1 = 1 OR   # individual works for government
            IS_BUSINESS = '1' OR     # wheter pay the fee
            RED_MARK = 1 OR          # 省市领导级别的人
            PL_BUSINESS_FLAG = 1 OR  # business user
            IS_ZQJN = 1 OR           # whether the gorvernment & enterprise cluster
            IS_ZQHY = 1              # whether the government & enterprise industry
        )
    THEN 1 ELSE 0 END AS employ_flag,
    CASE WHEN
        DETAIL_NAME = '高等和职业院校' OR
        PL_IVPN_CAT = '校园V网' OR
        PL_CAMPUS_FLAG = 1 OR
        IS_SCHOOL = '1'
    THEN 1 ELSE 0 END AS student_flag
FROM tb_asz_cdma_0838_201308
WHERE
    CERT_NBR RLIKE '[1-8][0-7]-[0-7][0-9]-[0-9][0-9]-[12][09][0-9][0-9]-[01][0-9]-[0-3][0-9]-[01]' AND
    (CI_DISTRICT != '德阳市未知营业区' AND CI_DISTRICT != 'None')AND
    (
        SUBSTR(CERT_NBR,10,4)>='1920' AND
        CONVERT(SUBSTR(CERT_NBR,10,4),UNSIGNED)<=CONVERT(SUBSTR(MONTH_NO,1,4),UNSIGNED)-16 AND
        SUBSTR(CERT_NBR,15,2)>='01' AND
        SUBSTR(CERT_NBR,15,2)<='12' AND
        SUBSTR(CERT_NBR,18,2)>='01' AND
        SUBSTR(CERT_NBR,18,2)<='31'
    )
    AND
    PD_CDMA_STATUS = '正常' AND
    (HS_CDMA_BRAND != 'None' AND HS_CDMA_BRAND != '') AND
    HS_CDMA_LAYER != 'None' AND
    HS_CDMA_IS_EVDO != 'None' AND
    PL_BB_FLAG = 0 AND  # wireless ?
    IS_WX_FLAG = 0 AND  # wireless ?
    PAYMENT_FLAG = 1 AND
    IS_INTELLIGENT != 'None' AND
    HS_CDMA_MODEL NOT LIKE '%(固定台)' AND
    MSISDN IN (SELECT client_nbr FROM target_node)
);


SELECT 'create table telecom.clean_CDR' as '';
CREATE TABLE IF NOT EXISTS clean_CDR(
SELECT *
FROM clean_cdr 
WHERE 
    calling_nbr in (SELECT client_nbr FROM clean_user_info) OR
    called_nbr in (SELECT client_nbr FROM clean_user_info)
);


SELECT 'create table telecom.network' as '';
CREATE TABLE IF NOT EXISTS network(
SELECT
    *
FROM clean_CDR
WHERE 
    calling_nbr in (SELECT client_nbr FROM clean_user_info) AND
    called_nbr in (SELECT client_nbr FROM clean_user_info)
);
