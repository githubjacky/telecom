CREATE TEMPORARY TABLE demo AS
SELECT
    B.CI_BRANCH                                                                   AS branch,
    B.CI_DISTRICT                                                                 AS district,
    A.SERV_ID                                                                     AS id,
    A.ACC_NBR                                                                     AS client_nbr, 
    CONVERT(SUBSTRING_INDEX(SUBSTRING_INDEX(A.END_TIME,' ',-1),':',1),UNSIGNED)   AS time,
    DAYOFWEEK(SUBSTRING_INDEX(A.END_TIME,' ',1))                                  AS day_of_week,
    A.CALLING_NBR                                                                 AS calling_nbr,
    A.CALLING_AREA_CODE                                                           AS calling_area_code,
    A.DURATION                                                                    AS duration,
    A.CALLED_AREA_CODE                                                            AS called_area_code,
    A.CALLED_NBR                                                                  AS CALLED_NBR, 
    A.CHARGE                                                                      AS charge, 
    A.ACCT_ITEM_TYPE_CODE                                                         AS acct_item_type_code, 
    A.ETL_TYPE_ID                                                                 AS etl_type_id,
    A.CELL_ID                                                                     AS cell_id
FROM serv_acct_item_0838_201308 AS A 
LEFT JOIN tb_asz_cdma_0838_201308 B ON A.SERV_ID=B.SERV_ID AND A.ACC_NBR=B.MSISDN
WHERE 
    A.ACCT_ITEM_TYPE_CODE != -1                                                    AND
    (A.START_TIME LIKE "____/%/% %:__:__" AND A.END_TIME LIKE "____/%/% %:__:__")  AND
    (A.CALLING_NBR NOT LIKE 'NotMobile_%' AND A.CALLED_NBR NOT LIKE 'NotMobile_%') AND
    (A.CALLING_AREA_COdE != '-1' AND A.CALLED_AREA_CODE != '-1')                   AND
    (A.LAC != '-1' AND A.LAC != '0')                                               AND
    (A.CELL_ID != '-1' AND A.CELL_ID != '0')
