CREATE TABLE telecom.clean_cdr(
    SELECT
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
