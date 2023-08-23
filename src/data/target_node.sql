CREATE TABLE telecom.target_node(
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