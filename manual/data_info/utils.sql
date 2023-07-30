-----------------------------------------------------------------------------------------
# %%
DROP PROCEDURE IF EXISTS telecom.calculate_count_by_column;
DELIMITER //

CREATE PROCEDURE telecom.calculate_count_by_column(IN col VARCHAR(30))
BEGIN
    SET @sql = CONCAT
    ('
        SELECT
            ', col,  ' AS column_value, 
            COUNT(*) AS count_value
        FROM
            tb_asz_cdma_0838_201308
        GROUP BY
            ', col
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;
-----------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS telecom.calculate_count_by_column_temp;
DELIMITER //

CREATE PROCEDURE telecom.calculate_count_by_column_temp(IN col VARCHAR(30))
BEGIN
    SET @table_name = CONCAT('count_result_', col);
 
    SET @sql = CONCAT
    ('
        CREATE TEMPORARY TABLE ', @table_name, ' AS
        SELECT
            ', col,  ' AS column_value, 
            COUNT(*) AS count_value
        FROM
            tb_asz_cdma_0838_201308
        GROUP BY
            ', col
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END //

DELIMITER ;
-----------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS telecom.calculate_decile_by_column;
DELIMITER //

CREATE PROCEDURE telecom.calculate_decile_by_column(IN col VARCHAR(100))
BEGIN
    SET @sql = CONCAT
    ('
        SELECT
            MAX(', col, ') AS column_value,
            decile
        FROM (
            SELECT
                ', col, ',
                NTILE(10) OVER (ORDER BY ', col, ') AS decile
            FROM tb_asz_cdma_0838_201308
        ) AS subquery
        GROUP BY 
            decile;'
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;
-----------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS telecom.calculate_decile_by_column_temp;
DELIMITER //

CREATE PROCEDURE telecom.calculate_decile_by_column_temp(IN col VARCHAR(100))
BEGIN
    SET @table_name = CONCAT('decile_result_', col);

    SET @sql = CONCAT
    ('
        CREATE TEMPORARY TABLE ', @table_name, ' AS
        SELECT
            MAX(', col, ') AS column_value,
            decile
        FROM (
            SELECT
                ', col, ',
                NTILE(10) OVER (ORDER BY ', col, ') AS decile
            FROM tb_asz_cdma_0838_201308
        ) AS subquery
        GROUP BY 
            decile;'
    );
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;
-----------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS telecom.decile_comparison;
DELIMITER //

CREATE PROCEDURE telecom.decile_comparison
(
    IN table1 VARCHAR(100),
    IN table2 VARCHAR(100),
    IN table3 VARCHAR(100),
    IN table4 VARCHAR(100)
)
BEGIN
    CALL calculate_decile_by_column_temp(table1);
    CALL calculate_decile_by_column_temp(table2);
    CALL calculate_decile_by_column_temp(table3);
    CALL calculate_decile_by_column_temp(table4);

    SET @table1_name = CONCAT('decile_result_', table1);
    SET @table2_name = CONCAT('decile_result_', table2);
    SET @table3_name = CONCAT('decile_result_', table3);
    SET @table4_name = CONCAT('decile_result_', table4);

    SET @SQL = CONCAT
    ('
        SELECT
            *
        FROM
        (
            SELECT
                column_value AS ', table1,',
                decile
            FROM
                ', @table1_name,'
        ) AS main

        RIGHT JOIN
        (
            SELECT
                column_value AS ', table2,',
                decile
            FROM
                ', @table2_name,'
        ) AS sub1
        ON main.decile = sub1.decile
        RIGHT JOIN
        (
            SELECT
                column_value AS ', table3,',
                decile
            FROM
                ', @table3_name,'
        ) AS sub2
        ON main.decile = sub2.decile
        RIGHT JOIN
        (
            SELECT
                column_value AS ', table4,',
                decile
            FROM
                ', @table4_name,'
        ) AS sub3
        ON main.decile = sub3.decile;'
    );

    PREPARE stmt FROM @SQL;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;
-----------------------------------------------------------------------------------------
