# Tower Information


```sql
SHOW DATABASES;
/*
+--------------------+
| Database           |
+--------------------+
| information_schema |
| performance_schema |
| telecom            |
| tutorial_database  |
+--------------------+
*/
```

```sql
USE telecom;
SHOW TABLES;

/*
+----------------------------+
| Tables_in_telecom          |
+----------------------------+
| cinfo_xy                   |
| sample_sight_201308        |
| sample_user_201308         |
| serv_acct_item_0838_201308 |
| serv_acct_item_0838_201406 |
| tb_asz_cdma_0838_201308    |
| tb_asz_cdma_0838_201406    |
| tower                      |
| tower_center               |
| tower_hot_201308           |
+----------------------------+
*/
```


```sql
DESCRIBE cinfo_xy;
/*
+---------+------------+------+-----+---------+-------+
| Field   | Type       | Null | Key | Default | Extra |
+---------+------------+------+-----+---------+-------+
| CELL_10 | varchar(5) | YES  |     | NULL    |       |
| CELL_16 | varchar(4) | YES  |     | NULL    |       |
| LON     | float      | YES  |     | NULL    |       |
| LAT     | float      | YES  |     | NULL    |       |
| X       | float      | YES  |     | NULL    |       |
| Y       | float      | YES  |     | NULL    |       |
+---------+------------+------+-----+---------+-------+
*/

DESCRIBE tower;
/*
+---------------+------------+------+-----+---------+-------+
| Field         | Type       | Null | Key | Default | Extra |
+---------------+------------+------+-----+---------+-------+
| CELL_10       | varchar(5) | YES  |     | NULL    |       |
| CELL_16       | varchar(4) | NO   | PRI | NULL    |       |
| LON           | float      | YES  | MUL | NULL    |       |
| LAT           | float      | YES  |     | NULL    |       |
| X             | float      | YES  |     | NULL    |       |
| Y             | float      | YES  |     | NULL    |       |
| DEYANG_CENTER | int        | NO   |     | 0       |       |
+---------------+------------+------+-----+---------+-------+
*/

DESCRIBE tower_center;
/*
+---------+------------+------+-----+---------+-------+
| Field   | Type       | Null | Key | Default | Extra |
+---------+------------+------+-----+---------+-------+
| CELL_10 | varchar(5) | YES  |     | NULL    |       |
| CELL_16 | varchar(4) | YES  |     | NULL    |       |
| LON     | float      | YES  |     | NULL    |       |
| LAT     | float      | YES  |     | NULL    |       |
| X       | float      | YES  |     | NULL    |       |
| Y       | float      | YES  |     | NULL    |       |
+---------+------------+------+-----+---------+-------+
*/

DESCRIBE tower_hot_201308;
/*
+------------------+-----------------+------+-----+---------+-------+
| Field            | Type            | Null | Key | Default | Extra |
+------------------+-----------------+------+-----+---------+-------+
| CELL_10          | varchar(5)      | YES  |     | NULL    |       |
| CELL_16          | varchar(4)      | NO   | PRI | NULL    |       |
| LON              | float           | YES  |     | NULL    |       |
| LAT              | float           | YES  |     | NULL    |       |
| X                | float           | YES  |     | NULL    |       |
| Y                | float           | YES  |     | NULL    |       |
| MAIN_CENTER_FLAG | int             | NO   |     | 0       |       |
| HOT_FLAG         | bigint          | NO   |     | 0       |       |
| HOT_MAIN_FLAG    | bigint unsigned | NO   |     | 0       |       |
| HOT_OTHER_FLAG   | bigint unsigned | NO   |     | 0       |       |
+------------------+-----------------+------+-----+---------+-------+
*/

```

```sql
-- the number of observations
SELECT 
    (SELECT COUNT(*) FROM cinfo_xy) as cinfo_xy,
    (SELECT COUNT(*) FROM tower) as tower,
    (SELECT COUNT(*) FROM tower_center) as tower_center,
    (SELECT COUNT(*) FROM tower_hot_201308) as tower_hot_201308;
/*
+----------+-------+--------------+------------------+
| cinfo_xy | tower | tower_center | tower_hot_201308 |
+----------+-------+--------------+------------------+
|    40569 | 40569 |          427 |            40569 |
+----------+-------+--------------+------------------+
*/
```

```sql
SELECT 
    * 
FROM 
    cinfo_xy 
LIMIT 3;
/*
+---------+---------+---------+---------+---------+---------+
| CELL_10 | CELL_16 | LON     | LAT     | X       | Y       |
+---------+---------+---------+---------+---------+---------+
| 1       | 0001    | 103.993 | 30.7214 | 403.546 | 3400.52 |
| 2       | 0002    | 103.993 | 30.7214 | 403.546 | 3400.52 |
| 3       | 0003    | 103.993 | 30.7214 | 403.546 | 3400.52 |
+---------+---------+---------+---------+---------+---------+
*/

SELECT
    *
FROM
    tower_hot_201308
LIMIT 3;
/*
+---------+---------+---------+---------+---------+---------+------------------+----------+---------------+----------------+
| CELL_10 | CELL_16 | LON     | LAT     | X       | Y       | MAIN_CENTER_FLAG | HOT_FLAG | HOT_MAIN_FLAG | HOT_OTHER_FLAG |
+---------+---------+---------+---------+---------+---------+------------------+----------+---------------+----------------+
| 1       | 0001    | 103.993 | 30.7214 | 403.546 | 3400.52 |                0 |        0 |             0 |              0 |
| 2       | 0002    | 103.993 | 30.7214 | 403.546 | 3400.52 |                0 |        0 |             0 |              0 |
| 3       | 0003    | 103.993 | 30.7214 | 403.546 | 3400.52 |                0 |        0 |             0 |              0 |
+---------+---------+---------+---------+---------+---------+------------------+----------+---------------+----------------+
*/
```

```sql
-- number of observations for each flag  in table tower_hot_201308
SELECT
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE MAIN_CENTER_FLAG = 1) as MAIN_CENTER_FLAG,
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE HOT_FLAG = 1) as HOT_FLAG,
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE HOT_MAIN_FLAG = 1) as HOT_MAIN_FLAG,
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE HOT_OTHER_FLAG = 1) as HOT_OTHER_FLAG;
/*
+------------------+----------+---------------+----------------+
| MAIN_CENTER_FLAG | HOT_FLAG | HOT_MAIN_FLAG | HOT_OTHER_FLAG |
+------------------+----------+---------------+----------------+
|              427 |      425 |           132 |            293 |
+------------------+----------+---------------+----------------+
*/
```
