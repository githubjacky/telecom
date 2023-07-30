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
#%%
SELECT 
    (SELECT COUNT(*) FROM cinfo_xy) AS cinfo_xy,
    (SELECT COUNT(*) FROM tower) AS tower,
    (SELECT COUNT(*) FROM tower_center) AS tower_center,
    (SELECT COUNT(*) FROM tower_hot_201308) AS tower_hot_201308;
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
    COUNT(*)
FROM 
    tower
WHERE DEYANG_CENTER = 1;
/*
+----------+
| COUNT(*) |
+----------+
|      427 |
+----------+
*/

SELECT 
    *
FROM 
    tower
WHERE DEYANG_CENTER = 1
LIMIT 10;
/*
+---------+---------+---------+---------+---------+---------+---------------+
| CELL_10 | CELL_16 | LON     | LAT     | X       | Y       | DEYANG_CENTER |
+---------+---------+---------+---------+---------+---------+---------------+
| 13401   | 3459    | 104.374 | 31.1269 | 440.293 | 3445.21 |             1 |
| 13402   | 345A    | 104.374 | 31.1269 | 440.293 | 3445.21 |             1 |
| 13403   | 345B    | 104.374 | 31.1269 | 440.293 | 3445.21 |             1 |
| 13407   | 345F    | 104.415 | 31.1127 | 444.195 | 3443.62 |             1 |
| 13408   | 3460    | 104.415 | 31.1127 | 444.195 | 3443.62 |             1 |
| 13409   | 3461    | 104.415 | 31.1127 | 444.195 | 3443.62 |             1 |
| 13410   | 3462    | 104.389 | 31.1355 | 441.729 | 3446.16 |             1 |
| 13411   | 3463    | 104.389 | 31.1355 | 441.729 | 3446.16 |             1 |
| 13412   | 3464    | 104.389 | 31.1355 | 441.729 | 3446.16 |             1 |
| 13422   | 346E    | 104.382 | 31.0913 | 441.034 | 3441.26 |             1 |
+---------+---------+---------+---------+---------+---------+---------------+
*/

SELECT 
    * 
FROM 
    tower_center 
LIMIT 10;
/*
+---------+---------+---------+---------+---------+---------+
| CELL_10 | CELL_16 | LON     | LAT     | X       | Y       |
+---------+---------+---------+---------+---------+---------+
| 13401   | 3459    | 104.374 | 31.1269 | 440.293 | 3445.21 |
| 13402   | 345A    | 104.374 | 31.1269 | 440.293 | 3445.21 |
| 13403   | 345B    | 104.374 | 31.1269 | 440.293 | 3445.21 |
| 13407   | 345F    | 104.415 | 31.1127 | 444.195 | 3443.62 |
| 13408   | 3460    | 104.415 | 31.1127 | 444.195 | 3443.62 |
| 13409   | 3461    | 104.415 | 31.1127 | 444.195 | 3443.62 |
| 13410   | 3462    | 104.389 | 31.1355 | 441.729 | 3446.16 |
| 13411   | 3463    | 104.389 | 31.1355 | 441.729 | 3446.16 |
| 13412   | 3464    | 104.389 | 31.1355 | 441.729 | 3446.16 |
| 13422   | 346E    | 104.382 | 31.0913 | 441.034 | 3441.26 |
+---------+---------+---------+---------+---------+---------+
*/
```
table *tower_center* is basically the table *tower* with `DEYANG_CENTER` = 1

```sql
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


-- number of observations for each flag in table tower_hot_201308
SELECT
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE MAIN_CENTER_FLAG = 1) AS MAIN_CENTER_FLAG,
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE HOT_FLAG = 1) AS HOT_FLAG,
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE HOT_MAIN_FLAG = 1) AS HOT_MAIN_FLAG,
    (SELECT COUNT(*) FROM tower_hot_201308 WHERE HOT_OTHER_FLAG = 1) AS HOT_OTHER_FLAG;
/*
+------------------+----------+---------------+----------------+
| MAIN_CENTER_FLAG | HOT_FLAG | HOT_MAIN_FLAG | HOT_OTHER_FLAG |
+------------------+----------+---------------+----------------+
|              427 |      425 |           132 |            293 |
+------------------+----------+---------------+----------------+
*/
```
