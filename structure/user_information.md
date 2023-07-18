# User Information


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

## Terminology
- [CDMA](https://en.wikipedia.org/wiki/Code-division_multiple_access): Code-division multiple access
- [CDMA2000](https://zh.wikipedia.org/zh-tw/CDMA2000)
- [EVDO](https://en.wikipedia.org/wiki/Evolution-Data_Optimized): Evolution-Data Optimized
- ONEX: 1X

- ARPU: monthly average revenu per unit
- [MOU](https://www.docomo.ne.jp/english/corporate/ir/binary/pdf/library/presentation/060428/p33_e.pdf): miniuted of usage

## Basic
- tables: sample_user_{month}

### column names(keys of table)
| columns | type | explanations | examples | variable name in user_account_information |
| --- | --- | --- | --- | --- | 
| SERV_ID | `varchar(8)` | user ID |  | serv_id |
| MSISDN | `varchar(64)` | cellphone number |  | msisdn |
| MONTH_NO | `varchar(6)` | which month |  | month_no |
| CERT_NBR | `varchar(255)` | identity numbr |  | cert_nbr |         
| CI_BRANCH | `varchar(255)` | branch office | 德阳市市辖区, 广汉市 | ci_branch |
| EMPLOY_FLAG | `int` |  |  | is_zqhy ? | 
| STUDENT_FLAG | `int` |  |  | is_school ? |  
| RURAL_FLAG | `int` |  |  | bss_org_zj_flag ? |
| TERMINAL_BRAND | `varchar(255)` | cellphone's brand | SAMSUNG | hs_cdma_brand |
| TERMINAL_MODEL | `varchar(255)` | cellphone's model | SAMSUNG-B189 | hs_cdma_model |
| TERMINAL_SMART_FLAG | `int` | wheter the cellphone is a smartphone |  | is_intelligent |
| TERMINAL_TYPE | `varchar(8)` |  | 1X, 3G | terminal_type |
| TERMINAL_LEVEL | `int` |  | 1,2,3,4,5 | hs_cdma_layer ? |
| TERMINAL_PRICE | `int` | cellphone's price |  | hs_cdma_ter_price |
| HS_CDMA_CT_DATE | `varchar(20)` | when the users start their services? |  | hs_cdma_ct_date |
| PD_EVDO_FLAG | `varchar(1)` | wheter to use 4G wireless network |  | pd_evdo_flag_m1 |
| PD_1X_FLAG | `varchar(1)` | wheter the use 1x wireless network |  | pd_1x_flag_m1 |        
| ARPU | `float` | user average fee |  | mb_arpu_cdma_m1 | 
| ARPU_ALL | `float` | ARPU includes reconciliation |  | mb_arpu_cdma_all_m1 |    
| CONTRACT_FLAG | `varchar(1)` | wheter there is an agreement | | pl_contract_flag |
| CONTRACT_EXPIRE_MONTH | `int` | the number of months due from the agreement |  | pl_expire_month |
| VO_MOU_LOCAL | `float` | MOU of local | 0.45, 28.75, 176.08 | vo_cdma_mouout_local_m1 |
| VO_MOU_DIST | `float` | MOU of long distance | 0, 0.25, 37.55  | vo_cdma_mou_dist_m1 |

```sql
SELECT
    *
FROM
    sample_user_201308
LIMIT 3;
/*
+----------+----------+----------+-----------------------+--------------------+-------------+--------------+------------+----------------+--------------------+---------------------+---------------+----------------+----------------+--------------------+--------------+------------+------+----------+---------------+-----------------------+--------------+-------------+
| SERV_ID  | MSISDN   | MONTH_NO | CERT_NBR              | CI_BRANCH          | EMPLOY_FLAG | STUDENT_FLAG | RURAL_FLAG | TERMINAL_BRAND | TERMINAL_MODEL     | TERMINAL_SMART_FLAG | TERMINAL_TYPE | TERMINAL_LEVEL | TERMINAL_PRICE | HS_CDMA_CT_DATE    | PD_EVDO_FLAG | PD_1X_FLAG | ARPU | ARPU_ALL | CONTRACT_FLAG | CONTRACT_EXPIRE_MONTH | VO_MOU_LOCAL | VO_MOU_DIST |
+----------+----------+----------+-----------------------+--------------------+-------------+--------------+------------+----------------+--------------------+---------------------+---------------+----------------+----------------+--------------------+--------------+------------+------+----------+---------------+-----------------------+--------------+-------------+
| 9uxt3ojw | 6m6s3n3s | 201308   | 51-06-26-1981-01-02-1 | 德阳市市辖区       |           1 |            0 |          0 | SAMSUNG        | SAMSUNG-N719(3G)   |                   1 | 3G            |              5 |           4890 | 2013/1/11 10:29:21 | 1            | 1          |    0 |        0 | 0             |                  NULL |       176.08 |       37.55 |
| a2nl3ojo | 6twk3n3s | 201308   | 51-06-26-1981-01-02-1 | 德阳市市辖区       |           1 |            0 |          0 | 中兴           | 中兴-N600(3G)      |                   1 | 3G            |              3 |            899 | 2011/6/11 13:02:54 | 1            | 0          |  0.1 |      0.1 | 0             |                  NULL |        28.75 |        0.43 |
| 90893o09 | mm7pjcxf | 201308   | 51-06-02-1947-06-05-0 | 德阳市市辖区       |           1 |            0 |          0 | 华立时代       | 华立时代-LC101     |                   0 | 1X            |              2 |            330 | 2011/6/11 16:02:55 | 0            | 0          |    8 |        8 | 0             |                  NULL |         11.5 |           0 |
+----------+----------+----------+-----------------------+--------------------+-------------+--------------+------------+----------------+--------------------+---------------------+---------------+----------------+----------------+--------------------+--------------+------------+------+----------+---------------+-----------------------+--------------+-------------+
*/

SELECT
    COUNT(*)
FROM
    sample_user_201308;
/*
+----------+
| COUNT(*) |
+----------+
|   384490 |
+----------+
*/
```

```sql
SELECT DISTINCT
    MONTH_NO
FROM
    sample_user_201308;
/*
+----------+
| MONTH_NO |
+----------+
| 201308   |
+----------+
*/

SELECT
    CI_BRANCH, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY 
    CI_BRANCH;
/*
+--------------------------+----------+
| CI_BRANCH                | COUNT(*) |
+--------------------------+----------+
| 德阳市市辖区             |   130821 |
| 广汉市                   |    82059 |
| 中江县                   |    45441 |
| 什邡市                   |    52252 |
| 罗江县                   |    18473 |
| 绵竹市                   |    55347 |
| 德阳市未知营业区         |       97 |
+--------------------------+----------+
*/

SELECT
    EMPLOY_FLAG, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    EMPLOY_FLAG;
/*
+-------------+----------+
| EMPLOY_FLAG | COUNT(*) |
+-------------+----------+
|           1 |   381931 |
|           0 |     2559 |
+-------------+----------+
*/


SELECT
    STUDENT_FLAG, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    STUDENT_FLAG;
/*
+--------------+----------+
| STUDENT_FLAG | COUNT(*) |
+--------------+----------+
|            0 |   352969 |
|            1 |    31521 |
+--------------+----------+
*/

SELECT
    RURAL_FLAG, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    RURAL_FLAG;
/*
+------------+----------+
| RURAL_FLAG | COUNT(*) |
+------------+----------+
|          0 |   232218 |
|          1 |   152272 |
+------------+----------+
*/


SELECT
    TERMINAL_SMART_FLAG, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    TERMINAL_SMART_FLAG;
/*
+---------------------+----------+
| TERMINAL_SMART_FLAG | COUNT(*) |
+---------------------+----------+
|                   1 |   245107 |
|                   0 |   139383 |
+---------------------+----------+
*/

SELECT
    TERMINAL_TYPE, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    TERMINAL_TYPE;
/*
+---------------+----------+
| TERMINAL_TYPE | COUNT(*) |
+---------------+----------+
| 3G            |   294288 |
| 1X            |    90202 |
+---------------+----------+
*/

SELECT
    TERMINAL_LEVEL, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    TERMINAL_LEVEL
ORDER BY
    TERMINAL_LEVEL;
/*
+----------------+----------+
| TERMINAL_LEVEL | COUNT(*) |
+----------------+----------+
|              1 |    80522 |
|              2 |   122395 |
|              3 |   136266 |
|              4 |    33081 |
|              5 |    12226 |
+----------------+----------+
*/

SELECT
    PD_EVDO_FLAG, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    PD_EVDO_FLAG;
/*
+--------------+----------+
| PD_EVDO_FLAG | COUNT(*) |
+--------------+----------+
| 1            |   224196 |
| 0            |   160294 |
+--------------+----------+
*/


SELECT
    PD_1X_FLAG, COUNT(*) 
FROM
    sample_user_201308 
GROUP BY
    PD_1X_FLAG;
/*
+------------+----------+
| PD_1X_FLAG | COUNT(*) |
+------------+----------+
| 1          |    66648 |
| 0          |   317842 |
+------------+----------+
*/
```

## More
- tables: tb_asz_cdma_0838_{month}

### column names(keys of table)
