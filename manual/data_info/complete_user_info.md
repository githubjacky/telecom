# More User Information


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


- table: tb_asz_cdma_0838_{month}
```sql
SELECT
    'sample_user_201308' AS table_name,
    COUNT(*) AS count_value
FROM
    sample_user_201308
UNION
SELECT
    'tb_asz_cdma_0838_201308' AS table_name,
    COUNT(*) AS count_value
FROM
    tb_asz_cdma_0838_201308;
/*
+-------------------------+-------------+
| table_name              | count_value |
+-------------------------+-------------+
| sample_user_201308      |      384490 |
| tb_asz_cdma_0838_201308 |      591218 |
+-------------------------+-------------+
*/
```
## terminology
- [landline](https://en.wikipedia.org/wiki/landline#:~:text=a%20landline%20\(land%20line%2c%20land,radio%20waves%20for%20signal%20transmission.): telephone connection that uses metal wires or optical fiber telephone line

- [CDMA](https://www.techtarget.com/searchnetworking/definition/CDMA-Code-Division-Multiple-Access#:~:text=CDMA%20\(Code%2DDivision%20Multiple%20Access\)%20refers%20to%20any%20of,the%20use%20of%20available%20bandwidth.): Code-division multiple access
- [CDMA2000](https://zh.wikipedia.org/zh-tw/CDMA2000)
- [EVDO](https://en.wikipedia.org/wiki/Evolution-Data_Optimized): Evolution-Data Optimized
- [ONEX](https://zhidao.baidu.com/question/481540439.html): 1X

The difference between EVDO and ONEX flag is wheter the cellphone supoort the internet?

- ARPU: monthly average revenu per unit
- [MOU](https://www.docomo.ne.jp/english/corporate/ir/binary/pdf/library/presentation/060428/p33_e.pdf): minutes of usage
- [RED MARK](https://zhidao.baidu.com/question/687512891865237084.html)
- [E6_E8https://zhidao.baidu.com/question/204032713.html?fr=search&word=E6%2C+E8%2C+E9_E9](https://zhidao.baidu.com/question/296625569.html)
- [E6_E8_E9](https://zhidao.baidu.com/question/204032713.html?fr=search&word=E6%2C+E8%2C+E9)

## column names(keys of table)

| columns | type | explanations | examples | variable name in user_account_information |
| --- | --- | --- | --- | --- | 
| MONTH_NO | `varchar(6)` | which month |  | month_no |
| SERV_ID | `varchar(8)` | user ID |  | serv_id |
| CERT_NBR | `varchar(255)` | identity numbr |  | cert_nbr |         
| CI_BRANCH | `varchar(255)` | branch office | 德阳市市辖区, 广汉市 | ci_branch |
| MSISDN | `varchar(64)` | cellphone number |  | msisdn |
| [CI_CUSTYPE](#CI_CUSTYPE) | `varchar(255)` | customer type(pre paid or post paid) | 家庭, 政企, 個人, 其他 | ci_custype |
| [CI_IVPN_FLAG](#CI_IVPN_FLAG) | `varchar(1)` | wheter the switch board user |  | ci_ivpn_flag | 
| [CI_CITY](#CI_CITY) | `varchar(255)` | state(all observations are 德阳) | 德阳 | ci_city |
| [CI_DISTRICT](#CI_DISTRICT) | `varchar(255)` |  | 德阳现业, 广汉, ... | ci_district |
| [CI_TENURE](#CI_TENURE) | `int` | user time in the network | | ci_tenure |
| [PD_CDMA_STATUS](#PD_CDMA_STATUS) |`varchar(255)` | |正常, 欠费双停, 用户要求停机, ... | pd_cdma_status | 
| [PD_CDMA_TENURE](#PD_CDMA_TENURE) | `int` | CDMA time in the network | | pd_cdma_tenure |
| [PL_CONTRACT_FLAG](#PL_CONTRACT_FLAG) | `varchar(1)` | whether there is agreement |  | pl_contract_flag |
| PL_INVAMON_MAX | `varchar(8)` | agreement expiration data | | pl_invamon_max
| PL_EXPIRE_MONTH | `int` | the number of months due from the agreement |  | pl_expire_month |
| [PL_COM_FLAG](#PL_COM_FLAG) | `varchar(1)` | wheter the package containg land line + moblie line | | pl_com_flag |
| HS_CDMA_BRAND | `varchar(255)` | cellphone's brand | SAMSUNG | hs_cdma_brand |
| HS_CDMA_MODEL | `varchar(255)` | cellphone's model | SAMSUNG-B189 | hs_cdma_model |
| [HS_CDMA_LAYER](#HS_CDMA_LAYER) | `varchar(255)` |  | 超高端, 高端, 中端  | hs_cdma_layer |
| [HS_CDMA_TER_PRICE](#HS_CDMA_TER_PRICE) | `int` | cellphone's price |  | hs_cdma_ter_price |
| HS_CDMA_CT_DATE | `varchar(20)` | terminal usage start time | | hs_cdma_ct_data |
| [HS_CDMA_SYS](#HS_CDMA_SYS) | `varchar(255)` | CDMA terminal operating system | Android, Windows Mobile, Linux, BlackBerry | hs_cdma_sys |
| [HS_CDMA_IS_EVDO](#HS_CDMA_IS_EVDO) | `varchar(1)` | does CDMA terminal support EVDO | | HS_CDMA_IS_evdo |
| [HS_CDMA_IS_1X](#HS_CDMA_IS_1X) | `varchar(1)` | does CDMA terminal support 1X | | HS_CDMA_IS_1X |
| [VO_CDMA_MOU_M1](#MOU) | `float` | minutes of usage(out and in) | | vo_cdma_mou_m1 |
| [VO_CDMA_MOUOUT_LOCAL_M1](#MOU) | `float` | local callout MOU | | vo_cdma_mouout_local_m1 |
| [VO_CDMA_MOU_DIST_M1](#MOU) | `float` | log distance call out MOU | | vo_cdma_mout_dist_m1 |
| [VO_CDMA_MOU_ROAM_M1](#MOU) | `float` | roaming MOU(user outside) | | vo_cdma_mou_roam_m1 |
| [VO_CDMA_COUNT_M1](#CALL_COUNT) | `int` | monthe average calls | | vo_cdma_cout_m1 |
| [VO_CDMA_COUNT_LOCAL_M1](#CALL_COUNT) | `int` | month average local calls | | vo_cdma_count_local_m1 |
| [VO_CDMA_COUNTOUT_DIST_M1](#CALL_COUNT) | `int` | month average long distance out calls | | vo_cdma_countout_dist_m1 |
| [VO_CDMA_COUNT_ROAM_M1](#CALL_COUNT) | `int` | month average roaming calls | vo_cdma_count_roam_m1 |
| VO_NET_VOL_M1 | `float` | monthly internet usage traffic | | vo_net_vol_m1 |
| VO_NET_TIME_M1 | `float` | monthly internet usage time | | vo_net_time_m1 |
| [PD_EVDO_FLAG_M1](#FLAG) | `varchar(1)` | wheter to use EVDO wireless network(not wheter support`HS_CDMA_IS_EVDO`) | | PD_EVDO_FLAG_M1 |
| [PD_1X_FLAG_M1](#FLAG) | `varchar(1)` | wheter to use 1X wireless network | | pd_1x_flag_m1 |
| [VO_EVDO_VOL_M1](#EVDO_VOL) | `float` | monthly EVDO traffic average | | vo_evdo_vol_m1 |
| [VO_EVDO_TIME_M1](#EVDO_TIME) | `float` | monthly EVDO duration average | | vo_evdo_time_m1 |
| VO_EVDO_LOCALVOL_M1 | `float` | monthly EVDO local traffic average | | vo_evdo_localvol_m1 |
| VO_EVDO_LOCALTIME_M1 | `float` | monthly EVDO local duration average | | vo_evdo_localtime_m1 |
| VO_EVDO_ROAMVOL_M1 | `float` | monthly EVDO roaming traffic average | | vo_evdo_roamvol_m1 |
| VO_EVDO_ROAMTIME_M1 | `float` | monthly EVDO roaming duration average | | vo_evdo_roamtime_m1 |
| [VO_1X_VOL_M1](#1X_VOL) | `float` | monthly onex traffic average | | vo_1x_vol_m1 |
| [VO_1X_TIME_M1](#1x_TIME) | `float` | monthly onex duration average | | vo_1x_time _m1 |
| VO_1X_LOCALVO_M1 | `float` | monthly onex local traffic average | | vo_1x_localvol_m1 |
| VO_1XLOCALTIME_M1 | `float` | monthly onex local duration average | | vo_1x_localtime_m1 |
| VO_1X_ROAMVOL_M1 | `float` | monthly onex roaming traffic average | | vo_1x_roamvol_m1 |
| VO_1X_ROAMTIME_M1 | `float` | monthly onex roamiing duration average | | vo_1x_roamtime_m1 |
| [MB_ARPU_CDMA_M1](#ARPU) | `float` | monthly average revene per unit(fee) | | mb_arpu_cdma_m1 |
| [MB_ARPU_CDMA_ALL_M1](#ARPU) | `float` | monthly arpu(include reconciliation) | | mb_arpu_cdma_all_m1 |
| [MB_ENPR_FLAG_M1](#MB_ENPR_FLAG_M1) | `varchar(1)` | wheter the government pays in Januar-March(if individual works for government) | | mb_enpr_flag_m1 |
| [IS_BUSINESS](#IS_BUSINESS) | `varchar(255)` | wheter the unit pay the fee | 0, 1 | is_business |
| [RED_MARK](#RED_MARK) | `varchar(1)` | wheter it is a red list | | red_mark |
| HS_CDMA_COL_DATE | `varchar(20)` | data aqusition time | 2013/9/8 9:58:04 | hs_cdma_col_data | 
| [PL_IVPN_CAT](#PL_IVPN_CAT) | `varchar(255)` | v network type | NULL, 虚拟网, 家庭V网| pl_ivpn_cat |
| [PL_E9_FLAG](#E) | `varchar(255)` | whether E9 type | 0, 1 | pl_e9_flag |
| [PL_E6_FLAG](#E) | `varchar(255)` | wheter E6 type | 0, 1 | pl_e6_flag |
| [PL_BUSINESS_FLAG](#PL_BUSINESS_FLAG) | `varchar(1)` | wheter business piloting | | pl_business_flag |
| [PL_WIRELESS_FLAG](#PL_WIRELESS_FLAG) | `varchar(1)` | wheter super cordless | | pl_wireless_flag |
| [PL_CAMPUS_FLAG](#PL_CAMPUS_FLAG) | `varchar(1)` | campus integration | | pl_camplus_flag |
| [PL_BB_FLAG](#PL_BB_FLAG) | `varchar(1)` | wheter wireless | | pl_bb_flag |
| [VPN_FLAG](#VPN_FLAG) | `varchar(1)` | wheter virtual network user | | vpn_flag |
| [PAYMENT_FLAG](#PAYMENT_FLAG) | `varchar(1)` | wheter to pay out users | | payment_flag |
| [IS_WX_FLAG](#IS_WX_FLAG) | `varchar(1)` | wheter wireless | | is_wx_flag |
| [IS_INTELLIGENT](#IS_INTELLIGENT) | `varchar(255)` | wheter a smartphone | 是, 否, NULL | is_intelligent |
| [TERMINAL_TYPE](#TERMINAL_TYPE) | `varchar(8)` | terminal type | 3G, 1X, NULL | terminal_type | 
| [BSS_ORG_ZJ_NAME](#BSS_ORG_ZJ_NAME) | `varchar(255)` | branch name | NULL, 中江分公司-万福支局-万福 | bss_org_zj_name |
| [BSS_ORG_ZJ_FLAG](#BSS_ORG_ZJ_FLAG) | `varchar(255)` | wheter the rural branch office | NULL, 0, 1 | bss_org_zj_flag |
| [IS_SCHOOL](#IS_SCHOOL) | `varchar(255)` | wheter the government and enterprise campus users | 0, 1 | is_school |
| [IS_E9ZX](#IS_E9ZX) | `varchar(1)` | wheter E9(尊享) | | is_e9zx | 
| [IS_ZQJN](#IS_ZQJN) | `varchar(1)` | whether the gorvernment and enterprise cluster | | is_zqjn |
| [IS_ZQHY](#IS_ZQHY) | `varchar(1)` | whether the government and enterprise industry | | is_zqhy |
| PROFILE_VALUE | `varchar(255)` | the way to contact user | 996999-8253819, DYC1305, null, 先裝機後付款 | profile_value |
| [DETAIL_NAME](#DETAIL_NAME) | `varchar(255)` | 21 customer segmenttation names | NULL, 个人客户, 现业区级党政单位, 农村政企【不含大企业】| detail_name |
| IS_WL_FLAG | `varchar(255)` | ? | only with 0 value | is_wl_flag |
| [IS_HVL_FLAG](#IS_HVL_FLAG) | `varchar(255)` | ? | 0, 1 | is_hvl_flag | 
| [IS_LVL_FLAG](#IS_LVL_FLAG) | `varchar(255)` | ? | 0, 1 | is_lvl_flag |
| [IS_8CARD](#IS_8CARD) | `varchar(255)` | whether it's 8-point(there is an 8-point card package since 2012) | 是, 否| is_8card |
| [IS_CARDPHONE](#IS_CARDPHONE)| `varchar(255)` | wheter it's a card phone | 是, 否 | is_cardphone |
| [IS_CLOUDCARD](#IS_CLOUDCARD) | `varchar(255)` | wheter cloud card | 是, 否 | is_cloudcard |
| [SUM_EVDO](#SUM_EVDO) | `int` | EVDO traffic | | sum_evdo |



- helper function(PROCEDURE)
```text
mysql> source ./utils.sql
```


<a name="CI_CUSTYPE"></a>
```sql
CALL calculate_count_by_column('CI_CUSTYPE');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 家庭         |      406090 |
| 政企         |      106143 |
| 个人         |       78951 |
| 其他         |          34 |
+--------------+-------------+
*/
```


<a name="CI_IVPN_FLAG"></a>
```sql
CALL calculate_count_by_column('CI_IVPN_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      442382 |
| 1            |      148836 |
+--------------+-------------+
*/
```


<a name="CI_CITY"></a>
```sql
CALL calculate_count_by_column('CI_CITY');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 德阳         |      591218 |
+--------------+-------------+
*/
```


<a name="CI_DISTRICT"></a>
```sql
CALL calculate_count_by_column('CI_DISTRICT');
/*
+--------------------------+-------------+
| column_value             | count_value |
+--------------------------+-------------+
| 德阳现业                 |      187854 |
| 广汉                     |      120184 |
| 中江                     |      101434 |
| 什邡                     |       77564 |
| 罗江                     |       29409 |
| 绵竹                     |       74771 |
| NULL                     |           1 |
| 德阳市未知营业区         |           1 |
+--------------------------+-------------+
*/
```


<a name="CI_TENURE"></a>
```sql
CALL calculate_decile_by_column('CI_TENURE');
/*
+-----------+--------+
| max_value | decile |
+-----------+--------+
|         5 |      1 |
|        11 |      2 |
|        17 |      3 |
|        23 |      4 |
|        32 |      5 |
|        43 |      6 |
|        59 |      7 |
|        86 |      8 |
|       127 |      9 |
|       308 |     10 |
+-----------+--------+
*/
```


<a name="PD_CDMA_STATUS"></a>
```sql
CALL calculate_count_by_column('PD_CDMA_STATUS');
/*
+--------------------+-------------+
| column_value       | count_value |
+--------------------+-------------+
| 正常               |      537294 |
| 欠费双停           |       25867 |
| 用户要求停机       |        3067 |
| 欠费停机           |        6274 |
| 未知状态           |         106 |
| 本月新装           |       18610 |
+--------------------+-------------+
*/
```


<a name="PD_CDMA_TENURE"></a>
```sql
CALL calculate_decile_by_column('PD_CDMA_TENURE');
/*
+--------------+--------+
| column_value | decile |
+--------------+--------+
|            3 |      1 |
|            7 |      2 |
|           11 |      3 |
|           15 |      4 |
|           19 |      5 |
|           24 |      6 |
|           30 |      7 |
|           37 |      8 |
|           44 |      9 |
|          137 |     10 |
+--------------+--------+
*/
```


<a name="PL_CONTRACT_FLAG"></a>
```sql
CALL calculate_count_by_column('PL_CONTRACT_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      476117 |
| 1            |      115101 |
+--------------+-------------+
*/

```


<a name="PL_COM_FLAG"></a>
```sql
CALL calculate_count_by_column('PL_COM_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 1            |      397226 |
| 0            |      193992 |
+--------------+-------------+
*/

```


<a name="HS_CDMA_LAYER"></a>
```sql
CALL calculate_count_by_column('HS_CDMA_LAYER');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 超高端       |       17379 |
| 中端         |      175674 |
| NULL         |       67624 |
| 低端         |      162878 |
| 超低端       |      123202 |
| 高端         |       44461 |
+--------------+-------------+
*/
```


<a name="HS_CDMA_TER_PRICE"></a>
```sql
CALL calculate_decile_by_column('HS_CDMA_TER_PRICE');
/*
+--------------+--------+
| column_value | decile |
+--------------+--------+
|         NULL |      1 |
|          199 |      2 |
|          278 |      3 |
|          399 |      4 |
|          599 |      5 |
|          750 |      6 |
|          899 |      7 |
|          999 |      8 |
|         1590 |      9 |
|        10600 |     10 |
+--------------+--------+
*/
```


<a name="HS_CDMA_SYS"></a>
```sql
CALL calculate_count_by_column('HS_CDMA_SYS');
/*
+----------------+-------------+
| column_value   | count_value |
+----------------+-------------+
| Android        |      301318 |
| NULL           |      282210 |
| Windows Mobile |        1835 |
| Symbian        |        5619 |
| Windows CE     |         101 |
| Linux          |          95 |
| BlackBerry OS  |          40 |
+----------------+-------------+
*/
```


<a name="HS_CDMA_IS_EVDO"></a>
```sql
CALL calculate_count_by_column('HS_CDMA_IS_EVDO');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 1            |      386855 |
| 0            |      149779 |
| NULL         |       54584 |
+--------------+-------------+
*/
```


<a name="HS_CDMA_IS_1X"></a>
```sql
CALL calculate_count_by_column('HS_CDMA_IS_1X');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 1            |      536634 |
| NULL         |       54584 |
+--------------+-------------+
*/
```


<a name="MOU"></a>
```sql
CALL decile_comparison
(
    'VO_CDMA_MOU_M1', 
    'VO_CDMA_MOUOUT_LOCAL_M1', 
    'VO_CDMA_MOU_DIST_M1', 
    'VO_CDMA_MOU_ROAM_M1'
);

/*
+----------------+--------+-------------------------+--------+---------------------+--------+---------------------+--------+
| VO_CDMA_MOU_M1 | decile | VO_CDMA_MOUOUT_LOCAL_M1 | decile | VO_CDMA_MOU_DIST_M1 | decile | VO_CDMA_MOU_ROAM_M1 | decile |
+----------------+--------+-------------------------+--------+---------------------+--------+---------------------+--------+
|              0 |      1 |                       0 |      1 |                   0 |      1 |                   0 |      1 |
|              0 |      2 |                       0 |      2 |                   0 |      2 |                   0 |      2 |
|           6.52 |      3 |                    0.37 |      3 |                   0 |      3 |                   0 |      3 |
|          32.32 |      4 |                     4.8 |      4 |                   0 |      4 |                   0 |      4 |
|             67 |      5 |                   13.75 |      5 |                   0 |      5 |                   0 |      5 |
|         111.28 |      6 |                   28.02 |      6 |                1.48 |      6 |                   0 |      6 |
|         171.73 |      7 |                   49.95 |      7 |                6.52 |      7 |                   0 |      7 |
|         263.63 |      8 |                   86.15 |      8 |               18.58 |      8 |                4.85 |      8 |
|          444.4 |      9 |                  160.48 |      9 |               52.08 |      9 |               41.87 |      9 |
|        22968.3 |     10 |                 11619.8 |     10 |              8136.4 |     10 |             6316.22 |     10 |
+----------------+--------+-------------------------+--------+---------------------+--------+----
*/
```

<a name="CALL_COUNT"></a>
```sql
CALL decile_comparison
(
    'VO_CDMA_COUNT_M1', 
    'VO_CDMA_COUNT_LOCAL_M1', 
    'VO_CDMA_COUNTOUT_DIST_M1', 
    'VO_CDMA_COUNT_ROAM_M1'
);
/*
+------------------+--------+------------------------+--------+--------------------------+--------+-----------------------+--------+
| VO_CDMA_COUNT_M1 | decile | VO_CDMA_COUNT_LOCAL_M1 | decile | VO_CDMA_COUNTOUT_DIST_M1 | decile | VO_CDMA_COUNT_ROAM_M1 | decile |
+------------------+--------+------------------------+--------+--------------------------+--------+-----------------------+--------+
|                0 |      1 |                      0 |      1 |                        0 |      1 |                     0 |      1 |
|                0 |      2 |                      0 |      2 |                        0 |      2 |                     0 |      2 |
|                6 |      3 |                      2 |      3 |                        0 |      3 |                     0 |      3 |
|               22 |      4 |                     12 |      4 |                        0 |      4 |                     0 |      4 |
|               42 |      5 |                     29 |      5 |                        0 |      5 |                     0 |      5 |
|               69 |      6 |                     51 |      6 |                        1 |      6 |                     0 |      6 |
|              105 |      7 |                     83 |      7 |                        3 |      7 |                     0 |      7 |
|              161 |      8 |                    133 |      8 |                        7 |      8 |                     5 |      8 |
|              269 |      9 |                    230 |      9 |                       16 |      9 |                    25 |      9 |
|             6876 |     10 |                   6876 |     10 |                     2010 |     10 |                  2749 |     10 |
+------------------+--------+------------------------+--------+--------------------------+--------+-----------------------+--------+
*/
```


<a name="FLAG"></a>
```sql
CALL calculate_count_by_column('PD_EVDO_FLAG_M1');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| 1           |      277356 |
| 0           |      313862 |
+-------------+-------------+
*/


CALL calculate_count_by_column('PD_1X_FLAG_M1');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| 1           |       86739 |
| 0           |      504479 |
+-------------+-------------+
*/
```

<a name="EVDO_VOL"></a>
```sql
CALL decile_comparison
(
    'VO_NET_VOL_M1',
    'VO_EVDO_VOL_M1',
    'VO_EVDO_LOCALVOL_M1',
    'VO_EVDO_ROAMVOL_M1'
);
/*
+---------------+--------+----------------+--------+---------------------+--------+--------------------+--------+
| VO_NET_VOL_M1 | decile | VO_EVDO_VOL_M1 | decile | VO_EVDO_LOCALVOL_M1 | decile | VO_EVDO_ROAMVOL_M1 | decile |
+---------------+--------+----------------+--------+---------------------+--------+--------------------+--------+
|             0 |      1 |              0 |      1 |                   0 |      1 |                  0 |      1 |
|             0 |      2 |              0 |      2 |                   0 |      2 |                  0 |      2 |
|             0 |      3 |              0 |      3 |                   0 |      3 |                  0 |      3 |
|             0 |      4 |              0 |      4 |                   0 |      4 |                  0 |      4 |
|          0.01 |      5 |              0 |      5 |                   0 |      5 |                  0 |      5 |
|          2.41 |      6 |           1.33 |      6 |                0.22 |      6 |                  0 |      6 |
|         26.47 |      7 |          24.11 |      7 |               11.94 |      7 |                  0 |      7 |
|         92.68 |      8 |          89.31 |      8 |               56.12 |      8 |                  0 |      8 |
|         266.2 |      9 |         261.04 |      9 |              190.31 |      9 |              11.74 |      9 |
|        158243 |     10 |         158243 |     10 |              158243 |     10 |              66439 |     10 |
+
*/

```


<a name="EVDO_TIME"></a>
```sql
CALL decile_comparison
(
    'VO_NET_TIME_M1',
    'VO_EVDO_TIME_M1',
    'VO_EVDO_LOCALTIME_M1',
    'VO_EVDO_ROAMTIME_M1'
);
/*
+----------------+--------+-----------------+--------+----------------------+--------+---------------------+--------+
| VO_NET_TIME_M1 | decile | VO_EVDO_TIME_M1 | decile | VO_EVDO_LOCALTIME_M1 | decile | VO_EVDO_ROAMTIME_M1 | decile |
+----------------+--------+-----------------+--------+----------------------+--------+---------------------+--------+
|              0 |      1 |               0 |      1 |                    0 |      1 |                   0 |      1 |
|              0 |      2 |               0 |      2 |                    0 |      2 |                   0 |      2 |
|              0 |      3 |               0 |      3 |                    0 |      3 |                   0 |      3 |
|              0 |      4 |               0 |      4 |                    0 |      4 |                   0 |      4 |
|           0.05 |      5 |               0 |      5 |                    0 |      5 |                   0 |      5 |
|          80.87 |      6 |           33.22 |      6 |                  5.4 |      6 |                   0 |      6 |
|        1063.37 |      7 |          890.33 |      7 |               426.12 |      7 |                   0 |      7 |
|        4340.17 |      8 |         4052.35 |      8 |              2550.78 |      8 |                   0 |      8 |
|        14291.8 |      9 |         13879.6 |      9 |              10256.4 |      9 |              616.23 |      9 |
|        48201.3 |     10 |         46559.3 |     10 |              44973.5 |     10 |             46559.3 |     10 |
+----------------+--------+-----------------+--------+----------------------+--------+---------------------+--------+
*/


<a name="1X_VOL"></a>
```sql
CALL decile_comparison
(
    'VO_NET_VOL_M1',
    'VO_1X_VOL_M1',
    'VO_1X_LOCALVOL_M1',
    'VO_1X_ROAMVOL_M1'
);
/*
+---------------+--------+--------------+--------+-------------------+--------+------------------+--------+
| VO_NET_VOL_M1 | decile | VO_1X_VOL_M1 | decile | VO_1X_LOCALVOL_M1 | decile | VO_1X_ROAMVOL_M1 | decile |
+---------------+--------+--------------+--------+-------------------+--------+------------------+--------+
|             0 |      1 |            0 |      1 |                 0 |      1 |                0 |      1 |
|             0 |      2 |            0 |      2 |                 0 |      2 |                0 |      2 |
|             0 |      3 |            0 |      3 |                 0 |      3 |                0 |      3 |
|             0 |      4 |            0 |      4 |                 0 |      4 |                0 |      4 |
|          0.01 |      5 |            0 |      5 |                 0 |      5 |                0 |      5 |
|          2.41 |      6 |            0 |      6 |                 0 |      6 |                0 |      6 |
|         26.47 |      7 |            0 |      7 |                 0 |      7 |                0 |      7 |
|         92.68 |      8 |            0 |      8 |                 0 |      8 |                0 |      8 |
|         266.2 |      9 |         0.04 |      9 |                 0 |      9 |                0 |      9 |
|        158243 |     10 |      1389.71 |     10 |           1389.71 |     10 |           922.24 |     10 |
+---------------+--------+--------------+--------+-------------------+--------+------------------+--------+
*/


<a name="1X_TIME"></a>
```sql
CALL decile_comparison
(
    'VO_NET_TIME_M1',
    'VO_1X_TIME_M1',
    'VO_1X_LOCALTIME_M1',
    'VO_1X_ROAMTIME_M1'
);
/*
+----------------+--------+---------------+--------+--------------------+--------+-------------------+--------+
| VO_NET_TIME_M1 | decile | VO_1X_TIME_M1 | decile | VO_1X_LOCALTIME_M1 | decile | VO_1X_ROAMTIME_M1 | decile |
+----------------+--------+---------------+--------+--------------------+--------+-------------------+--------+
|              0 |      1 |             0 |      1 |                  0 |      1 |                 0 |      1 |
|              0 |      2 |             0 |      2 |                  0 |      2 |                 0 |      2 |
|              0 |      3 |             0 |      3 |                  0 |      3 |                 0 |      3 |
|              0 |      4 |             0 |      4 |                  0 |      4 |                 0 |      4 |
|           0.05 |      5 |             0 |      5 |                  0 |      5 |                 0 |      5 |
|          80.87 |      6 |             0 |      6 |                  0 |      6 |                 0 |      6 |
|        1063.37 |      7 |             0 |      7 |                  0 |      7 |                 0 |      7 |
|        4340.17 |      8 |             0 |      8 |                  0 |      8 |                 0 |      8 |
|        14291.8 |      9 |         10.32 |      9 |                  0 |      9 |                 0 |      9 |
|        48201.3 |     10 |         44479 |     10 |              44479 |     10 |           41024.9 |     10 |
+----------------+--------+---------------+--------+--------------------+--------+-------------------+--------+
*/
```


<a name="ARPU"></a>
```sql
CALL calculate_decile_by_column('MB_ARPU_CDMA_M1');
/*
+--------------+--------+
| column_value | decile |
+--------------+--------+
|            0 |      1 |
|         4.55 |      2 |
|           10 |      3 |
|           19 |      4 |
|           26 |      5 |
|           38 |      6 |
|           48 |      7 |
|         59.8 |      8 |
|         86.6 |      9 |
|      1934.57 |     10 |
+--------------+--------+
*/
CALL calculate_decile_by_column('MB_ARPU_CDMA_ALL_M1');
/*
+--------------+--------+
| column_value | decile |
+--------------+--------+
|            0 |      1 |
|            0 |      2 |
|            6 |      3 |
|           11 |      4 |
|           20 |      5 |
|         28.2 |      6 |
|           40 |      7 |
|         58.1 |      8 |
|           75 |      9 |
|         2339 |     10 |
+--------------+--------+
*/
```

<a name="MB_ENPR_FLAG_M1"></a>
```sql
CALL calculate_count_by_column('MB_ENPR_FLAG_M1');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| 0           |      354859 |
| 1           |      236359 |
+-------------+-------------+
*/
```


<a name="IS_BUSINESS"></a>
```sql
CALL calculate_count_by_column('IS_BUSINESS');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| 1           |       53152 |
| 0           |      538066 |
+-------------+-------------+
*/
```


<a name="RED_MARK"></a>
```sql
CALL calculate_count_by_column('RED_MARK');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| 0           |      588516 |
| 1           |        2702 |
+-------------+-------------+
*/
```


<a name="PL_IVPN_CAT"></a>
```sql
CALL calculate_count_by_column('PL_IVPN_CAT');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| NULL        |      569429 |
| 虚拟网      |       21773 |
| 家庭V网     |           6 |
| 集团v网     |           6 |
| 校园V网     |           1 |
| 乡情v网     |           3 |
+-------------+-------------+
*/
```


<a name="E"></a>
```sql
CALL calculate_count_by_column('PL_E9_FLAG');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| 1           |      333503 |
| 0           |      257715 |
+-------------+-------------+
*/

CALL calculate_count_by_column('PL_E6_FLAG');
/*
+-------------+-------------+
| column_value | count_value |
+-------------+-------------+
| 0           |      566898 |
| 1           |       24320 |
+-------------+-------------+
*/
```


<a name="PL_BUSINESS_FLAG"></a>
```sql
CALL calculate_count_by_column('PL_BUSINESS_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      563007 |
| 1            |       28211 |
+--------------+-------------+
*/
```


<a name="PL_WIRELESS_FLAG"></a>
```sql
CALL calculate_count_by_column('PL_WIRELESS_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      587518 |
| 1            |        3700 |
+--------------+-------------+
*/
```


<a name="PL_CAMPUS_FLAG"></a>
```sql
CALL calculate_count_by_column('PL_CAMPUS_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      583316 |
| 1            |        7902 |
+--------------+-------------+
*/
```


<a name="PL_BB_FLAG"></a>
```sql
CALL calculate_count_by_column('PL_BB_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      577965 |
| 1            |       13253 |
+--------------+-------------+
*/
```


<a name="VPN_FLAG"></a>
```sql
CALL calculate_count_by_column('VPN_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 1            |      383915 |
| 0            |      207303 |
+--------------+-------------+
*/
```


<a name="PAYMENT_FLAG"></a>
```sql
CALL calculate_count_by_column('PAYMENT_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 1            |      542169 |
| 0            |       49049 |
+--------------+-------------+
*/
```


<a name="IS_WX_FLAG"></a>
```sql
CALL calculate_count_by_column('IS_WX_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      578162 |
| 1            |       13056 |
+--------------+-------------+
*/
```


<a name="IS_INTELLIGENT"></a>
```sql
CALL calculate_count_by_column('IS_INTELLIGENT');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 是           |      315109 |
| 否           |      233140 |
| NULL         |       42969 |
+--------------+-------------+
*/
```


<a name="TERMINAL_TYPE"></a>
```sql
CALL calculate_count_by_column('TERMINAL_TYPE');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 是           |      315109 |
| 否           |      233140 |
| NULL         |       42969 |
+--------------+-------------+
*/
```



<a name="BSS_ORG_ZJ_NAME"></a>
```sql
CALL calculate_count_by_column_temp('BSS_ORG_ZJ_NAME');
SELECT
    *
FROM
    count_result_BSS_ORG_ZJ_NAME
LIMIT 3;
/*
+-------------------------------------+-------------+
| column_value                        | count_value |
+-------------------------------------+-------------+
| NULL                                |       76647 |
| 中江分公司-万福支局-万福            |        1195 |
| 中江分公司-万福支局-普兴            |         564 |
+-------------------------------------+-------------+
*/

SELECT
    COUNT(*)
FROM
    count_result_BSS_ORG_ZJ_NAME;
/*
+----------+
| COUNT(*) |
+----------+
|      253 |
+----------+
*/
```


<a name="BSS_ORG_ZJ_FLAG"></a>
```sql
CALL calculate_count_by_column('BSS_ORG_ZJ_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| NULL         |       76647 |
| 0            |      299985 |
| 1            |      214586 |
+--------------+-------------+
*/
```


<a name="IS_SCHOOL"></a>
```sql
CALL calculate_count_by_column('IS_SCHOOL');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      551079 |
| 1            |       40139 |
+--------------+-------------+
*/
```



<a name="IS_E9ZX"></a>
```sql
CALL calculate_count_by_column('IS_E9ZX');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      589471 |
| 1            |        1747 |
+--------------+-------------+
*/
```



<a name="IS_ZQJN"></a>
```sql
CALL calculate_count_by_column('IS_ZQJN');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      572873 |
| 1            |       18345 |
+--------------+-------------+
*/
```



<a name="IS_ZQHY"></a>
```sql
CALL calculate_count_by_column('IS_ZQHY');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 1            |      531143 |
| 0            |       60075 |
+--------------+-------------+
*/
```


<a name="DETAIL_NAME"></a>
```sql
CALL calculate_count_by_column('DETAIL_NAME');
/*
+--------------------------------------------------+-------------+
| column_value                                     | count_value |
+--------------------------------------------------+-------------+
| NULL                                             |       16609 |
| 专业市场                                         |         172 |
| 个人客户                                         |       54904 |
| 中小企业                                         |         967 |
| 中小学【九年制义务教育】及幼儿园                 |         279 |
| 临街商铺                                         |         235 |
| 产业园区                                         |         200 |
| 其他中小聚类                                     |         404 |
| 农村公众（家庭及个人）                           |      201497 |
| 农村客户                                         |           2 |
| 农村政企【不含大企业】                           |       29942 |
| �院                                             |        3040 |
�| 商务楼宇                                         |          51 |
| 城市个人客户                                     |          22 |
| 城市家庭客户                                     |         484 |
| 大企业【不含农村】                               |       37148 |
| 家庭客户                                         |      219081 |
| 宾馆酒店                                         |         225 |
| 市县级党政军                                     |       11737 |
| 现业区级党政单位                                 |          93 |
| 网吧                                             |         488 |
| 聚类客户                                         |          52 |
| 行业客户                                         |           1 |
| 金融                                             |        4352 |
| 高中                                             |        2402 |
| 高等和�业院校                                   |        6831 |
�+--------------------------------------------------+-------------+
*/
```


<a name="IS_HVL_FLAG"></a>
```sql
CALL calculate_count_by_column('IS_HVL_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 0            |      589665 |
| 1            |        1553 |
+--------------+-------------+
*/
```


<a name="IS_LVL_FLAG"></a>
```sql
CALL calculate_count_by_column('IS_LVL_FLAG');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 1            |      589665 |
| 0            |        1553 |
+--------------+-------------+
*/
```


<a name="IS_8CARD"></a>
```sql
CALL calculate_count_by_column('IS_8CARD');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 否           |      578969 |
| 是           |       12249 |
+--------------+-------------+
*/
```


<a name="IS_CARDPHONE"></a>
```sql
CALL calculate_count_by_column('IS_CARDPHONE');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 否           |      589706 |
| 是           |        1512 |
+--------------+-------------+
*/
```



<a name="IS_CLOUDCARD"></a>
```sql
CALL calculate_count_by_column('IS_CLOUDCARD');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
| 否           |      582416 |
| 是           |        8802 |
+--------------+-------------+
*/
```

<a name="SUM_EVDO"></a>
```sql
CALL calculate_count_by_column('SUM_EVDO');
/*
+--------------+-------------+
| column_value | count_value |
+--------------+-------------+
|          500 |       57422 |
|         NULL |      479560 |
|           30 |       32675 |
|          300 |        1503 |
|          530 |         645 |
|         2048 |         296 |
|           60 |        7946 |
|          150 |        3683 |
|          800 |         658 |
|          120 |         277 |
|          100 |        2631 |
|         1300 |         169 |
|         1000 |         485 |
|         2148 |           3 |
|          650 |         188 |
|         3048 |           1 |
|          600 |         385 |
|         1100 |          16 |
|         5120 |         212 |
|          250 |         404 |
|          330 |          98 |
|         2548 |          82 |
|          560 |         188 |
|          130 |         255 |
|          750 |           4 |
|          160 |          39 |
|         1524 |           4 |
|         1024 |          13 |
|           90 |         346 |
|          180 |         192 |
|          830 |          32 |
|         1500 |          24 |
|         1560 |           1 |
|          900 |           4 |
|         2078 |           4 |
|         1800 |           4 |
|          400 |           5 |
|          630 |           5 |
|         1150 |           2 |
|         5620 |           3 |
|          210 |           2 |
|          350 |         707 |
|          410 |           1 |
|          280 |           7 |
|         1400 |           6 |
|         2024 |           2 |
|         1624 |           1 |
|          190 |           7 |
|          430 |           1 |
|         1130 |           1 |
|         1054 |           2 |
|          360 |           1 |
|         2000 |           1 |
|         5150 |           2 |
|         1330 |           1 |
|          590 |           2 |
|         2578 |           1 |
|         2648 |           1 |
|          930 |           2 |
|          660 |           3 |
|         1030 |           1 |
|         1600 |           1 |
|          200 |           1 |
+--------------+-------------+
*/
```
