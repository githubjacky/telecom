import cudf
import dask_cudf
from dask_cudf import read_parquet, concat, from_cudf
import dask
import dask.dataframe as dd
from geopy.geocoders import Nominatim
from loguru import logger
import orjson
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, sql
import time
from typing import Optional, List, Dict
from tqdm import tqdm



class Preprocess:
    def __init__(self,
                 mysql_user: Optional[str] = None,
                 mysql_pass: Optional[str] = None,
                 db: str = 'telecom',
                 output_dir: str = 'data/processed',
                 month: int = 201308
                 ):
        user = (
            mysql_user
            if mysql_user is not None
            else
            os.getenv('MYSQL_USER')
        )
        password = (
            mysql_pass
            if mysql_pass is not None
            else
            os.getenv('MYSQL_PASS')
        )
        url = f'mysql+pymysql://{user}:{password}@localhost:3306/{db}'
        self.engine = create_engine(url)
        self.output_dir = Path(output_dir) / str(month)
        if not self.output_dir.exists():
            self.output_dir.mkdir(exist_ok=True, parents=True)
        self.month = str(month)

        try:
            import torch
            if torch.cuda.is_available():
                self.device = 'cuda'
                logger.info('activated device is: cuda')
            else:
                self.device = 'cpu'
                logger.info('1: activated device is: cpu')
        except:
            self.device = 'cpu'
            logger.info('2: activated device is: cpu')


    @staticmethod
    def read_jsonl(input_file: Path) -> List[Dict]:
        return [
            orjson.loads(i)
            for i in Path(input_file).read_text().split("\n")[:-1]
        ]


    def latlon2addr(self, timeout: int = 30, sleep: int = 1) -> Dict[List, str]:
        with self.engine.connect() as conn, conn.begin():
            df = pd.read_sql_table('tower', conn)

        output_path = self.output_dir / 'latlon2addr.jsonl'
        output_dict = {}
        if output_path.exists():
            raw = self.read_jsonl(output_path)
            output_dict = dict(zip(
                [tuple(i.get('latlon')) for i in raw],
                [i.get('addr') for i in raw]
            ))

        geolocator = Nominatim(user_agent="telecom")
        for i in tqdm(df.groupby(['LAT', 'LON']).groups.keys()):
            if i not in output_dict:
                output_dict[i] = geolocator.reverse(i, timeout=timeout).address
                with output_path.open('ab') as f:
                    f.write(orjson.dumps(
                        {'latlon': i, 'addr': output_dict[i]},
                        option=orjson.OPT_APPEND_NEWLINE
                    ))
            time.sleep(sleep)

        return output_dict

    def get_from_sql(self,
                     query: str,
                     output_path: Path,
                     write: bool = True,
                     parquet = False,
                     return_df = False
                     ) -> pd.DataFrame:
        if not output_path.exists():
            with self.engine.connect() as conn, conn.begin():
                df = cudf.from_pandas(pd.read_sql_query(sql.text(query), conn))

            if write:
                if parquet and self.device == 'cuda':
                    df['calling_area_code'] = cudf.to_numeric(
                        df['calling_area_code'],
                        downcast="integer",
                        errors = 'coerce'
                    )
                    df['called_area_code'] = cudf.to_numeric(
                        df['called_area_code'],
                        downcast="integer",
                        errors = 'coerce'
                    )
                    df.dropna(inplace=True)
                    dask_cudf.from_cudf(df, npartitions=12).to_parquet(output_path)
                else:
                    df.to_csv(str(output_path)+'.csv', index=False)
            df = df.to_pandas()
        else:
            if parquet and self.device == 'cuda':
                df = read_parquet(output_path).compute().to_pandas()
            else:
                df = cudf.read_csv(str(output_path) + '.csv').to_pandas()

        if return_df:
            return df


    def write_to_sql(self, df: pd.DataFrame, table: str) -> None:
        with self.engine.connect() as conn, conn.begin():
            try:
                df.to_sql(table, conn, index=False)
            except:
                logger.info(f'{table} has already existed in mysql')


    def create_clean_cdr(self) -> None:
        query = f"""\
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
FROM serv_acct_item_0838_{self.month}
WHERE
    ACCT_ITEM_TYPE_CODE != -1                                                  AND
    (START_TIME LIKE "____/%/% %:__:__" AND END_TIME LIKE "____/%/% %:__:__")  AND
    (CALLING_NBR NOT LIKE 'NotMobile_%' AND CALLED_NBR NOT LIKE 'NotMobile_%') AND
    (CALLING_AREA_CODE != '-1' AND CALLED_AREA_CODE != '-1')                   AND
    (CELL_ID != '-1' AND CELL_ID != '0')                                       AND
    CALLING_NBR != CALLED_NBR                                                  AND
    BILLING_DURATION > 10                                                      AND
    (CALLING_NBR in (SELECT MSISDN FROM tb_asz_cdma_0838_{self.month}) OR CALLED_NBR in (SELECT MSISDN FROM tb_asz_cdma_0838_{self.month}))\
"""
        # df = self.get_from_sql(query, self.output_dir / 'clean_cdr', parquet=True)
        self.get_from_sql(query, self.output_dir / 'clean_cdr', parquet=True)
        # self.write_to_sql(df, f'clean_cdr_{self.month}')


    def create_cdr_node(self) -> None:
        query = f"""\
SELECT DISTINCT subquery.client_nbr
FROM(
    (
        SELECT calling_nbr AS client_nbr
        FROM clean_cdr_{self.month}
        WHERE calling_nbr in (SELECT MSISDN FROM tb_asz_cdma_0838_201308)
    )
    UNION
    (
        SELECT called_nbr AS client_nbr
        FROM clean_cdr_{self.month}
        WHERE called_nbr in (SELECT MSISDN FROM tb_asz_cdma_0838_201308)
    )
) AS subquery\
"""
        df = self.get_from_sql(query, self.output_dir / 'cdr_node')
        self.write_to_sql(df, f'cdr_node_{self.month}')


    def create_clean_user_info(self) -> None:
        year = self.month[:4]

        query = f"""\
SELECT
    SERV_ID                                                                        AS serv_id,
    MSISDN                                                                         AS client_nbr,
    CONCAT(SUBSTR(CERT_NBR, 1, 2), SUBSTR(CERT_NBR, 4, 2), SUBSTR(CERT_NBR, 7, 2)) AS born_area_code,
    {year} - CONVERT(SUBSTR(CERT_NBR,10,4), UNSIGNED)                              AS age,
    CONVERT(SUBSTR(CERT_NBR, 21, 1), UNSIGNED)                                     AS male_flag,
    CI_TENURE                                                                      AS tenure,
    HS_CDMA_LAYER                                                                  AS phone_level,
    HS_CDMA_TER_PRICE                                                              AS phone_price,
    CONVERT(HS_CDMA_IS_EVDO, UNSIGNED)                                             AS evdo_support_flag,
    MB_ARPU_CDMA_ALL_M1                                                            AS arpu,
    VO_CDMA_MOU_M1                                                                 AS mou_total,
    VO_CDMA_MOUOUT_LOCAL_M1                                                        AS mou_local_callout,
    VO_CDMA_MOU_DIST_M1                                                            AS mou_dist_callout,
    VO_NET_TIME_M1                                                                 AS network_usage_time,
    CONVERT(PD_EVDO_FLAG_M1, UNSIGNED)                                             AS use_evdo_flag,
    CONVERT(PD_1X_FLAG_M1, UNSIGNED)                                               AS use_onex_flag,
    CONVERT(PL_E9_FLAG, UNSIGNED)                                                  AS e9_service_flag,
    CONVERT(PL_E6_FLAG, UNSIGNED)                                                  AS e6_service_flag,
    CONVERT(IS_E9ZX, UNSIGNED)                                                     AS e9_service_premium_flag,
    CASE IS_8CARD WHEN '是' THEN 1 ELSE 0 END                                      AS 8card_service_flag,
    CASE IS_INTELLIGENT WHEN '是' THEN 1 ELSE 0 END                                AS smart_phone_flag,
    CASE IS_CARDPHONE WHEN '是' THEN 1 ELSE 0 END                                  AS card_phone_flag,
    MB_ENPR_FLAG_M1                                                                AS govern_worker_flag,
    IS_BUSINESS                                                                    AS business_purpose_flag,
    RED_MARK                                                                       AS red_mark_flag,
    IS_ZQJN                                                                        AS govern_cluster_flag,
    IS_ZQHY                                                                        AS govern_industry_flag,
    VPN_FLAG                                                                       AS vpn_support_flag,
    CASE HS_CDMA_BRAND WHEN '华为' THEN 1 ELSE 0 END                                AS brand_huawei_flag,
    CASE HS_CDMA_BRAND WHEN 'SAMSUNG' THEN 1 ELSE 0 END                            AS brand_samsung_flag,
    CASE HS_CDMA_BRAND WHEN '酷派' THEN 1 ELSE 0 END                                AS brand_coolpad_flag,
    CASE HS_CDMA_BRAND WHEN '中兴' THEN 1 ELSE 0 END                                AS brand_zhongxing_flag,
    CASE HS_CDMA_BRAND WHEN '海信' THEN 1 ELSE 0 END                                AS brand_hisense_flag,
    CASE HS_CDMA_BRAND WHEN '蓝天' THEN 1 ELSE 0 END                                AS brand_bluesky_flag,
    CASE HS_CDMA_BRAND WHEN '广信' THEN 1 ELSE 0 END                                AS brand_guangxin_flag,
    CASE HS_CDMA_BRAND WHEN '联想' THEN 1 ELSE 0 END                                AS brand_lenova_flag,
    CASE HS_CDMA_BRAND WHEN 'HTC' THEN 1 ELSE 0 END                                AS brand_htc_flag,
    CASE HS_CDMA_BRAND WHEN '同威' THEN 1 ELSE 0 END                                AS brand_tongwei_flag,
    CASE HS_CDMA_BRAND WHEN '苹果' THEN 1 ELSE 0 END                                AS brand_apple_flag,
    CASE CI_DISTRICT WHEN '德阳现业' THEN 1 ELSE 0 END                               AS register_jingyang_flag,
    CASE CI_DISTRICT WHEN '广汉' THEN 1 ELSE 0 END                                  AS register_guanghan_flag,
    CASE CI_DISTRICT WHEN '绵竹' THEN 1 ELSE 0 END                                  AS register_mianzhu_flag,
    CASE CI_DISTRICT WHEN '什邡' THEN 1 ELSE 0 END                                  AS register_shifang_flag,
    CASE CI_DISTRICT WHEN '罗江' THEN 1 ELSE 0 END                                  AS register_luojiang_flag,
    CASE CI_DISTRICT WHEN '中江' THEN 1 ELSE 0 END                                  AS register_zhongjiang_flag,
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
            IS_BUSINESS = '1' OR     # wheter unit pay the fee
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
FROM tb_asz_cdma_0838_{self.month}
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
    HS_CDMA_MODEL NOT LIKE '%(固定台)'\
"""
        # df = self.get_from_sql(query, self.output_dir / 'clean_user_info')
        self.get_from_sql(query, self.output_dir / 'clean_user_info')
        # self.write_to_sql(df, f'clean_user_info_{self.month}')


    @staticmethod
    def create_call_area_code() -> Dict[str, str]:
        call_area_code = {
            '中江县': '0838',
            '乐山市': '0833',
            '内江市': '0832',
            '凉山彝族自治州': '0834',
            '南充市': '0817',
            '合川区': '023',
            '夹江县': '0833',
            '宜宾市': '0831',
            '巴中市': '0827',
            '广元市': '0839',
            '广安市': '0826',
            '德阳市': '0838',
            '成都市': '028',
            '攀枝花市': '0812',
            '昭通市': '0870',
            '楚雄彝族自治州': '0878',
            '汉中市': '0916',
            '泸州市': '0830',
            '甘孜藏族自治州': '0836',
            '眉山市': '028',
            '绵阳市': '0816',
            '自贡市': '0813',
            '资阳市': '028',
            '达州市': '0818',
            '遂宁市': '0825',
            '遵义市': '0852',
            '阿坝藏族羌族自治州': '0837',
            '陇南市': '0939',
            '雅安市': '0835',
        }
        with Path('data/processed/call_area_code.json').open('wb') as f:
            f.write(orjson.dumps(call_area_code))

        return call_area_code


    def get_tower_locmeta(self) -> None:
        call_area_code_path = Path('data/processed/call_area_code.json')
        if call_area_code_path.exists():
            call_area_code = orjson.loads(call_area_code_path.read_text())
        else:
            call_area_code = self.create_call_area_code()

        data = self.read_jsonl(Path('data/processed/latlon2addr.jsonl'))
        with Path('data/processed/tower_locmeta.jsonl').open('wb') as f:
            for i in data:
                target = i['addr'].split(', ')
                county = []
                try:
                    int(target[-2])
                    county.append(target[-3])
                    district = target[-4].split(' ')
                    if len(district) > 1:
                        county.append(district[0])
                    else:
                        county.append(target[-4])
                except ValueError as _:
                    county.append(target[-2])
                    district = target[-3].split(' ')
                    if len(district) > 1:
                        county.append(district[0])
                    else:
                        county.append(target[-3])

                if county[1] == 'Boundary':
                    county[1] = '夹江县'
                elif county[1] == '中江县':
                    county[1] = '德阳市'
                elif county[1] == '夹江县':
                    county[1] = '乐山市'

                i['county'] = tuple(county)
                i['call_area_code'] = call_area_code[county[1]]

                f.write(orjson.dumps(i, option=orjson.OPT_APPEND_NEWLINE))

    def create_meta_tower(self) -> None:
        query = "SELECT * FROM tower"
        tower = self.get_from_sql(query, Path("data/processed/error.csv"), write = False)

        loc_data_path = Path('data/processed/tower_locmeta.jsonl')
        if not loc_data_path.exists():
            self.get_tower_locmeta()
        loc_data = self.read_jsonl(loc_data_path)

        latlon2call_area_code = dict()
        for i in loc_data:
            latlon2call_area_code[tuple(i['latlon'])] = i['call_area_code']

        tower['tower_area_code'] = tower.apply(
            lambda x: latlon2call_area_code[(x['LAT'], x['LON'])],
            axis = 1
        )
        tower.drop(columns=['CELL_10', 'X', 'Y'], inplace=True)
        tower.rename(
            columns = {
                'CELL_16': 'cell_id',
                'LON': 'lon',
                'LAT': 'lat',
                'DEYANG_CENTER': 'deyang_center_flag'
            },
            inplace = True
        )
        tower_ = tower.astype({'tower_area_code': int})
        tower_.to_csv('data/processed/meta_tower.csv', index=False)

    def aggregate(self) -> None:
        dask.config.set({'dataframe.backend': 'cudf'})

        meta_tower = dd.read_csv('data/processed/meta_tower.csv')
        user_info = dd.read_csv(f'{self.output_dir}/clean_user_info.csv')

        if self.device == 'cuda':
            clean_cdr = read_parquet(f'{self.output_dir}/clean_cdr')
        else:
            clean_cdr = dd.read_csv(f'{self.output_dir}/clean_cdr.csv')

        df1 = dd.merge(clean_cdr, meta_tower, on='cell_id')
        df2 = dd.merge(df1, user_info, on='client_nbr')

        if self.device == 'cuda':
            df1.to_parquet(self.output_dir / 'cdr_loc')
            df2.to_parquet(self.output_dir / 'cdr_loc_userinfo')
        else:
            df1.to_csv(self.output_dir / 'cdr_loc.csv', index=False)
            df2.to_csv(self.output_dir / 'cdr_loc_userinfo.csv', index=False)


    def preprocess(self) -> None:
        logger.info(f'create telecom.clean_cdr_{self.month}')
        self.create_clean_cdr()
        # logger.info(f'create telecom.cdr_node_{self.month}')
        # self.create_cdr_node()
        logger.info(f'create telecom.clean_user_info_{self.month}')
        self.create_clean_user_info()
        logger.info(f'aggregation')
        self.aggregate()


    @staticmethod
    def create_worklife_attr(clean_cdr, order: List[str] = ['calling_nbr', 'called_nbr']):
        worklife_group = (
            clean_cdr.groupby(order + ['day_of_week'])
            .agg({'client_nbr': 'size', 'duration': 'sum'})
            .reset_index()
            .rename(columns={'client_nbr': 'count'})
        )

        _worklife_work = worklife_group.copy()
        _worklife_work[['count', 'duration']] = _worklife_work[['count', 'duration']].mask(
            (_worklife_work['day_of_week'] == 1) |
            (_worklife_work['day_of_week'] == 7),
            0
        )
        worklife_work = (
            _worklife_work.groupby(order)
            .sum()
            .drop(columns='day_of_week')
            .rename(columns={'count': 'count_work', 'duration': 'duration_work'})
            .reset_index()
        )

        _worklife_life = worklife_group.copy()
        _worklife_life[['count', 'duration']] = _worklife_life[['count', 'duration']].mask(
            (_worklife_life['day_of_week'] != 1) &
            (_worklife_life['day_of_week'] != 7),
            0
        )
        worklife_life = (
            _worklife_life.groupby(order)
            .sum()
            .drop(columns='day_of_week')
            .rename(columns={'count': 'count_life', 'duration': 'duration_life'})
            .reset_index()
        )

        worklife_prop =  worklife_work.merge(worklife_life, on=order, how='inner')
        worklife_prop['count'] = worklife_prop['count_work'] + worklife_prop['count_life']
        worklife_prop['duration'] = worklife_prop['duration_work'] + worklife_prop['duration_life']

        return worklife_prop


    @staticmethod
    def create_daynight_attr(clean_cdr, order: List[str] = ['calling_nbr', 'called_nbr'], day_cut: int = 9):
        daynight_group = (
            clean_cdr.groupby(order + ['time'])
            .agg({'client_nbr': 'size', 'duration': 'sum'})
            .reset_index()
            .rename(columns={'client_nbr': 'count'})
        )

        _daynight_day = daynight_group.copy()
        _daynight_day[['count', 'duration']] = _daynight_day[['count', 'duration']].mask(
            (_daynight_day['time'] <= day_cut) |
            (_daynight_day['time'] >= day_cut + 12),
            0
        )
        daynight_day = (
            _daynight_day.groupby(order)
            .sum()
            .drop(columns='time')
            .rename(columns={'count': 'count_day', 'duration': 'duration_day'})
            .reset_index()
        )

        _daynight_night = daynight_group.copy()
        _daynight_night[['count', 'duration']] = _daynight_night[['count', 'duration']].mask(
            (_daynight_night['time'] > day_cut) &
            (_daynight_night['time'] < day_cut + 12),
            0
        )
        daynight_night = (
            _daynight_night.groupby(order)
            .sum()
            .drop(columns='time')
            .rename(columns={'count': 'count_night', 'duration': 'duration_night'})
            .reset_index()
        )

        return daynight_day.merge(daynight_night, on=order, how='inner')


    @staticmethod
    def create_inout_attr(clean_cdr, order: List[str] = ['calling_nbr', 'called_nbr']):
        clean_cdr['local_call_flag'] = 0
        clean_cdr['local_call_flag'] = clean_cdr['local_call_flag'].mask(
            (clean_cdr['calling_area_code'] == 838) &
            (clean_cdr['called_area_code'] == 838),
            1
        )
        clean_cdr['external_area_code'] = 838

        clean_cdr['callout_flag'] = 0
        clean_cdr['callout_flag'] = clean_cdr['callout_flag'].mask(
            (clean_cdr['calling_area_code'] == 838) &
            (clean_cdr['called_area_code'] != 838),
            1
        )
        clean_cdr['external_area_code'] = clean_cdr['external_area_code'].mask(
            (clean_cdr['calling_area_code'] == 838) &
            (clean_cdr['called_area_code'] != 838),
            clean_cdr['called_area_code']
        )

        clean_cdr['callin_flag'] = 0
        clean_cdr['callin_flag'] = clean_cdr['callin_flag'].mask(
            (clean_cdr['calling_area_code'] != 838) &
            (clean_cdr['called_area_code'] == 838),
            1
        )
        clean_cdr['external_area_code'] = clean_cdr['external_area_code'].mask(
            (clean_cdr['calling_area_code'] != 838) &
            (clean_cdr['called_area_code'] == 838),
            clean_cdr['calling_area_code']
        )

        return (
            clean_cdr
            .groupby(order)
            .agg({
                'local_call_flag': 'sum',
                'callout_flag': 'sum',
                'callin_flag': 'sum',
                'external_area_code': 'mean'
            })
            .rename(columns={
                'local_call_flag': 'n_local_call',
                'callout_flag': 'n_callout',
                'callin_flag': 'n_callin'
            })
        )


    @staticmethod
    def to_undirected(edges):
        edges['sorted_row'] = [
            sorted([a,b])
            for a,b in zip(edges['source'].values_host, edges['destination'].values_host)
        ]
        edges['sorted_row'] = edges['sorted_row'].astype(str)
        return edges.drop_duplicates(subset='sorted_row').drop(columns='sorted_row')


    def create_edge_attr(self, thres_n_calls: int = 2, thres_duration: int = 0, save = True):
        clean_cdr = read_parquet(self.output_dir / 'clean_cdr')

        group1 = (
            self
            .create_worklife_attr(clean_cdr, ['calling_nbr', 'called_nbr'])
            .merge(
                self.create_daynight_attr(clean_cdr, ['calling_nbr', 'called_nbr']),
                on=['calling_nbr', 'called_nbr']
            )
            .merge(
                self.create_inout_attr(clean_cdr, ['calling_nbr', 'called_nbr']),
                on=['calling_nbr', 'called_nbr']
            )
            .rename(columns={
                'calling_nbr': 'source',
                'called_nbr': 'destination',
            })
        )
        group1['freq'] = 1

        group2 = (
            self
            .create_worklife_attr(clean_cdr, ['called_nbr', 'calling_nbr'])
            .merge(
                self.create_daynight_attr(clean_cdr, ['called_nbr', 'calling_nbr']),
                on=['called_nbr', 'calling_nbr']
            )
            .merge(
                self.create_inout_attr(clean_cdr, ['called_nbr', 'calling_nbr']),
                on=['called_nbr', 'calling_nbr']
            )
            .rename(columns={
                'called_nbr': 'source',
                'calling_nbr': 'destination',
            })
        )
        group2['freq'] = 1

        _edges = (
            concat([group1, group2])
            .groupby(['source', 'destination'])
            .sum()
            .reset_index()
        )
        edges_ = (
            _edges[
                (_edges['count'] >= thres_n_calls) &
                (_edges['duration'] >= thres_duration) &
                (_edges['freq'] == 2)
            ]
            .drop(columns='freq')
        )
        undirected_edges = self.to_undirected(edges_.compute())
        undirected_edges['external_area_code'] = undirected_edges['external_area_code'].div(2).astype(int)

        user_info = cudf.read_csv(self.output_dir / 'clean_user_info.csv')
        client = user_info['client_nbr']

        undirected_closed_edges = undirected_edges.loc[
            (undirected_edges['source'].isin(client)) &
            (undirected_edges['destination'].isin(client))
        ]
        if save:
            (
                from_cudf(undirected_edges, npartitions=18)
                .repartition(partition_size='100MB')
                .to_parquet(self.output_dir/'undirected_edges')
            )
            (
                undirected_closed_edges
                .to_csv(f'{self.output_dir}/undirected_closed_edges.csv', index=False)
            )

        return undirected_edges, undirected_closed_edges
