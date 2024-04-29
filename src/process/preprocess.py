import cudf
import dask
import dask_cudf
from dask_cudf import read_parquet
import dask.dataframe as dd
from geopy.geocoders import Nominatim
from loguru import logger
import orjson
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, sql
import time
from typing import Optional, List, Dict, Literal
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
    (CELL_ID != '-1' AND CELL_ID != '0')\
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
    CI_DISTRICT                                                                    AS register_district,
    {year} - CONVERT(SUBSTR(CERT_NBR,10,4), UNSIGNED)                              AS age,
    CONVERT(SUBSTR(CERT_NBR, 21, 1), UNSIGNED)                                     AS male_flag,
    CI_TENURE                                                                      AS tenure,
    HS_CDMA_BRAND                                                                  AS phone_brand,
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
            df1.to_csv(self.output_dir / 'cdr_loc.csv')
            df2.to_csv(self.output_dir / 'cdr_loc_userinfo.csv')

    def get_cdr_from_csv(self, input_dir: str = 'data/raw') -> None:
        dask.config.set({'dataframe.backend': 'cudf'})
        cdr = (
            dd
            .read_csv(f'{input_dir}/cdr/{self.month}.csv')
            .rename(columns={
                'SERV_ID': 'serv_id',
                'ACC_NBR': 'client_nbr',
                'CALLING_NBR': 'calling_nbr',
                'CALLING_AREA_CODE': 'calling_area_code',
                'DURATION': 'duration',
                'CALLED_AREA_CODE': 'called_area_code',
                'CALLED_NBR': 'called_nbr',
                'CELL_ID': 'cell_id',
            })
        )
        cdr['calling_area_code'] = cdr['calling_area_code'].astype(str)
        cdr['called_area_code'] = cdr['called_area_code'].astype(str)
        cdr['cell_id'] = cdr['cell_id'].astype(str)


        time_match = '^(2013|2014)/(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01]) (0?[0-9]|1[0-9]|2[0-3]):.*'
        cdr_output = cdr[
            (cdr['ACCT_ITEM_TYPE_CODE'] != -1) &
            (cdr['START_TIME'].str.match(time_match) == True) &
            (cdr['END_TIME'].str.match(time_match) == True) &
            (cdr['calling_nbr'].str.match('^NotMobile.*') == False) &
            (cdr['called_nbr'].str.match('^NotMobile.*') == False) &
            (cdr['calling_area_code'] != '-1') &
            (cdr['called_area_code'] != '-1') &
            (cdr['cell_id'] != '-1') &
            (cdr['cell_id'] != '0')
        ][[
            'serv_id',
            'client_nbr',
            'calling_nbr',
            'calling_area_code',
            'duration',
            'called_area_code',
            'called_nbr',
            'cell_id',
            'START_TIME'
        ]].compute()
        date_time = pd.to_datetime(cdr_output['START_TIME'].to_pandas(), format='%Y/%m/%d %H:%M:%S')
        cdr_output['time'] = date_time.dt.hour
        cdr_output['day_of_week'] = date_time.dt.dayofweek

        cdr_output['calling_area_code'] = cudf.to_numeric(
            cdr_output['calling_area_code'], 'coerce', 'integer'
        )
        cdr_output['called_area_code'] = cudf.to_numeric(
            cdr_output['called_area_code'], 'coerce', 'integer'
        )

        (
            dask_cudf
            .from_cudf(
                cdr_output.dropna().drop(columns='START_TIME'),
                npartitions=20
            )
            .repartition(partition_size='100MB')
            .to_parquet(f'{self.output_dir}/clean_cdr')
        )



    def get_userinfo_from_csv(self, input_dir: str = 'data/raw') -> None:
        _user_info = cudf.read_csv(f'{input_dir}/user_info/{self.month}.csv')
        _user_info = _user_info[
            _user_info['CERT_NBR'].str.match('[1-8][0-7]-[0-7][0-9]-[0-9][0-9]-[12][09][0-9][0-9]-[01][0-9]-[0-3][0-9]-[01]') == True
        ]
        user_info = _user_info[
            (_user_info['CI_DISTRICT'] != '德阳市未知营业区') &
            (~_user_info['CI_DISTRICT'].isna()) &
            (_user_info['CERT_NBR'].str.slice(start=9, stop=13).astype(int) >= 1920) &
            (_user_info['CERT_NBR'].str.slice(start=9, stop=13).astype(int) <= _user_info['MONTH_NO'].astype(str).str.slice(stop=4).astype(int) - 16) &
            (_user_info['CERT_NBR'].str.slice(start=14, stop=16).astype(int) >= 1) &
            (_user_info['CERT_NBR'].str.slice(start=14, stop=16).astype(int) <= 12) &
            (_user_info['CERT_NBR'].str.slice(start=17, stop=19).astype(int) >= 1) &
            (_user_info['CERT_NBR'].str.slice(start=17, stop=19).astype(int) <= 31) &
            (_user_info['PD_CDMA_STATUS'] == '正常') &
            (_user_info['HS_CDMA_BRAND'] != '') &
            (~_user_info['HS_CDMA_BRAND'].isna()) &
            (~_user_info['HS_CDMA_LAYER'].isna()) &
            (~_user_info['HS_CDMA_IS_EVDO'].isna()) &
            (~_user_info['IS_INTELLIGENT'].isna()) &
            (_user_info['PL_BB_FLAG'] == 0) &
            (_user_info['IS_WX_FLAG'] == 0) &
            (_user_info['PAYMENT_FLAG'] == 1) &
            (_user_info['HS_CDMA_MODEL'].str.match('.*固定台') == False)
        ]
        user_info['born_area_code'] = (
            user_info['CERT_NBR'].str.slice(stop=2) +
            user_info['CERT_NBR'].str.slice(start=3, stop=5) +
            user_info['CERT_NBR'].str.slice(start=6, stop=8)
        )
        user_info['age'] = (
            user_info['MONTH_NO'].astype(str).str.slice(stop=4).astype(int) -
            user_info['CERT_NBR'].str.slice(start=9, stop=13).astype(int)
        )
        user_info['male_flag'] = user_info['CERT_NBR'].str.slice(start=20).astype(int)
        user_info['8card_service_flag'] = cudf.get_dummies(user_info['IS_8CARD'])['是']
        user_info['smart_phone_flag'] = cudf.get_dummies(user_info['IS_INTELLIGENT'])['是']

        user_info['rural_flag'] = 0
        user_info['rural_flag'] = user_info['rural_flag'].mask(
            (
                (user_info['DETAIL_NAME'].fillna('') == '农村公众（家庭及个人) ') |
                (user_info['DETAIL_NAME'].fillna('') == '农村客户')
            ) |
            (user_info['PL_IVPN_CAT'].fillna('')  == '乡情v网') |
            (user_info['BSS_ORG_ZJ_FLAG'].fillna(0) == 1),
            1
        )
        user_info['employ_flag'] = 0
        user_info['employ_flag'] = user_info['employ_flag'].mask(
            (
                (
                    (user_info['DETAIL_NAME'].fillna('') != '') &
                    (user_info['DETAIL_NAME'].fillna('') != '个人客户') &
                    (user_info['DETAIL_NAME'].fillna('') != '家庭客户') &
                    (user_info['DETAIL_NAME'].fillna('') != '农村公众（家庭及个人）') &
                    (user_info['DETAIL_NAME'].fillna('') != '农村客户') &
                    (user_info['DETAIL_NAME'].fillna('') != '城市个人客户') &
                    (user_info['DETAIL_NAME'].fillna('') != '城市家庭客户')
                ) |
                (
                    ((user_info['CI_IVPN_FLAG'] == 1) | (user_info['VPN_FLAG'] == 1)) &
                    (
                        (user_info['PL_IVPN_CAT'].fillna('') == '') |
                        (user_info['PL_IVPN_CAT'].fillna('') == '虚拟网') |
                        (user_info['PL_IVPN_CAT'].fillna('') == '家庭V网')
                    )

                ) |
                (
                    (user_info['MB_ENPR_FLAG_M1'] == 1) |
                    (user_info['IS_BUSINESS'] == 1) |
                    (user_info['RED_MARK'] == 1) |
                    (user_info['PL_BUSINESS_FLAG'] == 1) |
                    (user_info['IS_ZQJN'] == 1) |
                    (user_info['IS_ZQHY'] == 1)
                )
            ),
            1
        )
        user_info['student_flag'] = 0
        user_info['student_flag'] = user_info['student_flag'].mask(
            (
                (user_info['DETAIL_NAME'].fillna('') == '高等和职业院校') |
                (user_info['PL_IVPN_CAT'].fillna('') == '校园V网') |
                (user_info['PL_CAMPUS_FLAG'] == 1) |
                (user_info['IS_SCHOOL'] == 1)
            ),
            1
        )
        user_info_output = user_info.rename(columns={
            'SERV_ID': 'serv_id',
            'MSISDN': 'client_nbr',
            'CI_DISTRICT': 'register_district',
            'CI_TENURE': 'tenure',
            'HS_CDMA_BRAND': 'phone_brand',
            'HS_CDMA_LAYER': 'phone_level',
            'HS_CDMA_TER_PRICE': 'phone_price',
            'MB_ARPU_CDMA_ALL_M1': 'arpu',
            'VO_CDMA_MOU_M1': 'mou_total',
            'VO_CDMA_MOUOUT_LOCAL_M1': 'mou_local_callout',
            'VO_CDMA_MOU_DIST_M1': 'mou_dist_callout',
            'VO_NET_TIME_M1': 'network_usage_time',
            'HS_CDMA_IS_EVDO': 'evdo_support_flag',
            'PD_EVDO_FLAG_M1': 'use_evdo_flag',
            'PD_1X_FLAG_M1': 'use_onex_flag',
            'PL_E9_FLAG': 'e9_service_flag',
            'PL_E6_FLAG': 'e6_service_flag',
            'IS_E9ZX': 'e9_service_premium_flag',
            'MB_ENPR_FLAG_M1': 'govern_worker_flag',
            'IS_BUSINESS': 'business_purpose_flag',
            'RED_MARK': 'red_mark_flag',
            'IS_ZQJN': 'govern_cluster_flag',
            'IS_ZQHY': 'govern_industry_flag',
            'VPN_FLAG': 'vpn_support_flag'
        })[[
            'serv_id',
            'client_nbr',
            'born_area_code',
            'register_district',
            'age',
            'male_flag',
            'tenure',
            'phone_brand',
            'phone_level',
            'phone_price',
            'evdo_support_flag',
            'arpu',
            'mou_total',
            'mou_local_callout',
            'mou_dist_callout',
            'network_usage_time',
            'use_evdo_flag',
            'use_onex_flag',
            'e9_service_flag',
            'e6_service_flag',
            'e9_service_premium_flag',
            '8card_service_flag',
            'smart_phone_flag',
            'govern_worker_flag',
            'business_purpose_flag',
            'red_mark_flag',
            'govern_cluster_flag',
            'govern_industry_flag',
            'vpn_support_flag'
        ]]
        user_info_output.dropna().to_csv(f'{self.output_dir}/clean_user_info.csv', index=False)

 

    def preprocess(self, strategy = Literal['mysql', 'csv']) -> None:
        match strategy:
            case 'mysql':
                logger.info(f'create telecom.clean_cdr({self.month})')
                self.create_clean_cdr()
                # logger.info(f'create telecom.cdr_node_{self.month}')
                # self.create_cdr_node()
                logger.info(f'create telecom.clean_user_info({self.month})')
                self.create_clean_user_info()
                logger.info(f'aggregation')
                self.aggregate()
            case _:
                logger.info(f'create clean_cdr/({self.month})')
                self.get_cdr_from_csv()

                logger.info(f'create clean_user_info({self.month}).csv')
                self.get_userinfo_from_csv()

                logger.info(f'aggregation')
                self.aggregate()
