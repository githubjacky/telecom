# %% [markdown]
"""
A Demo to run jupyter notebook
"""

# %%
# connect to GPU
from dask_cuda import LocalCUDACluster
from dask.distributed import Client
import dask

import os
os.chdir('../../')

cluster = LocalCUDACluster(
    CUDA_VISIBLE_DEVICES="0,1",
    protocol="ucx",
    enable_tcp_over_ucx=True,
    enable_infiniband=True,
    rmm_managed_memory=True,
    rmm_pool_size='24GB'
)
client = Client(cluster)

dask.config.set({"dataframe.backend": "cudf"})

from src.process.utils import CallDistanceUtil
driver = CallDistanceUtil()

# %%
user_info = driver.get_user_info(target='communication', method = 'mean')

# %%
list(user_info.columns)

# %%
import statsmodels.formula.api as smf
import pandas as pd

# %%
def encode_born_area_code(x):
    x_ = x // 100

    match x_:
        case 5106:
            return '德阳市'
        case 5101:
            return '成都市'
        case 5107:
            return '绵阳市'
        case 5110:
            return '内江市'
        case 5109:
            return '遂宁市'
        case 5130:
            return '已撤銷_达川地区'
        case 5102:
            return '已撤銷_乐山市市辖区'
        case 5108:
            return '广元市'
        case 5113:
            return '南充市'
        case 5129:
            return '已撤銷_南充地区'
        case 5103:
            return '自贡市'
        case 5111:
            return '乐山市'
        case 5131:
            return '已撤銷_雅安地區'
        case 5105:
            return '泸州市'
        case 5134:
            return '凉山彝族自治州'
        case 5139:
            return '已撤銷_眉山地区资阳地区'
        case 5137:
            return '已撤銷_巴中地區'
        case 5002:
            return '重慶市城口县'
        case 5138:
            return '已撤銷_眉山地区'
        case 5132:
            return '阿坝藏族羌族自治州'
        case 5125:
            return'已撤銷_宜宾市'
        case 5115:
            return '宜宾市'
        case 5116:
            return '广安市'
        case 5122:
            return '已撤銷_万县市'
        case 5104:
            return '攀枝花市'
        case 5133:
            return '甘孜藏族自治州'
        case 3303:
            return '浙江省温州市'
        case 6328:
            return '青海省海西蒙古族藏族自治州'
        case _:
            return '其他'

df = (
    user_info
    .born_area_code
    .apply(encode_born_area_code)
    .value_counts()[:26]
    .reset_index()
)
df['ratio'] = df['born_area_code'].apply(lambda x: x*100/user_info.shape[0])
df.rename(columns={'born_area_code': 'count'})

# %%
user_info['born_area'] = user_info['born_area_code'].apply(encode_born_area_code)

# %%
data = (
        user_info[[
            'age', 'male_flag', 'tenure', 'business_purpose_flag', 'smart_phone_flag',
            'govern_worker_flag', 'govern_cluster_flag', 'govern_industry_flag',
            'register_district', 'born_area', 'mean_communication_distance'
        ]]
        .join([
            # compare to 中端
            pd.get_dummies(user_info['phone_level'], drop_first=True),
            # compare to 德阳现业
            # pd.get_dummies(user_info['register_district'], prefix='註冊地'),
            # pd.get_dummies(user_info['born_area'], prefix='出生地')

        ])
)

formula = """\
    mean_communication_distance ~ age + male_flag + tenure + \
        business_purpose_flag + smart_phone_flag + govern_worker_flag + \
        govern_cluster_flag + govern_industry_flag + 低端 + 超低端 + 超高端 + \
        高端 + register_district:born_area\
"""
smf.ols(formula, data=data).fit(cov_type='HC0').summary()

# %%
def simple_encode_born_area_code(x):
    x_ = x // 100
    
    match x_:
        case 5133:
            return '甘孜藏族自治州'
        case 5134:
            return '凉山彝族自治州'
        case 5138:
            return '已撤銷_眉山地区'
        case 6328:
            return '青海省海西蒙古族藏族自治州'
        case _:
            return '其他'

user_info['born_area'] = user_info['born_area_code'].apply(simple_encode_born_area_code)

data = (
    user_info[[
        'age', 'male_flag', 'tenure', 'business_purpose_flag', 'smart_phone_flag',
        'govern_worker_flag', 'govern_cluster_flag', 'govern_industry_flag',
        'register_district', 'born_area', 'mean_communication_distance'
    ]]
    .join([
        # compare to 中端
        pd.get_dummies(user_info['phone_level'], drop_first=True),

    ])
)
formula = """\
    mean_communication_distance ~ age + male_flag + tenure + \
        business_purpose_flag + smart_phone_flag + govern_worker_flag + \
        govern_cluster_flag + govern_industry_flag + 低端 + 超低端 + 超高端 + \
        高端 + C(born_area):C(register_district)\
"""
(
    smf.ols(
        formula,
        data=data,
    )
    .fit(cov_type='HC0')
    .summary()
)

# %%
data = (
    user_info[[
        'age', 'male_flag', 'tenure', 'business_purpose_flag', 'smart_phone_flag',
        'govern_worker_flag', 'govern_cluster_flag', 'govern_industry_flag',
        'register_district', 'born_area', 'mean_communication_distance'
    ]]
    .join([
        # compare to 中端
        pd.get_dummies(user_info['phone_level'], drop_first=True),
        pd.get_dummies(user_info['register_district']),
        pd.get_dummies(user_info['born_area'])

    ])
)
formula = """\
    mean_communication_distance ~ age + male_flag + tenure + \
        business_purpose_flag + smart_phone_flag + govern_worker_flag + \
        govern_cluster_flag + govern_industry_flag + 低端 + 超低端 + 超高端 + \
        高端 + C(register_district) + C(born_area):C(register_district)\
"""
# (
#     smf.ols(
#         formula,
#         data=data,
#         drop_cols=['']
#     )
#     .fit(cov_type='HC0')
#     .summary()
# )

