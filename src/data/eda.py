import cudf
from dask_cuda import LocalCUDACluster
from dask.distributed import Client
import dask
import dask.dataframe as dd



class EDA:
    def __init__(self):
        cluster = LocalCUDACluster(
            CUDA_VISIBLE_DEVICES="0,1",
            protocol="ucx",
            enable_tcp_over_ucx=True,
            enable_infiniband=True,
            rmm_managed_memory=True,
            rmm_pool_size='24GB'
        )
        self.client = Client(cluster)

        dask.config.set({"dataframe.backend": "cudf"})
