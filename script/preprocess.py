from dask_cuda import LocalCUDACluster
from dask.distributed import Client
import dask
import os, sys
sys.path.append(os.path.abspath(f"{os.getcwd()}"))
import hydra
from loguru import logger
from omegaconf import DictConfig
from src.process.preprocess import Preprocess



@hydra.main(config_path='../config', config_name='main', version_base=None)
def main(cfg: DictConfig):
    driver = Preprocess(
        db = cfg.preprocess.db,
        output_dir = cfg.preprocess.output_dir,
        month = cfg.preprocess.month
    )
    logger.info(f'preprocess strategy: {cfg.preprocess.stage}')
    match cfg.preprocess.stage:
        case 'latlon2addr':
            driver.latlon2addr(cfg.preprocess.timeout, cfg.preprocess.sleep)
        case 'create_meta_tower':
            driver.create_meta_tower()
        case 'preprocess':
            driver.preprocess(cfg.preprocess.preprocess_strategy)


if __name__ == "__main__":
    cluster = LocalCUDACluster(
        CUDA_VISIBLE_DEVICES="0,1",
        protocol="ucx",
        enable_tcp_over_ucx=True,
        enable_infiniband=True,
        rmm_managed_memory=True,
        rmm_pool_size='24GB'
    )
    client = Client(cluster)
    main()
    client.close()
    cluster.close()
