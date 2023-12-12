import os, sys
sys.path.append(os.path.abspath(f"{os.getcwd()}"))

from dotenv import load_dotenv
import hydra
from loguru import logger
from omegaconf import DictConfig
import pandas as pd
import dask.dataframe as dd

from src.preprocess import Preprocess



@hydra.main(config_path='../config', config_name='main', version_base=None)
def main(cfg: DictConfig):
    load_dotenv()

    driver = Preprocess(
        db = cfg.preprocess.db,
        output_dir = cfg.preprocess.output_dir,
        month = cfg.preprocess.month
    )
    df_cdr = dd.read_parquet(f'{cfg.preprocess.output_dir}/{cfg.preprocess.month}/clean_cdr')
    logger.info(f'upload telecom.cdr_{cfg.preprocess.month}')
    driver.write_to_sql(df_cdr.compute(), f'cdr_{cfg.preprocess.month}')

    df_userinfo = pd.read_csv(f'{cfg.preprocess.output_dir}/{cfg.preprocess.month}/clean_user_info.csv')
    logger.info(f'upload telecom.userinfo_{cfg.preprocess.month}')
    driver.write_to_sql(df_userinfo, f'userinfo_{cfg.preprocess.month}')



if __name__ == "__main__":
    main()
