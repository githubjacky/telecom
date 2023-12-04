import os, sys
sys.path.append(os.path.abspath(f"{os.getcwd()}"))

from dotenv import load_dotenv
import hydra
from loguru import logger
from omegaconf import DictConfig
from src.preprocess import Preprocess



@hydra.main(config_path='../config', config_name='main', version_base=None)
def main(cfg: DictConfig):
    load_dotenv()
    logger.info(f'preprocess strategy({cfg.preprocess.month}): {cfg.preprocess.strategy}')
    if cfg.preprocess.strategy == 'preprocess':
        months = [
            201307,
            # 201308,
            # 201309,
            # 201310,
            # 201311,
            # 201312,
            # 201401,
            # 201402,
            # 201403,
            # 201404,
            # 201405,
            # 201406
        ]
        for month in months:
            driver = Preprocess(
                db = cfg.preprocess.db,
                output_dir = cfg.preprocess.output_dir,
                month = month
            )
            driver.clean_from_mysql()


if __name__ == "__main__":
    main()
