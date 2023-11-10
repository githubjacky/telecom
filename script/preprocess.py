import os, sys
sys.path.append(os.path.abspath(f"{os.getcwd()}"))
import hydra
from loguru import logger
from omegaconf import DictConfig
from src.data.preprocess import Preprocess



@hydra.main(config_path='../config', config_name='main', version_base=None)
def main(cfg: DictConfig):
    driver = Preprocess(
        db = cfg.preprocess.db,
        output_dir = cfg.preprocess.output_dir,
        month = cfg.preprocess.month
    )
    logger.info(f'preprocess strategy: {cfg.preprocess.strategy}')
    match cfg.preprocess.strategy:
        case 'latlon2addr':
            driver.latlon2addr(cfg.preprocess.timeout, cfg.preprocess.sleep)
        case 'preprocess':
            driver.preprocess()
        case 'create_meta_tower':
            driver.create_meta_tower()


if __name__ == "__main__":
    main()
