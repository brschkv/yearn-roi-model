import sys

from sqlalchemy import create_engine

from config import config
from tasks.update_blocks import update_blocks
from tasks.update_historic_prices import update_historic_prices
from tasks.create_model import create_model


if __name__ == '__main__':
    try:
        db_engine = create_engine(config.DATABASE_CONNECTION)
        update_blocks(db_engine, config)
        update_historic_prices(db_engine, config)
        create_model(db_engine, config)
    except Exception as e:
        print("JOB FAILED:")
        print(e)
        sys.exit(1)
