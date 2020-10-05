import os


class Config:
    EARLIEST_DATE_WITH_VAULT = "2020-07-30 00:00:00+00:00"
    BLOCKS_TABLE = 'blocks'
    HISTORIC_PRICES_TABLE = 'historic_prices'
    MODELS_TABLE = 'models'
    N_POSTERIOR_SAMPLES = 10_000
    MODEL_BASES_DAYS = [7, 30, 90, 365]
    MODEL_HORIZON_DAYS = 365

    def __init__(self):
        self.DATABASE_CONNECTION = f"{os.environ['DB_PROTOCOL']}://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_DATABASE']}"
        self.ARCHIVE_NODE_CONNECTION = f"https://api.archivenode.io/{os.environ['ARCHIVE_NODE_API_KEY']}"
        self.ETHERSCAN_API_KEY = os.environ['ETHERSCAN_API_KEY']
        self.SAMPLING_CORES = int(os.environ['SAMPLING_CORES'])


def get_config() -> Config:
    return Config()
