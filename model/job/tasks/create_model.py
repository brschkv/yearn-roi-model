from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import pymc3 as pm

from config import config
from config.config import Config
from vaults import get_vaults
from vaults.vaults import Vault


def get_historic_prices(db_engine, vault: Vault, config: Config) -> pd.DataFrame:
    query = f"""
    SELECT
        block,
        price,
        timestamp
    FROM
        {config.HISTORIC_PRICES_TABLE} as h
    INNER JOIN
        {config.BLOCKS_TABLE} as b ON
            b.number = h.block
    WHERE h.vault = '{vault.name}'
    ;
    """
    df = pd.read_sql(query, db_engine, parse_dates=True)
    df = df.set_index('timestamp')
    df = df.sort_index(ascending=True)
    return df


def to_float(mantissa: int, decimals: int) -> float:
    scale_factor = 10 ** decimals
    return mantissa / scale_factor


def build_model(historic_prices: pd.DataFrame, base_days: int, vault: Vault, config: Config) -> pd.DataFrame:
    pct_changes = historic_prices.price.pct_change().dropna().values[-base_days * 24:]
    with pm.Model() as model:
        mu = pm.Normal("mu", mu=0, sigma=0.1)
        sd = pm.HalfNormal("sd", sigma=0.1)
        obs = pm.Normal("obs", mu=mu, sigma=sd, observed=pct_changes)
        trace = pm.sample(5000, cores=config.SAMPLING_CORES, tune=5000)
    mus = np.random.choice(trace.get_values('mu'), size=config.N_POSTERIOR_SAMPLES, replace=True)
    sds = np.random.choice(trace.get_values('sd'), size=config.N_POSTERIOR_SAMPLES, replace=True)
    posterior_samples = np.random.normal(mus, sds, size=(config.MODEL_HORIZON_DAYS*24, config.N_POSTERIOR_SAMPLES))
    posterior_samples = np.transpose(posterior_samples)
    posterior_growths = np.cumsum(posterior_samples, axis=1)
    latest_known_price = historic_prices.loc[historic_prices.index.max()].price
    price_projections = to_float((1 + posterior_growths) * latest_known_price, vault.decimals)
    hpd_95 = pm.hpd(price_projections, hdi_prob=0.95)
    hpd_50 = pm.hpd(price_projections, hdi_prob=0.5)
    model = pd.DataFrame.from_dict(
        {
            'hpd_95_lower': hpd_95[:,0],
            'hpd_95_upper': hpd_95[:,1],
            'hpd_50_lower': hpd_50[:,0],
            'hpd_50_upper': hpd_50[:,1]
        }
    )
    index = pd.DatetimeIndex(pd.date_range(historic_prices.index.max() + pd.DateOffset(hours=1), periods=config.MODEL_HORIZON_DAYS*24, freq='H'))
    model = model.set_index(index).resample('1D').nearest()
    model['vault'] = vault.name
    model['base_days'] = base_days
    return model


def create_model(db_engine, config: Config) -> None:
    vaults = get_vaults(db_engine)
    for vault in vaults:
        historic_prices = get_historic_prices(db_engine, vault, config)
        for base_days in config.MODEL_BASES_DAYS:
            print(f'creating model for {vault.name} with base {base_days} days...')
            model = build_model(historic_prices, base_days, vault, config)
            print('saving data...')
            try:
                db_engine.execute(f"""
                DELETE FROM
                    {config.MODELS_TABLE}
                WHERE
                    vault = '{vault.name}'
                    AND base_days = {base_days}
                ;
            """)
            except Exception as e:
                print('COULD NOT DELETE DATA, DATABASE PROBABLY EMPTY!')
                print(e)
            model.to_sql(
                config.MODELS_TABLE,
                db_engine,
                if_exists='append',
                index=True,
                index_label='timestamp'
            )
            print('SUCCESSFUL!')


if __name__ == '__main__':
    print('Connecting to Database...')
    db_engine = create_engine(config.DATABASE_CONNECTION)
    print('Successful')
    create_model(db_engine, config)
