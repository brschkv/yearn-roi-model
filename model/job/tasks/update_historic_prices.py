from time import sleep
from requests.exceptions import HTTPError
from typing import List

from sqlalchemy import create_engine
from web3 import Web3, HTTPProvider
from web3.contract import Contract
from etherscan.contracts import Contract
from tqdm import tqdm
import pandas as pd
import numpy as np

from config import config
from config.config import Config
from vaults import get_vaults
from vaults.vaults import Vault


def connect_archivenode(connection: str) -> Web3:
    provider = HTTPProvider(connection)
    w3 = Web3(provider)
    assert w3.isConnected()
    return w3


def get_latest_block_with_historic_price(db_engine, vault: Vault) -> int:
    try:
        with db_engine.connect() as con:
            query = f"""
                SELECT 
                    MAX(block) 
                FROM 
                    {config.HISTORIC_PRICES_TABLE} as h
                WHERE 
                    h.vault = '{vault.name}'
                ;
            """
            result = con.execute(query)
            first = result.first()[0]
            if first is None:
                raise ValueError()
            return first
    except Exception as e:
        print(f'NO EXISTING HISTORIC PRICE FOUND FOR {vault.name}')
        print(e)
        return vault.first_block_with_price


def get_vault_contract(w3, vault: Vault, config: Config) -> Contract:
    api = Contract(
        address=vault.smart_contract_address,
        api_key=config.ETHERSCAN_API_KEY
    )
    abi = api.get_abi()
    contract = w3.eth.contract(
        address=vault.smart_contract_address,
        abi=abi
    )
    return contract


def get_blocks_to_query(db_engine, latest_block_with_price: int) -> np.ndarray:
    query = f"""
        SELECT 
            timestamp,
            number
        FROM 
            {config.BLOCKS_TABLE} as b
        WHERE 
            b.number > {latest_block_with_price}
        ;
    """
    df = pd.read_sql(query, db_engine, parse_dates=True)
    df.index = df['timestamp']
    df = df.resample('1H').nearest()
    blocks_to_query = df['number'].values.astype(int)
    blocks_to_query = np.sort(blocks_to_query)
    return blocks_to_query


def get_historic_prices(contract: Contract, blocks: List[int], vault: Vault) -> pd.DataFrame:
    results = []
    for block in tqdm(blocks):
        successful = False
        while not successful:
            try:
                price = contract.functions.getPricePerFullShare().call(block_identifier=int(block))
                results.append((block, price))
                successful = True
                sleep(0.5)
            except HTTPError:
                print(f'RETRYING FOR BLOCK: {block}')
                sleep(5)
    historic_prices = pd.DataFrame.from_records(results, columns=['block', 'price'])
    historic_prices['vault'] = vault.name
    return historic_prices


def update_historic_prices(db_engine, config: Config) -> None:
    print('Connecting to Archive Node')
    w3 = connect_archivenode(config.ARCHIVE_NODE_CONNECTION)
    print('Successful')

    vaults = get_vaults(db_engine)
    for vault in vaults:
        print(f"Getting Historic Prices for {vault.name}")
        latest_block_with_price = get_latest_block_with_historic_price(db_engine, vault)
        blocks_to_query = get_blocks_to_query(db_engine, latest_block_with_price)
        vault_contract = get_vault_contract(w3, vault, config)
        if len(blocks_to_query) < 1:
            print(f"No new block found for {vault.name}")
            continue
        print(f"From block {blocks_to_query[0]} to {blocks_to_query[-1]}")
        historic_prices = get_historic_prices(vault_contract, blocks_to_query, vault)
        print('Writing Result to Database....')
        historic_prices.to_sql(
            config.HISTORIC_PRICES_TABLE,
            db_engine,
            if_exists='append',
            index=False
        )
        print(f'Successfully updated Historic Prices for {vault.name}')


if __name__ == '__main__':
    db_engine = create_engine(config.DATABASE_CONNECTION)
    update_historic_prices(db_engine, config)
