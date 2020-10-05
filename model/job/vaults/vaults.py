from typing import List

from dataclasses import dataclass


@dataclass
class Vault:
    name: str
    smart_contract_address: str
    first_block_with_price: int
    decimals: int


def get_vaults(db_engine) -> List[Vault]:
    query = f"""
        SELECT
            name,
            smart_contract_address,
            first_block_with_price,
            decimals
        FROM
            vaults
            ;
    """
    with db_engine.connect() as con:
        result = con.execute(query)
        vaults = [
            Vault(
                name=row[0],
                smart_contract_address=row[1],
                first_block_with_price=row[2],
                decimals=row[3]
            )
            for row in result
        ]
    return vaults
